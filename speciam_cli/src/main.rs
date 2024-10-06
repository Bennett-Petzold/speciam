use std::{
    cmp::min,
    collections::HashMap,
    error::Error,
    future::Future,
    panic,
    path::PathBuf,
    pin::Pin,
    process::exit,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    thread::available_parallelism,
    time::Duration,
};

use bare_err_tree::{err_tree, print_tree, tree_unwrap};
use clap::Parser;

mod args;
mod init;
mod log;
mod progress;
use args::Args;
use futures::{stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use init::RunState;
use log::JsonAsync;
use progress::DlProgress;
use reqwest::{StatusCode, Version};
use speciam::{
    download, get_response, scrape, DepthLimit, DownloadError, DownloadErrorWrap, LimitedUrl,
    RobotsCheckStatus, RobotsErrWrap, ScrapeErrorWrap, UniqueUrls, WriteError, WriteHandle,
};
use thiserror::Error;
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    spawn,
    task::{spawn_blocking, JoinHandle},
    time::sleep,
};
use tracing::{event, Level};
use tracing_error::ErrorLayer;
use tracing_subscriber::{
    filter,
    fmt::{self, layer},
    layer::SubscriberExt,
    Layer,
};

#[cfg(feature = "resume")]
pub mod resume;

#[tokio::main]
async fn main() {
    let args = Args::parse();

    /*
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // New runtime has the set number of threads
        .worker_threads(
            args.units
                .unwrap_or(available_parallelism().unwrap().into()),
        )
        .build()
        .unwrap()
        .block_on(async {
    */
    let subscriber = tracing_subscriber::registry()
        .with(ErrorLayer::default())
        .with(
            fmt::layer()
                .json()
                .with_writer(JsonAsync::new(
                    std::fs::File::options()
                        .create(true)
                        .append(true)
                        .open("speciam.log.json")
                        .unwrap(),
                ))
                .with_filter(filter::LevelFilter::from_level(Level::INFO)),
        );

    #[cfg(feature = "tokio_console")]
    let subscriber = subscriber.with(console_subscriber::spawn());

    // set the subscriber as the default for the application
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Override for only a single thread panic
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        exit(1);
    }));

    let args = tree_unwrap::<60, _, _, _>(args.resolve().await);
    let (pending, run_state) = tree_unwrap::<60, _, _, _>(args.init().await);
    execute(pending, run_state).await
    /*
    });
            */
}

#[derive(Debug)]
enum ProcessReturn {
    NoOp(LimitedUrl),
    Download(
        (
            LimitedUrl,
            Vec<LimitedUrl>,
            Option<WriteHandle>,
            Option<Version>,
        ),
    ),
    MappingDomain(JoinHandle<LimitedUrl>),
}

#[derive(Debug, Error)]
#[err_tree(ProcessingErrWrap)]
pub enum ProcessingErr {
    #[error("failure while making initial request")]
    Reqwest(#[from] reqwest::Error),
    #[error("failure while downloading")]
    Download(#[from] DownloadErrorWrap),
    #[error("failure while scraping")]
    Scrape(#[from] ScrapeErrorWrap),
    #[cfg(feature = "resume")]
    #[error("logging callback failed")]
    CB(#[from] Arc<async_sqlite::Error>),
}

static PROMPTING: Mutex<()> = Mutex::new(());

fn spawn_process(
    url: LimitedUrl,
    run_state: RunState,
) -> JoinHandle<Result<ProcessReturn, (LimitedUrl, ProcessingErrWrap)>> {
    spawn(async move {
        let domains_check = { run_state.domains.read().await.wait(&url).await };
        match domains_check {
            // Passed the depth check
            Ok(true) => {
                let (response, unique_urls) = get_response(&run_state.client, url.url().clone())
                    .await
                    .map_err(|e| (url.clone(), ProcessingErr::from(e).into()))?;

                let headers = response.headers().clone();
                let version = response.version();

                let unique_download = if let UniqueUrls::Two([_, ref unique]) = unique_urls {
                    if let Some(scraped) = run_state
                        .visited
                        .get_nonlimited(url.aliased(unique.clone()))
                    {
                        run_state.visited.insert(url.clone(), scraped);
                        false
                    } else {
                        true
                    }
                } else {
                    true
                };

                let write_handle = if unique_download {
                    // Wait for resources to free up
                    run_state.thread_limiter.mark(&url, version).await;

                    let download_res = download(
                        response,
                        run_state.base_path.as_path(),
                        run_state.progress.as_ref().map(|x| x.write_progress()),
                    )
                    .await;
                    run_state.thread_limiter.unmark(&url, version); // Free the resource

                    if let Err(ref e) = download_res {
                        if let DownloadError::LostWriter(e) = &**e {
                            if let WriteError::DupFile(_) = &*e.handle_err {
                                if let UniqueUrls::Two([_, ref unique]) = unique_urls {
                                    // Wait for and return the result from the shared-
                                    // resolve process.
                                    loop {
                                        if let Some(scraped) = run_state
                                            .visited
                                            .get_nonlimited(url.aliased(unique.clone()))
                                        {
                                            run_state.visited.insert(url.clone(), scraped);

                                            return Ok(ProcessReturn::Download((
                                                url.clone(),
                                                run_state.visited.get(url).unwrap(),
                                                None,
                                                Some(version),
                                            )));
                                        }
                                        sleep(Duration::from_secs(1)).await;
                                    }
                                } else {
                                    return Ok(ProcessReturn::NoOp(url));
                                }
                            }
                        }
                    };
                    let (content, write_handle) =
                        download_res.map_err(|e| (url.clone(), ProcessingErr::from(e).into()))?;

                    let scraped: Vec<_> = scrape(url.url(), headers, content)
                        .await
                        .map_err(|e| (url.clone(), ProcessingErr::from(e).into()))?;

                    // Add unique urls to visit map
                    if let UniqueUrls::Two([_, unique]) = unique_urls {
                        let unique = url.aliased(unique);
                        if !run_state.config_only {
                            if let Some(db) = run_state.db.clone() {
                                db.log_visited(&unique, scraped.clone())
                                    .map_err(|e| (url.clone(), ProcessingErr::from(e).into()))?;
                            }
                        }
                        run_state.visited.insert(unique, scraped.clone());
                    }

                    if !run_state.config_only {
                        if let Some(db) = run_state.db.clone() {
                            db.log_visited(&url, scraped.clone())
                                .map_err(|e| (url.clone(), ProcessingErr::from(e).into()))?;
                        }
                    }
                    run_state.visited.insert(url.clone(), scraped.clone());

                    write_handle
                } else {
                    None
                };

                Ok(ProcessReturn::Download((
                    url.clone(),
                    run_state.visited.get(url).unwrap(),
                    write_handle,
                    Some(version),
                )))
            }
            // Failed the depth check
            Ok(false) => {
                #[cfg(feature = "resume")]
                if !run_state.config_only {
                    if let Some(db) = &run_state.db {
                        db.drop_pending(url.url()).unwrap();
                    }
                }

                Ok(ProcessReturn::NoOp(url))
            }
            // The domain needs to be initialized
            Err(e) => {
                let url = e.0;
                let domains = run_state.domains.clone();
                let handle = spawn_blocking(move || {
                    let depth = if run_state.interactive {
                        // Mutexing prevents duplicate prompting by serializing
                        // all prompts.
                        let prompt_handle = PROMPTING.lock().unwrap();
                        if domains.blocking_read().has_limit(&url) {
                            return url;
                        };

                        let depth = if let Some(progress) = &run_state.progress {
                            // Hide progress bar during prompt
                            progress.suspend(|| prompt_depth(&url, run_state.secondary_depth))
                        } else {
                            prompt_depth(&url, run_state.secondary_depth)
                        };

                        drop(prompt_handle);
                        depth
                    } else {
                        run_state.secondary_depth
                    };

                    {
                        let domains_handle = domains.blocking_write();
                        // Avoid duplicate entries
                        if !domains_handle.has_limit(&url) {
                            domains_handle.add_limit(&url, run_state.secondary_depth);
                        }
                    }

                    #[cfg(feature = "resume")]
                    if let Some(db) = run_state.db {
                        let _ = db.log_domain(url.url().as_str(), depth.into());
                    }

                    url
                });
                Ok(ProcessReturn::MappingDomain(handle))
            }
        }
    })
}

fn prompt_depth(url: &LimitedUrl, default: DepthLimit) -> DepthLimit {
    println!(
        "Encountered {}, provide a depth (none for default):",
        url.url_base()
    );

    let mut input = String::new();
    loop {
        std::io::stdin().read_line(&mut input).unwrap();
        if input.trim().is_empty() {
            return default;
        } else if let Ok(depth) = input.parse::<usize>() {
            return depth.into();
        } else {
            println!("Invalid input, try again");
            input.clear()
        }
    }
}

/// Intermediate layer before spawning handles.
///
/// Prioritizes the domains with the smallest pending list.
pub struct Dispatcher {
    rx: std::sync::mpsc::Receiver<LimitedUrl>,
    pos: usize,
    pending: Vec<Vec<LimitedUrl>>,
}

impl Dispatcher {
    pub fn new(rx: std::sync::mpsc::Receiver<LimitedUrl>) -> Self {
        Self {
            rx,
            pos: 0,
            pending: Vec::new(),
        }
    }

    pub fn push(&mut self, url: LimitedUrl) {
        let search_res = self
            .pending
            .binary_search_by_key(&Some(url.url_base()), |x| x.first().map(|x| x.url_base()));

        match search_res {
            Ok(existing) => self.pending[existing].push(url),
            Err(new_idx) => self.pending.insert(new_idx, vec![url]),
        }
    }
}

impl Stream for Dispatcher {
    type Item = LimitedUrl;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Update map from rx
        while let Ok(recv) = this.rx.try_recv() {
            this.push(recv);
        }

        if !this.pending.is_empty() {
            // Update positition until we find an entry
            let mut moving_pos = this.pos;
            loop {
                if let Some(val) = this.pending[moving_pos].pop() {
                    return Poll::Ready(Some(val));
                }

                moving_pos = moving_pos.saturating_add(1);
                moving_pos %= this.pending.len();

                // Went through entire vec, no results
                if moving_pos == this.pos {
                    return Poll::Ready(None);
                }
            }
        } else {
            Poll::Ready(None)
        }
    }
}

#[derive(Debug, Error)]
#[err_tree(RobotsCheckErrWrap)]
pub enum RobotsCheckErr {
    #[error("failure while checking robots.txt")]
    RobotsCheck(#[source] RobotsErrWrap),
    #[cfg(feature = "resume")]
    #[error("logging callback failed")]
    CB(#[source] Arc<async_sqlite::Error>),
}

#[derive(Debug)]
struct HandleWithPayload<T, U> {
    pub handle: JoinHandle<T>,
    pub payload: U,
}

impl<T, U: Clone + Unpin> Future for HandleWithPayload<T, U> {
    type Output = (U, <JoinHandle<T> as Future>::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_mut()
            .get_mut()
            .handle
            .poll_unpin(cx)
            .map(|x| (self.payload.clone(), x))
    }
}

async fn execute(pending: Vec<LimitedUrl>, mut run_state: RunState) {
    let prog_reg = |x: &LimitedUrl, progress: &Option<DlProgress>| {
        if let Some(progress) = progress {
            progress.register(x);
        }
    };
    let prog_free = |x: &LimitedUrl, version: Option<Version>| {
        if let Some(progress) = &run_state.progress {
            progress.free(x, version);
        }
    };
    let prog_reg_write = |x: PathBuf, predict: Option<u64>| {
        if let Some(progress) = &run_state.progress {
            progress.register_write(x, predict)
        }
    };
    let prog_free_write = |x: PathBuf, actual_size: u64| {
        if let Some(progress) = &run_state.progress {
            progress.free_write(x, actual_size)
        }
    };

    let mut map_handles = FuturesUnordered::new();
    let mut write_handles = FuturesUnordered::new();

    // NEW STUFF
    let preprocessing = Arc::new(AtomicUsize::new(0));
    // Background robots.txt checking
    let (robot_check_tx, mut robot_passed_rx, robot_check_thread) = {
        let (robot_check_tx, mut robot_check_rx) = tokio::sync::mpsc::unbounded_channel();
        let (robot_passed_tx, robot_passed_rx) = tokio::sync::mpsc::unbounded_channel();

        let preprocessing = preprocessing.clone();
        let robots = run_state.robots.clone();
        let db = run_state.db.clone();

        let robot_check_thread = tokio::spawn(async move {
            while let Some(url) = robot_check_rx.recv().await {
                let robots_check_status = robots
                    .check(&url)
                    .await
                    .map_err(RobotsCheckErr::RobotsCheck)?;

                #[cfg(feature = "resume")]
                if let RobotsCheckStatus::Added((_, robots_txt)) = &robots_check_status {
                    if !run_state.config_only {
                        if let Some(db) = db.clone() {
                            db.log_robots(url.url_base(), robots_txt.as_str())
                                .map_err(RobotsCheckErr::CB)?;
                        }
                    }
                }

                // Pass on or remove the URL
                if *robots_check_status {
                    // Don't increment progress yet, hasn't reached dispatch
                    robot_passed_tx.send(url).unwrap();
                } else {
                    #[cfg(feature = "resume")]
                    if !run_state.config_only {
                        if let Some(db) = db.clone() {
                            db.drop_pending(url.url()).map_err(RobotsCheckErr::CB)?;
                        }
                    }

                    // Note that this dropped out of preprocessing
                    preprocessing.fetch_sub(1, Ordering::Release);
                }
            }
            Ok::<_, RobotsCheckErrWrap>(())
        });

        (robot_check_tx, robot_passed_rx, robot_check_thread)
    };

    // Background robots.txt checking
    let (visited_passed_rx, visited_check_thread) = {
        let (visited_passed_tx, visited_passed_rx) = std::sync::mpsc::channel();
        let robot_check_tx = robot_check_tx.clone();

        let preprocessing = preprocessing.clone();
        let visited = run_state.visited.clone();
        let db = run_state.db.clone();
        let progress = run_state.progress.clone();

        let visited_check_thread = tokio::spawn(async move {
            while let Some(url) = robot_passed_rx.recv().await {
                match visited.probe(url.clone()) {
                    // Dispatch unique URLs
                    speciam::VisitCacheRes::Unique => {
                        // Considered committed to dispatch for progress tracking
                        prog_reg(&url, &progress);

                        visited_passed_tx.send(url.clone()).unwrap();

                        // Note that this dropped out of preprocessing
                        preprocessing.fetch_sub(1, Ordering::Release);
                    }
                    // Push newly pending urls back to robot check stage
                    speciam::VisitCacheRes::SmallerThanCached(urls) => {
                        let urls_len = urls.len();

                        // Adjust preprocessing count before submitting, so
                        // other atomic count adjustments don't underflow.
                        // Skip a useless 0 add atomic op
                        if urls_len > 1 {
                            preprocessing.fetch_add(urls_len - 1, Ordering::Release);
                        }

                        for url in urls {
                            #[cfg(feature = "resume")]
                            if !run_state.config_only {
                                if let Some(db) = db.clone() {
                                    db.push_pending(url.url())?;
                                }
                            }

                            robot_check_tx.send(url).unwrap();
                        }

                        // URL is fully processed at this point
                        #[cfg(feature = "resume")]
                        if !run_state.config_only {
                            if let Some(db) = db.clone() {
                                db.drop_pending(url.url())?;
                            }
                        }

                        // Need to decrement if children were empty
                        if urls_len == 0 {
                            preprocessing.fetch_sub(1, Ordering::Release);
                        }
                    }
                    // Discard the URL
                    speciam::VisitCacheRes::CachedNoRepeat => {
                        #[cfg(feature = "resume")]
                        if !run_state.config_only {
                            if let Some(db) = db.clone() {
                                db.drop_pending(url.url())?;
                            }
                        }

                        // Note that this dropped out of preprocessing
                        preprocessing.fetch_sub(1, Ordering::Release);
                    }
                };
            }

            #[cfg(feature = "resume")]
            Ok::<_, Arc<async_sqlite::Error>>(())
        });

        (visited_passed_rx, visited_check_thread)
    };
    // END NEW STUFF

    //let in_flight = Arc::new((Mutex::default(), AtomicUsize::new(0)));
    //let parallel_cap = run_state.thread_limiter.get_limit();
    let mut dispatcher = Dispatcher::new(
        visited_passed_rx,
        //in_flight.clone(),
        //pending.len(),
        //parallel_cap,
    );

    // TODO: also needs to go in a resume log
    let mut renamed_urls = Vec::new();

    // Initialize process handles with the base urls
    let mut handles = FuturesUnordered::<
        JoinHandle<Result<ProcessReturn, (LimitedUrl, ProcessingErrWrap)>>,
    >::new();
    preprocessing.fetch_add(pending.len(), Ordering::Release);
    for initial in pending {
        robot_check_tx.send(initial).unwrap();
    }

    loop {
        tokio::select! {
            // Prioritize new dispatch inputs, dispatching, and then catching
            // any errors/fixing counters.
            biased;

            Some(next) = handles.next() => {
                // TODO: actually handle HTTP download errors
                match next.unwrap() {
                    Ok(next) => match next {
                        ProcessReturn::NoOp(source) => {
                            prog_free(&source, None);

                            #[cfg(feature = "resume")]
                            if !run_state.config_only {
                                if let Some(db) = &run_state.db {
                                    db.drop_pending(source.url()).unwrap();
                                }
                            }
                        },
                        ProcessReturn::Download((source, mut scrape, wh, ver)) => {
                            prog_free(&source, ver);
                            scrape.retain(|x| (source.url_base() == x.url_base()) || run_state.primary_domains.contains(&source.url_base().to_string()));

                            preprocessing.fetch_add(scrape.len(), Ordering::Release);
                            for url in scrape {
                                #[cfg(feature = "resume")]
                                if !run_state.config_only {
                                    if let Some(db) = &run_state.db {
                                        db.push_pending(url.url()).unwrap();
                                    }
                                }
                                robot_check_tx.send(url).unwrap();
                            }

                            if let Some(h) = wh {
                                prog_reg_write(h.target.clone(), h.size_prediction);
                                write_handles.push(HandleWithPayload { payload: (h.target, source.url().clone()), handle: h.handle });
                            } else {
                                #[cfg(feature = "resume")]
                                if !run_state.config_only {
                                    if let Some(db) = &run_state.db {
                                        db.drop_pending(source.url()).unwrap();
                                    }
                                }
                            }
                        }
                        ProcessReturn::MappingDomain(mapping) => {
                            map_handles.push(mapping);
                        }
                    },
                    Err((url, e)) => {
                        let mut error = String::new();
                        print_tree::<60, dyn Error, _, _>(&e as &dyn Error, &mut error).unwrap();
                        event!(Level::ERROR, "{:#?}", error);

                        if let ProcessingErr::Reqwest(e) = &*e {
                            prog_free(&url, None);

                            #[cfg(feature = "resume")]
                            if !run_state.config_only && e.status() == Some(StatusCode::GONE) {
                                if let Some(db) = &run_state.db {
                                    db.drop_pending(url.url()).unwrap();
                                }
                            }
                        }
                    },
                }
            }

            Some(dispatch_next) = dispatcher.next() => {
                //in_flight.1.fetch_add(1, std::sync::atomic::Ordering::Release);
                handles.push(spawn_process(dispatch_next, run_state.clone()))
            }

            // Panic if a write fails
            Some((target, fin_write)) = write_handles.next() => {
                let fin_write = fin_write.unwrap();
                if let Err(e) = &fin_write {
                    if let WriteError::DupFile(e) = &**e {
                        event!(Level::INFO, "Duplicate download (not written to disk) for: {:?}", e.file);
                        continue;
                    }
                }
                let (final_path, write_size) = tree_unwrap::<60, _, _, _>(fin_write);
                prog_free_write(target.0.clone(), write_size);

                if let Some(changed_path) = final_path {
                    renamed_urls.push((target.0, changed_path));
                }

                #[cfg(feature = "resume")]
                if !run_state.config_only {
                    if let Some(db) = &run_state.db {
                        db.drop_pending(target.1).unwrap();
                    }
                }
            }

            Some(fin_map) = map_handles.next() => {
                dispatcher.push(fin_map.unwrap());
            }

            // All processing queues emptied
            else => {
                if preprocessing.load(Ordering::Acquire) == 0 {
                    // Finished scraping if preprocessing is also emptied
                    let stats = "FINISHED! STATS TODO";
                    if let Some(progress) = &run_state.progress {
                        progress.println(stats);
                    } else {
                        println!("{}", stats);
                    }
                    break;
                } else {
                    // If preprocessing isn't emptied, we need to busy loop
                    // dispatch until it gets one of the preprocessing tasks.

                    // Exit if one of the preprocessing threads crashed
                    if robot_check_thread.is_finished() || visited_check_thread.is_finished() {
                        break;
                    }

                    // Put in a small buffer so preprocessing futures can
                    // resolve.
                    sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    // Catch and unwrap preprocessing thread failure
    if robot_check_thread.is_finished() {
        tree_unwrap::<60, _, _, _>(robot_check_thread.await.unwrap());
    }
    if visited_check_thread.is_finished() {
        if let Err(e) = visited_check_thread.await.unwrap() {
            let mut output = String::new();
            print_tree::<60, dyn Error, _, _>(&e as &dyn Error, &mut output).unwrap();
            panic!("{output}");
        }
    }

    if let Some(db) = run_state.db {
        let pending_ops = db.pending_ops();

        let mut current_pending_ops = pending_ops.0.load(Ordering::Acquire);

        if current_pending_ops != 0 {
            let num_db_ops_pending = format!(
                "Waiting for {} resume database transactions to complete...",
                current_pending_ops
            );
            if let Some(progress) = &mut run_state.progress {
                progress.println(num_db_ops_pending);
                progress.init_transactions(current_pending_ops as u64);
            } else {
                println!("{}", num_db_ops_pending);
            }

            while pending_ops.0.load(Ordering::Acquire) != 0 {
                if let Some(e) = pending_ops.1.lock().unwrap().take() {
                    let mut output = String::new();
                    print_tree::<60, dyn Error, _, _>(&e as &dyn Error, &mut output).unwrap();
                    panic!("{output}");
                };
                sleep(Duration::from_secs(1)).await;

                current_pending_ops = pending_ops.0.load(Ordering::Acquire);
                if let Some(progress) = &run_state.progress {
                    progress.update_transactions(current_pending_ops as u64);
                }
            }
        }
    }

    exit(0)
}
