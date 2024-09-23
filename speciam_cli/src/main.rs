use std::{
    cmp::min,
    collections::HashMap,
    error::Error,
    path::PathBuf,
    pin::Pin,
    process::exit,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use bare_err_tree::{err_tree, print_tree, tree_unwrap};
use clap::Parser;

mod args;
mod init;
mod progress;
use args::Args;
use futures::{stream::FuturesUnordered, Stream, StreamExt};
use init::RunState;
use progress::DlProgress;
use reqwest::Version;
use speciam::{
    download, get_response, scrape, DepthLimit, DownloadErrorWrap, LimitedUrl, RobotsCheckStatus,
    RobotsErrWrap, ScrapeErrorWrap, UniqueUrls, WriteHandle,
};
use thiserror::Error;
use tokio::{
    spawn,
    task::{spawn_blocking, JoinHandle},
    time::sleep,
};
use tracing::{event, Level};
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;

#[cfg(feature = "resume")]
pub mod resume;

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::registry().with(ErrorLayer::default());

    // set the subscriber as the default for the application
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let args = tree_unwrap::<60, _, _, _>(Args::parse().resolve().await);
    let (pending, run_state) = tree_unwrap::<60, _, _, _>(args.init().await);
    execute(pending, run_state).await;
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

// Placeholder when not using callbacks
#[cfg(not(feature = "resume"))]
type CbErr = std::io::Error;
#[cfg(feature = "resume")]
type CbErr = async_sqlite::Error;

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
    CB(#[from] async_sqlite::Error),
}

static PROMPTING: Mutex<()> = Mutex::new(());

fn spawn_process(
    url: LimitedUrl,
    run_state: RunState,
) -> JoinHandle<Result<ProcessReturn, ProcessingErrWrap>> {
    spawn(async move {
        let domains_check = { run_state.domains.read().await.wait(&url).await };
        match domains_check {
            // Passed the depth check
            Ok(true) => {
                let (response, unique_urls) = get_response(&run_state.client, url.url().clone())
                    .await
                    .map_err(ProcessingErr::from)?;
                let headers = response.headers().clone();

                let version = response.version();
                // Wait for resources to free up
                run_state.thread_limiter.mark(&url, version).await;

                let download_res = download(
                    response,
                    run_state.base_path.as_path(),
                    run_state.progress.as_ref().map(|x| x.write_progress()),
                )
                .await;
                run_state.thread_limiter.unmark(&url, version); // Free the resource
                let (content, write_handle) = download_res.map_err(ProcessingErr::from)?;

                let scraped: Vec<_> = scrape(url.url(), headers, content)
                    .await
                    .map_err(ProcessingErr::from)?;

                let scraped_limited = scraped
                    .iter()
                    .flat_map(|scrape| LimitedUrl::new(&url, scrape.clone()))
                    .collect();
                let ret = (scraped_limited, Some(write_handle), Some(version));

                // Add unique urls to visit map
                if let UniqueUrls::Two([_, unique]) = unique_urls {
                    if let Ok(unique) = LimitedUrl::new(&url, unique) {
                        if !run_state.config_only {
                            if let Some(db) = run_state.db.clone() {
                                db.log_visited(&unique, scraped.clone())
                                    .map_err(ProcessingErr::from)?;
                            }
                        }
                        run_state.visited.insert(unique, scraped.clone());
                    }
                }

                if !run_state.config_only {
                    if let Some(db) = run_state.db.clone() {
                        db.log_visited(&url, scraped.clone())
                            .map_err(ProcessingErr::from)?;
                    }
                }
                run_state.visited.insert(url.clone(), scraped.clone());

                Ok(ProcessReturn::Download((url, ret.0, ret.1, ret.2)))

                /*
                let res = dl_and_scrape(
                    run_state.client.clone(),
                    run_state.visited,
                    run_state.robots,
                    run_state.base_path,
                    run_state.thread_limiter,
                    url.clone(),
                    #[cfg(feature = "resume")]
                    |url: &str, body: &String| {
                        if !run_state.config_only {
                            if let Some(db) = run_state.db.clone() {
                                db.log_robots(url, body.as_str())?;
                            }
                        }
                        Ok::<_, async_sqlite::Error>(())
                    },
                    #[cfg(not(feature = "resume"))]
                    |_, _| Ok(()),
                    #[cfg(feature = "resume")]
                    |parent: &LimitedUrl, children: Vec<Url>| {
                        if !run_state.config_only {
                            if let Some(db) = run_state.db.clone() {
                                db.log_visited(parent, children)?;
                            }
                        }
                        Ok(())
                    },
                    #[cfg(not(feature = "resume"))]
                    |_, _| Ok(()),
                    run_state.progress.map(|x| x.write_progress()),
                )
                .await
                .map(|(x, y, z)| ProcessReturn::Download((url.clone(), x, y, z)))?;

                #[cfg(feature = "resume")]
                if !run_state.config_only {
                    if let Some(db) = &run_state.db {
                        db.drop_pending(url.url()).unwrap();
                    }
                }

                Ok(res)
                    */
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
                        let _ = db.log_domain(url.url_base(), depth.into());
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
    pending: HashMap<String, (usize, Vec<LimitedUrl>)>,
    in_flight: Arc<(Mutex<Option<Waker>>, AtomicUsize)>,
    burst: usize,
    parallel_cap: usize,
}

impl Dispatcher {
    pub fn new(
        rx: std::sync::mpsc::Receiver<LimitedUrl>,
        in_flight: Arc<(Mutex<Option<Waker>>, AtomicUsize)>,
        burst: usize,
        parallel_cap: usize,
    ) -> Self {
        Self {
            rx,
            pending: HashMap::default(),
            in_flight,
            burst,
            parallel_cap,
        }
    }

    pub fn push(&mut self, url: LimitedUrl) {
        let (count, vec) = self
            .pending
            .entry(url.url_base().to_string())
            .or_insert((0, Vec::new()));
        *count += 1;
        vec.push(url);
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

        if this.burst == 0 {
            let mut waker_handle = this.in_flight.0.lock().unwrap();
            if this.in_flight.1.load(Ordering::Acquire) > this.parallel_cap {
                // Re-check condition after lock
                if this.in_flight.1.load(Ordering::Acquire) > this.parallel_cap {
                    *waker_handle = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }

            this.burst = min(
                this.pending.values().filter(|x| !x.1.is_empty()).count(),
                this.parallel_cap * this.parallel_cap,
            );
            if this.burst == 0 {
                return Poll::Ready(None);
            }
        }

        this.burst -= 1;

        Poll::Ready(
            this.pending
                .values_mut()
                .filter(|x| !x.1.is_empty())
                .min_by_key(|x| x.0)
                .and_then(|x| x.1.pop()),
        )
    }
}

#[derive(Debug, Error)]
#[err_tree(RobotsCheckErrWrap)]
pub enum RobotsCheckErr {
    #[error("failure while checking robots.txt")]
    RobotsCheck(#[source] RobotsErrWrap),
    #[cfg(feature = "resume")]
    #[error("logging callback failed")]
    CB(#[source] async_sqlite::Error),
}

async fn execute(pending: Vec<LimitedUrl>, run_state: RunState) {
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
                    preprocessing.fetch_sub(1, Ordering::Acquire);
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
                        preprocessing.fetch_sub(1, Ordering::Acquire);
                    }
                    // Push newly pending urls back to robot check stage
                    speciam::VisitCacheRes::SmallerThanCached(urls) => {
                        let urls_len = urls.len();

                        // Adjust preprocessing count before submitting, so
                        // other atomic count adjustments don't underflow.
                        // Skip a useless 0 add atomic op
                        if urls_len > 1 {
                            preprocessing.fetch_add(urls_len - 1, Ordering::Acquire);
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
                            preprocessing.fetch_sub(1, Ordering::Acquire);
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
                        preprocessing.fetch_sub(1, Ordering::Acquire);
                    }
                };
            }

            #[cfg(feature = "resume")]
            Ok::<_, async_sqlite::Error>(())
        });

        (visited_passed_rx, visited_check_thread)
    };
    // END NEW STUFF

    let in_flight = Arc::new((Mutex::default(), AtomicUsize::new(0)));
    let parallel_cap = run_state.thread_limiter.get_limit();
    let mut dispatcher = Dispatcher::new(
        visited_passed_rx,
        in_flight.clone(),
        pending.len(),
        parallel_cap,
    );

    // Initialize process handles with the base urls
    let mut handles = FuturesUnordered::<JoinHandle<Result<ProcessReturn, _>>>::new();
    for initial in pending {
        prog_reg(&initial, &run_state.progress);
        dispatcher.push(initial);
    }

    loop {
        tokio::select! {
            Some(dispatch_next) = dispatcher.next() => {
                in_flight.1.fetch_add(1, std::sync::atomic::Ordering::Release);
                handles.push(spawn_process(dispatch_next, run_state.clone()))
            }
            Some(next) = handles.next() => {
                // TODO: actually handle HTTP download errors
                match next.unwrap() {
                    Ok(next) => match next {
                        ProcessReturn::NoOp(source) => {
                            prog_free(&source, None);
                        },
                        ProcessReturn::Download((source, scrape, wh, ver)) => {
                            prog_free(&source, ver);

                            for url in scrape {
                                if source.url_base() == url.url_base() ||
                                    run_state.primary_domains.contains(&source.url_base().to_string()) {
                                    #[cfg(feature = "resume")]
                                    if !run_state.config_only {
                                        if let Some(db) = &run_state.db {
                                            db.push_pending(url.url()).unwrap();
                                        }
                                    }

                                    prog_reg(&url, &run_state.progress);
                                    dispatcher.push(url);
                                }
                            }

                            if let Some(h) = wh {
                                prog_reg_write(h.target, h.size_prediction);
                                write_handles.push(h.handle);
                            }
                        }
                        ProcessReturn::MappingDomain(mapping) => {
                            map_handles.push(mapping);
                        }
                    }
                    Err(e) => {
                        let mut error = String::new();
                        print_tree::<60, dyn Error, _, _>(&e as &dyn Error, &mut error).unwrap();
                        event!(Level::ERROR, "{:#?}", error)
                    },
                }

                // Update in flight counter, if it goes below parallel cap and
                // a waker exists, use it.
                let cur_counter = in_flight.1.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                if cur_counter <= parallel_cap + 1 {
                    if let Some(waker) = in_flight.0.lock().unwrap().take() {
                        waker.wake()
                    }
                };
            }
            // Panic if a write fails
            Some(fin_write) = write_handles.next() => {
                let (path, write_size) = tree_unwrap::<60, _, _, _>(fin_write.unwrap());
                prog_free_write(path, write_size);
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

    exit(0)
}
