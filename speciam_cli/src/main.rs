use std::{
    cmp::min,
    collections::HashMap,
    io::Write,
    panic::{self, Location},
    path::PathBuf,
    pin::Pin,
    process::{self, exit},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    task::{Context, Poll, Waker},
};

use clap::Parser;

mod args;
mod init;
mod progress;
use args::Args;
use error_stack::Report;
use futures::{stream::FuturesUnordered, Stream, StreamExt};
use init::RunState;
use reqwest::Version;
use speciam::{
    dl_and_scrape, DepthLimit, DlAndScrapeErr, DomainNotMapped, LimitedUrl, ThreadLimiter,
    WriteHandle,
};
use tokio::{
    spawn,
    task::{spawn_blocking, JoinHandle},
};
use tracing::{event, Level};
use url::Url;

#[cfg(feature = "resume")]
pub mod resume;

#[tokio::main]
async fn main() {
    error_stack::Report::install_debug_hook::<Location>(|_location, _context| {
        // Intentionally left empty so nothing will be printed
        // Temporary hack around not actually propogating backtrace numbers
        // with stack
    });

    // Exit when any thread panics
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        process::exit(1);
    }));

    let args = Args::parse().resolve().await.unwrap();
    let (pending, run_state) = args.init().await.unwrap();
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

static PROMPTING: Mutex<()> = Mutex::new(());

fn spawn_process(
    url: LimitedUrl,
    run_state: RunState,
) -> JoinHandle<Result<ProcessReturn, DlAndScrapeErr<CbErr>>> {
    spawn(async move {
        let domains_check = { run_state.domains.read().await.wait(&url).await };
        match domains_check {
            // Passed the depth check
            Ok(true) => {
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
            Err(DomainNotMapped(url)) => {
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
    pending: HashMap<String, (usize, Vec<LimitedUrl>)>,
    in_flight: Arc<(Mutex<Option<Waker>>, AtomicUsize)>,
    burst: usize,
    parallel_cap: usize,
}

impl Dispatcher {
    pub fn new(
        in_flight: Arc<(Mutex<Option<Waker>>, AtomicUsize)>,
        burst: usize,
        parallel_cap: usize,
    ) -> Self {
        Self {
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

        if this.burst == 0 {
            let mut waker_handle = this.in_flight.0.lock().unwrap();
            if this.in_flight.1.load(Ordering::Acquire) > this.parallel_cap {
                // Re-check condition after lock
                if this.in_flight.1.load(Ordering::Acquire) > this.parallel_cap {
                    *waker_handle = Some(cx.waker().clone());
                }
                return Poll::Pending;
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

async fn execute(pending: Vec<LimitedUrl>, run_state: RunState) {
    let prog_reg = |x: &LimitedUrl| {
        if let Some(progress) = &run_state.progress {
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

    let in_flight = Arc::new((Mutex::default(), AtomicUsize::new(0)));
    let parallel_cap = run_state.thread_limiter.get_limit();
    let mut dispatcher = Dispatcher::new(in_flight.clone(), pending.len(), parallel_cap);

    // Initialize process handles with the base urls
    let mut handles = FuturesUnordered::<JoinHandle<Result<ProcessReturn, _>>>::new();
    for initial in pending {
        prog_reg(&initial);
        dispatcher.push(initial);
    }

    let mut map_handles = FuturesUnordered::new();
    let mut write_handles = FuturesUnordered::new();

    loop {
        tokio::select! {
            Some(next) = handles.next() => {
                // TODO: actually handle HTTP download errors
                match next.map_err(Report::new).unwrap() {
                    Ok(next) => match next {
                        ProcessReturn::NoOp(source) => {
                            prog_free(&source, None);
                        },
                        ProcessReturn::Download((source, scrape, wh, ver)) => {
                            prog_free(&source, ver);

                            for url in scrape {
                                #[cfg(feature = "resume")]
                                if !run_state.config_only {
                                    if let Some(db) = &run_state.db {
                                        db.push_pending(url.url()).unwrap();
                                    }
                                }

                                prog_reg(&url);
                                dispatcher.push(url);
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
                    Err(e) => event!(Level::ERROR, "{:#?}", e),
                }

                // Update in flight counter, if it reaches parallel cap and a
                // waker exists, use it.
                let cur_counter = in_flight.1.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                if cur_counter == parallel_cap + 1 {
                    if let Some(waker) = in_flight.0.lock().unwrap().take() {
                        waker.wake()
                    }
                };

            }
            Some(dispatch_next) = dispatcher.next() => {
                in_flight.1.fetch_add(1, std::sync::atomic::Ordering::Release);
                handles.push(spawn_process(dispatch_next, run_state.clone()))
            }
            // Panic if a write fails
            Some(fin_write) = write_handles.next() => {
                let (path, write_size) = fin_write.unwrap().unwrap();
                prog_free_write(path, write_size);
            }
            Some(fin_map) = map_handles.next() => {
                dispatcher.push(fin_map.unwrap());
            }
            // End of scraping, all queues emptied
            else => {
                // Finished scraping
                let stats = "FINISHED! STATS TODO";
                if let Some(progress) = &run_state.progress {
                    progress.println(stats);
                } else {
                    println!("{}", stats);
                }
                break;
            }
        }
    }

    exit(0)
}
