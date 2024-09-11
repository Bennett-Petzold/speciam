use std::{
    panic::{self, Location},
    process::{self, exit},
    sync::Mutex,
};

use clap::Parser;

mod args;
mod init;
mod progress;
use args::Args;
use error_stack::Report;
use futures::{stream::FuturesUnordered, StreamExt};
use init::RunState;
use reqwest::Version;
use speciam::{
    dl_and_scrape, DepthLimit, DlAndScrapeErr, DomainNotMapped, LimitedUrl, WriteHandle,
};
use tokio::{
    spawn,
    task::{spawn_blocking, JoinHandle},
};
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
                    run_state.client,
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

    // Initialize process handles with the base urls
    let mut handles = FuturesUnordered::from_iter(
        pending
            .into_iter()
            .inspect(prog_reg)
            .map(|x| spawn_process(x, run_state.clone())),
    );

    let mut map_handles = FuturesUnordered::new();
    let mut write_handles = FuturesUnordered::new();

    loop {
        tokio::select! {
            Some(next) = handles.next() => {
                match next.map_err(Report::new).unwrap().map_err(Report::new).unwrap() {
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
                            handles.push(spawn_process(url, run_state.clone()));
                        }
                        if let Some(h) = wh {
                            write_handles.push(h);
                        }
                    }
                    ProcessReturn::MappingDomain(mapping) => {
                        map_handles.push(mapping);
                    }
                }
            }
            // Panic if a write fails
            Some(fin_write) = write_handles.next() => {
                fin_write.unwrap().unwrap();
            }
            Some(fin_map) = map_handles.next() => {
                handles.push(spawn_process(fin_map.unwrap(), run_state.clone()));
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
