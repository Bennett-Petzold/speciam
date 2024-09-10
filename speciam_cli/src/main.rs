use std::{
    borrow::Borrow,
    collections::HashMap,
    iter::repeat,
    panic::Location,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex, RwLock,
    },
};

use clap::Parser;

mod args;
mod init;
use args::{Args, ResolvedArgs};
use error_stack::Report;
use futures::{
    select,
    stream::{self, FuturesUnordered},
    Stream, StreamExt,
};
use init::RunState;
use speciam::{dl_and_scrape, DlAndScrapeErr, DomainNotMapped, Domains, LimitedUrl, WriteHandle};
use tokio::{
    spawn,
    sync::mpsc::unbounded_channel,
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

    let args = Args::parse().resolve().await.unwrap();
    let (pending, run_state) = args.init().await.unwrap();
    execute(pending, run_state).await;
}

#[derive(Debug)]
enum ProcessReturn {
    NoOp,
    Download((Vec<LimitedUrl>, Option<WriteHandle>)),
    MappingDomain(JoinHandle<LimitedUrl>),
}

// Placeholder when not using callbacks
#[cfg(not(feature = "resume"))]
type CbErr = std::io::Error;
#[cfg(feature = "resume")]
type CbErr = async_sqlite::Error;

fn spawn_process(
    url: LimitedUrl,
    run_state: RunState,
) -> JoinHandle<Result<ProcessReturn, DlAndScrapeErr<CbErr>>> {
    spawn(async move {
        match run_state.domains.read().await.wait(&url).await {
            // Passed the depth check
            Ok(true) => {
                println!("Getting URL: {:#?}", url);
                dl_and_scrape(
                    run_state.client,
                    run_state.visited,
                    run_state.robots,
                    run_state.base_path,
                    run_state.thread_limiter,
                    url.clone(),
                    #[cfg(feature = "resume")]
                    |url: Url, body: &String| {
                        if let Some(db) = run_state.db.clone() {
                            db.log_robots(&url, body.as_str())?;
                        }
                        Ok::<_, async_sqlite::Error>(())
                    },
                    #[cfg(feature = "resume")]
                    |parent: &LimitedUrl, children: Vec<Url>| {
                        if let Some(db) = run_state.db.clone() {
                            db.log_visited(parent, children)?;
                        }
                        Ok(())
                    },
                )
                .await
                .map(|(x, y)| ProcessReturn::Download((x, y)))
            }
            // Failed the depth check
            Ok(false) => Ok(ProcessReturn::NoOp),
            // The domain needs to be initialized
            Err(DomainNotMapped(url)) => {
                let domains = run_state.domains.clone();
                let handle = spawn_blocking(move || {
                    domains
                        .blocking_write()
                        .add_limit(&url, run_state.secondary_depth);
                    url
                });
                Ok(ProcessReturn::MappingDomain(handle))
            }
        }
    })
}

async fn execute(pending: Vec<LimitedUrl>, run_state: RunState) {
    // Initialize process handles with the base urls
    let mut handles = FuturesUnordered::from_iter(
        pending
            .into_iter()
            .map(|x| spawn_process(x, run_state.clone())),
    );

    let mut map_handles = FuturesUnordered::new();
    let mut write_handles = FuturesUnordered::new();

    loop {
        tokio::select! {
            Some(next) = handles.next() => {
                match next.map_err(Report::new).unwrap().map_err(Report::new).unwrap() {
                    ProcessReturn::NoOp => (),
                    ProcessReturn::Download((scrape, wh)) => {
                        for url in scrape {
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
                println!("FINISHED!");
                break;
            }
        }
    }
}
