use std::{iter::repeat, panic::Location};

use async_channel::unbounded;
use clap::Parser;

mod args;
mod init;
use args::{Args, ResolvedArgs};
use init::RunState;
use tokio::{spawn, sync::mpsc::unbounded_channel};

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
    let run_state = args.init().await.unwrap();
    execute(run_state).await;
}

async fn execute(run_state: RunState) {
    // Initialize the pending queue
    let (pending_tx, pending_rx) = async_channel::unbounded();
    for start_pending in run_state.pending {
        pending_tx.send(start_pending).await.unwrap();
    }

    let run_handles: Vec<_> = repeat((pending_tx, pending_rx))
        .map(|(pending_tx, pending_rx)| {
            spawn(async move {
                while let Ok(url) = pending_rx.recv().await {
                    //let processed =
                }
            });
            //Ok(())
        })
        .take(run_state.concurrency)
        .collect();
}
