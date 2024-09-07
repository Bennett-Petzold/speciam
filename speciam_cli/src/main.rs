use std::panic::Location;

use clap::Parser;

mod args;
use args::{Args, ResolvedArgs};
use init::RunState;

mod init;

#[tokio::main]
async fn main() {
    error_stack::Report::install_debug_hook::<Location>(|_location, _context| {
        // Intentionally left empty so nothing will be printed
    });

    let args = Args::parse().resolve().await.unwrap();
    let run_state = args.init().await.unwrap();
    execute(run_state).await;
}

async fn execute(run_state: RunState) {
    todo!()
}
