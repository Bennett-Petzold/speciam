use std::{
    collections::HashSet,
    env::current_dir,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use clap::Parser;
use error_stack::Report;
use reqwest::{Client, ClientBuilder, Url};
use speciam::{CannotBeABase, DepthLimit, Domains, LimitedUrl, RobotsCheck, ZeroLengthDuration};
use thiserror::Error;
use tokio::fs::File;

use crate::args::ResolvedArgs;

#[derive(Debug, Error)]
pub enum InitErr {
    #[error("failed to build web client")]
    ClientBuild(#[source] reqwest::Error),
    #[error("current directory is invalid")]
    InvalidDir(#[source] std::io::Error),
    #[error("{0:?}")]
    InvalidDelay(#[source] ZeroLengthDuration),
}

#[derive(Debug)]
pub struct RunState {
    pub client: Arc<Client>,
    pub visited: Arc<RwLock<HashSet<Url>>>,
    pub robots: Arc<RobotsCheck>,
    pub base_path: PathBuf,
    pub domains: Domains,
}

impl ResolvedArgs {
    pub async fn init(self) -> Result<RunState, Report<InitErr>> {
        let user_agent = env!("CARGO_PKG_NAME").to_string() + " " + env!("CARGO_PKG_VERSION");
        let client = Arc::new(
            ClientBuilder::new()
                .user_agent(&user_agent)
                .build()
                .map_err(InitErr::ClientBuild)?,
        );
        let visited: Arc<RwLock<_>> = Arc::default();
        let robots = Arc::new(RobotsCheck::new(
            client.clone(),
            visited.clone(),
            user_agent,
        ));
        let base_path = current_dir().map_err(InitErr::InvalidDir)?;
        let domains = Domains::new(self.delay, self.jitter).map_err(InitErr::InvalidDelay)?;

        Ok(RunState {
            client,
            visited,
            robots,
            base_path,
            domains,
        })
    }
}
