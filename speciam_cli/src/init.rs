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
use speciam::{
    CannotBeABase, DepthLimit, Domains, LimitedUrl, RobotsCheck, VisitCache, ZeroLengthDuration,
};
use thiserror::Error;
use tokio::{
    fs::File,
    join,
    sync::mpsc::{unbounded_channel, Receiver, Sender},
    try_join,
};

use crate::args::ResolvedArgs;

#[derive(Debug, Error)]
pub enum InitErr {
    #[error("failed to build web client")]
    ClientBuild(#[source] reqwest::Error),
    #[error("current directory is invalid")]
    InvalidDir(#[source] std::io::Error),
    #[error("{0:?}")]
    InvalidDelay(#[source] ZeroLengthDuration),
    #[cfg(feature = "resume")]
    #[error("{0:?}")]
    GenRecoveryErr(#[from] crate::resume::GenRecoveryErr),
    #[cfg(feature = "resume")]
    #[error("{0:?}")]
    LimitRecoveryErr(#[from] crate::resume::LimitRecoveryErr),
}

/// Accumulation of all configured and restored state.
///
/// Expected to be immediately destroyed for execution parts.
pub struct RunState {
    pub client: Arc<Client>,
    pub visited: Arc<VisitCache>,
    pub robots: Arc<RobotsCheck>,
    pub pending: Vec<LimitedUrl>,
    pub base_path: Arc<PathBuf>,
    pub domains: Domains,
    #[cfg(feature = "resume")]
    pub db: Option<crate::resume::SqliteLogging>,
}

impl ResolvedArgs {
    pub async fn init(mut self) -> Result<RunState, Report<InitErr>> {
        let user_agent = env!("CARGO_PKG_NAME").to_string() + " " + env!("CARGO_PKG_VERSION");
        let client = Arc::new(
            ClientBuilder::new()
                .user_agent(&user_agent)
                .build()
                .map_err(InitErr::ClientBuild)?,
        );

        let domains = Domains::new(self.delay, self.jitter).map_err(InitErr::InvalidDelay)?;
        let base_path = Arc::new(current_dir().map_err(InitErr::InvalidDir)?);

        let no_db = || {
            let visited: Arc<VisitCache> = Arc::default();
            let robots = Arc::new(RobotsCheck::new(
                client.clone(),
                visited.clone(),
                user_agent.clone(),
            ));
            let pending = Vec::new();
            (visited, robots, pending)
        };

        #[cfg(feature = "resume")]
        let db = self.resume.take().map(Arc::new);

        #[cfg(feature = "resume")]
        let (visited, robots, pending) = if let Some(db) = db {
            let db_clone = db.clone();
            let visited_fut =
                tokio::spawn(
                    async move { db_clone.restore_visited().await.map_err(InitErr::from) },
                );

            let db_clone = db.clone();
            let robots_fut =
                tokio::spawn(async move { db_clone.restore_robots().await.map_err(InitErr::from) });

            let db_clone = db.clone();
            let domain_lines_fut =
                tokio::spawn(
                    async move { db_clone.restore_domains().await.map_err(InitErr::from) },
                );

            let db_clone = db.clone();
            let pending_fut =
                tokio::spawn(
                    async move { db_clone.restore_pending().await.map_err(InitErr::from) },
                );

            let (visited, robots, domain_lines_res, pending) =
                try_join!(visited_fut, robots_fut, domain_lines_fut, pending_fut).unwrap();

            let visited = Arc::new(visited?);
            let robots = Arc::new(RobotsCheck::with_database(
                client.clone(),
                visited.clone(),
                user_agent,
                robots?,
            ));
            domain_lines_res?;

            (visited, robots, pending?)
        } else {
            no_db()
        };

        #[cfg(not(feature = "resume"))]
        let (visited, robots, pending) = { no_db() };

        Ok(RunState {
            client,
            visited,
            robots,
            pending,
            base_path,
            domains,
            #[cfg(feature = "resume")]
            db: self.resume,
        })
    }
}
