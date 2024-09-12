use std::{env::current_dir, path::PathBuf, sync::Arc};

use error_stack::Report;
use once_map::{LazyMap, RandomState};
use reqwest::{Client, ClientBuilder};
use speciam::{
    CannotBeABase, DepthLimit, Domains, LimitedUrl, RobotsCheck, ThreadLimiter, VisitCache,
    ZeroLengthDuration,
};
use thiserror::Error;
use tokio::{sync::RwLock, try_join};

use crate::{args::ResolvedArgs, progress::DlProgress};

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
#[derive(Clone)]
pub struct RunState {
    pub client: Arc<LazyMap<String, Client>>,
    pub visited: Arc<VisitCache>,
    pub robots: Arc<RobotsCheck<Client>>,
    pub base_path: PathBuf,
    pub domains: Arc<RwLock<Domains>>,
    pub thread_limiter: Arc<ThreadLimiter>,
    pub secondary_depth: DepthLimit,
    pub interactive: bool,
    pub progress: Option<DlProgress>,
    #[cfg(feature = "resume")]
    pub db: Option<Arc<crate::resume::SqliteLogging>>,
    #[cfg(feature = "resume")]
    pub config_only: bool,
}

fn new_client<'a>(_str: &'a String) -> Client {
    let user_agent = env!("CARGO_PKG_NAME").to_string() + " " + env!("CARGO_PKG_VERSION");
    ClientBuilder::new()
        .use_rustls_tls()
        .user_agent(&user_agent)
        .build()
        .unwrap()
}

impl ResolvedArgs {
    pub async fn init(mut self) -> Result<(Vec<LimitedUrl>, RunState), Report<InitErr>> {
        let user_agent = env!("CARGO_PKG_NAME").to_string() + " " + env!("CARGO_PKG_VERSION");
        let client = Arc::new(LazyMap::new(new_client as fn(&String) -> Client));

        let domains = Domains::new(self.delay, self.jitter).map_err(InitErr::InvalidDelay)?;
        let base_path = current_dir().map_err(InitErr::InvalidDir)?;

        let no_db = || {
            let visited: Arc<VisitCache> = Arc::default();
            let robots = Arc::new(RobotsCheck::new(
                client.get_cloned("robots"),
                visited.clone(),
                user_agent.clone(),
            ));
            let pending = Vec::new();
            (visited, robots, pending)
        };

        #[cfg(feature = "resume")]
        let db = self.resume.take().map(Arc::new);

        #[cfg(feature = "resume")]
        let (visited, robots, mut pending) = if let Some(db) = db.clone() {
            let (visited, robots, pending, domain_lines_res) =
                if self.config_only {
                    let domain_lines_fut =
                        tokio::spawn(
                            async move { db.restore_domains().await.map_err(InitErr::from) },
                        );
                    let (visited, robots, pending) = no_db();
                    (visited, robots, pending, domain_lines_fut.await.unwrap())
                } else {
                    let db_clone = db.clone();
                    let visited_fut = tokio::spawn(async move {
                        db_clone.restore_visited().await.map_err(InitErr::from)
                    });

                    let db_clone = db.clone();
                    let robots_fut = tokio::spawn(async move {
                        db_clone.restore_robots().await.map_err(InitErr::from)
                    });

                    let db_clone = db.clone();
                    let domain_lines_fut = tokio::spawn(async move {
                        db_clone.restore_domains().await.map_err(InitErr::from)
                    });

                    let db_clone = db.clone();
                    let pending_fut = tokio::spawn(async move {
                        db_clone.restore_pending().await.map_err(InitErr::from)
                    });

                    let (visited, robots, domain_lines_res, pending) =
                        try_join!(visited_fut, robots_fut, domain_lines_fut, pending_fut).unwrap();

                    let visited = Arc::new(visited?);
                    let robots = Arc::new(RobotsCheck::with_database(
                        client.get_cloned("robots"),
                        visited.clone(),
                        user_agent,
                        robots?,
                    ));

                    (visited, robots, pending?, domain_lines_res)
                };

            domain_lines_res?
                .into_iter()
                .flat_map(|(url, limit)| Ok::<_, CannotBeABase>((LimitedUrl::origin(url)?, limit)))
                .for_each(|(url, limit)| {
                    domains.add_limit(&url, limit);
                });

            (visited, robots, pending)
        } else {
            no_db()
        };

        #[cfg(not(feature = "resume"))]
        let (visited, robots, mut pending) = { no_db() };

        // Initialize primary domains
        self.start_urls.iter().for_each(|url| {
            domains.add_limit(url, self.primary_depth);
        });

        // Always push these URLs to pending to start the process
        pending.append(&mut self.start_urls);

        Ok((
            pending,
            RunState {
                client,
                visited,
                robots,
                base_path,
                domains: Arc::new(RwLock::new(domains)),
                thread_limiter: Arc::new(ThreadLimiter::new(self.units)),
                secondary_depth: self.secondary_depth,
                interactive: !self.no_prompt,
                progress: self.bars.then_some(DlProgress::new()),
                #[cfg(feature = "resume")]
                db,
                #[cfg(feature = "resume")]
                config_only: self.config_only,
            },
        ))
    }
}
