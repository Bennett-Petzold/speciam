use std::{env::current_dir, path::PathBuf, sync::Arc};

use bare_err_tree::{tree, AsErrTree, ErrTreePkg};
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
    GenRecoveryErr(#[from] crate::resume::GenRecoveryErrWrap),
    #[cfg(feature = "resume")]
    #[error("{0:?}")]
    LimitRecoveryErr(#[from] crate::resume::LimitRecoveryErrWrap),
}

#[derive(Debug)]
pub struct InitErrWrap {
    inner: InitErr,
    _err_tree_pkg: ErrTreePkg,
}

impl From<InitErr> for InitErrWrap {
    #[track_caller]
    fn from(inner: InitErr) -> Self {
        Self {
            inner,
            _err_tree_pkg: ErrTreePkg::new(),
        }
    }
}

impl AsErrTree for InitErrWrap {
    fn as_err_tree(&self, func: &mut dyn FnMut(bare_err_tree::ErrTree<'_>)) {
        match &self.inner {
            InitErr::ClientBuild(x) => tree!(dyn, func, self.inner, self._err_tree_pkg, x),
            InitErr::InvalidDir(x) => tree!(dyn, func, self.inner, self._err_tree_pkg, x),
            InitErr::InvalidDelay(x) => tree!(func, self.inner, self._err_tree_pkg, x),
            #[cfg(feature = "resume")]
            InitErr::GenRecoveryErr(x) => tree!(func, self.inner, self._err_tree_pkg, x),
            #[cfg(feature = "resume")]
            InitErr::LimitRecoveryErr(x) => tree!(func, self.inner, self._err_tree_pkg, x),
        }
    }
}

/// Accumulation of all configured and restored state.
///
/// Expected to be immediately destroyed for execution parts.
#[derive(Clone)]
pub struct RunState {
    pub client: Client,
    pub visited: Arc<VisitCache>,
    pub robots: Arc<RobotsCheck<Client>>,
    pub base_path: PathBuf,
    pub domains: Arc<RwLock<Domains>>,
    pub thread_limiter: Arc<ThreadLimiter>,
    pub secondary_depth: DepthLimit,
    pub primary_domains: Box<[String]>,
    pub interactive: bool,
    pub progress: Option<DlProgress>,
    #[cfg(feature = "resume")]
    pub db: Option<Arc<crate::resume::SqliteLogging>>,
    #[cfg(feature = "resume")]
    pub config_only: bool,
}

impl ResolvedArgs {
    #[tracing::instrument]
    pub async fn init(mut self) -> Result<(Vec<LimitedUrl>, RunState), InitErrWrap> {
        let user_agent = env!("CARGO_PKG_NAME").to_string() + " " + env!("CARGO_PKG_VERSION");
        let client = ClientBuilder::new()
            .use_rustls_tls()
            .user_agent(&user_agent)
            .build()
            .map_err(InitErr::ClientBuild)?;

        let domains = Domains::new(self.delay, self.jitter).map_err(InitErr::InvalidDelay)?;
        let base_path = current_dir().map_err(InitErr::InvalidDir)?;

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
        let (visited, robots, mut pending) = if let Some(db) = db.clone() {
            let (visited, robots, pending, domain_lines_res) = if self.config_only {
                let domain_lines_fut = tokio::spawn(async move {
                    db.restore_domains()
                        .await
                        .map_err(InitErr::from)
                        .map_err(InitErrWrap::from)
                });
                let (visited, robots, pending) = no_db();
                (visited, robots, pending, domain_lines_fut.await.unwrap())
            } else {
                let db_clone = db.clone();
                let visited_fut = tokio::spawn(async move {
                    db_clone
                        .restore_visited()
                        .await
                        .map_err(InitErr::from)
                        .map_err(InitErrWrap::from)
                });

                let db_clone = db.clone();
                let user_agent_clone = user_agent.clone();
                let robots_fut = tokio::spawn(async move {
                    db_clone
                        .restore_robots(user_agent_clone.as_str())
                        .await
                        .map_err(InitErr::from)
                        .map_err(InitErrWrap::from)
                });

                let db_clone = db.clone();
                let domain_lines_fut = tokio::spawn(async move {
                    db_clone
                        .restore_domains()
                        .await
                        .map_err(InitErr::from)
                        .map_err(InitErrWrap::from)
                });

                let db_clone = db.clone();
                let pending_fut = tokio::spawn(async move {
                    db_clone
                        .restore_pending()
                        .await
                        .map_err(InitErr::from)
                        .map_err(InitErrWrap::from)
                });

                let (visited, robots, domain_lines_res, pending) =
                    try_join!(visited_fut, robots_fut, domain_lines_fut, pending_fut).unwrap();

                let visited = Arc::new(visited?);
                let robots = Arc::new(RobotsCheck::with_database(
                    client.clone(),
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
                primary_domains: self.primary_domains.into(),
                interactive: !self.no_prompt,
                progress: self.bars.then(|| DlProgress::new()),
                #[cfg(feature = "resume")]
                db,
                #[cfg(feature = "resume")]
                config_only: self.config_only,
            },
        ))
    }
}
