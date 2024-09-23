use std::{cmp::min, fmt::Debug, path::PathBuf, thread::available_parallelism, time::Duration};

use bare_err_tree::{err_tree, tree, AsErrTree, ErrTreePkg};
use clap::Parser;
use reqwest::Url;
use speciam::{CannotBeABase, DepthLimit, LimitedUrl};
use thiserror::Error;
use tokio::fs::File;

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Args {
    /// Depth for primary domains; default unlimited.
    #[arg(short, long)]
    primary_depth: Option<usize>,
    /// Default depth limit for secondary domains found during scraping.
    #[arg(long)]
    secondary_depth: Option<usize>,
    /// Domain in `start_urls` that should be treated as secondary.
    #[arg(long)]
    disable_primary: Vec<Url>,
    /// Base delay in milliseconds between requests to a domain.
    #[arg(short, long)]
    delay: Option<u64>,
    /// Max number of extra milliseconds to delay requests.
    #[arg(short, long)]
    jitter: Option<u64>,
    /// Disable interactive prompting: always assume defaults.
    #[arg(short, long)]
    no_prompt: bool,
    /// Display progress bars.
    #[arg(short, long)]
    bars: bool,
    /// Save logs to this file (not currently implemented).
    #[arg(short, long)]
    write_logs: Option<PathBuf>,
    /// Cap on parallel downloads. Defaults to number of cores.
    #[arg(short, long)]
    units: Option<usize>,
    #[cfg(feature = "resume")]
    /// Write to/resume from session saved to this sqlite database.
    #[arg(short, long)]
    resume: Option<PathBuf>,
    #[cfg(feature = "resume")]
    /// Discard any non-configuration state in the sqlite database.
    #[arg(short, long)]
    config_only: bool,
    /// Urls to start scraping from.
    ///
    /// The domains of all urls will be treated as primary domains unless
    /// explicitly disabled via "--disable_primary".
    start_urls: Vec<Url>,
}

const DEFAULT_SECONDARY_DEPTH: usize = 1;
const DEFAULT_DELAY: u64 = 500;
const DEFAULT_JITTER: u64 = 1000;

pub struct ResolvedArgs {
    pub primary_depth: DepthLimit,
    pub secondary_depth: DepthLimit,
    pub primary_domains: Vec<String>,
    pub delay: Duration,
    pub jitter: Duration,
    pub no_prompt: bool,
    pub bars: bool,
    pub write_logs: Option<File>,
    pub units: usize,
    pub start_urls: Vec<LimitedUrl>,
    #[cfg(feature = "resume")]
    pub resume: Option<crate::resume::SqliteLogging>,
    #[cfg(feature = "resume")]
    pub config_only: bool,
}

impl Debug for ResolvedArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut fmt = f.debug_struct("ResolvedArgs");
        let fmt = fmt
            .field("primary_depth", &self.primary_depth)
            .field("secondary_depth", &self.secondary_depth)
            .field("primary_domains", &self.primary_domains)
            .field("delay", &self.delay)
            .field("jitter", &self.jitter)
            .field("no_prompt", &self.no_prompt)
            .field("bars", &self.bars)
            .field("write_logs", &self.write_logs)
            .field("units", &self.units)
            .field("start_urls", &self.start_urls);

        #[cfg(feature = "resume")]
        fmt.field("resume", &self.resume.is_some());
        #[cfg(feature = "resume")]
        fmt.field("config_only", &self.config_only);

        fmt.finish()
    }
}

#[derive(Debug)]
#[err_tree]
#[derive(Error)]
#[error("one of the start urls ({url:?}) does not have a valid domain")]
pub struct InvalidPrimaryDomain {
    #[source]
    source: CannotBeABase,
    url: Url,
}

impl InvalidPrimaryDomain {
    #[track_caller]
    pub fn new(source: CannotBeABase, url: Url) -> Self {
        Self::_tree(source, url)
    }
}

#[derive(Debug, Error)]
pub enum ResolveErr {
    #[error("{0:?}")]
    InvalidPrimaryDomain(#[source] Box<InvalidPrimaryDomain>),
    #[error("failed to create a logfile")]
    NoLogFile(#[from] std::io::Error),
    #[cfg(feature = "resume")]
    #[error("failed to open sqlite database")]
    SqliteOpen(#[source] async_sqlite::Error),
    #[cfg(feature = "resume")]
    #[error("failed to initialize sqlite database")]
    SqliteInit(#[source] async_sqlite::Error),
}

#[derive(Debug, Error)]
#[error("{inner:?}")]
pub struct ResolveErrWrap {
    inner: ResolveErr,
    _err_tree_pkg: ErrTreePkg,
}

impl From<ResolveErr> for ResolveErrWrap {
    #[track_caller]
    fn from(inner: ResolveErr) -> Self {
        Self {
            inner,
            _err_tree_pkg: ErrTreePkg::new(),
        }
    }
}

impl AsErrTree for ResolveErrWrap {
    fn as_err_tree(&self, func: &mut dyn FnMut(bare_err_tree::ErrTree<'_>)) {
        match &self.inner {
            ResolveErr::InvalidPrimaryDomain(x) => {
                tree!(func, self.inner, self._err_tree_pkg, x.as_ref())
            }
            ResolveErr::NoLogFile(x) => tree!(dyn, func, self.inner, self._err_tree_pkg, x),
            #[cfg(feature = "resume")]
            ResolveErr::SqliteOpen(x) => tree!(dyn, func, self.inner, self._err_tree_pkg, x),
            #[cfg(feature = "resume")]
            ResolveErr::SqliteInit(x) => tree!(dyn, func, self.inner, self._err_tree_pkg, x),
        }
    }
}

impl Args {
    #[tracing::instrument]
    pub async fn resolve(self) -> Result<ResolvedArgs, ResolveErrWrap> {
        let write_logs = if let Some(path) = self.write_logs {
            Some(File::create(path).await.map_err(ResolveErr::NoLogFile)?)
        } else {
            None
        };

        let start_urls = self
            .start_urls
            .into_iter()
            .map(|url| {
                LimitedUrl::origin(url.clone()).map_err(|source| {
                    ResolveErr::InvalidPrimaryDomain(InvalidPrimaryDomain::new(source, url).into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_domains = start_urls
            .iter()
            .map(|x| LimitedUrl::url_base(x).to_string())
            .collect();

        #[cfg(feature = "resume")]
        let resume = if let Some(resume) = self.resume {
            Some(
                async_sqlite::PoolBuilder::new()
                    .path(resume)
                    .journal_mode(async_sqlite::JournalMode::Wal)
                    .open()
                    .await
                    .map_err(ResolveErr::SqliteOpen)?
                    .try_into()
                    .map_err(ResolveErr::SqliteInit)?,
            )
        } else {
            None
        };

        Ok(ResolvedArgs {
            primary_depth: self.primary_depth.into(),
            secondary_depth: self
                .secondary_depth
                .unwrap_or(DEFAULT_SECONDARY_DEPTH)
                .into(),
            primary_domains,
            delay: Duration::from_millis(self.delay.unwrap_or(DEFAULT_DELAY)),
            jitter: Duration::from_millis(self.jitter.unwrap_or(DEFAULT_JITTER)),
            no_prompt: self.no_prompt,
            bars: self.bars,
            write_logs,
            // Units == 0 will spawn no threads, causing a deadlock
            units: min(
                1,
                self.units
                    .unwrap_or(available_parallelism().unwrap().into()),
            ),
            start_urls,
            #[cfg(feature = "resume")]
            resume,
            #[cfg(feature = "resume")]
            config_only: self.config_only,
        })
    }
}
