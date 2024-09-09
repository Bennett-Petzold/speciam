use std::{
    cmp::min,
    collections::HashSet,
    env::current_dir,
    fmt::Debug,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    thread::available_parallelism,
    time::Duration,
};

use clap::Parser;
use error_stack::Report;
use reqwest::{Client, ClientBuilder, Url};
use speciam::{CannotBeABase, DepthLimit, LimitedUrl, RobotsCheck};
use thiserror::Error;
use tokio::{fs::File, task::JoinHandle};

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
    /// Load an existing configuration file.
    #[arg(short, long)]
    load_config: Option<PathBuf>,
    /// Save a configuration for this run.
    #[arg(short, long)]
    save_config: bool,
    /// Disable interactive prompting: always assume defaults.
    #[arg(short, long)]
    no_prompt: bool,
    /// Display progress bars.
    #[arg(short, long)]
    bars: bool,
    /// Save logs to this file (not currently implemented).
    #[arg(short, long)]
    write_logs: Option<PathBuf>,
    /// Cap on concurrent downloads. Defaults to number of cores.
    #[arg(short, long)]
    concurrency: Option<usize>,
    #[cfg(feature = "resume")]
    /// Write to/resume from session saved to this sqlite database.
    #[arg(short, long)]
    resume: Option<PathBuf>,
    /// Urls to start scraping from.
    ///
    /// The domains of all urls will be treated as primary domains unless
    /// explicitly disabled via "--disable_primary".
    start_urls: Vec<Url>,
}

const DEFAULT_SECONDARY_DEPTH: usize = 5;
const DEFAULT_DELAY: u64 = 500;
const DEFAULT_JITTER: u64 = 1000;

pub struct ResolvedArgs {
    pub primary_depth: DepthLimit,
    pub secondary_depth: DepthLimit,
    pub primary_domains: Vec<Url>,
    pub delay: Duration,
    pub jitter: Duration,
    pub save_config: bool,
    pub no_prompt: bool,
    pub bars: bool,
    pub write_logs: Option<File>,
    pub concurrency: usize,
    pub start_urls: Vec<LimitedUrl>,
    #[cfg(feature = "resume")]
    pub resume: Option<crate::resume::SqliteLogging>,
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
            .field("save_config", &self.save_config)
            .field("no_prompt", &self.no_prompt)
            .field("bars", &self.bars)
            .field("write_logs", &self.write_logs)
            .field("concurrency", &self.concurrency)
            .field("start_urls", &self.start_urls);

        #[cfg(feature = "resume")]
        fmt.field("resume", &self.resume.is_some());

        fmt.finish()
    }
}

#[derive(Debug, Error)]
#[error("one of the start urls ({url:?}) does not have a valid domain")]
pub struct InvalidPrimaryDomain {
    #[source]
    source: CannotBeABase,
    url: Url,
}

#[derive(Debug, Error)]
pub enum ResolveErr {
    #[error("{0:?}")]
    InvalidPrimaryDomain(#[from] InvalidPrimaryDomain),
    #[error("failed to create a logfile")]
    NoLogFile(#[from] std::io::Error),
    #[cfg(feature = "resume")]
    #[error("failed to open sqlite database")]
    SqliteOpen(#[source] async_sqlite::Error),
    #[cfg(feature = "resume")]
    #[error("failed to initialize sqlite database")]
    SqliteInit(#[source] async_sqlite::Error),
}

impl Args {
    pub async fn resolve(self) -> Result<ResolvedArgs, Report<ResolveErr>> {
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
                    ResolveErr::InvalidPrimaryDomain(InvalidPrimaryDomain { source, url })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_domains = start_urls.iter().map(LimitedUrl::url_base).collect();

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
            save_config: self.save_config,
            no_prompt: self.no_prompt,
            bars: self.bars,
            write_logs,
            // Concurrency == 0 will spawn no threads, causing a deadlock
            concurrency: min(
                1,
                self.concurrency
                    .unwrap_or(available_parallelism().unwrap().into()),
            ),
            start_urls,
            #[cfg(feature = "resume")]
            resume,
        })
    }
}
