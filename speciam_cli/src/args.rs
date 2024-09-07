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
use speciam::{CannotBeABase, DepthLimit, LimitedUrl, RobotsCheck};
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
    bar: bool,
    /// Save logs to this file.
    #[arg(short, long)]
    write_logs: Option<PathBuf>,
    /// Resume from session saved to this file (currently unimplemented).
    #[arg(short, long)]
    resume: Option<PathBuf>,
    /// Write a resume session to this file (currently unimplemented).
    #[arg(long)]
    write_resume: Option<PathBuf>,
    /// Urls to start scraping from.
    ///
    /// The domains of all urls will be treated as primary domains unless
    /// explicitly disabled via "--disable_primary".
    start_urls: Vec<Url>,
}

const DEFAULT_SECONDARY_DEPTH: usize = 5;
const DEFAULT_DELAY: u64 = 500;
const DEFAULT_JITTER: u64 = 1000;

#[derive(Debug)]
pub struct ResolvedArgs {
    pub primary_depth: DepthLimit,
    pub secondary_depth: DepthLimit,
    pub primary_domains: Vec<Url>,
    pub delay: Duration,
    pub jitter: Duration,
    pub save_config: bool,
    pub no_prompt: bool,
    pub bar: bool,
    pub write_logs: Option<File>,
    pub start_urls: Vec<LimitedUrl>,
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
            bar: self.bar,
            write_logs,
            start_urls,
        })
    }
}
