use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt::Debug,
    str::FromStr,
    sync::{Arc, Mutex},
};

use async_sqlite::rusqlite::{self, ErrorCode, Params};
use async_sqlite::Pool;
use bare_err_tree::err_tree;
use reqwest::Url;
use speciam::{DepthLimit, LimitedUrl, UniqueLimitedUrl, VisitCache};
use texting_robots::Robot;
use thiserror::Error;
use tokio::{
    spawn,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::{spawn_blocking, JoinHandle},
    try_join,
};

struct FusedJoinHandle<E> {
    output: Arc<Mutex<Option<Result<(), E>>>>,
}

impl<E> FusedJoinHandle<E> {
    /// Returns [`None`], or [`Some`] with an exit error.
    ///
    /// Will only return [`Some`] once.
    pub fn check(&self) -> Option<Result<(), E>> {
        self.output.lock().unwrap().take()
    }
}

impl<E: Send + 'static> From<JoinHandle<Result<(), E>>> for FusedJoinHandle<E> {
    fn from(value: JoinHandle<Result<(), E>>) -> Self {
        let output = Arc::new(Mutex::new(None));
        let output_clone = output.clone();
        spawn(async move {
            let res = value.await.unwrap();
            *output_clone.lock().unwrap() = Some(res);
        });
        Self { output }
    }
}

struct UpdateHandle<T> {
    tx: UnboundedSender<T>,
    handle: FusedJoinHandle<async_sqlite::Error>,
}

fn sqlite_retry<F, T>(mut func: F) -> Result<T, rusqlite::Error>
where
    F: FnMut() -> Result<T, rusqlite::Error>,
{
    loop {
        match (func)() {
            Ok(x) => {
                return Ok(x);
            }
            Err(rusqlite::Error::SqliteFailure(e, desc)) => match e.code {
                ErrorCode::DatabaseBusy => (),
                ErrorCode::DatabaseLocked => (),
                _ => return Err(rusqlite::Error::SqliteFailure(e, desc)),
            },
            Err(e) => {
                return Err(e);
            }
        }
    }
}

impl<T> UpdateHandle<T> {
    pub fn new(pool: Pool, prepared_stmt: &'static str) -> UpdateHandle<T>
    where
        T: Params + Send + Debug + Clone + 'static,
    {
        let (tx, mut rx) = unbounded_channel::<T>();
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            // Using recv_many allows for batching writes when they're queued
            // up between processing/yields.
            let mut buffer = Vec::new();
            while rx.recv_many(&mut buffer, usize::MAX).await > 0 {
                // Clears up these lines from the buffer
                let lines = std::mem::take(&mut buffer);
                pool_clone
                    .conn(|conn| {
                        let mut stmt = sqlite_retry(|| conn.prepare_cached(prepared_stmt))?;
                        for arg in lines {
                            sqlite_retry(|| stmt.execute(arg.clone()))?;
                        }
                        Ok(())
                    })
                    .await?;
            }
            Ok(())
        });

        UpdateHandle {
            tx,
            handle: handle.into(),
        }
    }

    /// Send a new log line.
    ///
    /// Returns the error code, if one occured. A non-error return only
    /// occurs when [`self`] itself drops.
    pub fn send(&self, val: T) -> Result<&Self, async_sqlite::Error> {
        if let Some(end) = self.handle.check() {
            end?
        }

        self.tx.send(val).unwrap();
        Ok(self)
    }
}

/// All methods returns the error code, if one occured. For logging
/// methods, a non-error return only occurs when [`self`] itself drops.
pub struct SqliteLogging {
    pool: Pool,
    robots: UpdateHandle<(String, String)>,
    visited_depths: UpdateHandle<(String, isize)>,
    visited: UpdateHandle<(String, String)>,
    domains: UpdateHandle<(String, isize)>,
    domains_nolimit: UpdateHandle<[String; 1]>,
    push_pending: UpdateHandle<[String; 1]>,
    drop_pending: UpdateHandle<[String; 1]>,
}

const CREATE_ROBOTS: &str = "
CREATE TABLE IF NOT EXISTS robots(
  url TEXT PRIMARY KEY,
  body TEXT NOT NULL
) WITHOUT ROWID, STRICT;";

const CREATE_VISITED_DEPTHS: &str = "
CREATE TABLE IF NOT EXISTS visited_depths(
  base_url TEXT PRIMARY KEY,
  depth INTEGER NOT NULL
) WITHOUT ROWID, STRICT;";

const CREATE_VISITED: &str = "
CREATE TABLE IF NOT EXISTS visited(
  base_url TEXT NOT NULL,
  url TEXT NOT NULL,
  PRIMARY KEY (base_url, url)
) WITHOUT ROWID, STRICT;";

/// unlimited represents a boolean with an integer
///
/// limit is NULL for None
const CREATE_DOMAINS: &str = "
CREATE TABLE IF NOT EXISTS domains(
  url TEXT PRIMARY KEY,
  depth INTEGER
) WITHOUT ROWID, STRICT;";

const CREATE_PENDING: &str = "
CREATE TABLE IF NOT EXISTS pending(
  url TEXT PRIMARY KEY
) WITHOUT ROWID, STRICT;";

const UPDATE_ROBOTS: &str = "INSERT OR IGNORE INTO robots(url, body) VALUES (?1, ?2);";
const UPDATE_VISITED_DEPTHS: &str =
    "INSERT OR REPLACE INTO visited_depths(base_url, depth) VALUES (?1, ?2);";
const UPDATE_VISITED: &str = "INSERT OR REPLACE INTO visited(base_url, url) VALUES (?1, ?2);";
const UPDATE_DOMAINS: &str = "INSERT OR IGNORE INTO domains(url, depth) VALUES (?1, ?2)";
const UPDATE_DOMAINS_NOLIMIT: &str = "INSERT OR IGNORE INTO domains(url) VALUES (?1)";
const PUSH_PENDING: &str = "INSERT OR IGNORE INTO pending(url) VALUES (?1)";
const DROP_PENDING_URL: &str = "DELETE FROM pending WHERE url == (?1);";

const DROP_STATE: &str = "
DELETE FROM robots;
DELETE FROM visited_depths;
DELETE FROM visited;
DELETE FROM pending;
";

impl TryFrom<Pool> for SqliteLogging {
    type Error = async_sqlite::Error;
    fn try_from(pool: Pool) -> Result<Self, Self::Error> {
        pool.conn_blocking(|conn| sqlite_retry(|| conn.execute(CREATE_ROBOTS, [])))?;
        pool.conn_blocking(|conn| sqlite_retry(|| conn.execute(CREATE_VISITED_DEPTHS, [])))?;
        pool.conn_blocking(|conn| sqlite_retry(|| conn.execute(CREATE_VISITED, [])))?;
        pool.conn_blocking(|conn| sqlite_retry(|| conn.execute(CREATE_DOMAINS, [])))?;
        pool.conn_blocking(|conn| sqlite_retry(|| conn.execute(CREATE_PENDING, [])))?;

        let robots = UpdateHandle::new(pool.clone(), UPDATE_ROBOTS);
        let visited_depths = UpdateHandle::new(pool.clone(), UPDATE_VISITED_DEPTHS);
        let visited = UpdateHandle::new(pool.clone(), UPDATE_VISITED);
        let domains = UpdateHandle::new(pool.clone(), UPDATE_DOMAINS);
        let domains_nolimit = UpdateHandle::new(pool.clone(), UPDATE_DOMAINS_NOLIMIT);
        let push_pending = UpdateHandle::new(pool.clone(), PUSH_PENDING);
        let drop_pending = UpdateHandle::new(pool.clone(), DROP_PENDING_URL);

        Ok(Self {
            pool,
            robots,
            visited,
            visited_depths,
            domains,
            domains_nolimit,
            push_pending,
            drop_pending,
        })
    }
}

// Mid-execution log adjustments
impl SqliteLogging {
    /// Log a new `robots.txt` entry.
    pub fn log_robots<U: Borrow<str>, S: Borrow<str>>(
        &self,
        url: U,
        body: S,
    ) -> Result<&Self, async_sqlite::Error> {
        self.robots
            .send((url.borrow().to_string(), body.borrow().to_string()))
            .map(|_| self)
    }

    /// Log a new url visited entry, or update the existing one.
    pub fn log_visited<U: Borrow<LimitedUrl>, L: IntoIterator<Item = Url>>(
        &self,
        parent: U,
        children: L,
    ) -> Result<&Self, async_sqlite::Error> {
        let parent = parent.borrow();
        let parent_url = parent.url().to_string();

        self.visited_depths.send((
            parent_url.clone(),
            // Casts usize as isize for storage.
            // Naive usize transformation may lose the top bit.
            parent.depth() as isize,
        ))?;

        for child in children.into_iter() {
            self.visited.send((parent_url.clone(), child.to_string()))?;
        }

        Ok(self)
    }

    /// Log a new domain depth limit.
    pub fn log_domain<U: Borrow<str>>(
        &self,
        url: U,
        depth: Option<usize>,
    ) -> Result<&Self, async_sqlite::Error> {
        if let Some(depth) = depth {
            self.domains.send((
                url.borrow().to_string(),
                // Casts usize as isize for storage.
                // Naive usize transformation may lose the top bit.
                depth as isize,
            ))?;
        } else {
            self.domains_nolimit.send([url.borrow().to_string()])?;
        };

        Ok(self)
    }

    /// Log a pending URL.
    pub fn push_pending<U: Borrow<Url>>(&self, pending: U) -> Result<&Self, async_sqlite::Error> {
        self.push_pending
            .send([pending.borrow().to_string()])
            .map(|_| self)
    }

    /// Remove a URL from the pending list.
    pub fn drop_pending<U: Borrow<Url>>(&self, pending: U) -> Result<&Self, async_sqlite::Error> {
        self.drop_pending
            .send([pending.borrow().to_string()])
            .map(|_| self)
    }

    /// Drop all saved state.
    pub async fn drop_state(&self) -> Result<&Self, async_sqlite::Error> {
        self.pool
            .conn(|conn| conn.execute(DROP_STATE, []))
            .await
            .map(|_| self)
    }
}

#[derive(Debug)]
#[err_tree]
#[derive(Error)]
#[error("Database holds an invalid URL: {url:?}")]
pub struct UrlParseErr {
    url: String,
    #[source]
    err: url::ParseError,
}

impl UrlParseErr {
    #[track_caller]
    pub fn new(url: String, err: url::ParseError) -> Self {
        Self::_tree(url, err)
    }
}

#[derive(Debug, Error)]
#[err_tree(GenRecoveryErrWrap)]
pub enum GenRecoveryErr {
    #[dyn_err]
    #[error("SELECT statement failed")]
    SelectErr(#[from] async_sqlite::Error),
    #[tree_err]
    #[error(transparent)]
    InvalidUrl(#[from] UrlParseErr),
}

#[derive(Debug, Error)]
#[err_tree(LimitRecoveryErrWrap)]
pub enum LimitRecoveryErr {
    #[tree_err]
    #[error(transparent)]
    Gen(#[from] GenRecoveryErrWrap),
    #[tree_err]
    #[error(transparent)]
    NonBase(#[from] speciam::CannotBeABase),
}

// Recovering existing logs
impl SqliteLogging {
    /// Return any parsed `robots.txt` from a previous run.
    pub async fn restore_robots(
        &self,
        user_agent: &str,
    ) -> Result<HashMap<String, Option<Robot>>, GenRecoveryErrWrap> {
        let lines: Vec<(String, String)> = self
            .pool
            .conn(|conn| {
                conn.prepare("SELECT url, body FROM robots;")?
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect()
            })
            .await
            .map_err(GenRecoveryErr::from)?;
        Ok(lines
            .into_iter()
            .map(|(url, body)| (url, Robot::new(user_agent, body.as_bytes()).ok()))
            .collect())
    }

    /// Return any visited mappings from a previous run.
    pub async fn restore_visited(&self) -> Result<VisitCache, LimitRecoveryErrWrap> {
        // To avoid duplication, visited is split into two tables.

        let base_urls_fut = self.pool.conn(|conn| {
            conn.prepare("SELECT base_url, depth FROM visited_depths")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect()
        });

        let lines_fut = self.pool.conn(|conn| {
            // Sorting for consecutive base_url runs is critical for later logic
            conn.prepare("SELECT base_url, url FROM visited ORDER BY base_url")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect()
        });

        let (base_url_map, lines): (HashMap<String, isize>, Vec<(String, String)>) =
            try_join!(base_urls_fut, lines_fut).map_err(|x| {
                LimitRecoveryErr::from(GenRecoveryErrWrap::from(GenRecoveryErr::from(x)))
            })?;

        spawn_blocking(move || {
            let mut end_mapping = HashMap::new();

            let get_base_url = |url: String| {
                base_url_map.get(&url).map(|depth| {
                    LimitedUrl::at_depth(
                        Url::from_str(&url).map_err(|x| {
                            GenRecoveryErrWrap::from(GenRecoveryErr::from(UrlParseErr::new(
                                url.clone(),
                                x,
                            )))
                        })?,
                        // Reverse the isize storage transform
                        *depth as usize,
                    )
                    .map_err(LimitRecoveryErr::from)
                    .map(|transformed| (url, UniqueLimitedUrl::from(transformed)))
                })
            };

            // Mutable accumulation state
            let mut cur_url = None;
            let mut cur_url_str = "".to_string();
            let mut buffer = Vec::new();

            for (base_url_str, url) in lines {
                // The previous SQL query must have returned sorted data for
                // this to work.
                if base_url_str == cur_url_str {
                    buffer.push(Url::from_str(&url).map_err(|x| {
                        LimitRecoveryErr::from(GenRecoveryErrWrap::from(GenRecoveryErr::from(
                            UrlParseErr::new(url, x),
                        )))
                    })?);
                } else {
                    if !buffer.is_empty() {
                        if let Some(cur_url) = cur_url.take() {
                            end_mapping.insert(cur_url, std::mem::take(&mut buffer));
                        }
                    }

                    if let Some(res) = get_base_url(base_url_str) {
                        let (orig, transform) = res?;
                        cur_url = Some(transform);
                        cur_url_str = orig;
                    }
                }
            }

            Ok(end_mapping.into())
        })
        .await
        .unwrap()
    }

    /// Return any domain rules from a previous run.
    pub async fn restore_domains(&self) -> Result<Vec<(Url, DepthLimit)>, GenRecoveryErrWrap> {
        let lines: Vec<(String, Option<isize>)> = self
            .pool
            .conn(|conn| {
                conn.prepare("SELECT url, depth FROM domains;")?
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect()
            })
            .await
            .map_err(GenRecoveryErr::from)?;
        lines
            .into_iter()
            .map(|(url, depth)| {
                Ok((
                    Url::from_str(&url)
                        .map_err(|x| GenRecoveryErr::from(UrlParseErr::new(url, x)))?,
                    // Reverse the usize -> isize transform
                    depth.map(|x| x as usize).into(),
                ))
            })
            .collect()
    }

    /// Return the pending url queue from a previous run.
    pub async fn restore_pending(&self) -> Result<Vec<LimitedUrl>, LimitRecoveryErrWrap> {
        // Pending uses visited to avoid stored data duplication, and we
        // take the chance to dedup stored pending requests.
        const PENDING_RESTORE: &str = "
        SELECT DISTINCT url, depth
            FROM pending
            INNER JOIN visited_depths
                ON pending.url = visited_depths.base_url
            ORDER BY depth ASC;
        ";
        let lines: Vec<(String, isize)> = self
            .pool
            .conn(|conn| {
                conn.prepare(PENDING_RESTORE)?
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect()
            })
            .await
            .map_err(|x| {
                LimitRecoveryErr::from(GenRecoveryErrWrap::from(GenRecoveryErr::from(x)))
            })?;
        lines
            .into_iter()
            .map(|(url, depth)| {
                LimitedUrl::at_depth(
                    Url::from_str(&url).map_err(|x| {
                        LimitRecoveryErr::from(GenRecoveryErrWrap::from(GenRecoveryErr::from(
                            UrlParseErr::new(url, x),
                        )))
                    })?,
                    // Reverse the usize -> isize transform
                    depth as usize,
                )
                .map_err(LimitRecoveryErr::from)
                .map_err(LimitRecoveryErrWrap::from)
            })
            .collect()
    }
}
