use std::{
    borrow::{Borrow, BorrowMut},
    cell::{Cell, LazyCell, RefCell, UnsafeCell},
    cmp,
    collections::HashMap,
    net::IpAddr,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock, RwLockReadGuard,
    },
    time::Duration,
};

use console::{Style, Term};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use once_map::OnceMap;
use reqwest::Version;
use speciam::LimitedUrl;
use tokio::{task::yield_now, time::sleep};

#[derive(Debug, Clone, Copy)]
pub enum VerOpt {
    HTTP1,
    HTTP2,
    Both,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct ProgBarVer {
    pub bar: ProgressBar,
    base: String,
    ip: Arc<RwLock<Option<String>>>,
    version: Arc<RwLock<VerOpt>>,
}

// Bypass UnsafeCell lint
unsafe impl Send for ProgBarVer {}
unsafe impl Sync for ProgBarVer {}

impl ProgBarVer {
    pub fn new(base: String, ip: Option<String>, term_width: u16, before_bar: u16) -> Self {
        let this = Self {
            bar: ProgressBar::with_draw_target(Some(0), ProgressDrawTarget::hidden()),
            base,
            ip: Arc::new(ip.into()),
            version: Arc::new(VerOpt::Unknown.into()),
        };

        this.update_style(term_width, before_bar);
        this
    }

    // Returns true if version updated
    pub fn update_ver(&self, new_version: Option<Version>) -> bool {
        // Only update the version on actual state change
        if let Some(new_version) = new_version {
            let update = match (
                new_version > Version::HTTP_11,
                *self.version.read().unwrap(),
            ) {
                (true, VerOpt::Unknown) => Some(VerOpt::HTTP2),
                (false, VerOpt::Unknown) => Some(VerOpt::HTTP1),
                (true, VerOpt::HTTP1) => Some(VerOpt::Both),
                (false, VerOpt::HTTP2) => Some(VerOpt::Both),
                _ => None,
            };
            if let Some(up) = update {
                *self.version.write().unwrap() = up;
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn update_style(&self, term_width: u16, before_bar: u16) -> &Self {
        let version_str = match *self.version.read().unwrap() {
            VerOpt::Unknown => "(??????)",
            VerOpt::HTTP1 => "(HTTP/1)",
            VerOpt::HTTP2 => "(HTTP/2)",
            VerOpt::Both => "(HTTP/*)",
        };

        let (heading, heading_len) = if let Some(ip) = &*self.ip.read().unwrap() {
            let no_special = "(".len() + ip.len() + ") ".len() + self.base.len();
            (
                "(".to_string()
                    + &Style::new().yellow().apply_to(ip).to_string()
                    + ") "
                    + &self.base,
                no_special as u16,
            )
        } else {
            (self.base.clone(), self.base.len() as u16)
        };

        self.bar.set_style(
            ProgressStyle::with_template(
                &(heading
                    + &(0..(before_bar - heading_len))
                        .map(|_| " ")
                        .collect::<String>()
                    + " "
                    + &Style::new().green().apply_to(version_str).to_string()
                    + ": [{bar:"
                    + &term_width
                        .saturating_sub(before_bar + version_str.len() as u16 + 20)
                        .to_string()
                    + ".cyan/blue}] {pos}/{len}"),
            )
            .unwrap()
            .progress_chars("#>-"),
        );

        self
    }
}

impl Deref for ProgBarVer {
    type Target = ProgressBar;
    fn deref(&self) -> &Self::Target {
        &self.bar
    }
}

impl DerefMut for ProgBarVer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bar
    }
}

#[derive(Debug, Clone)]
pub struct DlProgress {
    multi: MultiProgress,
    total: ProgressBar,
    #[cfg(feature = "resume")]
    transactions: Option<ProgressBar>,
    total_predicts: Arc<Mutex<HashMap<PathBuf, u64>>>,
    write: ProgressBar,
    write_progress: Arc<AtomicU64>,
    count: Arc<AtomicUsize>,
    per_domain: Arc<OnceMap<String, ProgBarVer>>,
    per_domain_inc: Arc<OnceMap<String, Box<Mutex<()>>>>,
    columns: u16,
    before_bar: Arc<RwLock<u16>>,
}

// To fit within 24/25 lines nicely.
const DOMAIN_LINE_LIMIT: usize = 20;

impl DlProgress {
    pub fn new() -> Self {
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(1));

        let total = ProgressBar::new(0);
        total.set_style(
            ProgressStyle::with_template(
                &(Style::new().bold().apply_to("Total Download: ").to_string()
                    + &Style::new()
                        .green()
                        .apply_to("[{elapsed_precise}]")
                        .to_string()
                    + " [{wide_bar:.red/8}] {pos}/{len} "
                    + &Style::new()
                        .magenta()
                        .apply_to("[{eta_precise}]")
                        .to_string()
                    + "     "),
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        let total = multi.add(total);

        let write = ProgressBar::new(0);
        write.set_style(
            ProgressStyle::with_template(
                &(Style::new()
                    .bold()
                    .apply_to("Disk Write:               ")
                    .to_string()
                    + " [{wide_bar:.red/8}] {binary_bytes}/{binary_total_bytes} "
                    + &Style::new()
                        .magenta()
                        .apply_to("[{binary_bytes_per_sec}]")
                        .to_string()
                    + "     "),
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        let write = multi.add(write);
        let write_progress = Arc::new(AtomicU64::new(0));

        let total_clone = total.clone();
        let write_clone = write.clone();
        let write_progress_clone = write_progress.clone();
        let _background_monitor = tokio::spawn(async move {
            loop {
                total_clone.tick(); // Make sure the total exec time progresses
                let new_progress = write_progress_clone.swap(0, Ordering::AcqRel);
                write_clone.inc(new_progress);
                sleep(Duration::from_secs(1)).await;
            }
        });

        let per_domain: Arc<OnceMap<String, ProgBarVer>> = Arc::default();
        let per_domain_inc: Arc<OnceMap<String, Box<Mutex<()>>>> = Arc::default();

        let (_, columns) = Term::stderr().size();

        Self {
            multi,
            total,
            #[cfg(feature = "resume")]
            transactions: None,
            total_predicts: Arc::default(),
            write,
            write_progress,
            count: AtomicUsize::new(0).into(),
            per_domain,
            per_domain_inc,
            columns,
            before_bar: Arc::new(0.into()),
        }
    }

    pub fn write_progress(&self) -> Arc<AtomicU64> {
        self.write_progress.clone()
    }

    #[inline]
    pub fn println<I: AsRef<str>>(&self, msg: I) {
        self.multi.println(msg).unwrap()
    }

    #[inline]
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.multi.suspend(f)
    }

    fn empty_target(bar: &ProgBarVer) -> bool {
        let bar = &bar;
        !bar.is_hidden() && (bar.length().unwrap_or(0) == bar.position())
    }

    pub fn register<U: Borrow<LimitedUrl>>(&self, url: U, ip: Option<IpAddr>) {
        self.total.inc_length(1);

        let url = url.borrow();
        let base = url.url_base();
        let ip = ip.map(|x| x.to_string());

        let mut updated_before_bar = None;
        let bar = self.per_domain.map_insert(
            base.to_string(),
            |base| {
                let len = {
                    let new_len = if let Some(ref ip) = ip {
                        (base.len() + " (".len() + ip.len() + ")".len()) as u16
                    } else {
                        base.len() as u16
                    };

                    let before_bar_handle = self.before_bar.read().unwrap();
                    if *before_bar_handle < new_len {
                        drop(before_bar_handle);

                        let mut before_bar_write = self.before_bar.write().unwrap();
                        if *before_bar_write < new_len {
                            *before_bar_write = new_len;
                            updated_before_bar = Some(new_len);
                        }

                        new_len
                    } else {
                        *before_bar_handle
                    }
                };

                let mut bar = ProgBarVer::new(base.clone(), ip, self.columns, len);
                bar.bar = self.multi.add(bar.bar);

                if self.count.fetch_add(1, Ordering::AcqRel) >= DOMAIN_LINE_LIMIT {
                    self.count.fetch_sub(1, Ordering::Release);
                    bar.bar.set_draw_target(ProgressDrawTarget::hidden());
                }

                bar
            },
            |_, bar| bar.clone(),
        );

        bar.inc_length(1);

        if let Some(new_len) = updated_before_bar {
            let before_bar = self.before_bar.read().unwrap();
            if new_len == *before_bar {
                let read_handle = self.per_domain.read_only_view();

                read_handle.values().for_each(|v| {
                    v.update_style(self.columns, new_len);
                });
                read_handle.values().for_each(|v| {
                    v.tick();
                });
            }
        }

        // Un-hide a hidden bar, if there's space for it.
        if bar.is_hidden() {
            let read_handle = self.per_domain.read_only_view();
            if read_handle.values().any(Self::empty_target) {
                if let Some(empty_bar) = read_handle.values().find(|b| Self::empty_target(b)) {
                    // Hide the empty other bar
                    empty_bar.set_draw_target(ProgressDrawTarget::hidden());

                    // Make bar visible again
                    self.multi.add(bar.bar.clone());
                    bar.tick()
                }
            }
        }
    }

    pub async fn free<U: Borrow<LimitedUrl>>(
        &self,
        url: U,
        ip: Option<IpAddr>,
        version: Option<Version>,
    ) {
        self.total.inc(1);
        let base = url.borrow().url_base();
        let ip = ip.map(|x| x.to_string());

        loop {
            if let Some(bar) = self
                .per_domain
                .map_get(&base.to_string(), |_, bar| bar.clone())
            {
                // Exclusive access to avoid going over length
                let handle = self
                    .per_domain_inc
                    .insert(base.to_string(), |_| Box::new(Mutex::new(())))
                    .lock();

                // Yield if capacity hasn't been allocated for this free yet
                if bar.length().unwrap_or(0) > bar.position() {
                    bar.inc(1);
                    drop(handle);

                    if bar.update_ver(version) {
                        bar.update_style(self.columns, *self.before_bar.read().unwrap());
                    }

                    // Un-hide the bar, if possible
                    if bar.is_hidden() {
                        if self.count.fetch_add(1, Ordering::AcqRel) < DOMAIN_LINE_LIMIT {
                            // Make bar visible again
                            self.multi.add(bar.bar.clone());
                            bar.tick()
                        } else {
                            self.count.fetch_sub(1, Ordering::Release);
                            if self
                                .per_domain
                                .read_only_view()
                                .values()
                                .any(Self::empty_target)
                            {
                                if let Some(empty_bar) = self
                                    .per_domain
                                    .read_only_view()
                                    .values()
                                    .find(|b| Self::empty_target(b))
                                {
                                    // Hide the empty other bar
                                    empty_bar.set_draw_target(ProgressDrawTarget::hidden());

                                    // Make bar visible again
                                    self.multi.add(bar.bar.clone());
                                    bar.tick()
                                }
                            }
                        }
                    }

                    // Update IP address
                    if let Some(ip) = ip {
                        if (*bar.ip.read().unwrap()).as_ref() != Some(&ip) {
                            *bar.ip.write().unwrap() = Some(ip.clone());

                            let before_bar = self.before_bar.read().unwrap();
                            let new_len = (base.len() + " (".len() + ip.len() + ")".len()) as u16;
                            if new_len > *before_bar {
                                drop(before_bar);

                                let mut before_bar = self.before_bar.write().unwrap();
                                if new_len > *before_bar {
                                    *before_bar = new_len;
                                    let read_handle = self.per_domain.read_only_view();

                                    read_handle.values().for_each(|v| {
                                        v.update_style(self.columns, new_len);
                                    });
                                    read_handle.values().for_each(|v| {
                                        v.tick();
                                    });
                                } else {
                                    bar.update_style(self.columns, *before_bar);
                                }
                            } else {
                                bar.update_style(self.columns, *before_bar);
                            }
                        }
                    }
                } else {
                    drop(handle);
                    // Give the initializer a chance to run
                    yield_now().await;
                }
                break;
            } else {
                // Give the initializer a chance to run
                yield_now().await;
            }
        }
    }

    pub fn register_write(&self, path: PathBuf, predict: Option<u64>) {
        let predict = predict.unwrap_or(0);

        self.total_predicts.lock().unwrap().insert(path, predict);
        self.write.inc_length(predict);
    }

    pub fn free_write(&self, path: PathBuf, actual_size: u64) {
        if let Some(prediction) = self.total_predicts.lock().unwrap().remove(&path) {
            // Need to fix the write bar length
            match actual_size.cmp(&prediction) {
                cmp::Ordering::Less => {
                    self.write
                        .set_length(self.write.length().unwrap_or(0) - (prediction - actual_size));
                }
                cmp::Ordering::Greater => {
                    self.write.inc_length(actual_size - prediction);
                }
                cmp::Ordering::Equal => (),
            }
        }
    }

    #[cfg(feature = "resume")]
    pub fn init_transactions(&mut self, cur_count: u64) {
        let transactions = ProgressBar::new(cur_count);
        transactions.set_style(
            ProgressStyle::with_template(
                &(Style::new()
                    .bold()
                    .apply_to("SQLite trailing transactions: ")
                    .to_string()
                    + &Style::new()
                        .green()
                        .apply_to("[{elapsed_precise}]")
                        .to_string()
                    + " [{wide_bar:.red/8}] {pos}/{len} "
                    + &Style::new()
                        .magenta()
                        .apply_to("[{eta_precise}]")
                        .to_string()
                    + "     "),
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        self.multi.insert(0, transactions.clone());
        self.transactions = Some(transactions);

        // Clear out all this tracking information
        self.multi.remove(&self.total);
        self.multi.remove(&self.write);
        self.per_domain
            .read_only_view()
            .values()
            .for_each(|v| self.multi.remove(&v.bar));
    }

    #[cfg(feature = "resume")]
    pub fn update_transactions(&self, cur_count: u64) {
        if let Some(transactions) = &self.transactions {
            transactions.set_position(transactions.length().unwrap_or(0) - cur_count);
        }
    }
}
