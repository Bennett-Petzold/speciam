use std::{
    borrow::Borrow,
    cmp,
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
};

use console::{Style, Term};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use reqwest::Version;
use speciam::LimitedUrl;

#[derive(Debug, Clone, Copy)]
pub enum VerOpt {
    HTTP1,
    HTTP2,
    Both,
    Unknown,
}

#[derive(Debug)]
pub struct ProgBarVer {
    pub bar: ProgressBar,
    base: String,
    version: RwLock<VerOpt>,
}

impl Clone for ProgBarVer {
    fn clone(&self) -> Self {
        Self {
            bar: self.bar.clone(),
            base: self.base.clone(),
            version: RwLock::new(*self.version.read().unwrap()),
        }
    }
}

impl ProgBarVer {
    pub fn new(base: String, term_width: u16, before_bar: u16) -> Self {
        let this = Self {
            bar: ProgressBar::with_draw_target(Some(1), ProgressDrawTarget::hidden()),
            base,
            version: VerOpt::Unknown.into(),
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

        self.bar.set_style(
            ProgressStyle::with_template(
                &(self.base.to_string()
                    + &(0..(before_bar - self.base.len() as u16))
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

#[derive(Debug)]
pub struct DlProgress {
    multi: MultiProgress,
    total: ProgressBar,
    total_predicts: Arc<Mutex<HashMap<PathBuf, u64>>>,
    write: ProgressBar,
    count: Arc<AtomicUsize>,
    per_domain: Arc<RwLock<HashMap<String, ProgBarVer>>>,
    columns: u16,
    before_bar: RwLock<u16>,
}

impl Clone for DlProgress {
    fn clone(&self) -> Self {
        Self {
            multi: self.multi.clone(),
            total: self.total.clone(),
            total_predicts: Arc::default(),
            write: self.write.clone(),
            count: Arc::new(self.count.load(Ordering::Acquire).into()),
            per_domain: Arc::new(RwLock::new(self.per_domain.read().unwrap().clone())),
            columns: self.columns,
            before_bar: RwLock::new(*self.before_bar.read().unwrap()),
        }
    }
}

// To fit within 24/25 lines nicely.
const DOMAIN_LINE_LIMIT: usize = 20;

impl DlProgress {
    pub fn new() -> Self {
        let multi = MultiProgress::new();
        multi.set_move_cursor(true);

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
                &(Style::new().bold().apply_to("Disk Write:     ").to_string()
                    + &Style::new()
                        .green()
                        .apply_to("[{elapsed_precise}]")
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
        write.tick();

        let per_domain: Arc<RwLock<HashMap<String, ProgBarVer>>> = Arc::default();

        let (_, columns) = Term::stderr().size();

        Self {
            multi,
            total,
            total_predicts: Arc::default(),
            write,
            count: AtomicUsize::new(0).into(),
            per_domain,
            columns,
            before_bar: 0.into(),
        }
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

    pub fn register<U: Borrow<LimitedUrl>>(&self, url: U) {
        self.total.inc_length(1);

        let url = url.borrow();
        let base = url.url_base();
        let read_handle = self.per_domain.read().unwrap();

        if let Some(bar) = read_handle.get(base) {
            bar.inc_length(1);

            // Un-hide a hidden bar, if there's space for it.
            if bar.is_hidden() && read_handle.values().any(Self::empty_target) {
                drop(read_handle);

                let mut write_handle = self.per_domain.write().unwrap();
                if let Some(empty_bar) = write_handle.values().find(|b| Self::empty_target(b)) {
                    // Hide the empty other bar
                    empty_bar.set_draw_target(ProgressDrawTarget::hidden());

                    // Make bar visible again
                    let bar = write_handle.get_mut(base).unwrap();
                    bar.bar = self.multi.add(bar.bar.clone());
                    bar.tick()
                }
            }
        } else {
            // Update all lens for nice rendering, if necessary
            let len = {
                let before_bar_handle = self.before_bar.read().unwrap();
                if *before_bar_handle < base.len() as u16 {
                    drop(before_bar_handle);
                    let new_len = base.len() as u16;

                    *self.before_bar.write().unwrap() = new_len;
                    read_handle.values().for_each(|v| {
                        v.update_style(self.columns, new_len);
                    });
                    read_handle.values().for_each(|v| {
                        v.tick();
                    });

                    new_len
                } else {
                    *before_bar_handle
                }
            };

            drop(read_handle);

            // Create new bar entry

            let mut bar = ProgBarVer::new(base.to_string(), self.columns, len);

            let mut write_handle = self.per_domain.write().unwrap();
            if let Some(bar) = write_handle.get(base) {
                bar.inc_length(1);
            } else {
                if self.count.load(Ordering::Acquire) > DOMAIN_LINE_LIMIT {
                    if let Some(empty_bar) = write_handle.values().find(|b| Self::empty_target(b)) {
                        // Hide the empty other bar
                        empty_bar.set_draw_target(ProgressDrawTarget::hidden());
                    } else {
                        // Hide the smallest bar to make space
                        let mut unhidden_bars: Vec<_> =
                            write_handle.values().filter(|x| !x.is_hidden()).collect();
                        unhidden_bars.sort_unstable_by_key(|x| x.length());

                        unhidden_bars
                            .first()
                            .unwrap()
                            .set_draw_target(ProgressDrawTarget::hidden());
                    }
                } else {
                    self.count.fetch_add(1, Ordering::Release);
                }

                bar.bar = self.multi.add(bar.bar);
                bar.tick();
                write_handle.insert(base.to_string(), bar);
            }
        };
    }

    pub fn free<U: Borrow<LimitedUrl>>(&self, url: U, version: Option<Version>) {
        self.total.inc(1);

        let base = url.borrow().url_base();
        let read_handle = self.per_domain.read().unwrap();
        let bar = read_handle.get(base).unwrap();
        bar.inc(1);
        if bar.update_ver(version) {
            bar.update_style(self.columns, *self.before_bar.read().unwrap());
        }

        // Un-hide a bar, if possible
        if bar.is_hidden()
            && (self.count.load(Ordering::Acquire) < DOMAIN_LINE_LIMIT
                || read_handle.values().any(Self::empty_target))
        {
            drop(read_handle);

            let mut write_handle = self.per_domain.write().unwrap();
            if let Some(empty_bar) = write_handle.values().find(|b| Self::empty_target(b)) {
                // Hide the empty other bar
                empty_bar.set_draw_target(ProgressDrawTarget::hidden());

                // Make bar visible again
                let bar = write_handle.get_mut(base).unwrap();
                bar.bar = self.multi.add(bar.bar.clone());
                bar.tick()
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

            self.write.inc(actual_size);
        }
    }
}
