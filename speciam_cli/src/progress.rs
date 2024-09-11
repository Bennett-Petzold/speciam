use std::{
    borrow::Borrow,
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use speciam::LimitedUrl;
#[derive(Debug, Clone)]
pub struct DlProgress {
    multi: MultiProgress,
    total: ProgressBar,
    count: Arc<AtomicUsize>,
    per_domain: Arc<RwLock<HashMap<String, ProgressBar>>>,
}

// To fit within 24/25 lines nicely.
const DOMAIN_LINE_LIMIT: usize = 20;

impl DlProgress {
    pub fn new() -> Self {
        let multi = MultiProgress::new();

        let total = ProgressBar::new(0);
        total.set_style(
            ProgressStyle::with_template(
                "Total: [{elapsed_precise}] [{wide_bar:.red/8}] {pos}/{len} {eta_precise}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        let total = multi.add(total);

        // TODO remove
        //multi.set_draw_target(ProgressDrawTarget::hidden());

        Self {
            multi,
            total,
            count: AtomicUsize::new(0).into(),
            per_domain: Arc::default(),
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

    fn empty_target(bar: &ProgressBar) -> bool {
        !bar.is_hidden() && (bar.length().unwrap_or(0) == bar.position())
    }

    pub fn register<U: Borrow<LimitedUrl>>(&self, url: U) {
        self.total.inc_length(1);

        let base = url.borrow().url_base();
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
                    *bar = self.multi.add(bar.clone());
                }
            }
        } else {
            drop(read_handle);

            // Create new bar entry
            let bar = ProgressBar::with_draw_target(Some(1), ProgressDrawTarget::hidden());
            bar.set_style(
                ProgressStyle::with_template(
                    &(base.to_string()
                        + ": [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta_precise})"),
                )
                .unwrap()
                .progress_chars("#>-"),
            );

            let mut write_handle = self.per_domain.write().unwrap();
            if let Some(bar) = write_handle.get(base) {
                bar.inc_length(1);
            } else {
                if self.count.load(Ordering::Acquire) > DOMAIN_LINE_LIMIT {
                    if let Some(empty_bar) = write_handle.values().find(|b| Self::empty_target(b)) {
                        // Hide the empty other bar
                        empty_bar.set_draw_target(ProgressDrawTarget::hidden());
                    } else {
                        // Hide the top bar to make space
                        let top_bar = write_handle.values().find(|x| !x.is_hidden()).unwrap();
                        top_bar.set_draw_target(ProgressDrawTarget::hidden());
                    }
                } else {
                    self.count.fetch_add(1, Ordering::Release);
                }

                let bar = self.multi.add(bar);
                bar.tick();
                write_handle.insert(base.to_string(), bar);
            }
        };
    }

    pub fn free<U: Borrow<LimitedUrl>>(&self, url: U) {
        self.total.inc(1);

        let base = url.borrow().url_base();
        let read_handle = self.per_domain.read().unwrap();
        let bar = read_handle.get(base).unwrap();
        bar.inc(1);

        // Un-hide a bar, if possible
        if bar.is_hidden()
            && (self.count.load(Ordering::Acquire) < DOMAIN_LINE_LIMIT
                || read_handle
                    .values()
                    .any(|x| x.length().unwrap_or(0) == x.position()))
        {
            drop(read_handle);

            let mut write_handle = self.per_domain.write().unwrap();
            if let Some(empty_bar) = write_handle.values().find(|b| Self::empty_target(b)) {
                // Hide the empty other bar
                empty_bar.set_draw_target(ProgressDrawTarget::hidden());

                // Make bar visible again
                let bar = write_handle.get_mut(base).unwrap();
                *bar = self.multi.add(bar.clone());
            }
        }
    }
}
