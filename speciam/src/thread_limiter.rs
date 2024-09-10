use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex, RwLock,
    },
    task::{Context, Poll, Waker},
};

use reqwest::Version;
use url::Url;

use crate::LimitedUrl;

#[derive(Debug)]
pub struct ThreadLimiter {
    http2_domains: RwLock<HashMap<Url, AtomicUsize>>,
    limit: usize,
    parallelism: AtomicUsize,
    pending: Mutex<HashMap<Url, Vec<Waker>>>,
}

/// Waits until parallelism is available for this domain.
#[derive(Debug)]
pub struct MarkFut<'a> {
    http2_domains: Option<&'a RwLock<HashMap<Url, AtomicUsize>>>,
    pending: &'a Mutex<HashMap<Url, Vec<Waker>>>,
    url: Url,
    limit: usize,
    parallelism: &'a AtomicUsize,
    pos: Option<usize>,
}

impl Future for MarkFut<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(http2_domains) = this.http2_domains {
            if let Some(in_flight) = http2_domains.read().unwrap().get(&this.url) {
                if in_flight.load(Ordering::Acquire) > 0 {
                    in_flight.fetch_add(1, Ordering::Release);
                    return Poll::Ready(());
                }
            }
        }

        if this.parallelism.load(Ordering::Acquire) < this.limit {
            if let Some(http2_domains) = this.http2_domains {
                let domains_handle = http2_domains.read().unwrap();
                if let Some(counter) = domains_handle.get(&this.url) {
                    // Add to parallelism if reviving an empty counter
                    if counter.fetch_add(1, Ordering::AcqRel) == 0 {
                        this.parallelism.fetch_add(1, Ordering::Release);
                    }
                } else {
                    drop(domains_handle);
                    // Re-check in case of update after dropping read handle.
                    let mut domain_handle = http2_domains.write().unwrap();
                    if let Some(counter) = domain_handle.get(&this.url) {
                        // Add to parallelism if reviving an empty counter
                        if counter.fetch_add(1, Ordering::AcqRel) == 0 {
                            this.parallelism.fetch_add(1, Ordering::Release);
                        }
                    } else {
                        // Add in new domain entry and add to parallelism
                        domain_handle.insert(this.url.clone(), AtomicUsize::new(1));
                        this.parallelism.fetch_add(1, Ordering::Release);
                    }
                }
            } else {
                // HTTP/1 connections always count towards parallelism.
                this.parallelism.fetch_add(1, Ordering::Release);
            };

            Poll::Ready(())
        } else {
            // Update queue entries
            let mut pending = this.pending.lock().unwrap();
            match (this.pos, pending.get_mut(&this.url)) {
                (Some(x), Some(queue)) if queue.len() > x => {
                    Waker::clone_from(&mut queue[x], cx.waker())
                }
                (Some(_), Some(queue)) | (None, Some(queue)) => {
                    this.pos = Some(queue.len());
                    queue.push(cx.waker().clone());
                }
                (Some(_), None) | (None, None) => {
                    pending.insert(this.url.clone(), vec![cx.waker().clone()]);
                    this.pos = Some(0);
                }
            }

            Poll::Pending
        }
    }
}

impl ThreadLimiter {
    /// Create [`self`] with some parallelism limit.
    ///
    /// Setting a `limit` of zero means [`Self::mark`] never resolves.
    pub fn new(limit: usize) -> Self {
        Self {
            http2_domains: RwLock::default(),
            limit,
            parallelism: AtomicUsize::new(0),
            pending: Mutex::default(),
        }
    }

    /// Create a future that resolves when there is enough parallelism for this
    /// url.
    pub fn mark(&self, url: &LimitedUrl, version: Version) -> MarkFut<'_> {
        let http2_domains = if version > Version::HTTP_11 {
            Some(&self.http2_domains)
        } else {
            None
        };
        MarkFut {
            http2_domains,
            pending: &self.pending,
            url: url.url_base(),
            limit: self.limit,
            parallelism: &self.parallelism,
            pos: None,
        }
    }

    /// Decrement parallelism count for `url`, if appropriate.
    pub fn unmark(&self, url: &LimitedUrl) {
        let base_url = url.url_base();

        let free_mark = || {
            // Always increments on add and decrements on drop
            self.parallelism.fetch_sub(1, Ordering::Release);

            // Wake a pending domain, if any
            let mut pending = self.pending.lock().unwrap();
            let key = pending.keys().next().cloned();
            if let Some(key) = key {
                let queue = pending.remove(&key).unwrap();
                drop(pending);
                for waker in queue {
                    waker.wake()
                }
            }
        };

        if let Some(counter) = self.http2_domains.read().unwrap().get(&base_url) {
            let current_count = counter.fetch_sub(1, Ordering::AcqRel);
            if current_count == 1 {
                free_mark()
            }
        } else {
            // Must be http/1 (or http/3, but the library doesn't support that)
            free_mark()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures::poll;

    use crate::test::{GOOGLE_HOMEPAGE, RUST_HOMEPAGE};

    use super::*;

    #[tokio::test]
    async fn blanket_reject() {
        let url = LimitedUrl::origin(Url::from_str(GOOGLE_HOMEPAGE).unwrap()).unwrap();
        let limiter = ThreadLimiter::new(0);

        // Should register for pending
        let mut fut = limiter.mark(&url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut), Poll::Pending);
        {
            let pending_guard = fut.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, vec![&url.url_base()]);
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].len(), 1);
        }

        // Should register a second item for pending in the same queue
        let mut fut2 = limiter.mark(&url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut2), Poll::Pending);
        {
            let pending_guard = fut2.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, vec![&url.url_base()]);
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].len(), 2);
        }

        let second_url = LimitedUrl::origin(Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        let url_base = url.url_base();
        let second_url_base = second_url.url_base();
        let mut two_url_keys = vec![&url_base, &second_url_base];
        two_url_keys.sort();

        // Should register a second item for pending in a new queue
        let mut fut3 = limiter.mark(&second_url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut3), Poll::Pending);
        {
            let pending_guard = fut3.pending.lock().unwrap();
            let (mut keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            keys.sort();
            let mut pending: Vec<_> = pending.into_iter().map(|x| x.len()).collect();
            pending.sort();
            assert_eq!(keys, two_url_keys);
            assert_eq!(pending.len(), 2);
            assert_eq!(pending[0], 1);
            assert_eq!(pending[1], 2);
        }

        // Should re-use initial registration
        assert_eq!(poll!(&mut fut), Poll::Pending);
        {
            let pending_guard = fut.pending.lock().unwrap();
            let (mut keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            keys.sort();
            let mut pending: Vec<_> = pending.into_iter().map(|x| x.len()).collect();
            pending.sort();
            assert_eq!(keys, two_url_keys);
            assert_eq!(pending.len(), 2);
            assert_eq!(pending[0], 1);
            assert_eq!(pending[1], 2);
        }
    }

    #[tokio::test]
    async fn pass_one_domain() {
        let url = LimitedUrl::origin(Url::from_str(GOOGLE_HOMEPAGE).unwrap()).unwrap();
        let limiter = ThreadLimiter::new(1);

        // Should register as complete
        let mut fut = limiter.mark(&url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut), Poll::Ready(()));
        {
            let pending_guard = fut.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, Vec::<&Url>::new());
            assert_eq!(pending.len(), 0);
        }

        // Should immediately pass as well, same domain
        let mut fut2 = limiter.mark(&url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut2), Poll::Ready(()));
        {
            let pending_guard = fut2.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, Vec::<&Url>::new());
            assert_eq!(pending.len(), 0);
        }

        // Should register for pending in a new queue
        let second_url = LimitedUrl::origin(Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        let mut fut3 = limiter.mark(&second_url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut3), Poll::Pending);
        {
            let pending_guard = fut3.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, vec![&second_url.url_base()]);
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].len(), 1);
        }
    }

    #[tokio::test]
    async fn pass_one_http1() {
        let url = LimitedUrl::origin(Url::from_str(GOOGLE_HOMEPAGE).unwrap()).unwrap();
        let limiter = ThreadLimiter::new(1);

        // Should register as complete
        let mut fut = limiter.mark(&url, Version::HTTP_11);
        assert_eq!(poll!(&mut fut), Poll::Ready(()));
        {
            let pending_guard = fut.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, Vec::<&Url>::new());
            assert_eq!(pending.len(), 0);
        }

        // Should register for pending with HTTP/1
        let mut fut2 = limiter.mark(&url, Version::HTTP_11);
        assert_eq!(poll!(&mut fut2), Poll::Pending);
        {
            let pending_guard = fut2.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, vec![&url.url_base()]);
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].len(), 1);
        }
    }

    #[tokio::test]
    async fn allow_free() {
        let url = LimitedUrl::origin(Url::from_str(GOOGLE_HOMEPAGE).unwrap()).unwrap();
        let limiter = ThreadLimiter::new(1);

        // Should register as complete
        let mut fut = limiter.mark(&url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut), Poll::Ready(()));
        {
            let pending_guard = fut.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, Vec::<&Url>::new());
            assert_eq!(pending.len(), 0);
        }

        // Should register for pending in a new queue
        let second_url = LimitedUrl::origin(Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        let mut fut3 = limiter.mark(&second_url, Version::HTTP_2);
        assert_eq!(poll!(&mut fut3), Poll::Pending);
        {
            let pending_guard = fut3.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert_eq!(keys, vec![&second_url.url_base()]);
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].len(), 1);
        }

        // Freeing first URL should empty the queue and allow completion
        limiter.unmark(&url);
        {
            let pending_guard = fut3.pending.lock().unwrap();
            let (keys, pending): (Vec<_>, Vec<_>) = pending_guard.iter().unzip();
            assert!(keys.is_empty());
            assert!(pending.is_empty());
        }
        assert_eq!(poll!(&mut fut3), Poll::Ready(()));
    }
}
