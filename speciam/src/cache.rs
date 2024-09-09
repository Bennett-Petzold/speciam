use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    ops::Deref,
    sync::RwLock,
};

use url::Url;

use crate::LimitedUrl;

#[derive(Debug, Clone, Eq)]
#[repr(transparent)]
/// [`LimitedUrl`] wrapped to match identical URLs on [`Hash`] and [`Eq`].
pub struct UniqueLimitedUrl(LimitedUrl);

impl From<LimitedUrl> for UniqueLimitedUrl {
    fn from(value: LimitedUrl) -> Self {
        Self(value)
    }
}

impl From<UniqueLimitedUrl> for LimitedUrl {
    fn from(value: UniqueLimitedUrl) -> Self {
        value.0
    }
}

impl Hash for UniqueLimitedUrl {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.url().hash(state)
    }
}

impl PartialEq for UniqueLimitedUrl {
    fn eq(&self, other: &Self) -> bool {
        self.0.url() == other.0.url()
    }
}

impl UniqueLimitedUrl {
    /// Test if this has a smaller depth than `rhs`.
    pub fn smaller_depth(&self, rhs: &Self) -> bool {
        self.0.depth() < rhs.0.depth()
    }
}

/// Stores a cache of visited [`LimitedUrl`] values and their scraped urls.
///
/// This is built on the general assumption that a site will have many copies
/// of the same link. In that case a cache avoids repeat downloads, parses, and
/// traversals of the same path.
#[derive(Debug, Default)]
pub struct VisitCache(RwLock<HashMap<UniqueLimitedUrl, Vec<Url>>>);

impl From<HashMap<UniqueLimitedUrl, Vec<Url>>> for VisitCache {
    fn from(value: HashMap<UniqueLimitedUrl, Vec<Url>>) -> Self {
        Self(RwLock::new(value))
    }
}

impl FromIterator<(UniqueLimitedUrl, Vec<Url>)> for VisitCache {
    fn from_iter<T: IntoIterator<Item = (UniqueLimitedUrl, Vec<Url>)>>(iter: T) -> Self {
        HashMap::from_iter(iter).into()
    }
}

/// This provides the cached urls when a re-run is appropriate.
///
/// The cached urls will be updated for the new depth.
pub enum VisitCacheRes {
    /// This url is not in the cache.
    Unique,
    /// This url is in the cache, but at a higher depth.
    SmallerThanCached(Vec<LimitedUrl>),
    /// This url is in the cache and not at a higher depth.
    CachedNoRepeat,
}

impl VisitCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check the status of `url` against the cache.
    pub fn probe(&self, url: LimitedUrl) -> VisitCacheRes {
        let url = url.into();
        let cache_handle = self.0.read().unwrap();
        if let Some((stored_url, cache_entry)) = cache_handle.get_key_value(&url) {
            if url.smaller_depth(stored_url) {
                // Clone and drop; avoids holding lock during processing
                let cache_entry = cache_entry.clone();
                drop(cache_handle);

                // Set depths appropriately
                let ret_cache = cache_entry
                    .clone()
                    .into_iter()
                    .flat_map(|entry| LimitedUrl::new(&url.0, entry))
                    .collect();

                // Checks need to be repeated, in case the entry was updated
                // between dropping and read lock and now.
                let mut mut_handle = self.0.write().unwrap();
                if mut_handle
                    .get_key_value(&url)
                    .map(|(stored_url, _)| url.smaller_depth(stored_url))
                    == Some(true)
                {
                    mut_handle.remove_entry(&url);
                    mut_handle.insert(url, cache_entry);
                    VisitCacheRes::SmallerThanCached(ret_cache)
                } else {
                    VisitCacheRes::CachedNoRepeat
                }
            } else {
                VisitCacheRes::CachedNoRepeat
            }
        } else {
            VisitCacheRes::Unique
        }
    }

    /// Add `url` to the cache if unique or smaller than the current entry.
    pub fn insert(&self, url: LimitedUrl, children: Vec<Url>) {
        let url = url.into();
        let mut mut_handle = self.0.write().unwrap();

        let compare = mut_handle
            .get_key_value(&url)
            .map(|(stored_url, _)| url.smaller_depth(stored_url));

        if compare == Some(true) {
            mut_handle.remove_entry(&url);
        }

        if compare == Some(true) || compare.is_none() {
            mut_handle.insert(url, children);
        }
    }

    pub fn inner(&self) -> HashMap<UniqueLimitedUrl, Vec<Url>> {
        (*self.0.read().unwrap()).clone()
    }
}
