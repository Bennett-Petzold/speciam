use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
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

    /// Test if this has an equal depth to `rhs`.
    pub fn eq_depth(&self, rhs: &Self) -> bool {
        self.0.depth() == rhs.0.depth()
    }
}

/// Stores a cache of visited [`LimitedUrl`] values and their scraped urls.
///
/// This is built on the general assumption that a site will have many copies
/// of the same link. In that case a cache avoids repeat downloads, parses, and
/// traversals of the same path.
#[derive(Debug, Default)]
pub struct VisitCache(RwLock<HashMap<UniqueLimitedUrl, Option<Vec<Url>>>>);

impl From<HashMap<UniqueLimitedUrl, Option<Vec<Url>>>> for VisitCache {
    fn from(value: HashMap<UniqueLimitedUrl, Option<Vec<Url>>>) -> Self {
        Self(RwLock::new(value))
    }
}

impl FromIterator<(UniqueLimitedUrl, Vec<Url>)> for VisitCache {
    fn from_iter<T: IntoIterator<Item = (UniqueLimitedUrl, Vec<Url>)>>(iter: T) -> Self {
        HashMap::from_iter(iter.into_iter().map(|(x, y)| (x, Some(y)))).into()
    }
}

impl From<HashMap<UniqueLimitedUrl, Vec<Url>>> for VisitCache {
    fn from(value: HashMap<UniqueLimitedUrl, Vec<Url>>) -> Self {
        Self::from_iter(value)
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

        let update = |url: UniqueLimitedUrl| {
            // Checks need to be repeated, in case the entry was updated
            // between dropping and read lock and now.
            let mut mut_handle = self.0.write().unwrap();
            if let Some((stored_url, cache_entry)) = mut_handle.get_key_value(&url) {
                if url.smaller_depth(stored_url) {
                    let cache_entry = cache_entry.clone();
                    let ret_cache = cache_entry.clone().map(|x| {
                        x.into_iter()
                            .flat_map(|entry| LimitedUrl::new(&url.0, entry))
                            .collect()
                    });

                    mut_handle.remove_entry(&url);
                    mut_handle.insert(url, cache_entry);

                    if let Some(ret_cache) = ret_cache {
                        VisitCacheRes::SmallerThanCached(ret_cache)
                    } else {
                        VisitCacheRes::CachedNoRepeat
                    }
                } else {
                    VisitCacheRes::CachedNoRepeat
                }
            } else {
                mut_handle.insert(url, None);
                VisitCacheRes::Unique
            }
        };

        // Scoped for handle drop
        {
            if let Some((stored_url, _)) = self.0.read().unwrap().get_key_value(&url) {
                if !url.smaller_depth(stored_url) {
                    return VisitCacheRes::CachedNoRepeat;
                }
            };
        }

        update(url)
    }

    /// Add `url` to the cache.
    pub fn insert(&self, url: LimitedUrl, children: Vec<Url>) {
        self.0.write().unwrap().insert(url.into(), Some(children));
    }

    pub fn get(&self, url: LimitedUrl) -> Option<Vec<LimitedUrl>> {
        self.0
            .read()
            .unwrap()
            .get_key_value(&url.into())
            .and_then(|(key, vec)| {
                let key = key.clone().into();
                vec.as_ref().map(|v| {
                    v.iter()
                        .flat_map(|scrape| LimitedUrl::new(&key, scrape.clone()))
                        .collect()
                })
            })
    }

    pub fn get_nonlimited(&self, url: LimitedUrl) -> Option<Vec<Url>> {
        self.0
            .read()
            .unwrap()
            .get(&url.into())
            .and_then(|vec| vec.clone())
    }

    pub fn inner(&self) -> HashMap<UniqueLimitedUrl, Option<Vec<Url>>> {
        (*self.0.read().unwrap()).clone()
    }
}
