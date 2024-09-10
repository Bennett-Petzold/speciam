use std::time::Duration;

use governor::{DefaultKeyedRateLimiter, Jitter, Quota, RateLimiter};
use once_map::OnceMap;
use thiserror::Error;
use url::Url;

#[cfg(feature = "serde")]
use {
    serde::{
        de::{Deserializer, MapAccess, SeqAccess, Visitor},
        ser::{SerializeStruct, Serializer},
        Deserialize, Serialize,
    },
    std::str::FromStr,
};

use crate::url_base;

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
/// Represents a maximum traversal depth (at most N items deep).
///
/// `None` represents no limit. The first page on a domain is at depth 0.
pub struct LimitedUrl {
    #[cfg_attr(feature = "serde", serde(serialize_with = "url_serialize"))]
    #[cfg_attr(feature = "serde", serde(deserialize_with = "url_deserialize"))]
    url: Url,
    depth: usize,
}

#[derive(Debug, Error, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[error("url cannot-be-a-base: {0:#?}")]
pub struct CannotBeABase(Url);

impl LimitedUrl {
    /// Construct and calculate depth based on `parent`.
    ///
    /// `url` must be resolvable to some base [`Url`].
    pub fn new(parent: &Self, url: Url) -> Result<Self, CannotBeABase> {
        let base_url = url_base(url.clone()).ok_or(CannotBeABase(url.clone()))?;

        // Depth starts again on zero when the domain changes.
        let depth = if base_url == parent.url_base() {
            parent.depth.saturating_add(1)
        } else {
            0
        };

        Ok(Self { url, depth })
    }

    /// Construct a URL at some `depth`.
    ///
    /// `url` must be resolvable to some base [`Url`].
    pub fn at_depth(url: Url, depth: usize) -> Result<Self, CannotBeABase> {
        url_base(url.clone()).ok_or(CannotBeABase(url.clone()))?;
        Ok(Self { url, depth })
    }

    /// Construct an originating URL.
    ///
    /// `url` must be resolvable to some base [`Url`].
    pub fn origin(url: Url) -> Result<Self, CannotBeABase> {
        Self::at_depth(url, 0)
    }

    /// Returns the base [`Url`].
    pub fn url_base(&self) -> Url {
        url_base(self.url.clone()).unwrap_or_else(|| panic!("Construction requires that the URL can be converted to a base URL. Failing url: {:#?}", self.url))
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Recalculate depth against a new `parent`.
    pub fn change_parent(&mut self, parent: &Self) -> &mut Self {
        // Depth starts again on zero when the domain changes.
        self.depth = if self.url_base() == parent.url_base() {
            parent.depth.saturating_add(1)
        } else {
            0
        };

        self
    }
}

#[cfg(feature = "serde")]
fn url_serialize<S: Serializer>(val: &Url, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(val.as_str())
}

#[cfg(feature = "serde")]
fn url_deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Url, D::Error> {
    let str_form: &str = Deserialize::deserialize(deserializer)?;
    Url::from_str(str_form).map_err(serde::de::Error::custom)
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
/// Represents a maximum traversal depth (at most N items deep).
///
/// `None` represents no limit. The first page on a domain is at depth 0.
pub struct DepthLimit(Option<usize>);

impl From<Option<usize>> for DepthLimit {
    fn from(value: Option<usize>) -> Self {
        Self(value)
    }
}

impl From<usize> for DepthLimit {
    fn from(value: usize) -> Self {
        Self(Some(value))
    }
}

impl From<DepthLimit> for Option<usize> {
    fn from(value: DepthLimit) -> Self {
        value.0
    }
}

impl DepthLimit {
    pub fn new(value: Option<usize>) -> Self {
        value.into()
    }

    /// Returns true if `depth` is inside the limit.
    pub fn within(&self, depth: usize) -> bool {
        match self.0 {
            Some(limit) => depth < limit,
            None => true,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[error("Domain of URL is not mapped in Domains. URL: {0:#?}")]
pub struct DomainNotMapped(pub LimitedUrl);

#[derive(Debug, Error, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[error("Duration is zero length")]
pub struct ZeroLengthDuration {}

/// A store for rate and depth limits on a domain.
#[derive(Debug)]
pub struct Domains {
    /// All URLs share the same rate limit.
    rate_limiter: DefaultKeyedRateLimiter<Url>,
    depth_limits: OnceMap<Url, DepthLimit>,
    jitter: Duration,
    /// Work around for rate_limiter not [`Deserialize`]-ing.
    #[cfg(feature = "serde")]
    rate_period: Duration,
}

impl Domains {
    pub fn new(rate_period: Duration, jitter: Duration) -> Result<Self, ZeroLengthDuration> {
        let quota = Quota::with_period(rate_period).ok_or(ZeroLengthDuration {})?;
        Ok(Self {
            rate_limiter: RateLimiter::keyed(quota),
            depth_limits: OnceMap::default(),
            jitter,
            #[cfg(feature = "serde")]
            rate_period,
        })
    }

    /// Adds or replaces the limit for `url`'s domain.
    pub fn replace_limit(&mut self, url: &LimitedUrl, limit: DepthLimit) -> &mut Self {
        let domain = url.url_base();
        if self.depth_limits.contains_key(&domain) {
            self.depth_limits.remove(&domain);
        };
        self.depth_limits.map_insert(domain, |_| limit, |_, _| ());
        self
    }

    /// Adds the limit for `url`'s domain. Does not overwrite if existing.
    pub fn add_limit(&self, url: &LimitedUrl, limit: DepthLimit) -> &Self {
        self.depth_limits
            .map_insert(url.url_base(), |_| limit, |_, _| ());
        self
    }

    /// Waits on the rate limit and returns true if within depth limit.
    ///
    /// `url`'s domain must be mapped. Use [`Self::add_limit`] or
    /// [`Self::replace_limit`] to add a missing domain.
    ///
    /// This immediately returns false and may delay before returning true.
    pub async fn wait(&self, url: &LimitedUrl) -> Result<bool, DomainNotMapped> {
        let base_url = url.url_base();
        let limit = self
            .depth_limits
            .map_get(&base_url, |_, x| *x)
            .ok_or(DomainNotMapped(url.clone()))?;

        Ok(if limit.within(url.depth()) {
            let jitter = Jitter::new(Duration::ZERO, self.jitter);
            self.rate_limiter
                .until_key_ready_with_jitter(&base_url, jitter)
                .await;
            true
        } else {
            false
        })
    }
}

#[cfg(feature = "serde")]
impl Serialize for Domains {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Domains", 3)?;
        state.serialize_field(
            "depth_limits",
            &self
                .depth_limits
                .read_only_view()
                .iter()
                .map(|(url, limit)| (url.as_str(), limit))
                .collect::<Vec<_>>(),
        )?;
        state.serialize_field("jitter", &self.jitter)?;
        state.serialize_field("rate_period", &self.rate_period)?;
        state.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for Domains {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        enum Field {
            DepthLimits,
            Jitter,
            RatePeriod,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                        formatter.write_str("`depth_limits`, `jitter`, `rate_period`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "depth_limits" => Ok(Field::DepthLimits),
                            "jitter" => Ok(Field::Jitter),
                            "rate_period" => Ok(Field::RatePeriod),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct DomainVisitor;

        impl DomainVisitor {
            fn reconstruct_domains<E: serde::de::Error>(
                &self,
                rate_period: Duration,
                jitter: Duration,
                depth_limits: Vec<(&str, DepthLimit)>,
            ) -> Result<Domains, E> {
                let mut domains =
                    Domains::new(rate_period, jitter).map_err(serde::de::Error::custom)?;

                for (url_str, limit) in depth_limits {
                    let url = Url::from_str(url_str).map_err(serde::de::Error::custom)?;
                    domains.replace_limit(
                        &LimitedUrl::origin(url).map_err(serde::de::Error::custom)?,
                        limit,
                    );
                }

                Ok(domains)
            }
        }

        impl<'de> Visitor<'de> for DomainVisitor {
            type Value = Domains;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Domains")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Domains, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let depth_limits: Vec<(&str, DepthLimit)> = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let jitter = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let rate_period = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &self))?;
                self.reconstruct_domains(rate_period, jitter, depth_limits)
            }

            fn visit_map<V>(self, mut map: V) -> Result<Domains, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut depth_limits = None;
                let mut jitter = None;
                let mut rate_period = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::DepthLimits => {
                            if depth_limits.is_some() {
                                return Err(serde::de::Error::duplicate_field("depth_limits"));
                            }
                            depth_limits = Some(map.next_value()?);
                        }
                        Field::Jitter => {
                            if jitter.is_some() {
                                return Err(serde::de::Error::duplicate_field("rate_period"));
                            }
                            jitter = Some(map.next_value()?);
                        }
                        Field::RatePeriod => {
                            if rate_period.is_some() {
                                return Err(serde::de::Error::duplicate_field("rate_period"));
                            }
                            rate_period = Some(map.next_value()?);
                        }
                    }
                }
                let depth_limits =
                    depth_limits.ok_or_else(|| serde::de::Error::missing_field("depth_limits"))?;
                let jitter = jitter.ok_or_else(|| serde::de::Error::missing_field("jitter"))?;
                let rate_period =
                    rate_period.ok_or_else(|| serde::de::Error::missing_field("rate_period"))?;
                self.reconstruct_domains(rate_period, jitter, depth_limits)
            }
        }

        const FIELDS: &[&str] = &["depth_limits", "rate_period"];
        deserializer.deserialize_struct("Domains", FIELDS, DomainVisitor {})
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::poll;

    use crate::test::{RUST_HOMEPAGE, YAHOO_HOMEPAGE};

    use super::*;

    #[tokio::test]
    async fn fail_on_uninit() {
        let url = Url::from_str(RUST_HOMEPAGE).unwrap();
        let domains = Domains::new(Duration::from_secs(1), Duration::from_secs(0)).unwrap();
        assert!(domains
            .wait(&LimitedUrl::origin(url).unwrap())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn duration_limiting() {
        let url = LimitedUrl::origin(Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        let domains =
            Domains::new(Duration::from_secs(100_000_000), Duration::from_secs(0)).unwrap();
        domains.add_limit(&url, None.into());

        // First should pass
        assert_eq!(poll!(Box::pin(domains.wait(&url))), Poll::Ready(Ok(true)));

        // Second should stall for rate limit
        assert_eq!(poll!(Box::pin(domains.wait(&url))), Poll::Pending);
    }

    #[tokio::test]
    async fn domain_counting() {
        let url = LimitedUrl::origin(Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        assert_eq!(url.depth, 0);

        let nested_url = LimitedUrl::new(&url, Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        assert_eq!(nested_url.depth, 1);

        let remote_url = LimitedUrl::new(&url, Url::from_str(YAHOO_HOMEPAGE).unwrap()).unwrap();
        assert_eq!(remote_url.depth, 0);
    }
}
