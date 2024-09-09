use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{ready, Context, Poll, Waker},
};

use reqwest::{Client, StatusCode, Url};
use robotstxt::DefaultMatcher;
use thiserror::Error;

use crate::{url_base, LimitedUrl, VisitCache};

#[derive(Debug, Error)]
pub enum RobotsErr {
    #[error("url_on_site cannot-be-a-base")]
    InvalidUrl,
    #[error("failure while getting response")]
    Response(#[source] reqwest::Error),
    #[error("failure while getting text body")]
    Text(#[source] reqwest::Error),
}

/// Add the robots.txt for `url_on_site`'s domain to `robots`.
///
/// The robots.txt [`Url`] will also be added to `visited`.
///
/// # Returns
/// The robots.txt, or an error.
///
/// # Parameters
/// * `client`: [`Client`] to use for this operation.
/// * `visited`: A set of already visited [`Url`]s.
/// * `robots`: The map of base [`Url`]s to `robot.txt` bodies.
/// * `base_url`: The base site [`Url`] (e.g. `https://google.com`).
pub async fn get_robots<C, V, R>(
    client: C,
    visited: V,
    robots: R,
    base_url: Url,
) -> Result<String, RobotsErr>
where
    C: Borrow<Client>,
    V: Borrow<VisitCache>,
    R: Borrow<RwLock<HashMap<Url, String>>>,
{
    let robots_url = LimitedUrl::origin(base_url.join("robots.txt").unwrap_or_else(|e| {
        panic!(
            "\"{}/robots.txt\" is unconditionally valid. Error with base url: {e}",
            base_url.as_str()
        )
    }))
    .map_err(|_| RobotsErr::InvalidUrl)?;

    {
        let robots_clone = robots_url.clone();
        visited.borrow().insert(robots_clone, vec![]);
    }

    let robots_txt = match client.borrow().get(robots_url.url().clone()).send().await {
        Ok(response) => response.text().await.map_err(RobotsErr::Text)?,
        // Treat no `robots.txt` as full permission.
        Err(e) if e.status() == Some(StatusCode::NOT_FOUND) => "".to_string(),
        Err(e) => return Err(RobotsErr::Response(e)),
    };

    let _ = robots
        .borrow()
        .write()
        .unwrap()
        .insert(base_url.clone(), robots_txt.clone());

    Ok(robots_txt)
}

/// Maintains state to check for validity under `robots.txt`.
///
/// # Generics
/// * `Client`: [`Client`] to operate with.
/// * `Visited`: A set of already visited [`Url`]s.
/// * `Robots`: The map of base [`Url`]s to `robot.txt` bodies.
/// * `Processing`: A queue of all currently processing base urls.
#[derive(Debug)]
pub struct RobotsCheck<Client = Arc<reqwest::Client>, Visited = Arc<VisitCache>> {
    client: Client,
    visited: Visited,
    robots: RwLock<HashMap<Url, String>>,
    processing: Mutex<HashMap<Url, Vec<Waker>>>,
    user_agent: String,
}

impl<C, V> RobotsCheck<C, V> {
    pub fn new(client: C, visited: V, user_agent: String) -> Self {
        Self {
            client,
            visited,
            robots: RwLock::default(),
            processing: Mutex::default(),
            user_agent,
        }
    }

    pub fn with_database(
        client: C,
        visited: V,
        user_agent: String,
        robots: HashMap<Url, String>,
    ) -> Self {
        Self {
            client,
            visited,
            robots: RwLock::new(robots),
            processing: Mutex::default(),
            user_agent,
        }
    }
}

#[expect(
    clippy::type_complexity,
    reason = "Internal implementation, not public API."
)]
enum RobotsCheckFutState<'a, Parent> {
    /// Finished checking
    Computed(Result<RobotsCheckStatus, RobotsErr>),
    /// Use result from other instance
    AttemptCompute((&'a Parent, &'a LimitedUrl)),
    /// Queued for wakeup with Url and position
    Queuing((&'a Parent, &'a LimitedUrl, Url, Option<usize>)),
    /// Loading from remote
    Loading(
        (
            &'a Parent,
            &'a LimitedUrl,
            Url,
            Pin<Box<dyn Future<Output = Result<String, RobotsErr>> + 'a>>,
        ),
    ),
}

impl<Parent: Debug> Debug for RobotsCheckFutState<'_, Parent> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RobotsCheckFutState::Computed(x) => f
                .debug_tuple("RobotsCheckFutState::Computed")
                .field(&x)
                .finish(),
            RobotsCheckFutState::AttemptCompute((parent, url)) => f
                .debug_tuple("RobotsCheckFutState::Computed")
                .field(parent)
                .field(url)
                .finish(),
            RobotsCheckFutState::Queuing((parent, url, base_url, pos)) => f
                .debug_tuple("RobotsCheckFutState::Computed")
                .field(parent)
                .field(url)
                .field(&base_url)
                .field(&pos)
                .finish(),
            RobotsCheckFutState::Loading((parent, url, base_url, fut)) => {
                let fut_ptr: *const _ = &*fut.as_ref();
                f.debug_tuple("RobotsCheckFutState::Computed")
                    .field(parent)
                    .field(url)
                    .field(&base_url)
                    .field(&fut_ptr)
                    .finish()
            }
        }
    }
}

/// Encodes whether the check passed and if the robot.txt was in cache.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RobotsCheckStatus {
    Cached(bool),
    Added(bool),
}

impl From<RobotsCheckStatus> for bool {
    fn from(value: RobotsCheckStatus) -> Self {
        match value {
            RobotsCheckStatus::Cached(x) => x,
            RobotsCheckStatus::Added(x) => x,
        }
    }
}

impl Deref for RobotsCheckStatus {
    type Target = bool;
    fn deref(&self) -> &Self::Target {
        match self {
            RobotsCheckStatus::Cached(x) => x,
            RobotsCheckStatus::Added(x) => x,
        }
    }
}

impl Borrow<bool> for RobotsCheckStatus {
    fn borrow(&self) -> &bool {
        self
    }
}

/// Implementor for [`RobotsCheck::check`].
#[derive(Debug)]
#[repr(transparent)]
pub struct RobotsCheckFut<'a, Client, Visited, Parent> {
    state: RobotsCheckFutState<'a, Parent>,
    _phantom: (PhantomData<Client>, PhantomData<Visited>),
}

impl<'a, C, V, P> Future for RobotsCheckFut<'a, C, V, P>
where
    C: Borrow<Client> + Unpin + 'a,
    V: Borrow<VisitCache> + Unpin + 'a,
    P: Borrow<RobotsCheck<C, V>> + Unpin + 'a,
{
    type Output = Result<RobotsCheckStatus, RobotsErr>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let this = self.as_mut().get_mut();

            match &mut this.state {
                RobotsCheckFutState::Computed(res) => {
                    return Poll::Ready(std::mem::replace(
                        res,
                        Ok(RobotsCheckStatus::Cached(false)),
                    ));
                }

                RobotsCheckFutState::Loading((parent, url, base_url, fut)) => {
                    let load_res = ready!(fut.as_mut().poll(cx));

                    let parent = (*parent).borrow();

                    // Wake up all children
                    let wake_queue = parent.processing
                        .lock()
                        .unwrap()
                        .remove(base_url)
                        .unwrap_or_else(|| panic!("The processing map is fully owned by RobotsCheck, entries are only deleted here."));
                    wake_queue.into_iter().for_each(|waker| waker.wake());

                    this.state = RobotsCheckFutState::Computed(match load_res {
                        Ok(robots_txt) => Ok(RobotsCheckStatus::Added(
                            DefaultMatcher::default().one_agent_allowed_by_robots(
                                &robots_txt,
                                &parent.user_agent,
                                url.url().as_str(),
                            ),
                        )),
                        Err(e) => Err(e),
                    });
                }

                RobotsCheckFutState::Queuing((parent, url, url_base, pos)) => {
                    let mut wake_map = (*parent).borrow().processing.lock().unwrap();

                    if let Some(queue) = wake_map.get_mut(url_base) {
                        // Still hasn't loaded robots.txt

                        // Update waker in queue
                        if let Some(pos) = pos {
                            queue[*pos] = cx.waker().clone();
                        } else {
                            *pos = Some(queue.len());
                            queue.push(cx.waker().clone());
                        }

                        return Poll::Pending;
                    } else {
                        // robots.txt either loaded or failed to
                        this.state = RobotsCheckFutState::AttemptCompute((parent, url));
                    }
                }

                RobotsCheckFutState::AttemptCompute((parent, url)) => {
                    let parent = *parent;
                    let url = *url;
                    self.set(Self::new(parent, url))
                }
            }
        }
    }
}

impl<'a, C, V, P> RobotsCheckFut<'a, C, V, P>
where
    C: Borrow<Client> + 'a,
    V: Borrow<VisitCache> + 'a,
    P: Borrow<RobotsCheck<C, V>> + 'a,
{
    pub fn new(parent: &'a P, url: &'a LimitedUrl) -> Self {
        let parent_handle = parent.borrow();

        let url_base = url.url_base();
        let state = {
            let robots_handle = parent_handle.robots.borrow().read().unwrap();
            if let Some(robots_txt) = robots_handle.get(&url_base) {
                let valid = DefaultMatcher::default().one_agent_allowed_by_robots(
                    robots_txt,
                    &parent_handle.user_agent,
                    url.url().as_str(),
                );
                RobotsCheckFutState::Computed(Ok(RobotsCheckStatus::Cached(valid)))
            } else {
                drop(robots_handle);

                let mut processing_handle = parent_handle.processing.lock().unwrap();
                if processing_handle.contains_key(&url_base) {
                    RobotsCheckFutState::Queuing((parent, url, url_base, None))
                } else {
                    processing_handle.insert(url_base.clone(), Vec::new());
                    RobotsCheckFutState::Loading((
                        parent,
                        url,
                        url_base.clone(),
                        Box::pin(get_robots(
                            parent.borrow().client.borrow(),
                            parent.borrow().visited.borrow(),
                            parent.borrow().robots.borrow(),
                            url_base,
                        )),
                    ))
                }
            }
        };

        Self {
            state,
            _phantom: (PhantomData, PhantomData),
        }
    }
}

impl<C, V> RobotsCheck<C, V>
where
    C: Borrow<Client>,
    V: Borrow<VisitCache>,
{
    /// Checks if a url is allowed by its `robots.txt`.
    ///
    /// The future resolves to [`Result<bool>`]. After the first
    /// [`Poll::Ready`], it may short-circuit to `Ok(false)`.
    pub fn check<'a>(&'a self, url: &'a LimitedUrl) -> RobotsCheckFut<'_, C, V, Self> {
        RobotsCheckFut::new(self, url)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::test::{CLIENT, GOOGLE_HOMEPAGE, USER_AGENT, YAHOO_HOMEPAGE};

    use super::*;

    #[tokio::test]
    async fn parse_robots() {
        let base_url = Url::from_str(GOOGLE_HOMEPAGE).unwrap();

        let search_url = LimitedUrl::origin(base_url.join("search").unwrap()).unwrap();
        let about_url = LimitedUrl::origin(base_url.join("search/about").unwrap()).unwrap();
        let static_url = LimitedUrl::origin(base_url.join("search/static").unwrap()).unwrap();

        let visited = VisitCache::default();

        let robots = RobotsCheck::new(&*CLIENT, &visited, USER_AGENT.to_string());

        // First two futures
        let mut search_url_fut = robots.check(&search_url);
        let mut about_url_fut = robots.check(&about_url);

        // First one should claim loading
        assert!(matches!(futures::poll!(&mut search_url_fut), Poll::Pending));
        assert!(matches!(
            search_url_fut.state,
            RobotsCheckFutState::Loading(_)
        ));

        // Second overlapping one should enqueue
        assert!(matches!(futures::poll!(&mut about_url_fut), Poll::Pending));
        assert!(matches!(
            about_url_fut.state,
            RobotsCheckFutState::Queuing((_, _, _, Some(0)))
        ));

        // Resolve both correctly
        assert_eq!(
            search_url_fut.await.unwrap(),
            RobotsCheckStatus::Added(false)
        );
        assert_eq!(
            about_url_fut.await.unwrap(),
            RobotsCheckStatus::Cached(true)
        );

        // Third one, after robots.txt is loaded, should immediately resolve
        let static_url_fut = robots.check(&static_url);
        assert!(matches!(
            static_url_fut.state,
            RobotsCheckFutState::Computed(_)
        ));
        let static_poll = futures::poll!(static_url_fut);
        assert!(matches!(
            static_poll,
            Poll::Ready(Ok(RobotsCheckStatus::Cached(true)))
        ));

        // Expected state is one visit, one robots entry, zero current
        // processing.
        assert_eq!(
            robots
                .visited
                .inner()
                .keys()
                .map(|k| {
                    let k: LimitedUrl = k.clone().into();
                    k.url().clone()
                })
                .collect::<Vec<_>>(),
            [base_url.join("robots.txt").unwrap()]
        );
        assert_eq!(
            robots
                .robots
                .read()
                .unwrap()
                .iter()
                .map(|(x, _)| x)
                .collect::<Vec<_>>(),
            [&base_url]
        );
        assert!(robots.processing.lock().unwrap().is_empty());

        // Query to a different domain should require a new load
        let yahoo_url = LimitedUrl::origin(Url::from_str(YAHOO_HOMEPAGE).unwrap()).unwrap();
        let mut yahoo_fut = robots.check(&yahoo_url);
        assert!(matches!(futures::poll!(&mut yahoo_fut), Poll::Pending));
        assert!(matches!(yahoo_fut.state, RobotsCheckFutState::Loading(_)));
    }
}
