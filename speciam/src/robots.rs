use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{ready, Context, Poll, Waker},
};

use reqwest::{Client, StatusCode, Url};
use robotstxt::DefaultMatcher;
use thiserror::Error;

use crate::url_base;

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
/// * `url`: The [`Url`] to extract a base [`Url`] from.
pub async fn get_robots<C, V, R>(
    client: C,
    visited: V,
    robots: R,
    url_on_site: Url,
) -> Result<String, RobotsErr>
where
    C: Borrow<Client>,
    V: Borrow<RwLock<HashSet<Url>>>,
    R: Borrow<RwLock<HashMap<Url, String>>>,
{
    let base_url = url_base(url_on_site).ok_or(RobotsErr::InvalidUrl)?;
    let robots_url = base_url.join("robots.txt").unwrap_or_else(|e| {
        panic!(
            "\"{}/robots.txt\" is unconditionally valid. Error: {e}",
            base_url.as_str()
        )
    });

    {
        let robots_clone = robots_url.clone();
        visited.borrow().write().unwrap().insert(robots_clone);
    }

    let robots_txt = match client.borrow().get(robots_url.clone()).send().await {
        Ok(response) => response.text().await.map_err(RobotsErr::Text)?,
        // Treat no `robots.txt` as full permission.
        Err(e) if e.status() == Some(StatusCode::NOT_FOUND) => "".to_string(),
        Err(e) => return Err(RobotsErr::Response(e)),
    };

    let _ = robots
        .borrow()
        .write()
        .unwrap()
        .insert(base_url, robots_txt.clone());

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
pub struct RobotsCheck<Client = Arc<reqwest::Client>, Visited = Arc<RwLock<HashSet<Url>>>> {
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
}

#[expect(
    clippy::type_complexity,
    reason = "Internal implementation, not public API."
)]
enum RobotsCheckFutState<'a, Parent> {
    /// Finished checking
    Computed(Result<bool, RobotsErr>),
    /// Use result from other instance
    AttemptCompute((&'a Parent, &'a Url)),
    /// Queued for wakeup with Url and position
    Queuing((&'a Parent, &'a Url, Url, Option<usize>)),
    /// Loading from remote
    Loading(
        (
            &'a Parent,
            &'a Url,
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
    V: Borrow<RwLock<HashSet<Url>>> + Unpin + 'a,
    P: Borrow<RobotsCheck<C, V>> + Unpin + 'a,
{
    type Output = Result<bool, RobotsErr>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let this = self.as_mut().get_mut();

            match &mut this.state {
                RobotsCheckFutState::Computed(res) => {
                    return Poll::Ready(std::mem::replace(res, Ok(false)));
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
                        Ok(robots_txt) => Ok(DefaultMatcher::default()
                            .one_agent_allowed_by_robots(
                                &robots_txt,
                                &parent.user_agent,
                                url.as_str(),
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
    V: Borrow<RwLock<HashSet<Url>>> + 'a,
    P: Borrow<RobotsCheck<C, V>> + 'a,
{
    pub fn new(parent: &'a P, url: &'a Url) -> Self {
        let parent_handle = parent.borrow();

        let state = if let Some(url_base) = url_base(url.clone()) {
            let robots_handle = parent_handle.robots.borrow().read().unwrap();
            if let Some(robots_txt) = robots_handle.get(&url_base) {
                let valid = DefaultMatcher::default().one_agent_allowed_by_robots(
                    robots_txt,
                    &parent_handle.user_agent,
                    url.as_str(),
                );
                RobotsCheckFutState::Computed(Ok(valid))
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
        } else {
            RobotsCheckFutState::Computed(Err(RobotsErr::InvalidUrl))
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
    V: Borrow<RwLock<HashSet<Url>>>,
{
    /// Checks if a url is allowed by its `robots.txt`.
    ///
    /// The future resolves to [`Result<bool>`]. After the first
    /// [`Poll::Ready`], it may short-circuit to `Ok(false)`.
    pub fn check<'a>(&'a self, url: &'a Url) -> RobotsCheckFut<'_, C, V, Self> {
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

        let search_url = base_url.join("search").unwrap();
        let about_url = base_url.join("search/about").unwrap();
        let static_url = base_url.join("search/static").unwrap();

        let visited = RwLock::default();

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
        assert!(!search_url_fut.await.unwrap());
        assert!(about_url_fut.await.unwrap());

        // Third one, after robots.txt is loaded, should immediately resolve
        let static_url_fut = robots.check(&static_url);
        assert!(matches!(
            static_url_fut.state,
            RobotsCheckFutState::Computed(_)
        ));
        let static_poll = futures::poll!(static_url_fut);
        assert!(matches!(static_poll, Poll::Ready(Ok(true))));

        // Expected state is one visit, one robots entry, zero current
        // processing.
        assert_eq!(
            robots.visited.read().unwrap().iter().collect::<Vec<_>>(),
            [&base_url.join("robots.txt").unwrap()]
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
        let yahoo_url = Url::from_str(YAHOO_HOMEPAGE).unwrap();
        let mut yahoo_fut = robots.check(&yahoo_url);
        assert!(matches!(futures::poll!(&mut yahoo_fut), Poll::Pending));
        assert!(matches!(yahoo_fut.state, RobotsCheckFutState::Loading(_)));
    }
}
