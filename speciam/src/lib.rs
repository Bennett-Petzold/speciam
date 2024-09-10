mod robots;
use std::{borrow::Borrow, collections::HashSet, path::Path, sync::RwLock};

use reqwest::{header::CONTENT_TYPE, Client, Response};
pub use robots::*;

mod scrape;
pub use scrape::*;

mod download;
pub use download::*;

mod domain;
pub use domain::*;

mod cache;
pub use cache::*;

mod thread_limiter;
pub use thread_limiter::*;

#[cfg(test)]
pub mod test;

use thiserror::Error;
use url::Url;

/// Converts `url` to a base [`Url`], if possible.
pub fn url_base(mut url: Url) -> Option<Url> {
    url.path_segments_mut().ok()?.clear();
    url.set_query(None);
    Some(url)
}

/// Adds the index.TYPE ending, if the URL ends with "/".
///
/// Otherwise this copies the URL unmodified.
pub fn add_index(response: &Response) -> Url {
    let mut url = response.url().clone();

    if let Some(segments) = url.path_segments() {
        if let Some(ending) = segments.last() {
            if ending.is_empty() {
                // Extract the file extension from the MIME type.
                let maybe_ext = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|header| header.to_str().ok())
                    .and_then(|header| header.split_once('/').map(|x| x.1))
                    .map(|mime| mime.split_once(';').map(|x| x.0).unwrap_or(mime));

                if let Some(ext) = maybe_ext {
                    let mut mut_segments = url.path_segments_mut().unwrap_or_else(|_| {
                    panic!("The previous path_segments call returning Some(...) means this must return Ok(...).")
                });
                    mut_segments.pop().push(&("index".to_string() + ext));
                }
            }
        }
    }

    url
}

/// Error that occured during download and scrape.
///
/// The callback error `C` is used when the `callback` feature is enabled.
#[derive(Debug, Error)]
pub enum DlAndScrapeErr<C = ()> {
    /// Contains [`RobotsErr`].
    #[error("failure while checking robots.txt")]
    RobotsCheck(#[source] RobotsErr),
    #[error("failure while making an initial request")]
    GetResponse(#[source] reqwest::Error),
    #[error("failure while downloading")]
    Download(#[source] DownloadError),
    #[error("failure while scraping")]
    Scrape(#[source] ScrapeError),
    #[error("logging callback failed")]
    CB(#[source] C),
}

/// For a unique URL, download into `base_path` and return any scraped URLs.
///
/// Composes [`RobotsCheck::check`], [`get_response`], [`download`], and
/// [`scrape`] into one action.
///
/// Set `CbErr` to `()` if it is unused.
///
/// # Parameters
/// * `client`: [`Client`] to use for this operation.
/// * `visited`: A set of already visited [`Url`]s.
/// * `url`: The potentially unique [`Url`] to pull.
/// * `base_path`: The base path for all files to write into.
///
/// # Return
/// * Scraped urls and the background write handle, if any.
pub async fn dl_and_scrape<
    C,
    V,
    R,
    P,
    T,
    CbErr,
    #[cfg(feature = "callbacks")] Rcb: FnOnce(Url, &String) -> Result<(), CbErr>,
    #[cfg(feature = "callbacks")] Vcb: Fn(&LimitedUrl, Vec<Url>) -> Result<(), CbErr>,
>(
    client: C,
    visited: V,
    robots: R,
    base_path: P,
    thread_limiter: T,
    url: LimitedUrl,
    #[cfg(feature = "callbacks")] new_robot_cb: Rcb,
    #[cfg(feature = "callbacks")] new_visit_cb: Vcb,
) -> Result<(Vec<LimitedUrl>, Option<WriteHandle>), DlAndScrapeErr<CbErr>>
where
    C: Borrow<Client> + Unpin,
    V: Borrow<VisitCache> + Unpin,
    R: Borrow<RobotsCheck<C, V>>,
    P: Borrow<Path>,
    T: Borrow<ThreadLimiter>,
{
    let visited = visited.borrow();
    let thread_limiter = thread_limiter.borrow();

    let robots_check_status = robots
        .borrow()
        .check(&url)
        .await
        .map_err(DlAndScrapeErr::RobotsCheck)?;

    #[cfg(feature = "callbacks")]
    {
        if let RobotsCheckStatus::Added((_, robots_txt)) = &robots_check_status {
            (new_robot_cb)(url.url_base(), robots_txt).map_err(DlAndScrapeErr::CB)?
        }
    }

    if *robots_check_status {
        match visited.probe(url.clone()) {
            VisitCacheRes::Unique => {
                let (response, unique_urls) = get_response(client, url.url().clone())
                    .await
                    .map_err(DlAndScrapeErr::GetResponse)?;
                let headers = response.headers().clone();

                // Wait for resources to free up
                thread_limiter.mark(&url, response.version()).await;

                let (content, write_handle) = download(response, base_path)
                    .await
                    .map_err(DlAndScrapeErr::Download)?;
                let scraped: Vec<_> = scrape(url.url(), headers, content)
                    .await
                    .map_err(DlAndScrapeErr::Scrape)?;

                let scraped_limited = scraped
                    .iter()
                    .flat_map(|scrape| LimitedUrl::new(&url, scrape.clone()))
                    .collect();
                let ret = Ok((scraped_limited, Some(write_handle)));

                // Add unique urls to visit map
                if let UniqueUrls::Two([_, unique]) = unique_urls {
                    if let Ok(unique) = LimitedUrl::new(&url, unique) {
                        #[cfg(feature = "callbacks")]
                        {
                            (new_visit_cb)(&unique, scraped.clone()).map_err(DlAndScrapeErr::CB)?;
                        }
                        visited.insert(unique, scraped.clone());
                    }
                }

                #[cfg(feature = "callbacks")]
                {
                    (new_visit_cb)(&url, scraped.clone()).map_err(DlAndScrapeErr::CB)?;
                }
                visited.insert(url, scraped);

                ret
            }
            VisitCacheRes::SmallerThanCached(urls) => {
                #[cfg(feature = "callbacks")]
                {
                    (new_visit_cb)(&url, urls.iter().map(|x| x.url().clone()).collect())
                        .map_err(DlAndScrapeErr::CB)?;
                }

                Ok((urls, None))
            }
            VisitCacheRes::CachedNoRepeat => Ok((vec![], None)),
        }
    } else {
        Ok((vec![], None))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use test::{CleaningTemp, CLIENT, RUST_HOMEPAGE, USER_AGENT};

    use super::*;

    #[tokio::test]
    async fn dl_and_scrape_invalid_url() {
        let homepage_url =
            LimitedUrl::origin(Url::from_str("https://weklrjwe.com").unwrap()).unwrap();
        let visited = VisitCache::default();

        let robots_check = RobotsCheck::new(&*CLIENT, &visited, USER_AGENT.to_string());
        assert!(dl_and_scrape(
            &*CLIENT,
            &visited,
            &robots_check,
            CleaningTemp::new(),
            ThreadLimiter::new(usize::MAX),
            homepage_url,
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn dl_and_scrape_valid_url() {
        let homepage_url = LimitedUrl::origin(Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        let visited = VisitCache::default();

        let robots_check = RobotsCheck::new(&*CLIENT, &visited, USER_AGENT.to_string());
        dl_and_scrape(
            &*CLIENT,
            &visited,
            &robots_check,
            CleaningTemp::new(),
            ThreadLimiter::new(usize::MAX),
            homepage_url,
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn remove_dup() {
        let url = LimitedUrl::origin(Url::from_str(RUST_HOMEPAGE).unwrap()).unwrap();
        let visited = VisitCache::default();

        let robots_check = RobotsCheck::new(&*CLIENT, &visited, USER_AGENT.to_string());
        let initial = dl_and_scrape(
            &*CLIENT,
            &visited,
            &robots_check,
            CleaningTemp::new(),
            ThreadLimiter::new(usize::MAX),
            url.clone(),
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
        )
        .await
        .unwrap();
        assert!(!initial.0.is_empty());
        assert!(initial.1.is_some());

        let repeat = dl_and_scrape(
            &*CLIENT,
            &visited,
            &robots_check,
            CleaningTemp::new(),
            ThreadLimiter::new(usize::MAX),
            url,
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
            #[cfg(feature = "callbacks")]
            |_, _| Ok::<_, ()>(()),
        )
        .await
        .unwrap();
        assert_eq!(repeat.0, vec![]);
        assert!(repeat.1.is_none());
    }
}
