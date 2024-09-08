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

#[derive(Debug, Error)]
pub enum DlAndScrapeErr {
    /// Contains [`RobotsErr`].
    #[error("failure while checking robots.txt")]
    RobotsCheck(#[from] RobotsErr),
    #[error("failure while making an initial request")]
    GetResponse(#[from] reqwest::Error),
    #[error("failure while downloading")]
    Download(#[from] DownloadError),
    #[error("failure while scraping")]
    Scrape(#[from] ScrapeError),
}

/// For a unique URL, download into `base_path` and return any scraped URLs.
///
/// Composes [`RobotsCheck::check`], [`get_response`], [`download`], and
/// [`scrape`] into one action.
///
/// # Parameters
/// * `client`: [`Client`] to use for this operation.
/// * `visited`: A set of already visited [`Url`]s.
/// * `url`: The potentially unique [`Url`] to pull.
/// * `base_path`: The base path for all files to write into.
///
/// # Return
/// * Scraped urls and the background write handle, if any.
pub async fn dl_and_scrape<C, V, R, P>(
    client: C,
    visited: V,
    robots: R,
    base_path: P,
    url: LimitedUrl,
) -> Result<(Vec<LimitedUrl>, Option<WriteHandle>), DlAndScrapeErr>
where
    C: Borrow<Client> + Unpin,
    V: Borrow<VisitCache> + Unpin,
    R: Borrow<RobotsCheck<C, V>>,
    P: Borrow<Path>,
{
    let visited = visited.borrow();

    if robots.borrow().check(&url).await? {
        match visited.probe(url.clone()) {
            VisitCacheRes::Unique => {
                let (response, unique_urls) = get_response(client, url.url().clone()).await?;
                let headers = response.headers().clone();

                let (content, write_handle) = download(response, base_path).await?;
                let scraped: Vec<_> = scrape(url.url(), headers, content)
                    .await?
                    .into_iter()
                    .flat_map(|scrape| LimitedUrl::new(&url, scrape))
                    .collect();
                let ret = Ok((scraped.clone(), Some(write_handle)));

                // Add unique urls to visit map
                if let UniqueUrls::Two([_, unique]) = unique_urls {
                    if let Ok(unique) = LimitedUrl::new(&url, unique) {
                        visited.insert(unique, scraped.clone());
                    }
                }
                visited.insert(url, scraped.clone());

                ret
            }
            VisitCacheRes::SmallerThanCached(urls) => Ok((urls, None)),
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
            homepage_url,
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
            homepage_url,
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
            url.clone(),
        )
        .await
        .unwrap();
        assert!(!initial.0.is_empty());
        assert!(initial.1.is_some());

        let repeat = dl_and_scrape(&*CLIENT, &visited, &robots_check, CleaningTemp::new(), url)
            .await
            .unwrap();
        assert_eq!(repeat.0, vec![]);
        assert!(repeat.1.is_none());
    }
}
