mod robots;
use std::{borrow::Borrow, collections::HashSet, path::Path, sync::RwLock};

use reqwest::{header::CONTENT_TYPE, Client, Response};
pub use robots::*;

mod scrape;
pub use scrape::*;

mod download;
pub use download::*;

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
/// * Scraped urls and the background write handle.
pub async fn dl_and_scrape<C, V, R, P>(
    client: C,
    visited: V,
    robots: R,
    base_path: P,
    url: Url,
) -> Result<Option<(Vec<Url>, WriteHandle)>, DlAndScrapeErr>
where
    C: Borrow<Client> + Unpin,
    V: Borrow<RwLock<HashSet<Url>>> + Unpin,
    R: Borrow<RobotsCheck<C, V>>,
    P: Borrow<Path>,
{
    if robots.borrow().check(&url).await? {
        if let Some(response) = get_response(client, visited, url.clone()).await? {
            let headers = response.headers().clone();

            let (content, write_handle) = download(response, base_path).await?;
            let scraped = scrape(url, headers, content).await?;
            Ok(Some((scraped, write_handle)))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::LazyLock};

    use test::CleaningTemp;

    use super::*;

    const RUST_HOMEPAGE: &str = "https://www.rust-lang.org/";

    const USER_AGENT: &str = "speciam";
    static CLIENT: LazyLock<Client> =
        LazyLock::new(|| Client::builder().user_agent(USER_AGENT).build().unwrap());

    #[tokio::test]
    async fn dl_and_scrape_invalid_url() {
        let homepage_url = Url::from_str("https://weklrjwe.com").unwrap();
        let visited = RwLock::default();

        let robots_check = RobotsCheck::new(&*CLIENT, &visited, USER_AGENT.to_string());
        assert!(dl_and_scrape(
            &*CLIENT,
            &visited,
            &robots_check,
            CleaningTemp::new(),
            homepage_url.clone(),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn dl_and_scrape_valid_url() {
        let homepage_url = Url::from_str(RUST_HOMEPAGE).unwrap();
        let visited = RwLock::default();

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
}
