use std::cell::LazyCell;

use error_stack::Report;
use reqwest::{
    header::{ToStrError, CONTENT_TYPE},
    Response, Url,
};
use select::{document::Document, predicate::Name};
use thiserror::Error;
use url::ParseError;

use crate::url_base;

#[derive(Debug, Error)]
pub enum ScrapeError {
    #[error("No CONTENT_TYPE header in the response.")]
    NoContentType,
    #[error("The CONTENT_TYPE header was not a valid string.")]
    InvalidContentType(ToStrError),
    #[error("Reqwest Error")]
    Reqwest(reqwest::Error),
}

impl From<reqwest::Error> for ScrapeError {
    fn from(value: reqwest::Error) -> Self {
        Self::Reqwest(value)
    }
}

/// Returns all referenced [`Url`]s, if this is an HTML document.
///
/// If this is a non-html document, this returns an empty [`Vec`].
pub async fn scrape(response: Response) -> Result<Vec<Url>, Report<ScrapeError>> {
    let response_url = response.url().clone();

    if let Some(content_type) = response.headers().get(CONTENT_TYPE) {
        let content_type = content_type
            .to_str()
            .map_err(ScrapeError::InvalidContentType)?;

        if content_type.contains("text/html") {
            // Adapted from the rust cookbook
            // <https://rustwiki.org/en/rust-cookbook/web/scraping.html>

            let body = Document::from(
                response
                    .text()
                    .await
                    .map_err(ScrapeError::Reqwest)?
                    .as_str(),
            );

            let base_url = LazyCell::new(|| url_base(response_url));

            Ok(body
                .find(Name("a"))
                .filter_map(|n| n.attr("href"))
                .flat_map(|n| match Url::parse(n) {
                    Ok(url) => Some(url),
                    // If it was a relative URL, retry with the base
                    Err(ParseError::RelativeUrlWithoutBase) => (*base_url).as_ref()?.join(n).ok(),
                    Err(_) => None,
                })
                .collect())
        } else {
            Ok(vec![])
        }
    } else {
        Err(ScrapeError::NoContentType.into())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    const RUST_HOMEPAGE: &str = "https://www.rust-lang.org/";

    #[tokio::test]
    async fn scrape_for_url() {
        let homepage_url = Url::from_str(RUST_HOMEPAGE).unwrap();
        let install_url = homepage_url
            .join("tools/")
            .unwrap()
            .join("install")
            .unwrap();

        let scraped_urls = scrape(reqwest::get(homepage_url).await.unwrap())
            .await
            .unwrap();

        assert!(scraped_urls.contains(&install_url));
    }
}
