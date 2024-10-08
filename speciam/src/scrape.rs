use std::{borrow::Borrow, cell::LazyCell, str::Utf8Error};

use bare_err_tree::err_tree;
use reqwest::{
    header::{HeaderMap, ToStrError, CONTENT_TYPE},
    Url,
};
use select::{document::Document, predicate::Name};
use thiserror::Error;
use url::ParseError;

use crate::url_base;

#[derive(Debug, Error)]
#[err_tree(ScrapeErrorWrap)]
pub enum ScrapeError {
    #[error("no CONTENT_TYPE header in the response")]
    NoContentType,
    #[dyn_err]
    #[error("CONTENT_TYPE header is not a valid string")]
    InvalidContentType(#[from] ToStrError),
    #[dyn_err]
    #[error("HTML cannot be parsed as a UTF-8 string")]
    InvalidBody(#[from] Utf8Error),
}

/// Returns all referenced [`Url`]s, if this is an HTML document.
///
/// If this is a non-html document, this returns an empty [`Vec`].
pub async fn scrape<U, H, B>(url: U, headers: H, body: B) -> Result<Vec<Url>, ScrapeErrorWrap>
where
    U: Borrow<Url>,
    H: Borrow<HeaderMap>,
    B: Borrow<[u8]>,
{
    if let Some(content_type) = headers.borrow().get(CONTENT_TYPE) {
        let content_type = content_type
            .to_str()
            .map_err(ScrapeError::InvalidContentType)?;

        if content_type.contains("text/html") {
            // Adapted from the rust cookbook
            // <https://rustwiki.org/en/rust-cookbook/web/scraping.html>

            let body = Document::from(String::from_utf8_lossy(body.borrow()).as_ref());

            let base_url = LazyCell::new(|| url_base(url.borrow().clone()));

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

    use crate::test::RUST_HOMEPAGE;

    use super::*;

    #[tokio::test]
    async fn scrape_for_url() {
        let homepage_url = Url::from_str(RUST_HOMEPAGE).unwrap();
        let install_url = homepage_url
            .join("tools/")
            .unwrap()
            .join("install")
            .unwrap();

        let homepage_response = reqwest::get(homepage_url).await.unwrap();
        let scraped_urls = scrape(
            homepage_response.url().clone(),
            homepage_response.headers().clone(),
            homepage_response.bytes().await.unwrap(),
        )
        .await
        .unwrap();

        assert!(scraped_urls.contains(&install_url));
    }
}
