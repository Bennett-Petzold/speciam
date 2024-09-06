mod robots;
use reqwest::{header::CONTENT_TYPE, Response};
pub use robots::*;

mod scrape;
pub use scrape::*;

mod download;
pub use download::*;

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
