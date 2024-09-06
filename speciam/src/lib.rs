mod robots;
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
