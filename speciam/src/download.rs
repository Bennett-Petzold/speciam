use std::{borrow::Borrow, collections::HashSet, sync::RwLock};

use error_stack::Report;
use reqwest::{Client, Response, Url};

/// Return the download page, if the URL and resolved URL are unique.
///
/// If the URL and/or resolved URL is unique, they will be added to `visited`.
///
/// # Parameters
/// * `client`: [`Client`] to use for this operation.
/// * `visited`: A set of already visited [`Url`]s.
/// * `url`: The potentially unique [`Url`] to pull.
pub async fn download<C, V>(
    client: C,
    visited: V,
    url: Url,
) -> Result<Option<Response>, Report<reqwest::Error>>
where
    C: Borrow<Client>,
    V: Borrow<RwLock<HashSet<Url>>>,
{
    let visited = visited.borrow();

    if visited.read().unwrap().contains(&url) {
        Ok(None)
    } else {
        let page_response = client.borrow().get(url.clone()).send().await?;

        // Insert this url into the visited map, if not already present
        if visited.read().unwrap().contains(page_response.url()) {
            Ok(None)
        } else {
            //let orig_url = url.clone();
            if url != *page_response.url() {
                // Need to write in both urls
                let new_url = page_response.url().clone();
                let mut visited_handle = visited.write().unwrap();

                // Write in both urls, return None on a duplicate
                Ok::<_, Report<_>>(visited_handle.insert(url))?;
                Ok::<_, Report<_>>(visited_handle.insert(new_url))?;
            } else {
                // Only need to write in one url, return `None` on a duplicate
                Ok::<_, Report<_>>(visited.write().unwrap().insert(url))?;
            }

            Ok(Some(page_response))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::LazyLock};

    use super::*;

    const GOOGLE_ROBOTS: &str = "https://www.google.com/robots.txt";

    const USER_AGENT: &str = "speciam";
    static CLIENT: LazyLock<Client> =
        LazyLock::new(|| Client::builder().user_agent(USER_AGENT).build().unwrap());

    #[tokio::test]
    async fn remove_dup() {
        let url = Url::from_str(GOOGLE_ROBOTS).unwrap();
        let visited = RwLock::default();

        assert!(download(&*CLIENT, &visited, url.clone())
            .await
            .unwrap()
            .is_some());
        assert!(download(&*CLIENT, &visited, url).await.unwrap().is_none());
    }
}
