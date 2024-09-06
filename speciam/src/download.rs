use std::{borrow::Borrow, collections::HashSet, io::ErrorKind, path::Path, sync::RwLock};

use bytes::Bytes;
use error_stack::Report;
use reqwest::{Client, Response, Url};
use thiserror::Error;
use tokio::{
    fs::{create_dir_all, File},
    io::AsyncWriteExt,
    spawn,
    sync::mpsc::{self, error::SendError},
    task::JoinHandle,
};

use crate::add_index;

/// Return the page response, if the URL and resolved URL are unique.
///
/// If the URL and/or resolved URL is unique, they will be added to `visited`.
///
/// # Parameters
/// * `client`: [`Client`] to use for this operation.
/// * `visited`: A set of already visited [`Url`]s.
/// * `url`: The potentially unique [`Url`] to pull.
pub async fn get_response<C, V>(
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
            let new_url = add_index(&page_response);
            if url != new_url {
                // Need to write in both urls
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

/// Background write failure.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("Failed to create the path: {0:#?}")]
    PathCreate(std::io::Error),
    #[error("Failed to open the file for writing: {0:#?}")]
    FileOpen(std::io::Error),
    #[error("Failed during write: {0:#?}")]
    Write(std::io::Error),
}

/// Handle for a background async write task.
///
/// Errors are not recoverable.
pub type WriteHandle = JoinHandle<Result<(), Report<WriteError>>>;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("Lost connection to the writer thread: ({0:#?})")]
    LostWriter((SendError<Bytes>, WriteHandle)),
    #[error("Failure while pulling from remote: {0:#?}")]
    Reqwest(reqwest::Error),
}

/// Download the content of `response` into `base_path`.
///
/// The content of response will be written into `base_path`/(URL translation).
/// With base_path "/home/" and response url "https://www.linux.org/index.php",
/// this will write to "/home/www.linux.org/index.php".
///
/// At return, the full write will be enqueued but not necessarily executed.
/// The download is not complete unless the handle completes with a success.
///
/// This copies the data into two places. In the worst case (return before any
/// writes complete), this occupies roughly (data size) * 2 +
/// (unused [`Vec`] capacity) on the heap.
///
/// Spawn relies explicitly on a [`tokio`] runtime.
///
/// # Parameters
/// * `response`: The [`Response`] from a `GET` request.
/// * `base_path`: The base path for all files to write into.
pub async fn download<P>(
    mut response: Response,
    base_path: P,
) -> Result<(Vec<u8>, WriteHandle), Report<DownloadError>>
where
    P: Borrow<Path>,
{
    // Strip the leading "/" from the url path and combine
    let url = add_index(&response);
    let url_path = url.path();
    let dest = base_path.borrow().join(&url_path[1..]);

    // Tokio's mpsc guarantees read out in the same order as write in
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let write_thread = spawn(async move {
        println!("Creating dest: {:#?}", dest);
        // Create parent directory
        create_dir_all(
            dest.parent()
                .ok_or(WriteError::PathCreate(ErrorKind::NotFound.into()))?,
        )
        .await
        .map_err(WriteError::PathCreate)?;

        let mut dest = File::create(dest).await.map_err(WriteError::FileOpen)?;

        // Write out all the chunks as provided.
        while let Some(chunk) = rx.recv().await {
            dest.write_all(&chunk).await.map_err(WriteError::Write)?;
        }
        dest.flush().await.map_err(WriteError::Write)?;
        dest.shutdown().await.map_err(WriteError::Write)?;
        Ok(())
    });

    let mut content = Vec::new();

    while let Some(chunk) = response.chunk().await.map_err(DownloadError::Reqwest)? {
        content.extend_from_slice(&chunk);
        if let Err(e) = tx.send(chunk) {
            return Err(DownloadError::LostWriter((e, write_thread)).into());
        }
    }

    Ok((content, write_thread))
}

#[cfg(test)]
mod tests {
    use std::{
        env::temp_dir, fs::remove_dir_all, ops::Deref, path::PathBuf, str::FromStr, sync::LazyLock,
    };

    use reqwest::get;

    use super::*;

    const GOOGLE_ROBOTS: &str = "https://www.google.com/robots.txt";
    const LINUX_HOMEPAGE: &str = "https://www.linux.org/";

    const USER_AGENT: &str = "speciam";
    static CLIENT: LazyLock<Client> =
        LazyLock::new(|| Client::builder().user_agent(USER_AGENT).build().unwrap());

    /// Temporary directory that attempts to clean itself up on [`Drop`].
    #[derive(Debug)]
    struct CleaningTemp(PathBuf);

    impl CleaningTemp {
        pub fn new() -> Self {
            Self(temp_dir())
        }
    }

    impl Deref for CleaningTemp {
        type Target = PathBuf;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Borrow<Path> for CleaningTemp {
        fn borrow(&self) -> &Path {
            self.0.borrow()
        }
    }

    impl Drop for CleaningTemp {
        fn drop(&mut self) {
            let _ = remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn remove_dup() {
        let url = Url::from_str(GOOGLE_ROBOTS).unwrap();
        let visited = RwLock::default();

        assert!(get_response(&*CLIENT, &visited, url.clone())
            .await
            .unwrap()
            .is_some());
        assert!(get_response(&*CLIENT, &visited, url)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn download_linux() {
        let temp_path = CleaningTemp::new();
        let url = Url::from_str(LINUX_HOMEPAGE).unwrap();
        let homepage_response = get(url).await.unwrap();

        let (content, write_thread) = download(homepage_response, temp_path).await.unwrap();
        assert!(!content.is_empty());
        write_thread.await.unwrap().unwrap();
    }
}
