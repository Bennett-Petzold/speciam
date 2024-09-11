use std::{
    borrow::Borrow,
    io::ErrorKind,
    iter::FusedIterator,
    path::{Path, PathBuf},
};

use bytes::Bytes;
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

/// All unique [`Url`]s.
#[derive(Debug, Clone)]
pub enum UniqueUrls {
    One(Url),
    Two([Url; 2]),
}

/// Iterator through all unique [`Url`]s.
#[derive(Debug)]
pub struct UniqueUrlsIter {
    inner: Option<UniqueUrls>,
}

impl From<UniqueUrls> for UniqueUrlsIter {
    fn from(value: UniqueUrls) -> Self {
        Self { inner: Some(value) }
    }
}

impl Iterator for UniqueUrlsIter {
    type Item = Url;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.clone()? {
            UniqueUrls::One(x) => {
                let ret = Some(x);
                self.inner = None;
                ret
            }
            UniqueUrls::Two([x, y]) => {
                let ret = Some(y);
                self.inner = Some(UniqueUrls::One(x));
                ret
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self.inner {
            None => 0,
            Some(UniqueUrls::One(_)) => 1,
            Some(UniqueUrls::Two(_)) => 2,
        };
        (remaining, Some(remaining))
    }
}

impl FusedIterator for UniqueUrlsIter {}

/// Return the page response, if the URL and resolved URL are unique.
///
/// If the URL and/or resolved URL is unique, they will be added to `visited`.
///
/// # Parameters
/// * `client`: [`Client`] to use for this operation.
/// * `url`: The potentially unique [`Url`] to pull.
pub async fn get_response<C>(client: C, url: Url) -> Result<(Response, UniqueUrls), reqwest::Error>
where
    C: Borrow<Client>,
{
    let page_response = client.borrow().get(url.clone()).send().await?;

    // Return unique urls.
    let new_url = add_index(&page_response);
    let unique_urls = if url != new_url {
        UniqueUrls::Two([url, new_url])
    } else {
        UniqueUrls::One(url)
    };
    Ok((page_response, unique_urls))
}

#[derive(Debug, Error)]
#[error("File: {file:?}")]
pub struct FileErr {
    file: PathBuf,
    #[source]
    source: std::io::Error,
}

/// Background write failure.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("failed to create the path")]
    PathCreate(#[source] FileErr),
    #[error("failed to open the file for writing")]
    FileOpen(#[source] FileErr),
    #[error("failed during write")]
    Write(#[source] std::io::Error),
}

/// Handle for a background async write task.
///
/// Errors are not recoverable.
pub type WriteHandle = JoinHandle<Result<(), WriteError>>;

#[derive(Debug, Error)]
#[error("lost connection to the writer thread")]
pub struct LostWriter {
    send_err: SendError<Bytes>,
    #[source]
    handle_err: WriteError,
}

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("{0:?}")]
    LostWriter(#[source] LostWriter),
    #[error("failure while pulling from remote")]
    Reqwest(#[source] reqwest::Error),
    #[error("no top-level domain for URL: {0:?}")]
    NoDomain(Url),
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
///
/// # Return
/// * All bytes from remote and the background write handle.
pub async fn download<P>(
    mut response: Response,
    base_path: P,
) -> Result<(Vec<u8>, WriteHandle), DownloadError>
where
    P: Borrow<Path>,
{
    // Strip the leading "/" from the url path and combine
    let url = add_index(&response);
    if let Some(domain) = url.domain() {
        let mut dest = base_path.borrow().join(domain);
        if let Some(url_parts) = url.path_segments() {
            for part in url_parts {
                dest.push(part);
            }
        };

        // Tokio's mpsc guarantees read out in the same order as write in
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
        let write_thread = spawn(async move {
            // Create parent directory
            let _ = create_dir_all(dest.parent().ok_or(WriteError::PathCreate(FileErr {
                file: dest.clone(),
                source: ErrorKind::NotFound.into(),
            }))?)
            .await;

            let mut dest = File::create(&dest).await.map_err(|e| {
                WriteError::FileOpen(FileErr {
                    file: dest.clone(),
                    source: e,
                })
            })?;

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
                return Err(DownloadError::LostWriter(LostWriter {
                    send_err: e,
                    handle_err: write_thread.await.unwrap().unwrap_err(),
                }));
            }
        }

        Ok((content, write_thread))
    } else {
        Err(DownloadError::NoDomain(url))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use reqwest::get;

    use crate::test::{CleaningTemp, LINUX_HOMEPAGE};

    use super::*;

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
