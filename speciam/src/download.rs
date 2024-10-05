use std::{
    borrow::Borrow,
    fs::{self, create_dir_all, File},
    io::{ErrorKind, Write},
    iter::FusedIterator,
    path::{Path, PathBuf},
    sync::{
        atomic::AtomicU64,
        mpsc::{self, SendError},
        Arc, Mutex,
    },
};

use bare_err_tree::err_tree;
use bytes::Bytes;
use reqwest::{Client, Response, Url};
use thiserror::Error;
use tokio::task::{spawn_blocking, JoinHandle};

#[cfg(feature = "file_renaming")]
use uuid::Uuid;

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
    let page_response = client
        .borrow()
        .get(url.clone())
        .send()
        .await?
        .error_for_status()?;

    // Return unique urls.
    let new_url = add_index(&page_response);
    let unique_urls = if url != new_url {
        UniqueUrls::Two([url, new_url])
    } else {
        UniqueUrls::One(url)
    };
    Ok((page_response, unique_urls))
}

#[derive(Debug)]
#[err_tree]
#[derive(Error)]
#[error("File: {file:?}")]
pub struct FileErr {
    pub file: PathBuf,
    #[dyn_err]
    #[source]
    pub source: std::io::Error,
}

impl FileErr {
    #[track_caller]
    pub fn new(file: PathBuf, source: std::io::Error) -> Self {
        Self::_tree(file, source)
    }
}

/// Background write failure.
#[derive(Debug, Error)]
#[err_tree(WriteErrorWrap)]
pub enum WriteError {
    #[error("failed to create the path")]
    #[tree_err]
    PathCreate(#[source] FileErr),
    #[error("tried to create duplicate file")]
    #[tree_err]
    DupFile(#[source] FileErr),
    #[tree_err]
    #[error("failed to open the file for writing")]
    FileOpen(#[source] FileErr),
    #[error("failed during write")]
    #[dyn_err]
    Write(#[source] std::io::Error),
}

/// Handle for a background async write task.
///
/// Also encodes predicted size (from headers) and final written progress.
/// Errors are not recoverable.
#[derive(Debug)]
pub struct WriteHandle {
    pub handle: JoinHandle<Result<(Option<PathBuf>, u64), WriteErrorWrap>>,
    pub size_prediction: Option<u64>,
    pub target: PathBuf,
}

#[derive(Debug)]
#[err_tree]
#[derive(Error)]
#[error("lost connection to the writer thread")]
pub struct LostWriter {
    #[dyn_err]
    send_err: SendError<Bytes>,
    #[tree_err]
    #[source]
    pub handle_err: WriteErrorWrap,
}

impl LostWriter {
    #[track_caller]
    pub fn new(send_err: SendError<Bytes>, handle_err: WriteErrorWrap) -> Self {
        LostWriter::_tree(send_err, handle_err)
    }
}

#[derive(Debug, Error)]
#[err_tree(DownloadErrorWrap)]
pub enum DownloadError {
    #[tree_err]
    #[error("{0:?}")]
    LostWriter(#[source] LostWriter),
    #[dyn_err]
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
/// * `write_updates`: Optional counter to increment mid-write.
///
/// # Return
/// * All bytes from remote and the background write handle.
pub async fn download<P>(
    mut response: Response,
    base_path: P,
    write_updates: Option<Arc<AtomicU64>>,
) -> Result<(Vec<u8>, Option<WriteHandle>), DownloadErrorWrap>
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
        if let Some(url_query) = url.query() {
            dest.push(url_query);
        };
        if let Some(url_fragment) = url.fragment() {
            dest.push(url_fragment);
        };

        // stdlib's mpsc guarantees read out in the same order as write in
        let (tx, rx) = mpsc::channel::<Bytes>();
        let base_path_clone = base_path.borrow().to_path_buf();
        let dest_clone = dest.clone();
        let write_thread = spawn_blocking(move || {
            let base_path = base_path_clone;
            let dest = dest_clone;

            // Temporarily relocate when directory name == non-directory item
            let path_pairs = if let Some(parent) = dest.parent() {
                if parent.exists() && !parent.is_dir() {
                    let mut parent_temp = parent.as_os_str().to_os_string();
                    parent_temp.push("_");
                    let parent_temp = PathBuf::from(parent_temp);

                    let _ = fs::rename(parent, &parent_temp);
                    let post_locate = parent.join(parent.file_name().unwrap());

                    Some((parent_temp, post_locate))
                } else {
                    None
                }
            } else {
                None
            };

            // Create parent directory
            let _ = create_dir_all(dest.parent().ok_or(WriteError::PathCreate(FileErr::new(
                dest.clone(),
                ErrorKind::NotFound.into(),
            )))?);

            // Move conflicting names into final location
            if let Some((temp, index)) = path_pairs {
                let _ = fs::rename(temp, index);
            }

            let dest_noconflict = if dest.is_dir() {
                let _ = create_dir_all(&dest);
                dest.join(dest.file_name().unwrap())
            } else {
                dest.clone()
            };

            let mut alt_output_path = None;

            let mut dest_file = match File::create_new(&dest_noconflict) {
                Ok(dest_file) => dest_file,
                // Filename too long on UNIX
                #[cfg(all(target_family = "unix", feature = "file_renaming"))]
                Err(e) if e.raw_os_error() == Some(36) => {
                    alt_output_path = Some(base_path.join(Uuid::new_v4().to_string()));
                    File::create_new(alt_output_path.as_ref().unwrap()).map_err(|e| {
                        WriteError::FileOpen(FileErr::new(alt_output_path.clone().unwrap(), e))
                    })?
                }

                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(
                        WriteError::DupFile(FileErr::new(dest_noconflict.clone(), e)).into(),
                    )
                }
                Err(e) => {
                    return Err(
                        WriteError::FileOpen(FileErr::new(dest_noconflict.clone(), e)).into(),
                    )
                }
            };

            let mut write_counter = 0;
            // Write out all the chunks as provided.
            while let Ok(chunk) = rx.recv() {
                write_counter += chunk.len() as u64;

                if let Some(write_updates) = &write_updates {
                    write_updates
                        .fetch_add(chunk.len() as u64, std::sync::atomic::Ordering::Release);
                };

                dest_file.write_all(&chunk).map_err(WriteError::Write)?;
            }
            dest_file.flush().map_err(WriteError::Write)?;
            Ok((alt_output_path, write_counter))
        });
        let write_handle = WriteHandle {
            handle: write_thread,
            size_prediction: response.content_length(),
            target: dest,
        };

        let mut content = Vec::new();

        while let Some(chunk) = response.chunk().await.map_err(DownloadError::Reqwest)? {
            content.extend_from_slice(&chunk);
            if let Err(e) = tx.send(chunk) {
                if let Err(handle_err) = write_handle.handle.await.unwrap() {
                    return Err(DownloadError::LostWriter(LostWriter::new(e, handle_err)).into());
                } else {
                    return Ok((content, None));
                }
            }
        }

        Ok((content, Some(write_handle)))
    } else {
        Err(DownloadError::NoDomain(url).into())
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

        let (content, write_thread) = download(homepage_response, temp_path, None).await.unwrap();
        assert!(!content.is_empty());
        write_thread.unwrap().handle.await.unwrap().unwrap();
    }
}
