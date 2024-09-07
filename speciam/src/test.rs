use std::{
    borrow::Borrow,
    env::temp_dir,
    fs::remove_dir_all,
    ops::Deref,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use reqwest::Client;

pub const RUST_HOMEPAGE: &str = "https://www.rust-lang.org/";
pub const GOOGLE_ROBOTS: &str = "https://www.google.com/robots.txt";
pub const LINUX_HOMEPAGE: &str = "https://www.linux.org/";
pub const GOOGLE_HOMEPAGE: &str = "https://www.google.com/";
pub const YAHOO_HOMEPAGE: &str = "https://www.yahoo.com/";

pub const USER_AGENT: &str = "speciam";
pub static CLIENT: LazyLock<Client> =
    LazyLock::new(|| Client::builder().user_agent(USER_AGENT).build().unwrap());

/// Temporary directory that attempts to clean itself up on [`Drop`].
#[derive(Debug)]
pub struct CleaningTemp(PathBuf);

impl CleaningTemp {
    pub fn new() -> Self {
        Self(temp_dir())
    }
}

impl Default for CleaningTemp {
    fn default() -> Self {
        Self::new()
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
