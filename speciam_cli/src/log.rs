use std::{
    fs::File,
    io::Write,
    sync::mpsc::{self, channel, Receiver, Sender},
};

use futures::channel::mpsc::unbounded;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Debug)]
enum WriteCMD {
    Data(String),
    Flush,
}

#[derive(Debug)]
struct JsonAsyncRecv {
    rx: Receiver<WriteCMD>,
    file: File,
}

impl JsonAsyncRecv {
    pub fn exec(mut self) {
        while let Ok(cmd) = self.rx.recv() {
            match cmd {
                WriteCMD::Data(mut data) => {
                    // Remove extra slashes
                    while data.contains("\\\\") {
                        data = data.replace("\\\\", "\\");
                    }
                    self.file.write_all(data.as_bytes())
                }
                WriteCMD::Flush => self.file.flush(),
            }
            .unwrap()
        }
    }
}

#[derive(Debug, Clone)]
pub struct JsonAsync {
    tx: Sender<WriteCMD>,
}

impl JsonAsync {
    pub fn new(file: File) -> Self {
        let (tx, rx) = channel();
        tokio::task::spawn_blocking(|| JsonAsyncRecv { rx, file }.exec());
        JsonAsync { tx }
    }
}

impl Write for JsonAsync {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tx
            .send(WriteCMD::Data(
                std::str::from_utf8(buf).unwrap().to_string(),
            ))
            .unwrap();
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.tx.send(WriteCMD::Flush).unwrap();
        Ok(())
    }
}

impl MakeWriter<'_> for JsonAsync {
    type Writer = Self;
    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}
