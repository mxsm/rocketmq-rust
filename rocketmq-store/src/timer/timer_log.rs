// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use rocketmq_common::UtilAll::ensure_dir_ok;

const ACTIVE_FILE_NAME: &str = "00000000000000000000";

pub struct TimerLog {
    dir_path: PathBuf,
    _file_size: usize,
    next_offset: AtomicU64,
    io_lock: Mutex<()>,
}

impl TimerLog {
    pub fn new<P: AsRef<Path>>(dir_path: P, file_size: usize) -> Self {
        Self {
            dir_path: dir_path.as_ref().to_path_buf(),
            _file_size: file_size,
            next_offset: AtomicU64::new(0),
            io_lock: Mutex::new(()),
        }
    }

    pub fn load(&self) -> std::io::Result<bool> {
        let _guard = self.io_lock.lock();
        ensure_dir_ok(self.dir_path.to_string_lossy().as_ref());
        let file = self.open_active_file()?;
        self.next_offset.store(file.metadata()?.len(), Ordering::Release);
        Ok(true)
    }

    pub fn append(&self, payload: &[u8]) -> std::io::Result<u64> {
        let _guard = self.io_lock.lock();
        ensure_dir_ok(self.dir_path.to_string_lossy().as_ref());
        let mut file = self.open_active_file()?;
        let start_offset = file.metadata()?.len();
        file.seek(SeekFrom::End(0))?;
        file.write_all(payload)?;
        self.next_offset
            .store(start_offset + payload.len() as u64, Ordering::Release);
        Ok(start_offset)
    }

    pub fn read_at(&self, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
        let _guard = self.io_lock.lock();
        let mut file = OpenOptions::new().read(true).open(self.active_file_path())?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0; length];
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    pub fn len(&self) -> std::io::Result<u64> {
        if self.active_file_path().exists() {
            Ok(self.active_file_path().metadata()?.len())
        } else {
            Ok(0)
        }
    }

    pub fn is_empty(&self) -> std::io::Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let _guard = self.io_lock.lock();
        if !self.active_file_path().exists() {
            return Ok(());
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.active_file_path())?;
        file.sync_data()
    }

    pub fn shutdown(&self) -> std::io::Result<()> {
        self.flush()
    }

    pub fn active_file_path(&self) -> PathBuf {
        self.dir_path.join(ACTIVE_FILE_NAME)
    }

    fn open_active_file(&self) -> std::io::Result<File> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .truncate(false)
            .open(self.active_file_path())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn append_and_reload_preserves_timer_log_payload() {
        let temp_dir = tempdir().unwrap();
        let timer_log = TimerLog::new(temp_dir.path().join("timerlog"), 1024);
        assert!(timer_log.load().unwrap());

        let offset = timer_log.append(b"timer-log").unwrap();
        timer_log.flush().unwrap();

        let reloaded = TimerLog::new(temp_dir.path().join("timerlog"), 1024);
        assert!(reloaded.load().unwrap());
        assert_eq!(offset, 0);
        assert_eq!(reloaded.read_at(0, 9).unwrap(), b"timer-log");
        assert_eq!(reloaded.len().unwrap(), 9);
    }
}
