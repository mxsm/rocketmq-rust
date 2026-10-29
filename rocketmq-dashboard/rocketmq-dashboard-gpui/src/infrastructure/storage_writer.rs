// Copyright 2026 The RocketMQ Rust Authors
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

//! Process-lifetime writer ownership for the directory shared by all local stores.

use std::{
    fs::{File, OpenOptions, TryLockError},
    path::Path,
};

use super::config_store::ConfigStoreFailure;

/// The sidecar is never replaced or removed, so every process locks the same file object.
pub(super) struct StorageWriter {
    _file: File,
}

impl StorageWriter {
    pub fn acquire(data_path: &Path) -> Result<Self, ConfigStoreFailure> {
        let directory = data_path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(directory)
            .map_err(|source| ConfigStoreFailure::io("create data directory", directory.to_path_buf(), source))?;
        let path = directory.join(".dashboard-writer.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| ConfigStoreFailure::io("open writer lock", path.clone(), source))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(ConfigStoreFailure::writer_unavailable()),
            Err(TryLockError::Error(source)) => Err(ConfigStoreFailure::io("acquire writer lock", path, source)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIRECTORY_ENV: &str = "ROCKETMQ_GPUI_WRITER_TEST_DIRECTORY";
    const EXPECT_LOCKED_ENV: &str = "ROCKETMQ_GPUI_WRITER_TEST_EXPECT_LOCKED";

    #[test]
    fn writer_child_process() {
        let Some(directory) = std::env::var_os(DIRECTORY_ENV) else {
            return;
        };
        let result = StorageWriter::acquire(&std::path::PathBuf::from(directory).join("config.json"));
        assert_eq!(result.is_err(), std::env::var_os(EXPECT_LOCKED_ENV).is_some());
    }

    #[test]
    fn another_process_is_rejected_and_release_allows_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        let owner = StorageWriter::acquire(&path).unwrap();
        let child = |locked: bool| {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "infrastructure::storage_writer::tests::writer_child_process",
                    "--nocapture",
                ])
                .env(DIRECTORY_ENV, directory.path());
            if locked {
                command.env(EXPECT_LOCKED_ENV, "1");
            } else {
                command.env_remove(EXPECT_LOCKED_ENV);
            }
            let output = command.output().unwrap();
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stdout));
        };
        child(true);
        drop(owner);
        child(false);
    }
}
