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

use std::fs;
use std::path::Path;
use std::sync::LazyLock;

use sysinfo::System;
use tracing::error;
use tracing::info;

pub struct StoreUtil;

pub static TOTAL_PHYSICAL_MEMORY_SIZE: LazyLock<u64> = LazyLock::new(StoreUtil::get_total_physical_memory_size);

impl StoreUtil {
    pub fn get_total_physical_memory_size() -> u64 {
        let mut sys = System::new_all();
        sys.refresh_all();
        let physical_total = sys.total_memory();
        physical_total * 1024 // Convert from kilobytes to bytes
    }
}

static MULTI_PATH_SPLITTER: LazyLock<String> =
    LazyLock::new(|| std::env::var("rocketmq.broker.multiPathSplitter").unwrap_or_else(|_| ",".to_string()));

/// Formats a file start offset as the zero-padded, 20-digit file name.
#[inline]
pub(crate) fn offset_to_file_name(offset: u64) -> String {
    format!("{offset:020}")
}

/// Creates each directory in `dir_name`, which may list several paths
/// separated by the configured multi-path splitter.
pub(crate) fn ensure_dir_ok(dir_name: &str) {
    if dir_name.is_empty() {
        return;
    }
    let multi_path_splitter = MULTI_PATH_SPLITTER.as_str();
    if dir_name.contains(multi_path_splitter) {
        for dir in dir_name.trim().split(multi_path_splitter) {
            create_dir_if_not_exist(dir);
        }
    } else {
        create_dir_if_not_exist(dir_name);
    }
}

fn create_dir_if_not_exist(dir_name: &str) {
    let path = Path::new(dir_name);
    if !path.exists() {
        match fs::create_dir_all(path) {
            Ok(_) => info!("{} mkdir OK", dir_name),
            Err(_) => info!("{} mkdir Failed", dir_name),
        }
    }
}

/// Removes `path` if it is an empty directory.
pub(crate) fn delete_empty_directory<P: AsRef<Path>>(path: P) {
    let path = path.as_ref();
    if !path.is_dir() {
        return;
    }
    match fs::read_dir(path) {
        Ok(entries) => {
            if entries.count() == 0 {
                match fs::remove_dir(path) {
                    Ok(_) => info!("delete empty directory, {}", path.display()),
                    Err(e) => error!("Error deleting directory: {}", e),
                }
            }
        }
        Err(e) => error!("Error reading directory: {}", e),
    }
}

/// Returns whether `path` exists.
pub(crate) fn is_path_exists(path: &str) -> bool {
    Path::new(path).exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_to_file_name_is_zero_padded_to_twenty_digits() {
        assert_eq!(offset_to_file_name(123), "00000000000000000123");
        assert_eq!(offset_to_file_name(0), "00000000000000000000");
    }

    #[test]
    fn ensure_dir_ok_creates_every_listed_directory() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        let dirs = format!(
            "{}{}{}",
            first.display(),
            MULTI_PATH_SPLITTER.as_str(),
            second.display()
        );

        ensure_dir_ok(&dirs);

        assert!(first.is_dir());
        assert!(second.is_dir());
        assert!(is_path_exists(first.to_str().unwrap()));
    }

    #[test]
    fn delete_empty_directory_keeps_a_directory_with_entries() {
        let root = tempfile::tempdir().unwrap();
        let empty = root.path().join("empty");
        let full = root.path().join("full");
        fs::create_dir(&empty).unwrap();
        fs::create_dir(&full).unwrap();
        fs::write(full.join("file"), b"x").unwrap();

        delete_empty_directory(&empty);
        delete_empty_directory(&full);

        assert!(!empty.exists());
        assert!(full.exists());
        assert!(!is_path_exists(empty.to_str().unwrap()));
    }
}
