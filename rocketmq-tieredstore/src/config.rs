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

use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieredStorageLevel {
    Disable = 0,
    NotInDisk = 1,
    NotInMem = 2,
    Force = 3,
}

impl TieredStorageLevel {
    #[inline]
    pub fn enabled(self) -> bool {
        !matches!(self, Self::Disable)
    }

    #[inline]
    pub fn check(self, target: Self) -> bool {
        self as u8 >= target as u8
    }
}

#[derive(Debug, Clone)]
pub struct TieredStoreConfig {
    pub storage_level: TieredStorageLevel,
    pub store_path_root_dir: PathBuf,
    pub commit_log_segment_size: u64,
    pub consume_queue_segment_size: u64,
    pub index_file_max_hash_slot_num: u32,
    pub index_file_max_index_num: u32,
    pub message_index_enable: bool,
    pub backend_provider: String,
    pub metadata_provider: String,
    pub delete_file_enable: bool,
    pub file_reserved_time: Duration,
    pub delete_file_interval: Duration,
    pub commit_log_rolling_interval: Duration,
    pub commit_log_rolling_min_size: u64,
    pub group_commit: bool,
    pub group_commit_timeout: Duration,
    pub group_commit_count: usize,
    pub group_commit_size: usize,
    pub max_group_commit_count: usize,
    pub max_pending_tasks: usize,
    pub read_ahead_cache_enable: bool,
    pub read_ahead_message_count: usize,
    pub read_ahead_message_size: usize,
    pub read_ahead_cache_expire: Duration,
    pub crc_check_enable: bool,
}

impl Default for TieredStoreConfig {
    fn default() -> Self {
        Self {
            storage_level: TieredStorageLevel::NotInDisk,
            store_path_root_dir: default_store_root(),
            commit_log_segment_size: 1024 * 1024 * 1024,
            consume_queue_segment_size: 100 * 1024 * 1024,
            index_file_max_hash_slot_num: 5_000_000,
            index_file_max_index_num: 20_000_000,
            message_index_enable: true,
            backend_provider: "posix".to_owned(),
            metadata_provider: "json".to_owned(),
            delete_file_enable: true,
            file_reserved_time: Duration::from_secs(72 * 60 * 60),
            delete_file_interval: Duration::from_secs(60 * 60),
            commit_log_rolling_interval: Duration::from_secs(24 * 60 * 60),
            commit_log_rolling_min_size: 16 * 1024 * 1024,
            group_commit: true,
            group_commit_timeout: Duration::from_secs(30),
            group_commit_count: 4096,
            group_commit_size: 4 * 1024 * 1024,
            max_group_commit_count: 10_000,
            max_pending_tasks: 10_000,
            read_ahead_cache_enable: true,
            read_ahead_message_count: 4096,
            read_ahead_message_size: 16 * 1024 * 1024,
            read_ahead_cache_expire: Duration::from_secs(15),
            crc_check_enable: false,
        }
    }
}

fn default_store_root() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("store")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_java_tieredstore_defaults() {
        let config = TieredStoreConfig::default();

        assert_eq!(config.storage_level, TieredStorageLevel::NotInDisk);
        assert_eq!(config.commit_log_segment_size, 1024 * 1024 * 1024);
        assert_eq!(config.consume_queue_segment_size, 100 * 1024 * 1024);
        assert_eq!(config.index_file_max_hash_slot_num, 5_000_000);
        assert_eq!(config.index_file_max_index_num, 20_000_000);
        assert!(config.message_index_enable);
        assert!(config.delete_file_enable);
        assert_eq!(config.file_reserved_time, Duration::from_secs(72 * 60 * 60));
        assert_eq!(config.delete_file_interval, Duration::from_secs(60 * 60));
        assert_eq!(config.commit_log_rolling_interval, Duration::from_secs(24 * 60 * 60));
        assert_eq!(config.commit_log_rolling_min_size, 16 * 1024 * 1024);
        assert!(config.group_commit);
        assert_eq!(config.group_commit_timeout, Duration::from_secs(30));
        assert_eq!(config.group_commit_count, 4096);
        assert_eq!(config.group_commit_size, 4 * 1024 * 1024);
        assert_eq!(config.max_group_commit_count, 10_000);
        assert_eq!(config.max_pending_tasks, 10_000);
        assert!(config.read_ahead_cache_enable);
        assert_eq!(config.read_ahead_message_count, 4096);
        assert_eq!(config.read_ahead_message_size, 16 * 1024 * 1024);
        assert_eq!(config.read_ahead_cache_expire, Duration::from_secs(15));
        assert!(!config.crc_check_enable);
    }
}
