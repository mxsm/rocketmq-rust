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

use rocketmq_store_local::config::FlushDiskType;
use rocketmq_store_local::mapped_file::allocation_policy::mapped_file_allocation_capacity;
use rocketmq_store_local::mapped_file::allocation_policy::MappedFileAllocationPoolSnapshot;
use rocketmq_store_local::mapped_file::allocation_policy::MappedFileWarmupConfig;

#[test]
fn warmup_config_preserves_disabled_defaults() {
    let config = MappedFileWarmupConfig::disabled();

    assert!(!config.should_warm(u64::MAX));
    assert_eq!(config.flush_disk_type(), FlushDiskType::AsyncFlush);
    assert_eq!(config.flush_least_pages(), 0);
}

#[test]
fn warmup_config_applies_commitlog_size_threshold_and_flush_values() {
    let config = MappedFileWarmupConfig::new(true, FlushDiskType::SyncFlush, 1024, 3);

    assert!(!config.should_warm(1023));
    assert!(config.should_warm(1024));
    assert_eq!(config.flush_disk_type(), FlushDiskType::SyncFlush);
    assert_eq!(config.flush_least_pages(), 3);
}

#[test]
fn allocation_capacity_uses_default_without_an_active_fast_fail_pool() {
    let snapshot = MappedFileAllocationPoolSnapshot::new(1, 4);

    assert_eq!(mapped_file_allocation_capacity(2, false, true, Some(snapshot)), 2);
    assert_eq!(mapped_file_allocation_capacity(2, true, false, Some(snapshot)), 2);
    assert_eq!(mapped_file_allocation_capacity(2, true, true, None), 2);
}

#[test]
fn allocation_capacity_saturates_available_buffers_by_queued_requests() {
    assert_eq!(
        mapped_file_allocation_capacity(2, true, true, Some(MappedFileAllocationPoolSnapshot::new(5, 3)),),
        2
    );
    assert_eq!(
        mapped_file_allocation_capacity(2, true, true, Some(MappedFileAllocationPoolSnapshot::new(1, 4)),),
        0
    );
}
