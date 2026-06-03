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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RocksDbMetrics {
    pub write_count: u64,
    pub read_count: u64,
    pub batch_write_count: u64,
    pub scan_count: u64,
    pub flush_count: u64,
    pub manual_compaction_count: u64,
    pub checkpoint_count: u64,
    pub backup_count: u64,
    pub property_query_count: u64,
    pub error_count: u64,
}

#[derive(Debug, Default)]
pub(crate) struct RocksDbMetricsCollector {
    write_count: AtomicU64,
    read_count: AtomicU64,
    batch_write_count: AtomicU64,
    scan_count: AtomicU64,
    flush_count: AtomicU64,
    manual_compaction_count: AtomicU64,
    checkpoint_count: AtomicU64,
    backup_count: AtomicU64,
    property_query_count: AtomicU64,
    error_count: AtomicU64,
}

impl RocksDbMetricsCollector {
    pub(crate) fn snapshot(&self) -> RocksDbMetrics {
        RocksDbMetrics {
            write_count: self.write_count.load(Ordering::Relaxed),
            read_count: self.read_count.load(Ordering::Relaxed),
            batch_write_count: self.batch_write_count.load(Ordering::Relaxed),
            scan_count: self.scan_count.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            manual_compaction_count: self.manual_compaction_count.load(Ordering::Relaxed),
            checkpoint_count: self.checkpoint_count.load(Ordering::Relaxed),
            backup_count: self.backup_count.load(Ordering::Relaxed),
            property_query_count: self.property_query_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_write(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_read(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_batch_write(&self) {
        self.batch_write_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_scan(&self) {
        self.scan_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_flush(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_manual_compaction(&self) {
        self.manual_compaction_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_checkpoint(&self) {
        self.checkpoint_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_backup(&self) {
        self.backup_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_property_query(&self) {
        self.property_query_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }
}
