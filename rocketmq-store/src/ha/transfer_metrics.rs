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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use crate::ha::transfer_engine::TransferEngineKind;
use crate::ha::transfer_engine::TransferStats;

#[derive(Debug, Default)]
pub struct HaTransferMetrics {
    batch_total: AtomicU64,
    bytes_total: AtomicU64,
    body_bytes_total: AtomicU64,
    write_call_total: AtomicU64,
    sendfile_call_total: AtomicU64,
    sendfile_bytes_total: AtomicU64,
    fallback_bytes_total: AtomicU64,
    partial_write_total: AtomicU64,
    bytes_engine_total: AtomicU64,
    vectored_engine_total: AtomicU64,
    sendfile_engine_total: AtomicU64,
    io_uring_engine_total: AtomicU64,
    fallback_total: AtomicU64,
    fallbacks: Mutex<BTreeMap<TransferFallbackKey, u64>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransferEngineTotals {
    pub bytes: u64,
    pub vectored: u64,
    pub sendfile: u64,
    pub io_uring: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HaTransferMetricsSnapshot {
    pub batch_total: u64,
    pub bytes_total: u64,
    pub body_bytes_total: u64,
    pub write_call_total: u64,
    pub sendfile_call_total: u64,
    pub sendfile_bytes_total: u64,
    pub fallback_bytes_total: u64,
    pub partial_write_total: u64,
    pub engine_totals: TransferEngineTotals,
    pub fallback_total: u64,
    pub fallbacks: Vec<TransferFallbackSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferFallbackSnapshot {
    pub from: TransferEngineKind,
    pub to: TransferEngineKind,
    pub reason: &'static str,
    pub count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TransferFallbackKey {
    from: TransferEngineKind,
    to: TransferEngineKind,
    reason: &'static str,
}

impl HaTransferMetrics {
    pub fn record_transfer(&self, stats: &TransferStats) {
        self.batch_total.fetch_add(stats.frame_count as u64, Ordering::Relaxed);
        self.bytes_total
            .fetch_add(stats.bytes_written as u64, Ordering::Relaxed);
        self.body_bytes_total
            .fetch_add(stats.body_bytes as u64, Ordering::Relaxed);
        self.write_call_total
            .fetch_add(stats.write_call_count as u64, Ordering::Relaxed);
        self.sendfile_call_total
            .fetch_add(stats.sendfile_call_count as u64, Ordering::Relaxed);
        self.sendfile_bytes_total
            .fetch_add(stats.sendfile_bytes as u64, Ordering::Relaxed);
        self.fallback_bytes_total
            .fetch_add(stats.fallback_bytes as u64, Ordering::Relaxed);
        self.partial_write_total
            .fetch_add(stats.partial_write_count as u64, Ordering::Relaxed);

        match stats.engine {
            TransferEngineKind::Bytes => &self.bytes_engine_total,
            TransferEngineKind::Vectored => &self.vectored_engine_total,
            TransferEngineKind::Sendfile => &self.sendfile_engine_total,
            TransferEngineKind::IoUring => &self.io_uring_engine_total,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_fallback(&self, from: TransferEngineKind, to: TransferEngineKind, reason: &'static str) {
        self.fallback_total.fetch_add(1, Ordering::Relaxed);

        let key = TransferFallbackKey { from, to, reason };
        let mut fallbacks = self.fallbacks.lock().expect("lock HA transfer fallback metrics");
        *fallbacks.entry(key).or_insert(0) += 1;
    }

    pub fn snapshot(&self) -> HaTransferMetricsSnapshot {
        let fallbacks = self
            .fallbacks
            .lock()
            .expect("lock HA transfer fallback metrics")
            .iter()
            .map(|(key, count)| TransferFallbackSnapshot {
                from: key.from,
                to: key.to,
                reason: key.reason,
                count: *count,
            })
            .collect();

        HaTransferMetricsSnapshot {
            batch_total: self.batch_total.load(Ordering::Relaxed),
            bytes_total: self.bytes_total.load(Ordering::Relaxed),
            body_bytes_total: self.body_bytes_total.load(Ordering::Relaxed),
            write_call_total: self.write_call_total.load(Ordering::Relaxed),
            sendfile_call_total: self.sendfile_call_total.load(Ordering::Relaxed),
            sendfile_bytes_total: self.sendfile_bytes_total.load(Ordering::Relaxed),
            fallback_bytes_total: self.fallback_bytes_total.load(Ordering::Relaxed),
            partial_write_total: self.partial_write_total.load(Ordering::Relaxed),
            engine_totals: TransferEngineTotals {
                bytes: self.bytes_engine_total.load(Ordering::Relaxed),
                vectored: self.vectored_engine_total.load(Ordering::Relaxed),
                sendfile: self.sendfile_engine_total.load(Ordering::Relaxed),
                io_uring: self.io_uring_engine_total.load(Ordering::Relaxed),
            },
            fallback_total: self.fallback_total.load(Ordering::Relaxed),
            fallbacks,
        }
    }
}
