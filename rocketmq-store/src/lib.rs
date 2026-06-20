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

#![allow(dead_code)]
#![allow(unused_variables)]

pub mod base;
pub mod config;
pub mod consume_queue;
pub mod filter;
pub mod ha;
pub mod hook;
mod index;
mod kv;
pub mod log_file;
pub(crate) mod message_encoder;
pub mod message_store;
pub mod pop;
pub mod queue;
#[cfg(feature = "rocksdb_store")]
pub mod rocksdb;
pub(crate) mod runtime;
pub(crate) mod services;
pub mod stats;
pub mod store;
pub mod store_error;
pub mod store_path_config_helper;
#[cfg(feature = "tieredstore")]
pub mod tieredstore;
pub mod timer;
pub mod utils;

#[doc(hidden)]
pub mod bench_support {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use futures_util::future::join_all;
    use rocketmq_runtime::BlockingExecutorSnapshot;
    use rocketmq_runtime::ShutdownReport;
    use serde::Serialize;

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreBlockingIoProbe {
        pub task_count: usize,
        pub elapsed_us: u128,
        pub max_active: usize,
        pub queue_wait_min_us: u128,
        pub queue_wait_p50_us: u128,
        pub queue_wait_p95_us: u128,
        pub queue_wait_p99_us: u128,
        pub queue_wait_max_us: u128,
        pub snapshot: BlockingExecutorSnapshot,
        pub shutdown_report: ShutdownReport,
        pub healthy: bool,
    }

    pub async fn run_store_blocking_io_probe(task_count: usize, work_duration: Duration) -> StoreBlockingIoProbe {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let started_at = Instant::now();
        let runs = (0..task_count)
            .map(|_| {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let submitted_at = Instant::now();
                crate::runtime::spawn_io("store blocking benchmark", move || {
                    let queue_wait_us = submitted_at.elapsed().as_micros();
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    std::thread::sleep(work_duration);
                    active.fetch_sub(1, Ordering::SeqCst);
                    queue_wait_us
                })
            })
            .collect::<Vec<_>>();

        let mut queue_waits = Vec::with_capacity(task_count);
        for result in join_all(runs).await {
            queue_waits.push(result.expect("store blocking benchmark task should complete"));
        }
        let elapsed_us = started_at.elapsed().as_micros();
        let snapshot = crate::runtime::blocking_snapshot().expect("store blocking snapshot should be available");
        assert_eq!(snapshot.blocking_still_running, 0, "{snapshot:?}");
        assert!(snapshot.tasks.is_empty(), "{snapshot:?}");

        let mut shutdown_report = ShutdownReport::new("rocketmq-store.blocking", Duration::ZERO);
        shutdown_report.merge_blocking(snapshot.clone());
        let healthy = shutdown_report.is_healthy() && max_active.load(Ordering::SeqCst) <= snapshot.max_concurrency;

        StoreBlockingIoProbe {
            task_count,
            elapsed_us,
            max_active: max_active.load(Ordering::SeqCst),
            queue_wait_min_us: percentile_us(&queue_waits, 0),
            queue_wait_p50_us: percentile_us(&queue_waits, 50),
            queue_wait_p95_us: percentile_us(&queue_waits, 95),
            queue_wait_p99_us: percentile_us(&queue_waits, 99),
            queue_wait_max_us: percentile_us(&queue_waits, 100),
            snapshot,
            shutdown_report,
            healthy,
        }
    }

    fn percentile_us(values: &[u128], percentile: usize) -> u128 {
        if values.is_empty() {
            return 0;
        }

        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let percentile = percentile.min(100);
        let rank = ((sorted.len() * percentile).saturating_add(99) / 100).saturating_sub(1);
        sorted[rank.min(sorted.len() - 1)]
    }
}

#[cfg(test)]
mod bench_support_tests {
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_blocking_io_probe_reports_no_running_tasks() {
        let probe = super::bench_support::run_store_blocking_io_probe(4, Duration::from_millis(1)).await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.snapshot.blocking_still_running, 0, "{probe:?}");
        assert!(probe.snapshot.tasks.is_empty(), "{probe:?}");
    }
}
