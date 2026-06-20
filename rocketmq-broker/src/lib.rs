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
#![allow(incomplete_features)]
#![allow(clippy::mut_from_ref)]
#![allow(clippy::result_large_err)]
#![recursion_limit = "512"]

pub use broker_bootstrap::BrokerBootstrap;
pub use broker_bootstrap::Builder;
pub use proxy_facade::ProxyBrokerFacade;

pub mod command;
pub mod proxy_facade;
pub mod send_message_constants;

// Re-export types needed for benchmarking
#[doc(hidden)]
pub mod bench_support {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::filter::expression_type::ExpressionType;
    use rocketmq_rust::schedule::simple_scheduler::ScheduledShutdownReport;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use serde::Serialize;

    pub use crate::client::client_channel_info::ClientChannelInfo;
    pub use crate::client::consumer_group_event::ConsumerGroupEvent;
    pub use crate::client::consumer_group_info::ConsumerGroupInfo;
    pub use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
    pub use crate::client::manager::consumer_manager::ConsumerManager;
    pub use crate::filter::manager::consumer_filter_manager::ConsumerFilterManagerStatsSnapshot;

    pub struct ConsumerFilterBenchHarness {
        manager: crate::filter::manager::consumer_filter_manager::ConsumerFilterManager,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerRuntimeLifecycleProbe {
        pub pending_task_count: usize,
        pub scheduled_task_count_before_shutdown: usize,
        pub scheduled_task_count_after_shutdown: usize,
        pub scheduled_task_drop_count: usize,
        pub scheduled_shutdown_elapsed_us: u128,
        pub scheduled_shutdown_report: BrokerScheduledShutdownProbe,
        pub basic_shutdown_elapsed_us: u128,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerScheduledShutdownProbe {
        pub task_count: usize,
        pub completed: usize,
        pub aborted: usize,
        pub panicked: usize,
        pub timed_out: usize,
        pub elapsed_us: u128,
        pub healthy: bool,
    }

    impl From<ScheduledShutdownReport> for BrokerScheduledShutdownProbe {
        fn from(report: ScheduledShutdownReport) -> Self {
            Self {
                task_count: report.task_count,
                completed: report.completed,
                aborted: report.aborted,
                panicked: report.panicked,
                timed_out: report.timed_out,
                elapsed_us: report.elapsed.as_micros(),
                healthy: report.is_healthy(),
            }
        }
    }

    impl Default for ConsumerFilterBenchHarness {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ConsumerFilterBenchHarness {
        pub fn new() -> Self {
            Self {
                manager: crate::filter::manager::consumer_filter_manager::ConsumerFilterManager::new(
                    Arc::new(BrokerConfig::default()),
                    Arc::new(MessageStoreConfig::default()),
                ),
            }
        }

        pub fn resolve_sql(&self, topic: &str, group: &str, expression: &str, client_version: u64) -> bool {
            self.manager
                .resolve(
                    CheetahString::from_slice(topic),
                    CheetahString::from_slice(group),
                    Some(CheetahString::from_slice(expression)),
                    Some(CheetahString::from_static_str(ExpressionType::SQL92)),
                    client_version,
                )
                .is_some()
        }

        pub fn stats_snapshot(&self) -> ConsumerFilterManagerStatsSnapshot {
            self.manager.stats_snapshot()
        }
    }

    pub async fn run_broker_runtime_lifecycle_probe(
        root: PathBuf,
        pending_task_count: usize,
    ) -> BrokerRuntimeLifecycleProbe {
        struct DropMarker(Arc<AtomicUsize>);

        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("broker lifecycle benchmark root should be created");
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = crate::broker_runtime::BrokerRuntime::new(broker_config, message_store_config);

        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        for task_index in 0..pending_task_count {
            let started = Arc::clone(&started);
            let dropped = Arc::clone(&dropped);
            runtime
                .scheduled_task_manager()
                .add_fixed_delay_task(Duration::ZERO, Duration::from_secs(60), move |token| {
                    let started = Arc::clone(&started);
                    let dropped = Arc::clone(&dropped);
                    async move {
                        let _marker = DropMarker(dropped);
                        started.fetch_add(1, Ordering::AcqRel);
                        let _ = task_index;
                        token.cancelled().await;
                        Ok(())
                    }
                })
                .expect("broker lifecycle scheduled task should start");
        }

        tokio::time::timeout(Duration::from_secs(2), async {
            while started.load(Ordering::Acquire) < pending_task_count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("broker lifecycle scheduled tasks should start");

        let scheduled_task_count_before_shutdown = runtime.scheduled_task_manager().task_count();
        let scheduled_started_at = Instant::now();
        let scheduled_shutdown_report = runtime
            .shutdown_scheduled_tasks_with_timeout(Duration::from_secs(2))
            .await;
        let scheduled_shutdown_elapsed_us = scheduled_started_at.elapsed().as_micros();
        let scheduled_task_count_after_shutdown = runtime.scheduled_task_manager().task_count();
        let scheduled_task_drop_count = dropped.load(Ordering::Acquire);

        let basic_started_at = Instant::now();
        tokio::time::timeout(Duration::from_secs(5), runtime.shutdown_basic_service())
            .await
            .expect("broker basic service shutdown should be bounded");
        let basic_shutdown_elapsed_us = basic_started_at.elapsed().as_micros();

        let scheduled_shutdown_report = BrokerScheduledShutdownProbe::from(scheduled_shutdown_report);
        let healthy = scheduled_shutdown_report.healthy
            && scheduled_task_count_before_shutdown == pending_task_count
            && scheduled_task_count_after_shutdown == 0
            && scheduled_task_drop_count == pending_task_count;

        let _ = std::fs::remove_dir_all(root);
        BrokerRuntimeLifecycleProbe {
            pending_task_count,
            scheduled_task_count_before_shutdown,
            scheduled_task_count_after_shutdown,
            scheduled_task_drop_count,
            scheduled_shutdown_elapsed_us,
            scheduled_shutdown_report,
            basic_shutdown_elapsed_us,
            healthy,
        }
    }
}

pub(crate) mod broker;
pub(crate) mod broker_bootstrap;
pub(crate) mod broker_path_config_helper;
pub(crate) mod broker_runtime;
pub(crate) mod client;
pub(crate) mod coldctr;
pub(crate) mod config;
pub(crate) mod controller;
pub(crate) mod failover;
pub(crate) mod filter;
pub(crate) mod hook;
pub(crate) mod latency;
pub(crate) mod lite;
pub(crate) mod load_balance;
pub(crate) mod long_polling;
pub(crate) mod metrics;
pub(crate) mod mqtrace;
pub(crate) mod offset;
pub(crate) mod out_api;
pub(crate) mod plugin;
pub(crate) mod pop;
pub(crate) mod processor;
pub(crate) mod schedule;
pub(crate) mod slave;
pub(crate) mod subscription;
pub(crate) mod topic;
mod transaction;
pub(crate) mod types;
pub(crate) mod util;

pub(crate) mod auth;
