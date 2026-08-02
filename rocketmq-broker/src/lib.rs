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
#![allow(clippy::result_large_err)]
#![recursion_limit = "512"]

pub use broker_bootstrap::BrokerBootstrap;
pub use broker_bootstrap::Builder;
pub use broker_runtime::build_broker_telemetry_bootstrap_config;
pub use lifecycle::BrokerReadiness;
pub use lifecycle::BrokerStartupError;
pub use lifecycle::Configured;
pub use lifecycle::Initialized;
pub use lifecycle::Running;
pub use processor::query_message_processor::QueryMessageProcessor;
pub use processor::query_message_processor::QueryMessageStore;
pub use proxy_facade::ProxyBrokerFacade;

pub mod command;
pub mod proxy_facade;
pub mod send_message_constants;

mod lifecycle;

pub(crate) fn runtime_to_rocketmq_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::IO(std::io::Error::other(error))
}

#[cfg(test)]
pub(crate) fn test_service_context(name: impl Into<std::sync::Arc<str>>) -> rocketmq_runtime::ChildServiceContext {
    use std::sync::OnceLock;

    static OWNER: OnceLock<rocketmq_runtime::RuntimeOwner> = OnceLock::new();
    let name: std::sync::Arc<str> = name.into();
    OWNER
        .get_or_init(|| {
            rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::server_default(
                "rocketmq-broker-tests",
            ))
            .expect("broker test runtime owner should start")
        })
        .root_context()
        .component(name)
}

#[cfg(test)]
pub(crate) fn test_task_group(name: impl Into<std::sync::Arc<str>>) -> rocketmq_runtime::TaskGroup {
    test_service_context(name).task_group().clone()
}

// Re-export types needed for benchmarking
#[doc(hidden)]
pub mod bench_support {
    use std::collections::HashMap;
    use std::io;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use crate::config::broker_config::BrokerConfig;
    use crate::config::validated::ValidatedBrokerConfig;
    use crate::transaction::queue::transactional_message_bridge::resolve_op_queue;
    use cheetah_string::CheetahString;
    use futures::future::try_join_all;
    use rocketmq_model::common::filter::expression_type::ExpressionType;
    use rocketmq_model::common::message::message_queue::MessageQueue;
    use rocketmq_runtime::schedule::simple_scheduler::ScheduledShutdownReport;
    use rocketmq_runtime::ChildServiceContext;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::get_delay_offset_store_path;
    use rocketmq_store::MessageStoreConfig;
    use serde::Serialize;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Mutex;

    pub use crate::client::client_channel_info::ClientChannelInfo;
    pub use crate::client::consumer_group_event::ConsumerGroupEvent;
    pub use crate::client::consumer_group_info::ConsumerGroupInfo;
    pub use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
    pub use crate::client::manager::consumer_manager::ConsumerManager;
    pub use crate::filter::manager::consumer_filter_manager::ConsumerFilterManagerStatsSnapshot;

    fn validated_broker_config(
        broker: Arc<BrokerConfig>,
        store: Arc<MessageStoreConfig>,
    ) -> Arc<ValidatedBrokerConfig> {
        Arc::new(
            ValidatedBrokerConfig::try_from_parts(broker.as_ref().clone(), store.as_ref().clone())
                .expect("benchmark broker configuration should be valid"),
        )
    }

    pub struct ConsumerFilterBenchHarness {
        manager: crate::filter::manager::consumer_filter_manager::ConsumerFilterManager,
    }

    /// Observation from the transaction queue registry and isolated store-I/O probe.
    #[derive(Debug, Clone, Serialize)]
    pub struct TransactionQueueIoProbe {
        pub queue_count: usize,
        pub operation_count: usize,
        pub bytes_written: u64,
        pub operation_latencies_us: Vec<f64>,
        pub registry_entries: usize,
    }

    /// Exercises the production transaction queue resolver while file I/O stays outside its lock.
    pub async fn run_transaction_queue_io_probe(
        root: PathBuf,
        queue_count: usize,
        operations_per_queue: usize,
        payload_bytes: usize,
        sync_every: usize,
    ) -> io::Result<TransactionQueueIoProbe> {
        if queue_count == 0 || operations_per_queue == 0 || payload_bytes == 0 || sync_every == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transaction queue probe dimensions must be positive",
            ));
        }
        tokio::fs::create_dir_all(&root).await?;
        let registry = Arc::new(Mutex::new(HashMap::<i32, MessageQueue>::new()));
        let broker_name = CheetahString::from("architecture-performance-broker");
        let payload = Arc::new(vec![0x5au8; payload_bytes]);

        let queues = (0..queue_count).map(|queue_index| {
            let root = root.clone();
            let registry = Arc::clone(&registry);
            let broker_name = broker_name.clone();
            let payload = Arc::clone(&payload);
            async move {
                let queue_id = i32::try_from(queue_index)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "transaction queue id exceeds i32"))?;
                let path = root.join(format!("queue-{queue_id}.dat"));
                let mut file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(path)
                    .await?;
                let mut latencies = Vec::with_capacity(operations_per_queue);
                for operation in 0..operations_per_queue {
                    let started = Instant::now();
                    let queue = resolve_op_queue(&registry, queue_id, &broker_name).await;
                    if queue.queue_id() != queue_id {
                        return Err(io::Error::other("transaction queue resolver changed the queue id"));
                    }
                    file.write_all(payload.as_slice()).await?;
                    if (operation + 1).is_multiple_of(sync_every) {
                        file.sync_data().await?;
                    }
                    latencies.push(started.elapsed().as_secs_f64() * 1_000_000.0);
                }
                file.sync_data().await?;
                let bytes = u64::try_from(operations_per_queue)
                    .ok()
                    .and_then(|operations| {
                        u64::try_from(payload_bytes)
                            .ok()
                            .and_then(|payload| operations.checked_mul(payload))
                    })
                    .ok_or_else(|| io::Error::other("transaction queue probe byte count overflowed"))?;
                Ok::<_, io::Error>((bytes, latencies))
            }
        });
        let observations = try_join_all(queues).await?;
        let operation_count = queue_count
            .checked_mul(operations_per_queue)
            .ok_or_else(|| io::Error::other("transaction queue probe operation count overflowed"))?;
        let bytes_written = observations.iter().try_fold(0u64, |total, (bytes, _)| {
            total
                .checked_add(*bytes)
                .ok_or_else(|| io::Error::other("transaction queue probe byte count overflowed"))
        })?;
        let operation_latencies_us = observations.into_iter().flat_map(|(_, latencies)| latencies).collect();
        let registry_entries = registry.lock().await.len();
        Ok(TransactionQueueIoProbe {
            queue_count,
            operation_count,
            bytes_written,
            operation_latencies_us,
            registry_entries,
        })
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerRuntimeLifecycleProbe {
        pub pending_task_count: usize,
        pub scheduled_task_count_before_shutdown: usize,
        pub scheduled_task_count_after_shutdown: usize,
        pub scheduled_task_drop_count: usize,
        pub scheduled_shutdown_elapsed_us: u128,
        pub scheduled_shutdown_report: BrokerScheduledShutdownProbe,
        pub remoting_shutdown_elapsed_us: u128,
        pub remoting_shutdown_report: Option<BrokerRemotingShutdownProbe>,
        pub request_processor_shutdown_report: Option<rocketmq_runtime::ShutdownReport>,
        pub basic_shutdown_elapsed_us: u128,
        pub basic_shutdown_healthy: bool,
        pub basic_shutdown_report: BrokerBasicShutdownProbe,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerClientHousekeepingLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<rocketmq_runtime::ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerTopicQueueMappingCleanLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<rocketmq_runtime::ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerFastFailureLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub cleaned_response_received: bool,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<rocketmq_runtime::ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerSchedulePersistenceLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub persisted_offset_file: bool,
        pub shutdown_elapsed_us: u128,
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

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerRemotingShutdownProbe {
        pub task_group_healthy: bool,
        pub task_group_completed: usize,
        pub task_group_cancelled: usize,
        pub task_group_aborted: usize,
        pub task_group_timed_out: usize,
        pub server_report_count: usize,
        pub server_reports_healthy: bool,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct BrokerBasicShutdownProbe {
        pub component_names: Vec<&'static str>,
        pub healthy: bool,
        pub unhealthy_component_count: usize,
        pub unhealthy_components: Vec<&'static str>,
        pub timed_out_components: Vec<&'static str>,
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

    impl From<crate::broker_runtime::BrokerRemotingServerShutdownReport> for BrokerRemotingShutdownProbe {
        fn from(report: crate::broker_runtime::BrokerRemotingServerShutdownReport) -> Self {
            let server_report_count = report.server_reports.len();
            let server_reports_healthy = report.server_reports.iter().all(|server| {
                server
                    .report
                    .as_ref()
                    .is_some_and(rocketmq_runtime::ShutdownReport::is_healthy)
            });
            Self {
                task_group_healthy: report.task_group.is_healthy(),
                task_group_completed: report.task_group.completed,
                task_group_cancelled: report.task_group.cancelled,
                task_group_aborted: report.task_group.aborted,
                task_group_timed_out: report.task_group.timed_out,
                server_report_count,
                server_reports_healthy,
                healthy: report.is_healthy(),
            }
        }
    }

    impl From<&crate::broker_runtime::BrokerBasicServiceShutdownReport> for BrokerBasicShutdownProbe {
        fn from(report: &crate::broker_runtime::BrokerBasicServiceShutdownReport) -> Self {
            Self {
                component_names: report.component_names(),
                healthy: report.is_healthy(),
                unhealthy_component_count: report.unhealthy_component_count(),
                unhealthy_components: report.unhealthy_component_names(),
                timed_out_components: report.timed_out_component_names(),
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
        let runtime_context = RuntimeContext::from_current("broker-runtime-lifecycle-probe");
        let service_context = runtime_context.service_context("broker");
        let mut runtime = crate::broker_runtime::BrokerRuntime::new_with_validated_config(
            validated_broker_config(broker_config, message_store_config),
            service_context,
        );

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

        let remoting_probe_installed = runtime.install_remoting_server_report_probe();
        let request_processor_probe_installed = runtime.install_request_processor_task_probe();
        let basic_started_at = Instant::now();
        let mut basic_shutdown_report =
            tokio::time::timeout(Duration::from_secs(5), runtime.shutdown_basic_service_with_report())
                .await
                .expect("broker basic service shutdown should be bounded");
        let basic_shutdown_elapsed_us = basic_started_at.elapsed().as_micros();

        let scheduled_shutdown_report = BrokerScheduledShutdownProbe::from(scheduled_shutdown_report);
        let basic_shutdown_report_probe = BrokerBasicShutdownProbe::from(&basic_shutdown_report);
        let basic_shutdown_healthy = basic_shutdown_report_probe.healthy;
        let remoting_shutdown_report = basic_shutdown_report
            .remoting
            .take()
            .map(BrokerRemotingShutdownProbe::from);
        let request_processor_shutdown_report = basic_shutdown_report.request_processor.take();
        let remoting_shutdown_elapsed_us = basic_shutdown_elapsed_us;
        let healthy = scheduled_shutdown_report.healthy
            && basic_shutdown_healthy
            && remoting_probe_installed
            && remoting_shutdown_report
                .as_ref()
                .is_some_and(|report| report.healthy && report.server_report_count == 1)
            && request_processor_probe_installed
            && request_processor_shutdown_report
                .as_ref()
                .is_some_and(rocketmq_runtime::ShutdownReport::is_healthy)
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
            remoting_shutdown_elapsed_us,
            remoting_shutdown_report,
            request_processor_shutdown_report,
            basic_shutdown_elapsed_us,
            basic_shutdown_healthy,
            basic_shutdown_report: basic_shutdown_report_probe,
            healthy,
        }
    }

    pub async fn run_broker_client_housekeeping_lifecycle_probe(
        service_context: ChildServiceContext,
    ) -> BrokerClientHousekeepingLifecycleProbe {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = crate::broker_runtime::BrokerRuntime::new_with_validated_config(
            validated_broker_config(broker_config, message_store_config),
            service_context,
        );
        let inner = runtime.runtime_state_mut();
        let service = crate::client::client_housekeeping_service::ClientHousekeepingService::new(
            inner.producer_manager().connection_housekeeping(),
            inner.consumer_manager().connection_housekeeping(),
            inner.broker_stats_manager_handle(),
            inner.broker_service_context(),
        );
        service.start();

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service.shutdown_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(rocketmq_runtime::ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        BrokerClientHousekeepingLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_broker_topic_queue_mapping_clean_lifecycle_probe(
        service_context: ChildServiceContext,
    ) -> BrokerTopicQueueMappingCleanLifecycleProbe {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig {
            delete_when: "99".to_string(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = crate::broker_runtime::BrokerRuntime::new_with_validated_config(
            validated_broker_config(broker_config, message_store_config),
            service_context,
        );
        let service = runtime
            .runtime_state_mut()
            .topic_queue_mapping_clean_service_for_test()
            .expect("topic queue mapping clean service should be configured")
            .clone();
        service.start_with_schedule(Duration::ZERO, Duration::from_millis(1));

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service.shutdown_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(rocketmq_runtime::ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        BrokerTopicQueueMappingCleanLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_broker_fast_failure_lifecycle_probe(
        service_context: ChildServiceContext,
    ) -> BrokerFastFailureLifecycleProbe {
        let broker_config = Arc::new(BrokerConfig {
            broker_fast_failure_enable: true,
            wait_time_mills_in_send_queue: 0,
            wait_time_mills_in_pull_queue: 0,
            wait_time_mills_in_lite_pull_queue: 0,
            wait_time_mills_in_heartbeat_queue: 0,
            wait_time_mills_in_transaction_queue: 0,
            wait_time_mills_in_ack_queue: 0,
            wait_time_mills_in_admin_broker_queue: 0,
            ..BrokerConfig::default()
        });
        let service = crate::latency::broker_fast_failure::BrokerFastFailure::new_with_service_context(
            broker_config,
            service_context,
        );
        let (_task, response_rx) = service.enqueue(crate::latency::broker_fast_failure::FastFailureQueueKind::Send, 77);
        service.start_with_schedule(Duration::ZERO, Duration::from_millis(1));

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }
        let cleaned_response_received = tokio::time::timeout(Duration::from_millis(50), response_rx)
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .is_some();
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service.shutdown_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(rocketmq_runtime::ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = cleaned_response_received
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        BrokerFastFailureLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            cleaned_response_received,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_broker_schedule_persistence_lifecycle_probe(
        root: PathBuf,
    ) -> BrokerSchedulePersistenceLifecycleProbe {
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("broker schedule persistence benchmark root should be created");
        let root_dir = root.to_string_lossy().into_owned();
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: root_dir.clone().into(),
            auth_config_path: root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root_dir.clone().into(),
            flush_delay_offset_interval: 1,
            message_delay_level: "1s".to_string(),
            ..MessageStoreConfig::default()
        });
        let runtime_context = RuntimeContext::from_current("broker-schedule-persistence-probe");
        let service_context = runtime_context.service_context("broker");
        let mut runtime = crate::broker_runtime::BrokerRuntime::new_with_validated_config(
            validated_broker_config(broker_config, message_store_config),
            service_context,
        );
        let service = runtime
            .runtime_state_mut()
            .schedule_message_service_for_test()
            .expect("schedule message service should be configured")
            .clone();

        crate::schedule::schedule_message_service::ScheduleMessageService::start_persist_task_for_probe(
            service.clone(),
            Duration::ZERO,
        )
        .await
        .expect("broker schedule persist task should start");

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..100 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let persisted_offset_file = PathBuf::from(get_delay_offset_store_path(&root_dir)).exists();
        let shutdown_started_at = Instant::now();
        service
            .shutdown()
            .await
            .expect("broker schedule persist task should shutdown");
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && persisted_offset_file;

        let _ = std::fs::remove_dir_all(root);
        BrokerSchedulePersistenceLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            persisted_offset_file,
            shutdown_elapsed_us,
            healthy,
        }
    }
}

#[cfg(test)]
mod bench_support_tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_queue_io_probe_uses_all_queues_and_accounts_bytes() {
        let root = tempfile::tempdir().expect("transaction queue probe temp directory");
        let probe = super::bench_support::run_transaction_queue_io_probe(root.path().to_path_buf(), 4, 3, 128, 2)
            .await
            .expect("transaction queue probe");

        assert_eq!(probe.queue_count, 4);
        assert_eq!(probe.operation_count, 12);
        assert_eq!(probe.bytes_written, 1_536);
        assert_eq!(probe.operation_latencies_us.len(), probe.operation_count);
        assert_eq!(probe.registry_entries, probe.queue_count);
        for queue_id in 0..probe.queue_count {
            let metadata = std::fs::metadata(root.path().join(format!("queue-{queue_id}.dat")))
                .expect("transaction queue probe file");
            assert_eq!(metadata.len(), 384);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broker_client_housekeeping_lifecycle_probe_reports_clean_shutdown() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("broker-client-housekeeping-probe-test");
        let probe =
            super::bench_support::run_broker_client_housekeeping_lifecycle_probe(runtime.service_context("broker"))
                .await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broker_topic_queue_mapping_clean_lifecycle_probe_reports_clean_shutdown() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("broker-topic-clean-probe-test");
        let probe = super::bench_support::run_broker_topic_queue_mapping_clean_lifecycle_probe(
            runtime.service_context("broker"),
        )
        .await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broker_fast_failure_lifecycle_probe_reports_clean_shutdown() {
        let runtime = rocketmq_runtime::RuntimeContext::from_current("broker-fast-failure-probe-test");
        let probe =
            super::bench_support::run_broker_fast_failure_lifecycle_probe(runtime.service_context("broker")).await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.cleaned_response_received, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broker_schedule_persistence_lifecycle_probe_reports_clean_shutdown() {
        let root = std::env::temp_dir().join(format!(
            "rocketmq-rust-broker-schedule-persist-probe-{}",
            rocketmq_runtime::common::time_utils::current_millis()
        ));
        let probe = super::bench_support::run_broker_schedule_persistence_lifecycle_probe(root).await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.persisted_offset_file, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn broker_runtime_lifecycle_probe_reports_remoting_shutdown() {
        let root = std::env::temp_dir().join(format!(
            "rocketmq-rust-broker-runtime-probe-{}",
            rocketmq_runtime::common::time_utils::current_millis()
        ));
        let probe = super::bench_support::run_broker_runtime_lifecycle_probe(root, 1).await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.basic_shutdown_healthy, "{probe:?}");
        assert_eq!(probe.basic_shutdown_report.unhealthy_component_count, 0, "{probe:?}");
        assert!(probe.basic_shutdown_report.timed_out_components.is_empty(), "{probe:?}");
        assert!(probe.basic_shutdown_report.component_names.contains(&"message_store"));
        let remoting_report = probe
            .remoting_shutdown_report
            .as_ref()
            .expect("broker runtime lifecycle probe should include remoting shutdown report");
        assert!(remoting_report.healthy, "{probe:?}");
        assert_eq!(remoting_report.server_report_count, 1, "{probe:?}");
        assert!(remoting_report.server_reports_healthy, "{probe:?}");
        let request_processor_report = probe
            .request_processor_shutdown_report
            .as_ref()
            .expect("broker runtime lifecycle probe should include request processor shutdown report");
        assert!(
            request_processor_report.is_healthy(),
            "{}",
            request_processor_report.to_json()
        );
        assert_eq!(request_processor_report.leaked, 0, "{probe:?}");
    }
}

pub(crate) mod broker;
pub(crate) mod broker_bootstrap;
pub(crate) mod broker_path_config_helper;
pub(crate) mod broker_runtime;
pub(crate) mod client;
pub(crate) mod coldctr;
pub mod config;
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
pub(crate) mod store_read;
pub(crate) mod subscription;
pub(crate) mod topic;
mod transaction;
pub(crate) mod types;
pub(crate) mod util;

pub(crate) mod auth;
