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

#![warn(rust_2018_idioms)]
#![warn(clippy::all)]

//! RocketMQ Proxy runtime for the Rust implementation.
//!
//! This crate now provides the Phase 1 gRPC foundation and the first
//! route/assignment/heartbeat slice of the Java proxy architecture.

pub mod auth;
mod auth_metadata;
pub mod bootstrap;
pub mod cluster;
pub mod config;
pub mod context;
pub mod error;
pub mod grpc;
pub mod local;
mod message;
pub mod observability;
pub mod processor;
pub mod proto;
pub mod remoting;
pub mod service;
pub mod session;
pub mod status;

pub use auth::ProxyAuthRuntime;
pub use bootstrap::ProxyRuntime;
pub use bootstrap::ProxyRuntimeBuilder;
pub use cluster::ClusterClient;
pub use cluster::RocketmqClusterClient;
pub use config::ClusterConfig;
pub use config::GrpcConfig;
pub use config::LocalConfig;
pub use config::ProxyAuthConfig;
pub use config::ProxyConfig;
pub use config::ProxyMode;
pub use config::RemotingConfig;
pub use config::RuntimeConfig;
pub use config::SessionConfig;
pub use error::ProxyError;
pub use error::ProxyResult;
pub use observability::ProxyHook;
pub use observability::ProxyHookChain;
pub use observability::ProxyMetrics;
pub use observability::ProxyMetricsSnapshot;
pub use observability::ProxyRequestOutcome;
pub use observability::ProxyRpcMetricsSnapshot;
pub use processor::AckMessagePlan;
pub use processor::AckMessageRequest;
pub use processor::AckMessageResultEntry;
pub use processor::ChangeInvisibleDurationPlan;
pub use processor::ChangeInvisibleDurationRequest;
pub use processor::ConsumerFilterExpression;
pub use processor::DefaultMessagingProcessor;
pub use processor::EndTransactionPlan;
pub use processor::EndTransactionRequest;
pub use processor::ForwardMessageToDeadLetterQueuePlan;
pub use processor::ForwardMessageToDeadLetterQueueRequest;
pub use processor::GetOffsetPlan;
pub use processor::GetOffsetRequest;
pub use processor::MessageQueueTarget;
pub use processor::MessagingProcessor;
pub use processor::PullMessagePlan;
pub use processor::PullMessageRequest;
pub use processor::QueryAssignmentPlan;
pub use processor::QueryAssignmentRequest;
pub use processor::QueryOffsetPlan;
pub use processor::QueryOffsetPolicy;
pub use processor::QueryOffsetRequest;
pub use processor::QueryRoutePlan;
pub use processor::QueryRouteRequest;
pub use processor::RecallMessagePlan;
pub use processor::RecallMessageRequest;
pub use processor::ReceiveMessagePlan;
pub use processor::ReceiveMessageRequest;
pub use processor::ReceiveTarget;
pub use processor::ReceivedMessage;
pub use processor::SendMessageEntry;
pub use processor::SendMessagePlan;
pub use processor::SendMessageRequest;
pub use processor::SendMessageResultEntry;
pub use processor::TransactionResolution;
pub use processor::TransactionSource;
pub use processor::UpdateOffsetPlan;
pub use processor::UpdateOffsetRequest;
pub use remoting::ProxyRemotingDispatcher;
pub use rocketmq_proxy_core::ProxyMessage;
pub use rocketmq_proxy_core::ProxyMessageExt;
pub use service::AssignmentService;
pub use service::ClusterServiceManager;
pub use service::ConsumerService;
pub use service::DefaultAssignmentService;
pub use service::DefaultConsumerService;
pub use service::DefaultMessageService;
pub use service::DefaultMetadataService;
pub use service::DefaultRouteService;
pub use service::DefaultTransactionService;
pub use service::LocalServiceManager;
pub use service::MessageService;
pub use service::MetadataService;
pub use service::ProxyTopicMessageType;
pub use service::ResourceIdentity;
pub use service::RouteService;
pub use service::ServiceManager;
pub use service::StaticMessageService;
pub use service::StaticMetadataService;
pub use service::StaticRouteService;
pub use service::SubscriptionGroupMetadata;
pub use service::TransactionService;
pub use service::UnsupportedRouteService;
pub use session::build_lite_subscription_sync_request;
pub use session::ClientSession;
pub use session::ClientSessionRegistry;
pub use session::ClientSettingsSnapshot;
pub use session::LiteSubscriptionSnapshot;
pub use session::LiteSubscriptionSyncRequest;
pub use session::PreparedTransactionHandle;
pub use session::PreparedTransactionRegistration;
pub use session::ReapSummary;
pub use session::ReceiptHandleRegistration;
pub use session::SubscriptionSettingsSnapshot;
pub use session::TrackedReceiptHandle;
pub use status::ProxyPayloadStatus;

/// Default gRPC port used by the proxy runtime.
pub const DEFAULT_PROXY_GRPC_PORT: u16 = rocketmq_proxy_core::DEFAULT_PROXY_GRPC_PORT;

/// Default remoting port used by the proxy runtime.
pub const DEFAULT_PROXY_REMOTING_PORT: u16 = rocketmq_proxy_core::DEFAULT_PROXY_REMOTING_PORT;

#[doc(hidden)]
pub mod bench_support {
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use rocketmq_runtime::RuntimeHandle;
    use rocketmq_runtime::ShutdownReport;
    use rocketmq_runtime::TaskGroup;
    use serde::Serialize;
    use tokio::sync::oneshot;

    use crate::config::ProxyConfig;
    use crate::grpc::ProxyGrpcService;
    use crate::processor::DefaultMessagingProcessor;
    use crate::service::LocalServiceManager;
    use crate::service::ServiceManager;
    use crate::session::ClientSessionRegistry;

    #[derive(Clone, Debug, Serialize)]
    pub struct ProxyHousekeepingLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub shutdown_elapsed_us: u128,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub report: ShutdownReport,
        pub housekeeping_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    pub async fn run_proxy_housekeeping_lifecycle_probe(shutdown_delay: Duration) -> ProxyHousekeepingLifecycleProbe {
        let service = housekeeping_service();
        let runtime = tokio::runtime::Handle::current();
        let task_group = TaskGroup::root("rocketmq-proxy.bench.housekeeping", RuntimeHandle::new(runtime));
        let (started_tx, started_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (report_tx, report_rx) = oneshot::channel();
        let housekeeping = service.clone();

        task_group
            .spawn_service("proxy.grpc.housekeeping", async move {
                let _ = started_tx.send(());
                let run_report = housekeeping
                    .run_housekeeping_until_with_report(async move {
                        let _ = shutdown_rx.await;
                    })
                    .await;
                let _ = report_tx.send(run_report);
            })
            .expect("proxy housekeeping benchmark task should spawn");

        started_rx
            .await
            .expect("proxy housekeeping benchmark task should start");
        let task_count_before_shutdown = task_group.task_count();
        tokio::time::sleep(shutdown_delay).await;

        let shutdown_started_at = Instant::now();
        let _ = shutdown_tx.send(());
        let report = task_group.shutdown(Duration::from_secs(5)).await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let housekeeping_report = report_rx.await.ok();
        let schedule_snapshot = housekeeping_report
            .as_ref()
            .map(|report| report.schedule_snapshot.as_slice())
            .unwrap_or_default();
        let scheduled_runs = schedule_snapshot.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = schedule_snapshot.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = schedule_snapshot.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = schedule_snapshot.iter().map(|snapshot| snapshot.failures).sum();
        let housekeeping_shutdown_report = housekeeping_report
            .as_ref()
            .map(|report| report.shutdown_report.clone());
        let task_count_after_shutdown = task_group.task_count();
        let finished_tasks = report.completed + report.cancelled;
        let housekeeping_healthy = housekeeping_shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = report.is_healthy()
            && task_count_before_shutdown == 1
            && task_count_after_shutdown == 0
            && finished_tasks == 1
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && housekeeping_healthy;

        ProxyHousekeepingLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            shutdown_elapsed_us,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            report,
            housekeeping_report: housekeeping_shutdown_report,
            healthy,
        }
    }

    fn housekeeping_service() -> ProxyGrpcService<DefaultMessagingProcessor> {
        let manager: Arc<dyn ServiceManager> = Arc::new(LocalServiceManager::default());
        let processor = Arc::new(DefaultMessagingProcessor::new(manager));
        ProxyGrpcService::new(
            Arc::new(ProxyConfig::default()),
            processor,
            ClientSessionRegistry::default(),
        )
    }
}

#[cfg(test)]
mod bench_support_tests {
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn proxy_housekeeping_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_proxy_housekeeping_lifecycle_probe(Duration::ZERO).await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert!(probe.report.is_healthy(), "{}", probe.report.to_json());
    }
}
