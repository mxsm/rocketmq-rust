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

use std::sync::Arc;

use async_trait::async_trait;
use rocketmq_auth::AuthMetricsSnapshot;
use rocketmq_observability::metrics::proxy::ProxyRpcMetrics;
pub use rocketmq_observability::metrics::proxy::ProxyRpcMetricsSnapshot;
use rocketmq_observability::metrics::proxy::ProxyRpcOutcome;
use tonic::Code as TonicCode;
use tonic::Status as TonicStatus;
use tracing::warn;

#[cfg(feature = "observability")]
use crate::config::ProxyConfig;
#[cfg(feature = "observability")]
use crate::config::ProxyMode;
use crate::context::ProxyContext;
use crate::error::ProxyResult;
use crate::proto::v2;
use crate::session::ClientSessionRegistry;
use crate::status::ProxyPayloadStatus;

#[cfg(feature = "observability")]
pub(crate) fn init_observability_metrics(config: &ProxyConfig) {
    let _ = rocketmq_observability::metrics::proxy::init_global_with_proxy_up_attributes(proxy_up_attributes(config));
}

#[cfg(feature = "observability")]
fn proxy_up_attributes(config: &ProxyConfig) -> rocketmq_observability::metrics::proxy::ProxyUpAttributes {
    let (cluster, node_id) = match config.mode {
        ProxyMode::Cluster => (&config.cluster.broker_cluster_name, &config.cluster.instance_name),
        ProxyMode::Local => (&config.local.broker_cluster_name, &config.local.broker_name),
    };
    let proxy_mode = match config.mode {
        ProxyMode::Cluster => "cluster",
        ProxyMode::Local => "local",
    };

    rocketmq_observability::metrics::proxy::ProxyUpAttributes::new("proxy", cluster, node_id, proxy_mode)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyRequestOutcome {
    Payload(ProxyPayloadStatus),
    Transport { code: TonicCode, message: String },
}

impl ProxyRequestOutcome {
    pub fn from_payload_status(status: &v2::Status) -> Self {
        Self::Payload(ProxyPayloadStatus::new(status.code, status.message.clone()))
    }

    pub fn from_tonic_status(status: &TonicStatus) -> Self {
        Self::Transport {
            code: status.code(),
            message: status.message().to_owned(),
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Payload(status) if status.is_ok())
    }

    fn rpc_metrics_outcome(&self) -> ProxyRpcOutcome {
        match self {
            Self::Payload(status) if status.is_ok() => ProxyRpcOutcome::Succeeded,
            Self::Payload(_) => ProxyRpcOutcome::PayloadFailed,
            Self::Transport { .. } => ProxyRpcOutcome::TransportFailed,
        }
    }
}

#[async_trait]
pub trait ProxyHook: Send + Sync {
    async fn before_request(&self, _context: &ProxyContext) -> ProxyResult<()> {
        Ok(())
    }

    async fn after_request(&self, _context: &ProxyContext, _outcome: &ProxyRequestOutcome) -> ProxyResult<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct ProxyHookChain {
    hooks: Arc<Vec<Arc<dyn ProxyHook>>>,
}

impl ProxyHookChain {
    pub fn new(hooks: Vec<Arc<dyn ProxyHook>>) -> Self {
        Self { hooks: Arc::new(hooks) }
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }

    pub async fn before_request(&self, context: &ProxyContext) {
        for hook in self.hooks.iter() {
            if let Err(error) = hook.before_request(context).await {
                warn!(
                    rpc = context.rpc_name(),
                    request_id = %context.request_id(),
                    error = %error,
                    "proxy before_request hook failed",
                );
            }
        }
    }

    pub async fn after_request(&self, context: &ProxyContext, outcome: &ProxyRequestOutcome) {
        for hook in self.hooks.iter() {
            if let Err(error) = hook.after_request(context, outcome).await {
                warn!(
                    rpc = context.rpc_name(),
                    request_id = %context.request_id(),
                    error = %error,
                    "proxy after_request hook failed",
                );
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyMetricsSnapshot {
    pub rpcs: Vec<ProxyRpcMetricsSnapshot>,
    pub auth: Option<AuthMetricsSnapshot>,
    pub sessions: usize,
    pub tracked_receipt_handles: usize,
    pub lite_subscriptions: usize,
    pub prepared_transactions: usize,
    pub telemetry_links: usize,
    pub pending_telemetry_commands: usize,
    pub thread_stack_trace_reports: usize,
    pub verify_message_reports: usize,
    pub pending_lite_unsubscribe_notices: usize,
}

#[derive(Clone, Default)]
pub struct ProxyMetrics {
    rpcs: ProxyRpcMetrics,
}

impl ProxyMetrics {
    pub fn record_request_started(&self, rpc_name: &'static str) {
        self.rpcs.record_request_started(rpc_name);
    }

    pub fn record_request_completed(
        &self,
        rpc_name: &'static str,
        outcome: &ProxyRequestOutcome,
        elapsed: std::time::Duration,
    ) {
        self.rpcs
            .record_request_completed(rpc_name, outcome.rpc_metrics_outcome(), elapsed);
    }

    pub fn snapshot(
        &self,
        sessions: &ClientSessionRegistry,
        auth: Option<AuthMetricsSnapshot>,
    ) -> ProxyMetricsSnapshot {
        rocketmq_observability::metrics::proxy::record_active_connections(sessions.len() as u64);

        ProxyMetricsSnapshot {
            rpcs: self.rpcs.snapshot(),
            auth,
            sessions: sessions.len(),
            tracked_receipt_handles: sessions.tracked_handle_count(),
            lite_subscriptions: sessions.lite_subscription_count(),
            prepared_transactions: sessions.prepared_transaction_count(),
            telemetry_links: sessions.telemetry_link_count(),
            pending_telemetry_commands: sessions.pending_telemetry_command_count(),
            thread_stack_trace_reports: sessions.thread_stack_trace_report_count(),
            verify_message_reports: sessions.verify_message_report_count(),
            pending_lite_unsubscribe_notices: sessions.pending_lite_unsubscribe_notice_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tonic::Status;

    #[cfg(feature = "observability")]
    use super::proxy_up_attributes;
    use super::ProxyMetrics;
    use super::ProxyRequestOutcome;
    #[cfg(feature = "observability")]
    use crate::config::ProxyConfig;
    #[cfg(feature = "observability")]
    use crate::config::ProxyMode;
    use crate::proto::v2;
    use crate::session::ClientSessionRegistry;

    #[test]
    fn metrics_snapshot_tracks_payload_and_transport_outcomes() {
        let metrics = ProxyMetrics::default();
        metrics.record_request_started("QueryRoute");
        metrics.record_request_completed(
            "QueryRoute",
            &ProxyRequestOutcome::from_payload_status(&v2::Status {
                code: v2::Code::Ok as i32,
                message: "OK".to_owned(),
            }),
            std::time::Duration::from_millis(4),
        );
        metrics.record_request_started("QueryRoute");
        metrics.record_request_completed(
            "QueryRoute",
            &ProxyRequestOutcome::from_tonic_status(&Status::unavailable("network split")),
            std::time::Duration::from_millis(6),
        );

        let snapshot = metrics.snapshot(&ClientSessionRegistry::default(), None);
        assert_eq!(snapshot.rpcs.len(), 1);
        assert_eq!(snapshot.auth, None);
        let rpc = &snapshot.rpcs[0];
        assert_eq!(rpc.rpc_name, "QueryRoute");
        assert_eq!(rpc.started, 2);
        assert_eq!(rpc.completed, 2);
        assert_eq!(rpc.in_flight, 0);
        assert_eq!(rpc.succeeded, 1);
        assert_eq!(rpc.transport_failures, 1);
        assert_eq!(rpc.payload_failures, 0);
        assert_eq!(rpc.total_latency_micros, 10_000);
        assert_eq!(snapshot.pending_telemetry_commands, 0);
        assert_eq!(snapshot.thread_stack_trace_reports, 0);
        assert_eq!(snapshot.verify_message_reports, 0);
        assert_eq!(snapshot.pending_lite_unsubscribe_notices, 0);
    }

    #[cfg(feature = "observability")]
    #[test]
    fn proxy_up_attributes_use_runtime_identity() {
        let mut cluster_config = ProxyConfig::default();
        cluster_config.cluster.broker_cluster_name = "ClusterA".to_owned();
        cluster_config.cluster.instance_name = "proxy-a".to_owned();

        assert_eq!(
            proxy_up_attributes(&cluster_config),
            rocketmq_observability::metrics::proxy::ProxyUpAttributes::new("proxy", "ClusterA", "proxy-a", "cluster")
        );

        let mut local_config = ProxyConfig {
            mode: ProxyMode::Local,
            ..ProxyConfig::default()
        };
        local_config.local.broker_cluster_name = "LocalCluster".to_owned();
        local_config.local.broker_name = "local-proxy-a".to_owned();

        assert_eq!(
            proxy_up_attributes(&local_config),
            rocketmq_observability::metrics::proxy::ProxyUpAttributes::new(
                "proxy",
                "LocalCluster",
                "local-proxy-a",
                "local"
            )
        );
    }
}
