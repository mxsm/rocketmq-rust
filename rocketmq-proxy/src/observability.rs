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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tonic::Code as TonicCode;
use tonic::Status as TonicStatus;
use tracing::warn;

use crate::context::ProxyContext;
use crate::error::ProxyResult;
use crate::proto::v2;
use crate::session::ClientSessionRegistry;
use crate::status::ProxyPayloadStatus;

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
pub struct ProxyRpcMetricsSnapshot {
    pub rpc_name: String,
    pub started: u64,
    pub completed: u64,
    pub in_flight: u64,
    pub succeeded: u64,
    pub payload_failures: u64,
    pub transport_failures: u64,
    pub total_latency_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyMetricsSnapshot {
    pub rpcs: Vec<ProxyRpcMetricsSnapshot>,
    pub sessions: usize,
    pub tracked_receipt_handles: usize,
    pub lite_subscriptions: usize,
    pub prepared_transactions: usize,
    pub telemetry_links: usize,
}

#[derive(Default)]
struct RpcMetricsCell {
    started: AtomicU64,
    completed: AtomicU64,
    in_flight: AtomicU64,
    succeeded: AtomicU64,
    payload_failures: AtomicU64,
    transport_failures: AtomicU64,
    total_latency_micros: AtomicU64,
}

#[derive(Clone, Default)]
pub struct ProxyMetrics {
    rpcs: Arc<DashMap<&'static str, Arc<RpcMetricsCell>>>,
}

impl ProxyMetrics {
    pub fn record_request_started(&self, rpc_name: &'static str) {
        let bucket = self.bucket(rpc_name);
        bucket.started.fetch_add(1, Ordering::Relaxed);
        bucket.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_request_completed(&self, rpc_name: &'static str, outcome: &ProxyRequestOutcome, elapsed: Duration) {
        let bucket = self.bucket(rpc_name);
        bucket.completed.fetch_add(1, Ordering::Relaxed);
        decrement_saturating(&bucket.in_flight);
        bucket.total_latency_micros.fetch_add(
            elapsed.as_micros().clamp(0, u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );

        match outcome {
            ProxyRequestOutcome::Payload(status) if status.is_ok() => {
                bucket.succeeded.fetch_add(1, Ordering::Relaxed);
            }
            ProxyRequestOutcome::Payload(_) => {
                bucket.payload_failures.fetch_add(1, Ordering::Relaxed);
            }
            ProxyRequestOutcome::Transport { .. } => {
                bucket.transport_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self, sessions: &ClientSessionRegistry) -> ProxyMetricsSnapshot {
        let mut rpcs = self
            .rpcs
            .iter()
            .map(|entry| {
                let rpc_name = (*entry.key()).to_owned();
                let cell = entry.value();
                ProxyRpcMetricsSnapshot {
                    rpc_name,
                    started: cell.started.load(Ordering::Relaxed),
                    completed: cell.completed.load(Ordering::Relaxed),
                    in_flight: cell.in_flight.load(Ordering::Relaxed),
                    succeeded: cell.succeeded.load(Ordering::Relaxed),
                    payload_failures: cell.payload_failures.load(Ordering::Relaxed),
                    transport_failures: cell.transport_failures.load(Ordering::Relaxed),
                    total_latency_micros: cell.total_latency_micros.load(Ordering::Relaxed),
                }
            })
            .collect::<Vec<_>>();
        rpcs.sort_by(|left, right| left.rpc_name.cmp(&right.rpc_name));

        ProxyMetricsSnapshot {
            rpcs,
            sessions: sessions.len(),
            tracked_receipt_handles: sessions.tracked_handle_count(),
            lite_subscriptions: sessions.lite_subscription_count(),
            prepared_transactions: sessions.prepared_transaction_count(),
            telemetry_links: sessions.telemetry_link_count(),
        }
    }

    fn bucket(&self, rpc_name: &'static str) -> Arc<RpcMetricsCell> {
        self.rpcs
            .entry(rpc_name)
            .or_insert_with(|| Arc::new(RpcMetricsCell::default()))
            .clone()
    }
}

fn decrement_saturating(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

#[cfg(test)]
mod tests {
    use tonic::Status;

    use super::ProxyMetrics;
    use super::ProxyRequestOutcome;
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

        let snapshot = metrics.snapshot(&ClientSessionRegistry::default());
        assert_eq!(snapshot.rpcs.len(), 1);
        let rpc = &snapshot.rpcs[0];
        assert_eq!(rpc.rpc_name, "QueryRoute");
        assert_eq!(rpc.started, 2);
        assert_eq!(rpc.completed, 2);
        assert_eq!(rpc.in_flight, 0);
        assert_eq!(rpc.succeeded, 1);
        assert_eq!(rpc.transport_failures, 1);
        assert_eq!(rpc.payload_failures, 0);
        assert_eq!(rpc.total_latency_micros, 10_000);
    }
}
