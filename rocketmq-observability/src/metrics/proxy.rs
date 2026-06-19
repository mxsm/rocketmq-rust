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

pub use crate::semantic::metrics::PROXY_ACTIVE_CONNECTIONS;
pub use crate::semantic::metrics::PROXY_FORWARD_LATENCY;
pub use crate::semantic::metrics::PROXY_GRPC_REQUESTS_TOTAL;
pub use crate::semantic::metrics::PROXY_GRPC_REQUEST_LATENCY;
pub use crate::semantic::metrics::PROXY_UP;

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static PROXY_METRICS: OnceLock<ProxyMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
static PROXY_GLOBAL_METRICS: OnceLock<ProxyMetrics> = OnceLock::new();

#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    PROXY_METRICS.set(ProxyMetrics::new(meter)).is_ok()
}

#[cfg(feature = "otel-metrics")]
pub fn init_global_with_proxy_up(meter: &opentelemetry::metrics::Meter, attributes: ProxyUpAttributes) -> bool {
    PROXY_METRICS
        .set(ProxyMetrics::new_with_proxy_up(meter, attributes))
        .is_ok()
}

#[cfg(feature = "otel-metrics")]
pub fn init_global_with_proxy_up_attributes(attributes: ProxyUpAttributes) -> bool {
    init_global_with_proxy_up(&opentelemetry::global::meter("rocketmq-proxy"), attributes)
}

#[cfg(feature = "otel-metrics")]
fn global_metrics() -> &'static ProxyMetrics {
    if let Some(metrics) = PROXY_METRICS.get() {
        return metrics;
    }

    PROXY_GLOBAL_METRICS.get_or_init(|| ProxyMetrics::new(&opentelemetry::global::meter("rocketmq-proxy")))
}

pub fn record_grpc_requests_total(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_grpc_requests_total(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

pub fn record_grpc_request_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_grpc_request_latency(latency_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_forward_latency(latency_ms: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_forward_latency(latency_ms, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = latency_ms;
}

pub fn record_active_connections(count: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_active_connections(count, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = count;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyRpcOutcome {
    Succeeded,
    PayloadFailed,
    TransportFailed,
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

#[derive(Default)]
struct ProxyRpcMetricsCell {
    started: AtomicU64,
    completed: AtomicU64,
    in_flight: AtomicU64,
    succeeded: AtomicU64,
    payload_failures: AtomicU64,
    transport_failures: AtomicU64,
    total_latency_micros: AtomicU64,
}

#[derive(Clone, Default)]
pub struct ProxyRpcMetrics {
    rpcs: Arc<DashMap<&'static str, Arc<ProxyRpcMetricsCell>>>,
}

impl ProxyRpcMetrics {
    pub fn record_request_started(&self, rpc_name: &'static str) {
        let bucket = self.bucket(rpc_name);
        bucket.started.fetch_add(1, Ordering::Relaxed);
        bucket.in_flight.fetch_add(1, Ordering::Relaxed);
        record_grpc_requests_total(1);
    }

    pub fn record_request_completed(&self, rpc_name: &'static str, outcome: ProxyRpcOutcome, elapsed: Duration) {
        let bucket = self.bucket(rpc_name);
        bucket.completed.fetch_add(1, Ordering::Relaxed);
        decrement_saturating(&bucket.in_flight);
        bucket.total_latency_micros.fetch_add(
            elapsed.as_micros().clamp(0, u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );

        match outcome {
            ProxyRpcOutcome::Succeeded => {
                bucket.succeeded.fetch_add(1, Ordering::Relaxed);
            }
            ProxyRpcOutcome::PayloadFailed => {
                bucket.payload_failures.fetch_add(1, Ordering::Relaxed);
            }
            ProxyRpcOutcome::TransportFailed => {
                bucket.transport_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        record_grpc_request_latency(duration_millis_u64(elapsed));
    }

    pub fn snapshot(&self) -> Vec<ProxyRpcMetricsSnapshot> {
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
        rpcs
    }

    fn bucket(&self, rpc_name: &'static str) -> Arc<ProxyRpcMetricsCell> {
        self.rpcs
            .entry(rpc_name)
            .or_insert_with(|| Arc::new(ProxyRpcMetricsCell::default()))
            .clone()
    }
}

fn decrement_saturating(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

#[inline]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyUpAttributes {
    pub node_type: String,
    pub cluster: String,
    pub node_id: String,
    pub proxy_mode: String,
}

impl ProxyUpAttributes {
    pub fn new(
        node_type: impl Into<String>,
        cluster: impl Into<String>,
        node_id: impl Into<String>,
        proxy_mode: impl Into<String>,
    ) -> Self {
        Self {
            node_type: node_type.into(),
            cluster: cluster.into(),
            node_id: node_id.into(),
            proxy_mode: proxy_mode.into(),
        }
    }
}

impl Default for ProxyUpAttributes {
    fn default() -> Self {
        Self::new("proxy", "DefaultCluster", "rocketmq-proxy", "cluster")
    }
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Copy, Default)]
pub struct ProxyMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl ProxyMetrics {
    pub fn noop() -> Self {
        Self
    }

    #[inline]
    pub fn record_grpc_requests_total(&self, _count: u64) {}

    #[inline]
    pub fn record_grpc_request_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_forward_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_active_connections(&self, _count: u64) {}
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
pub struct ProxyMetrics {
    grpc_requests_total: opentelemetry::metrics::Counter<u64>,
    grpc_request_latency: opentelemetry::metrics::Histogram<u64>,
    forward_latency: opentelemetry::metrics::Histogram<u64>,
    active_connections: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "otel-metrics")]
impl ProxyMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self::new_with_proxy_up(meter, ProxyUpAttributes::default())
    }

    pub fn new_with_proxy_up(meter: &opentelemetry::metrics::Meter, proxy_up_attributes: ProxyUpAttributes) -> Self {
        let grpc_requests_total = meter
            .u64_counter(PROXY_GRPC_REQUESTS_TOTAL)
            .with_description("Total number of proxy gRPC requests")
            .with_unit("{request}")
            .build();

        let grpc_request_latency = meter
            .u64_histogram(PROXY_GRPC_REQUEST_LATENCY)
            .with_description("Proxy gRPC request latency")
            .with_unit("ms")
            .build();

        let forward_latency = meter
            .u64_histogram(PROXY_FORWARD_LATENCY)
            .with_description("Proxy request forwarding latency")
            .with_unit("ms")
            .build();

        let active_connections = meter
            .u64_gauge(PROXY_ACTIVE_CONNECTIONS)
            .with_description("Number of active proxy connections")
            .with_unit("{connection}")
            .build();

        let proxy_up_attributes = proxy_up_attributes.into_key_values();
        let _proxy_up = meter
            .i64_observable_gauge(PROXY_UP)
            .with_description("Proxy process availability")
            .with_unit("1")
            .with_callback(move |observer| {
                observer.observe(1, &proxy_up_attributes);
            })
            .build();

        Self {
            grpc_requests_total,
            grpc_request_latency,
            forward_latency,
            active_connections,
        }
    }

    #[inline]
    pub fn record_grpc_requests_total(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.grpc_requests_total.add(count, attributes);
    }

    #[inline]
    pub fn record_grpc_request_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.grpc_request_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_forward_latency(&self, latency_ms: u64, attributes: &[opentelemetry::KeyValue]) {
        self.forward_latency.record(latency_ms, attributes);
    }

    #[inline]
    pub fn record_active_connections(&self, count: u64, attributes: &[opentelemetry::KeyValue]) {
        self.active_connections.record(count, attributes);
    }
}

#[cfg(feature = "otel-metrics")]
impl ProxyUpAttributes {
    fn into_key_values(self) -> Vec<opentelemetry::KeyValue> {
        vec![
            opentelemetry::KeyValue::new(crate::semantic::labels::NODE_TYPE, self.node_type),
            opentelemetry::KeyValue::new(crate::semantic::labels::CLUSTER, self.cluster),
            opentelemetry::KeyValue::new(crate::semantic::labels::NODE_ID, self.node_id),
            opentelemetry::KeyValue::new(crate::semantic::labels::PROXY_MODE, self.proxy_mode),
        ]
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::*;

    #[test]
    fn proxy_metrics_constructs_and_records() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("proxy-metrics-test");
        let metrics = ProxyMetrics::new(&meter);
        let attrs = [opentelemetry::KeyValue::new("protocol", "grpc")];

        metrics.record_grpc_requests_total(1, &attrs);
        metrics.record_grpc_request_latency(6, &attrs);
        metrics.record_forward_latency(12, &attrs);
        metrics.record_active_connections(4, &attrs);
    }

    #[test]
    fn proxy_metrics_constructs_with_proxy_up_identity() {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("proxy-up-metrics-test");
        let metrics =
            ProxyMetrics::new_with_proxy_up(&meter, ProxyUpAttributes::new("proxy", "ClusterA", "proxy-a", "local"));

        metrics.record_active_connections(1, &[]);
    }

    #[test]
    fn proxy_global_recorders_lazy_initialize() {
        record_grpc_requests_total(1);
        record_grpc_request_latency(6);
        record_forward_latency(12);
        record_active_connections(4);

        assert!(PROXY_METRICS.get().is_some() || PROXY_GLOBAL_METRICS.get().is_some());
    }
}

#[cfg(test)]
mod rpc_tests {
    use super::*;

    #[test]
    fn proxy_rpc_metrics_snapshot_tracks_outcomes() {
        let metrics = ProxyRpcMetrics::default();
        metrics.record_request_started("QueryRoute");
        metrics.record_request_completed("QueryRoute", ProxyRpcOutcome::Succeeded, Duration::from_millis(4));
        metrics.record_request_started("QueryRoute");
        metrics.record_request_completed("QueryRoute", ProxyRpcOutcome::TransportFailed, Duration::from_millis(6));

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.len(), 1);
        let rpc = &snapshot[0];
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
