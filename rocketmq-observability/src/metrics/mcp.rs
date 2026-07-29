// Copyright 2026 The RocketMQ Rust Authors
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

//! Bounded, non-sensitive metric contracts for the RocketMQ MCP server.
//!
//! Operation names must be static catalog identifiers, never caller-provided
//! resource URIs, cluster names, tenant identifiers, or request arguments.

use std::time::Duration;

#[cfg(feature = "otel-metrics")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "otel-metrics")]
use std::sync::atomic::Ordering;
#[cfg(feature = "otel-metrics")]
use std::sync::Arc;

pub use crate::semantic::metrics::MCP_AUDIT_BACKLOG;
pub use crate::semantic::metrics::MCP_AUDIT_DROPPED_TOTAL;
pub use crate::semantic::metrics::MCP_AUDIT_FAILURES_TOTAL;
pub use crate::semantic::metrics::MCP_CACHE_OPERATIONS_TOTAL;
pub use crate::semantic::metrics::MCP_ERRORS_TOTAL;
pub use crate::semantic::metrics::MCP_RATE_LIMIT_TOTAL;
pub use crate::semantic::metrics::MCP_REQUESTS_TOTAL;
pub use crate::semantic::metrics::MCP_REQUEST_LATENCY;

#[cfg(feature = "otel-metrics")]
use std::sync::OnceLock;

#[cfg(feature = "otel-metrics")]
static MCP_METRICS: OnceLock<McpMetrics> = OnceLock::new();

/// Installs MCP instruments using the service-owned meter.
///
/// Returns `false` when an MCP metric set was already installed.
#[cfg(feature = "otel-metrics")]
pub fn init_global(meter: &opentelemetry::metrics::Meter) -> bool {
    MCP_METRICS.set(McpMetrics::new(meter)).is_ok()
}

#[cfg(feature = "otel-metrics")]
fn global_metrics() -> &'static McpMetrics {
    MCP_METRICS.get_or_init(|| McpMetrics::new(&opentelemetry::global::meter("rocketmq-mcp")))
}

/// Bounded MCP protocol surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOperationKind {
    Tool,
    Resource,
}

impl McpOperationKind {
    #[cfg(any(feature = "otel-metrics", test))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tool => "tool",
            Self::Resource => "resource",
        }
    }
}

/// Bounded terminal outcome for a Tool or Resource request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOperationOutcome {
    Success,
    Failure,
    Denied,
}

impl McpOperationOutcome {
    #[cfg(any(feature = "otel-metrics", test))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Denied => "denied",
        }
    }
}

/// Bounded error categories suitable for aggregate metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpErrorKind {
    InvalidRequest,
    PermissionDenied,
    RateLimited,
    SourceUnavailable,
    OutputTooLarge,
    Internal,
}

impl McpErrorKind {
    #[cfg(any(feature = "otel-metrics", test))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::PermissionDenied => "permission_denied",
            Self::RateLimited => "rate_limited",
            Self::SourceUnavailable => "source_unavailable",
            Self::OutputTooLarge => "output_too_large",
            Self::Internal => "internal",
        }
    }
}

/// Bounded cache events emitted by the MCP query cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpCacheEvent {
    Hit,
    Miss,
    Bypass,
    Eviction,
    Invalidation,
    CoalescedWaiter,
}

impl McpCacheEvent {
    #[cfg(any(feature = "otel-metrics", test))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass => "bypass",
            Self::Eviction => "eviction",
            Self::Invalidation => "invalidation",
            Self::CoalescedWaiter => "coalesced_waiter",
        }
    }
}

/// Bounded rate-limit decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpRateLimitOutcome {
    Accepted,
    Rejected,
}

impl McpRateLimitOutcome {
    #[cfg(any(feature = "otel-metrics", test))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }
}

/// Bounded reasons why an audit record could not enter the audit queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpAuditDropReason {
    Oversized,
    CountCapacity,
    ByteCapacity,
    Closed,
}

impl McpAuditDropReason {
    #[cfg(feature = "otel-metrics")]
    const ALL: [Self; 4] = [Self::Oversized, Self::CountCapacity, Self::ByteCapacity, Self::Closed];

    #[cfg(any(feature = "otel-metrics", test))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Oversized => "oversized",
            Self::CountCapacity => "count_capacity",
            Self::ByteCapacity => "byte_capacity",
            Self::Closed => "closed",
        }
    }

    #[cfg(feature = "otel-metrics")]
    const fn index(self) -> usize {
        match self {
            Self::Oversized => 0,
            Self::CountCapacity => 1,
            Self::ByteCapacity => 2,
            Self::Closed => 3,
        }
    }
}

/// Bounded audit persistence failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpAuditFailureKind {
    Sink,
    Flush,
}

impl McpAuditFailureKind {
    #[cfg(feature = "otel-metrics")]
    const ALL: [Self; 2] = [Self::Sink, Self::Flush];

    #[cfg(any(feature = "otel-metrics", test))]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sink => "sink",
            Self::Flush => "flush",
        }
    }

    #[cfg(feature = "otel-metrics")]
    const fn index(self) -> usize {
        match self {
            Self::Sink => 0,
            Self::Flush => 1,
        }
    }
}

/// Records a completed Tool or Resource request and its latency.
pub fn record_operation(
    kind: McpOperationKind,
    operation: &'static str,
    outcome: McpOperationOutcome,
    elapsed: Duration,
) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_operation(kind, operation, outcome, duration_millis_u64(elapsed));

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (kind, operation, outcome, elapsed);
}

/// Records a bounded error class without including an error message.
pub fn record_error(kind: McpOperationKind, operation: &'static str, error: McpErrorKind) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_error(kind, operation, error);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = (kind, operation, error);
}

/// Records one query-cache event.
pub fn record_cache_event(event: McpCacheEvent) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_cache_event(event);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = event;
}

/// Records one rate-limit decision.
pub fn record_rate_limit(outcome: McpRateLimitOutcome) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_rate_limit(outcome);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = outcome;
}

/// Records the current number of audit records waiting to be persisted.
pub fn record_audit_backlog(records: u64) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().audit_backlog.record(records, &[]);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = records;
}

/// Records one dropped audit record.
pub fn record_audit_drop(reason: McpAuditDropReason) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_audit_drop(reason);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = reason;
}

/// Records one audit sink or flush failure.
pub fn record_audit_failure(kind: McpAuditFailureKind) {
    #[cfg(feature = "otel-metrics")]
    global_metrics().record_audit_failure(kind);

    #[cfg(not(feature = "otel-metrics"))]
    let _ = kind;
}

#[cfg(feature = "otel-metrics")]
struct McpMetrics {
    requests_total: opentelemetry::metrics::Counter<u64>,
    request_latency: opentelemetry::metrics::Histogram<u64>,
    errors_total: opentelemetry::metrics::Counter<u64>,
    cache_operations_total: opentelemetry::metrics::Counter<u64>,
    rate_limit_total: opentelemetry::metrics::Counter<u64>,
    audit_backlog: opentelemetry::metrics::Gauge<u64>,
    audit_dropped: Arc<[AtomicU64; 4]>,
    _audit_dropped_total: opentelemetry::metrics::ObservableCounter<u64>,
    audit_failures: Arc<[AtomicU64; 2]>,
    _audit_failures_total: opentelemetry::metrics::ObservableCounter<u64>,
}

#[cfg(feature = "otel-metrics")]
impl McpMetrics {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let audit_dropped = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let observed_audit_dropped = Arc::clone(&audit_dropped);
        let audit_dropped_total = meter
            .u64_observable_counter(MCP_AUDIT_DROPPED_TOTAL)
            .with_description("MCP audit records dropped before persistence")
            .with_unit("{record}")
            .with_callback(move |observer| {
                for reason in McpAuditDropReason::ALL {
                    observer.observe(
                        observed_audit_dropped[reason.index()].load(Ordering::Relaxed),
                        &[opentelemetry::KeyValue::new(
                            crate::semantic::labels::REASON,
                            reason.as_str(),
                        )],
                    );
                }
            })
            .build();
        let audit_failures = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let observed_audit_failures = Arc::clone(&audit_failures);
        let audit_failures_total = meter
            .u64_observable_counter(MCP_AUDIT_FAILURES_TOTAL)
            .with_description("MCP audit sink and flush failures")
            .with_unit("{failure}")
            .with_callback(move |observer| {
                for kind in McpAuditFailureKind::ALL {
                    observer.observe(
                        observed_audit_failures[kind.index()].load(Ordering::Relaxed),
                        &[opentelemetry::KeyValue::new(
                            crate::semantic::labels::REASON,
                            kind.as_str(),
                        )],
                    );
                }
            })
            .build();
        let metrics = Self {
            requests_total: meter
                .u64_counter(MCP_REQUESTS_TOTAL)
                .with_description("Completed MCP Tool and Resource requests")
                .with_unit("{request}")
                .build(),
            request_latency: meter
                .u64_histogram(MCP_REQUEST_LATENCY)
                .with_description("MCP Tool and Resource request latency")
                .with_unit("ms")
                .build(),
            errors_total: meter
                .u64_counter(MCP_ERRORS_TOTAL)
                .with_description("MCP request errors grouped by bounded category")
                .with_unit("{error}")
                .build(),
            cache_operations_total: meter
                .u64_counter(MCP_CACHE_OPERATIONS_TOTAL)
                .with_description("MCP query-cache events grouped by bounded outcome")
                .with_unit("{operation}")
                .build(),
            rate_limit_total: meter
                .u64_counter(MCP_RATE_LIMIT_TOTAL)
                .with_description("MCP rate-limit decisions")
                .with_unit("{decision}")
                .build(),
            audit_backlog: meter
                .u64_gauge(MCP_AUDIT_BACKLOG)
                .with_description("MCP audit records waiting to be persisted")
                .with_unit("{record}")
                .build(),
            audit_dropped,
            _audit_dropped_total: audit_dropped_total,
            audit_failures,
            _audit_failures_total: audit_failures_total,
        };
        metrics.initialize_audit_health_series();
        metrics
    }

    fn initialize_audit_health_series(&self) {
        self.audit_backlog.record(0, &[]);
    }

    fn record_operation(
        &self,
        kind: McpOperationKind,
        operation: &'static str,
        outcome: McpOperationOutcome,
        latency_ms: u64,
    ) {
        let base_attributes = [
            opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION_KIND, kind.as_str()),
            opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION, operation),
        ];
        let request_attributes = [
            base_attributes[0].clone(),
            base_attributes[1].clone(),
            opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, outcome.as_str()),
        ];
        self.requests_total.add(1, &request_attributes);
        self.request_latency.record(latency_ms, &base_attributes);
    }

    fn record_error(&self, kind: McpOperationKind, operation: &'static str, error: McpErrorKind) {
        self.errors_total.add(
            1,
            &[
                opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION_KIND, kind.as_str()),
                opentelemetry::KeyValue::new(crate::semantic::labels::OPERATION, operation),
                opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, error.as_str()),
            ],
        );
    }

    fn record_cache_event(&self, event: McpCacheEvent) {
        self.cache_operations_total.add(
            1,
            &[opentelemetry::KeyValue::new(
                crate::semantic::labels::RESULT,
                event.as_str(),
            )],
        );
    }

    fn record_rate_limit(&self, outcome: McpRateLimitOutcome) {
        self.rate_limit_total.add(
            1,
            &[opentelemetry::KeyValue::new(
                crate::semantic::labels::RESULT,
                outcome.as_str(),
            )],
        );
    }

    fn record_audit_drop(&self, reason: McpAuditDropReason) {
        self.audit_dropped[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn record_audit_failure(&self, kind: McpAuditFailureKind) {
        self.audit_failures[kind.index()].fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "otel-metrics")]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_labels_are_stable() {
        assert_eq!(McpOperationKind::Tool.as_str(), "tool");
        assert_eq!(McpOperationOutcome::Denied.as_str(), "denied");
        assert_eq!(McpErrorKind::OutputTooLarge.as_str(), "output_too_large");
        assert_eq!(McpCacheEvent::CoalescedWaiter.as_str(), "coalesced_waiter");
        assert_eq!(McpRateLimitOutcome::Rejected.as_str(), "rejected");
        assert_eq!(McpAuditDropReason::ByteCapacity.as_str(), "byte_capacity");
        assert_eq!(McpAuditFailureKind::Flush.as_str(), "flush");
    }

    #[test]
    fn no_feature_recorders_remain_safe() {
        record_operation(
            McpOperationKind::Resource,
            "read_resource",
            McpOperationOutcome::Success,
            Duration::from_millis(2),
        );
        record_error(
            McpOperationKind::Tool,
            "get_cluster_overview",
            McpErrorKind::SourceUnavailable,
        );
        record_cache_event(McpCacheEvent::Hit);
        record_rate_limit(McpRateLimitOutcome::Accepted);
        record_audit_backlog(3);
        record_audit_drop(McpAuditDropReason::Closed);
        record_audit_failure(McpAuditFailureKind::Sink);
    }

    #[cfg(feature = "otel-metrics")]
    #[test]
    fn metrics_construct_and_record() {
        use opentelemetry::metrics::MeterProvider;

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let meter = provider.meter("mcp-metrics-test");
        let metrics = McpMetrics::new(&meter);

        metrics.record_operation(
            McpOperationKind::Tool,
            "get_cluster_overview",
            McpOperationOutcome::Success,
            4,
        );
        metrics.record_error(
            McpOperationKind::Resource,
            "read_resource",
            McpErrorKind::PermissionDenied,
        );
        metrics.record_cache_event(McpCacheEvent::Miss);
        metrics.record_rate_limit(McpRateLimitOutcome::Rejected);
        metrics.record_audit_drop(McpAuditDropReason::ByteCapacity);
        metrics.record_audit_failure(McpAuditFailureKind::Flush);

        assert_eq!(
            metrics.audit_dropped[McpAuditDropReason::ByteCapacity.index()].load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics.audit_failures[McpAuditFailureKind::Flush.index()].load(Ordering::Relaxed),
            1
        );
    }
}
