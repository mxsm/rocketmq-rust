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

pub use crate::semantic::metrics::RPC_LATENCY;
pub use crate::semantic::metrics::TRANSPORT_DEFERRED_INFLIGHT;
pub use crate::semantic::metrics::TRANSPORT_DEFERRED_RETAINED_BYTES;
pub use crate::semantic::metrics::TRANSPORT_DEFERRED_TERMINAL_TOTAL;
pub use crate::semantic::metrics::TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES;
pub use crate::semantic::metrics::TRANSPORT_LIFECYCLE_EVENTS_TOTAL;
pub use crate::semantic::metrics::TRANSPORT_LIFECYCLE_LISTENER_LATENCY;
pub use crate::semantic::metrics::TRANSPORT_OUTBOUND_ACCEPTED_PLAINTEXT_BYTES;
pub use crate::semantic::metrics::TRANSPORT_OUTBOUND_ATTEMPTED_PLAINTEXT_BYTES;
pub use crate::semantic::metrics::TRANSPORT_OUTBOUND_WRITTEN_PLAINTEXT_BYTES;
pub use crate::semantic::metrics::TRANSPORT_REQUESTS_TOTAL;
pub use crate::semantic::metrics::TRANSPORT_REQUEST_DURATION_SECONDS;
pub use crate::semantic::metrics::TRANSPORT_REQUEST_LATENCY;
pub use crate::semantic::metrics::TRANSPORT_RESPONSE_ABANDONED_TOTAL;
pub use crate::semantic::metrics::TRANSPORT_RESPONSE_DUPLICATE_TOTAL;
pub use crate::semantic::metrics::TRANSPORT_RESPONSE_QUEUE_WAIT_SECONDS;
pub use crate::semantic::metrics::TRANSPORT_RESPONSE_TOTAL;

use std::time::Duration;
use std::time::Instant;

const NO_RESPONSE_CODE: i32 = -1;
const RESULT_ONEWAY: &str = "oneway";
const RESULT_SUCCESS: &str = "success";
const RESULT_CANCELED: &str = "cancelled";
const RESULT_PROCESS_REQUEST_FAILED: &str = "process_request_failed";
const RESULT_WRITE_CHANNEL_FAILED: &str = "write_channel_failed";

/// Fixed request-code classification used by remoting metric labels.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RequestCodeClass {
    /// Pull-message requests.
    PullMessage,
    /// Pop-message requests.
    PopMessage,
    /// Notification requests.
    Notification,
    /// Every request code outside the fixed long-polling classes.
    Other,
}

impl RequestCodeClass {
    /// Returns the fixed low-cardinality metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PullMessage => "pull_message",
            Self::PopMessage => "pop_message",
            Self::Notification => "notification",
            Self::Other => "other",
        }
    }
}

/// Closed request outcome vocabulary used by remoting metric labels.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RequestOutcome {
    /// An inline reply with no body.
    ReplyEmpty,
    /// An inline reply backed by one byte buffer.
    ReplyBytes,
    /// An inline reply backed by multiple byte segments.
    ReplySegments,
    /// An inline reply backed by file regions.
    ReplyFileRegions,
    /// Deferred ownership was durably registered; this is a lifecycle event, not a terminal response.
    DeferredRegistered,
    /// A registered deferred request reached successful response completion.
    DeferredResumed,
    /// A one-way request completed without a response by protocol contract.
    Oneway,
    /// A request completed with an explicit protocol-level no-response outcome.
    ProtocolNoResponse,
    /// A request was cancelled before response completion.
    Cancelled,
    /// Processing or delivery failed.
    Failed,
    /// A V1 processor produced a legacy reply.
    LegacyReply,
}

impl RequestOutcome {
    /// Returns the fixed low-cardinality metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReplyEmpty => "reply_empty",
            Self::ReplyBytes => "reply_bytes",
            Self::ReplySegments => "reply_segments",
            Self::ReplyFileRegions => "reply_file_regions",
            Self::DeferredRegistered => "deferred_registered",
            Self::DeferredResumed => "deferred_resumed",
            Self::Oneway => "oneway",
            Self::ProtocolNoResponse => "protocol_no_response",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::LegacyReply => "legacy_reply",
        }
    }
}

/// Closed response delivery mode vocabulary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseMode {
    /// Response delivery completed in the original dispatch flow.
    Inline,
    /// Response delivery completed after durable deferred registration.
    Deferred,
    /// The request completed without a response write.
    NoResponse,
}

impl ResponseMode {
    /// Returns the fixed low-cardinality metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Deferred => "deferred",
            Self::NoResponse => "no_response",
        }
    }
}

/// Closed response terminal result vocabulary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseResult {
    /// The canonical transport accepted and wrote the response.
    TransportWritten,
    /// An embedded in-process sink accepted the response.
    InProcessAccepted,
    /// No response was required for a one-way request.
    Oneway,
    /// No response was produced by explicit protocol contract.
    ProtocolNoResponse,
    /// Response ownership was cancelled before completion.
    Cancelled,
    /// Response binding or delivery failed.
    Failed,
}

impl ResponseResult {
    /// Returns the fixed low-cardinality metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransportWritten => "transport_written",
            Self::InProcessAccepted => "in_process_accepted",
            Self::Oneway => "oneway",
            Self::ProtocolNoResponse => "protocol_no_response",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

/// Closed abandoned-response reason vocabulary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResponseAbandonedReason {
    /// The response owner explicitly cancelled it.
    Explicit,
    /// The response receiver was dropped.
    ReceiverDropped,
    /// The deferred responder was dropped without completion.
    Abandoned,
    /// A claimed deferred response was dropped before completion.
    ClaimDropped,
    /// The immutable owner deadline expired.
    OwnerDeadline,
    /// The parent lifecycle cancelled the response.
    ParentCancelled,
    /// The processor required to resume the response was unavailable.
    ProcessorUnavailable,
    /// The owning service began shutdown.
    ServiceStopping,
    /// The owning session closed.
    SessionClosed,
}

impl ResponseAbandonedReason {
    /// Returns the fixed low-cardinality metric label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::ReceiverDropped => "receiver_dropped",
            Self::Abandoned => "abandoned",
            Self::ClaimDropped => "claim_dropped",
            Self::OwnerDeadline => "owner_deadline",
            Self::ParentCancelled => "parent_cancelled",
            Self::ProcessorUnavailable => "processor_unavailable",
            Self::ServiceStopping => "service_stopping",
            Self::SessionClosed => "session_closed",
        }
    }
}

#[inline]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Records the metrics for one remoting request against one explicit telemetry runtime.
///
/// The guard records request duration and a classified outcome when the request reaches a terminal
/// state. A V2 deferred request may additionally record one `deferred_registered` lifecycle event
/// and one terminal `deferred_resumed`, `cancelled`, or `failed` outcome. Concurrent terminalization
/// may change exporter observation order, but each lifecycle event is recorded at most once. The
/// transport decoder records exact inbound frame bytes separately. Dropping an unfinished V1 guard
/// records cancellation; dropping an unfinished V2 guard records failure.
pub struct RequestMetricsGuard {
    metrics: RemotingMetrics,
    start: Instant,
    request_code: i32,
    is_long_polling: bool,
    rpc_recorded: bool,
    deferred_registered_recorded: bool,
    fallback: RequestOutcome,
    code_class: RequestCodeClass,
}

impl RequestMetricsGuard {
    #[inline]
    pub fn start(metrics: RemotingMetrics, request_code: i32, _request_bytes: u64, is_long_polling: bool) -> Self {
        Self {
            metrics,
            start: Instant::now(),
            request_code,
            is_long_polling,
            rpc_recorded: false,
            deferred_registered_recorded: false,
            fallback: RequestOutcome::Cancelled,
            code_class: RequestCodeClass::Other,
        }
    }

    /// Starts a V2 request guard with its trusted static request-code class.
    #[inline]
    pub fn start_v2(
        metrics: RemotingMetrics,
        request_code: i32,
        request_bytes: u64,
        is_long_polling: bool,
        code_class: RequestCodeClass,
    ) -> Self {
        let mut guard = Self::start(metrics, request_code, request_bytes, is_long_polling);
        guard.fallback = RequestOutcome::Failed;
        guard.code_class = code_class;
        guard
    }

    #[inline]
    pub fn complete_response(&mut self, response_code: i32) {
        self.record_terminal(response_code, RESULT_SUCCESS, RequestOutcome::LegacyReply);
    }

    /// Completes a V2 request with one terminal classified outcome.
    #[inline]
    pub fn complete_v2(&mut self, response_code: i32, outcome: RequestOutcome) {
        let result = if outcome == RequestOutcome::Failed {
            RESULT_PROCESS_REQUEST_FAILED
        } else {
            RESULT_SUCCESS
        };
        self.record_terminal(response_code, result, outcome);
    }

    /// Records durable deferred registration without completing the request.
    ///
    /// A deferred request emits this lifecycle event only after ownership is
    /// durably transferred to the registry and emits one terminal request
    /// outcome when it is resumed, cancelled, or fails. A concurrent terminal
    /// may be observed first; both events remain at-most-once. Response metrics
    /// remain terminal-only and are not recorded by this method.
    #[inline]
    pub fn record_v2_deferred_registered(&mut self) {
        if self.deferred_registered_recorded {
            return;
        }
        self.metrics.record_classified_request(
            self.code_class,
            RequestOutcome::DeferredRegistered,
            self.start.elapsed().as_secs_f64(),
        );
        self.deferred_registered_recorded = true;
    }

    #[inline]
    pub fn complete_oneway(&mut self) {
        self.record_terminal(NO_RESPONSE_CODE, RESULT_ONEWAY, RequestOutcome::Oneway);
    }

    #[inline]
    pub fn complete_cancelled(&mut self) {
        self.record_terminal(NO_RESPONSE_CODE, RESULT_CANCELED, RequestOutcome::Cancelled);
    }

    #[inline]
    pub fn complete_process_request_failed(&mut self, response_code: i32) {
        self.record_terminal(response_code, RESULT_PROCESS_REQUEST_FAILED, RequestOutcome::Failed);
    }

    #[inline]
    pub fn complete_write_channel_failed(&mut self, response_code: i32) {
        self.record_terminal(response_code, RESULT_WRITE_CHANNEL_FAILED, RequestOutcome::Failed);
    }

    #[inline]
    fn record_terminal(&mut self, response_code: i32, result: &'static str, outcome: RequestOutcome) {
        if self.rpc_recorded {
            return;
        }
        let elapsed = self.start.elapsed();
        self.metrics
            .record_classified_request(self.code_class, outcome, elapsed.as_secs_f64());
        self.metrics.record_rpc_latency(
            duration_millis_u64(elapsed),
            self.request_code,
            response_code,
            self.is_long_polling,
            result,
        );
        self.rpc_recorded = true;
    }
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        self.metrics
            .record_request_latency(duration_millis_u64(self.start.elapsed()));
        if !self.rpc_recorded {
            let fallback = self.fallback;
            self.record_terminal(
                NO_RESPONSE_CODE,
                if fallback == RequestOutcome::Cancelled {
                    RESULT_CANCELED
                } else {
                    RESULT_PROCESS_REQUEST_FAILED
                },
                fallback,
            );
        }
    }
}

#[cfg(not(feature = "otel-metrics"))]
#[derive(Debug, Clone, Default)]
pub struct RemotingMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl RemotingMetrics {
    /// Creates an instance-scoped no-op recorder.
    #[must_use]
    pub const fn noop() -> Self {
        Self
    }

    /// Creates a no-op recorder without reading process-global OpenTelemetry state.
    #[must_use]
    pub fn from_handle(_telemetry: &crate::TelemetryHandle) -> Self {
        Self::noop()
    }

    #[inline]
    pub fn record_requests_total(&self, _count: u64) {}

    /// Records one request lifecycle event and duration using fixed labels.
    #[inline]
    pub fn record_classified_request(&self, _code: RequestCodeClass, _outcome: RequestOutcome, _duration_seconds: f64) {
    }

    /// Records one terminal response using fixed mode and result labels.
    #[inline]
    pub fn record_response(&self, _mode: ResponseMode, _result: ResponseResult) {}

    /// Adjusts deferred gauges for one fixed request-code class.
    #[inline]
    pub fn adjust_deferred(&self, _code: RequestCodeClass, _inflight_delta: i64, _retained_bytes_delta: i64) {}

    /// Records time spent waiting in the canonical response queue.
    #[inline]
    pub fn record_response_queue_wait(&self, _duration_seconds: f64) {}

    /// Records a duplicate terminal response attempt for one fixed code class.
    #[inline]
    pub fn record_response_duplicate(&self, _code: RequestCodeClass) {}

    /// Records a terminal response abandonment reason from the closed vocabulary.
    #[inline]
    pub fn record_response_abandoned(&self, _reason: ResponseAbandonedReason) {}

    #[inline]
    pub fn record_request_latency(&self, _latency_ms: u64) {}

    #[inline]
    pub fn record_outbound_attempted_plaintext_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_outbound_accepted_plaintext_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_outbound_written_plaintext_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_inbound_decoded_plaintext_bytes(&self, _bytes: u64) {}

    #[inline]
    pub fn record_lifecycle_event(&self, _event: &'static str, _result: &'static str) {}

    #[inline]
    pub fn record_deferred_terminal(&self, _request_code_bucket: &'static str, _reason: &'static str) {}

    #[inline]
    pub fn record_lifecycle_listener_latency(&self, _latency_ms: u64, _event: &'static str) {}

    #[inline]
    pub fn record_rpc_latency(
        &self,
        _latency_ms: u64,
        _request_code: i32,
        _response_code: i32,
        _is_long_polling: bool,
        _result: &'static str,
    ) {
    }
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone, Default)]
pub struct RemotingMetrics {
    telemetry: Option<crate::TelemetryHandle>,
    instruments: Option<std::sync::Arc<RemotingMetricInstruments>>,
}

#[cfg(feature = "otel-metrics")]
#[derive(Clone)]
struct RemotingMetricInstruments {
    requests_total: opentelemetry::metrics::Counter<u64>,
    request_latency: opentelemetry::metrics::Histogram<u64>,
    request_duration_seconds: opentelemetry::metrics::Histogram<f64>,
    response_total: opentelemetry::metrics::Counter<u64>,
    deferred_inflight: opentelemetry::metrics::UpDownCounter<i64>,
    deferred_retained_bytes: opentelemetry::metrics::UpDownCounter<i64>,
    response_queue_wait_seconds: opentelemetry::metrics::Histogram<f64>,
    response_duplicate_total: opentelemetry::metrics::Counter<u64>,
    response_abandoned_total: opentelemetry::metrics::Counter<u64>,
    outbound_attempted_plaintext_bytes: opentelemetry::metrics::Counter<u64>,
    outbound_accepted_plaintext_bytes: opentelemetry::metrics::Counter<u64>,
    outbound_written_plaintext_bytes: opentelemetry::metrics::Counter<u64>,
    inbound_decoded_plaintext_bytes: opentelemetry::metrics::Counter<u64>,
    lifecycle_events: opentelemetry::metrics::Counter<u64>,
    deferred_terminals: opentelemetry::metrics::Counter<u64>,
    lifecycle_listener_latency: opentelemetry::metrics::Histogram<u64>,
    rpc_latency: opentelemetry::metrics::Histogram<u64>,
}

#[cfg(feature = "otel-metrics")]
impl RemotingMetrics {
    /// Creates an instance-scoped recorder that never emits metrics.
    #[must_use]
    pub fn noop() -> Self {
        Self::default()
    }

    /// Creates a recorder bound to the handle's fixed transport meter.
    ///
    /// A no-op, closing, or closed handle produces a no-op recorder. This method never reads the
    /// process-global OpenTelemetry meter provider.
    #[must_use]
    pub fn from_handle(telemetry: &crate::TelemetryHandle) -> Self {
        let Some(meter) = telemetry.meter(crate::TRANSPORT_METER_SCOPE) else {
            return Self::noop();
        };
        Self {
            telemetry: Some(telemetry.clone()),
            instruments: Some(std::sync::Arc::new(RemotingMetricInstruments::new(&meter))),
        }
    }

    /// Creates a recorder from an explicitly supplied meter.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            telemetry: None,
            instruments: Some(std::sync::Arc::new(RemotingMetricInstruments::new(meter))),
        }
    }

    #[inline]
    fn is_active(&self) -> bool {
        self.telemetry.as_ref().is_none_or(crate::TelemetryHandle::is_active)
    }

    #[inline]
    pub fn record_requests_total(&self, count: u64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.requests_total.add(
                    count,
                    &[
                        opentelemetry::KeyValue::new(crate::semantic::labels::CODE, RequestCodeClass::Other.as_str()),
                        opentelemetry::KeyValue::new(
                            crate::semantic::labels::OUTCOME,
                            RequestOutcome::LegacyReply.as_str(),
                        ),
                    ],
                );
            }
        }
    }

    /// Records one request lifecycle event and duration using fixed labels.
    #[inline]
    pub fn record_classified_request(&self, code: RequestCodeClass, outcome: RequestOutcome, duration_seconds: f64) {
        if !self.is_active() {
            return;
        }
        let Some(instruments) = &self.instruments else {
            return;
        };
        let attributes = [
            opentelemetry::KeyValue::new(crate::semantic::labels::CODE, code.as_str()),
            opentelemetry::KeyValue::new(crate::semantic::labels::OUTCOME, outcome.as_str()),
        ];
        instruments.requests_total.add(1, &attributes);
        instruments
            .request_duration_seconds
            .record(duration_seconds, &attributes);
    }

    /// Records one terminal response using fixed mode and result labels.
    #[inline]
    pub fn record_response(&self, mode: ResponseMode, result: ResponseResult) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.response_total.add(
                    1,
                    &[
                        opentelemetry::KeyValue::new(crate::semantic::labels::MODE, mode.as_str()),
                        opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result.as_str()),
                    ],
                );
            }
        }
    }

    /// Adjusts deferred gauges for one fixed request-code class.
    #[inline]
    pub fn adjust_deferred(&self, code: RequestCodeClass, inflight_delta: i64, retained_bytes_delta: i64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                let attributes = [opentelemetry::KeyValue::new(
                    crate::semantic::labels::CODE,
                    code.as_str(),
                )];
                instruments.deferred_inflight.add(inflight_delta, &attributes);
                instruments
                    .deferred_retained_bytes
                    .add(retained_bytes_delta, &attributes);
            }
        }
    }

    /// Records time spent waiting in the canonical response queue.
    #[inline]
    pub fn record_response_queue_wait(&self, duration_seconds: f64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.response_queue_wait_seconds.record(duration_seconds, &[]);
            }
        }
    }

    /// Records a duplicate terminal response attempt for one fixed code class.
    #[inline]
    pub fn record_response_duplicate(&self, code: RequestCodeClass) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.response_duplicate_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::CODE,
                        code.as_str(),
                    )],
                );
            }
        }
    }

    /// Records a terminal response abandonment reason from the closed vocabulary.
    #[inline]
    pub fn record_response_abandoned(&self, reason: ResponseAbandonedReason) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.response_abandoned_total.add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        crate::semantic::labels::REASON,
                        reason.as_str(),
                    )],
                );
            }
        }
    }

    #[inline]
    pub fn record_request_latency(&self, latency_ms: u64) {
        if self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.request_latency.record(latency_ms, &[]);
            }
        }
    }

    #[inline]
    pub fn record_outbound_attempted_plaintext_bytes(&self, bytes: u64) {
        if bytes != 0 && self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.outbound_attempted_plaintext_bytes.add(bytes, &[]);
            }
        }
    }

    #[inline]
    pub fn record_outbound_accepted_plaintext_bytes(&self, bytes: u64) {
        if bytes != 0 && self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.outbound_accepted_plaintext_bytes.add(bytes, &[]);
            }
        }
    }

    #[inline]
    pub fn record_outbound_written_plaintext_bytes(&self, bytes: u64) {
        if bytes != 0 && self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.outbound_written_plaintext_bytes.add(bytes, &[]);
            }
        }
    }

    #[inline]
    pub fn record_inbound_decoded_plaintext_bytes(&self, bytes: u64) {
        if bytes != 0 && self.is_active() {
            if let Some(instruments) = &self.instruments {
                instruments.inbound_decoded_plaintext_bytes.add(bytes, &[]);
            }
        }
    }

    #[inline]
    pub fn record_lifecycle_event(&self, event: &'static str, result: &'static str) {
        if !self.is_active() {
            return;
        }
        if let Some(instruments) = &self.instruments {
            instruments.lifecycle_events.add(
                1,
                &[
                    opentelemetry::KeyValue::new(crate::semantic::labels::EVENT, event),
                    opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result),
                ],
            );
        }
    }

    #[inline]
    pub fn record_deferred_terminal(&self, request_code_bucket: &'static str, reason: &'static str) {
        if !self.is_active() {
            return;
        }
        if let Some(instruments) = &self.instruments {
            instruments.deferred_terminals.add(
                1,
                &[
                    opentelemetry::KeyValue::new(crate::semantic::labels::REQUEST_CODE_BUCKET, request_code_bucket),
                    opentelemetry::KeyValue::new(crate::semantic::labels::REASON, reason),
                ],
            );
        }
    }

    #[inline]
    pub fn record_lifecycle_listener_latency(&self, latency_ms: u64, event: &'static str) {
        if !self.is_active() {
            return;
        }
        if let Some(instruments) = &self.instruments {
            instruments.lifecycle_listener_latency.record(
                latency_ms,
                &[opentelemetry::KeyValue::new(crate::semantic::labels::EVENT, event)],
            );
        }
    }

    #[inline]
    pub fn record_rpc_latency(
        &self,
        latency_ms: u64,
        request_code: i32,
        response_code: i32,
        is_long_polling: bool,
        result: &'static str,
    ) {
        if !self.is_active() {
            return;
        }
        let Some(instruments) = &self.instruments else {
            return;
        };
        let attributes = [
            opentelemetry::KeyValue::new(crate::semantic::labels::PROTOCOL_TYPE, "remoting"),
            opentelemetry::KeyValue::new(crate::semantic::labels::REQUEST_CODE, i64::from(request_code)),
            opentelemetry::KeyValue::new(crate::semantic::labels::RESPONSE_CODE, i64::from(response_code)),
            opentelemetry::KeyValue::new(crate::semantic::labels::IS_LONG_POLLING, is_long_polling),
            opentelemetry::KeyValue::new(crate::semantic::labels::RESULT, result),
        ];
        instruments.rpc_latency.record(latency_ms, &attributes);
    }
}

#[cfg(feature = "otel-metrics")]
impl RemotingMetricInstruments {
    fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        let requests_total = meter
            .u64_counter(TRANSPORT_REQUESTS_TOTAL)
            .with_description("Total number of remoting requests")
            .with_unit("{request}")
            .build();

        let request_latency = meter
            .u64_histogram(TRANSPORT_REQUEST_LATENCY)
            .with_description("Remoting request latency")
            .with_unit("ms")
            .build();

        let request_duration_seconds = meter
            .f64_histogram(TRANSPORT_REQUEST_DURATION_SECONDS)
            .with_description("V2 remoting request duration by fixed code class and terminal outcome")
            .with_unit("s")
            .build();

        let response_total = meter
            .u64_counter(TRANSPORT_RESPONSE_TOTAL)
            .with_description("Canonical response terminal outcomes by delivery mode")
            .with_unit("{response}")
            .build();

        let deferred_inflight = meter
            .i64_up_down_counter(TRANSPORT_DEFERRED_INFLIGHT)
            .with_description("V2 deferred responses currently retained by the transport")
            .with_unit("{request}")
            .build();

        let deferred_retained_bytes = meter
            .i64_up_down_counter(TRANSPORT_DEFERRED_RETAINED_BYTES)
            .with_description("Estimated bytes retained by V2 deferred responses")
            .with_unit("By")
            .build();

        let response_queue_wait_seconds = meter
            .f64_histogram(TRANSPORT_RESPONSE_QUEUE_WAIT_SECONDS)
            .with_description("Time accepted response writes wait before the canonical writer starts")
            .with_unit("s")
            .build();

        let response_duplicate_total = meter
            .u64_counter(TRANSPORT_RESPONSE_DUPLICATE_TOTAL)
            .with_description("Duplicate V2 response terminal attempts")
            .with_unit("{response}")
            .build();

        let response_abandoned_total = meter
            .u64_counter(TRANSPORT_RESPONSE_ABANDONED_TOTAL)
            .with_description("V2 responses terminated without response delivery")
            .with_unit("{response}")
            .build();

        let outbound_attempted_plaintext_bytes = meter
            .u64_counter(TRANSPORT_OUTBOUND_ATTEMPTED_PLAINTEXT_BYTES)
            .with_description("Plaintext bytes offered to the transport writer")
            .with_unit("By")
            .build();
        let outbound_accepted_plaintext_bytes = meter
            .u64_counter(TRANSPORT_OUTBOUND_ACCEPTED_PLAINTEXT_BYTES)
            .with_description("Plaintext bytes accepted by the transport writer")
            .with_unit("By")
            .build();
        let outbound_written_plaintext_bytes = meter
            .u64_counter(TRANSPORT_OUTBOUND_WRITTEN_PLAINTEXT_BYTES)
            .with_description("Plaintext bytes completely written and flushed")
            .with_unit("By")
            .build();
        let inbound_decoded_plaintext_bytes = meter
            .u64_counter(TRANSPORT_INBOUND_DECODED_PLAINTEXT_BYTES)
            .with_description("Plaintext bytes in successfully decoded frames")
            .with_unit("By")
            .build();

        let lifecycle_events = meter
            .u64_counter(TRANSPORT_LIFECYCLE_EVENTS_TOTAL)
            .with_description("Connection lifecycle events by enqueue and delivery result")
            .with_unit("{event}")
            .build();

        let deferred_terminals = meter
            .u64_counter(TRANSPORT_DEFERRED_TERMINAL_TOTAL)
            .with_description("Deferred responses terminated without response delivery")
            .with_unit("{response}")
            .build();

        let lifecycle_listener_latency = meter
            .u64_histogram(TRANSPORT_LIFECYCLE_LISTENER_LATENCY)
            .with_description("Connection lifecycle listener callback latency")
            .with_unit("ms")
            .build();

        let rpc_latency = meter
            .u64_histogram(RPC_LATENCY)
            .with_description("Rpc latency")
            .with_unit("milliseconds")
            .build();

        Self {
            requests_total,
            request_latency,
            request_duration_seconds,
            response_total,
            deferred_inflight,
            deferred_retained_bytes,
            response_queue_wait_seconds,
            response_duplicate_total,
            response_abandoned_total,
            outbound_attempted_plaintext_bytes,
            outbound_accepted_plaintext_bytes,
            outbound_written_plaintext_bytes,
            inbound_decoded_plaintext_bytes,
            lifecycle_events,
            deferred_terminals,
            lifecycle_listener_latency,
            rpc_latency,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_request_metrics_guard_accepts_each_terminal_outcome_once() {
        let metrics = RemotingMetrics::from_handle(&crate::TelemetryHandle::noop());

        let mut success = RequestMetricsGuard::start(metrics.clone(), 10, 128, false);
        success.complete_response(0);
        success.complete_cancelled();

        let mut process_failure = RequestMetricsGuard::start(metrics.clone(), 12, 32, true);
        process_failure.complete_process_request_failed(1);
        process_failure.complete_response(0);

        let mut write_failure = RequestMetricsGuard::start(metrics.clone(), 13, 16, false);
        write_failure.complete_write_channel_failed(2);
        write_failure.complete_oneway();

        drop(RequestMetricsGuard::start(metrics, 14, 8, false));
    }

    #[cfg(feature = "otel-metrics")]
    #[test]
    fn remoting_metrics_constructs_and_records() {
        use opentelemetry::metrics::MeterProvider;

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let meter = provider.meter("remoting-metrics-test");
        let metrics = RemotingMetrics::new(&meter);

        metrics.record_requests_total(1);
        metrics.record_classified_request(RequestCodeClass::PullMessage, RequestOutcome::ReplyBytes, 0.003);
        metrics.record_response(ResponseMode::Inline, ResponseResult::TransportWritten);
        metrics.adjust_deferred(RequestCodeClass::PullMessage, 1, 512);
        metrics.adjust_deferred(RequestCodeClass::PullMessage, -1, -512);
        metrics.record_response_queue_wait(0.001);
        metrics.record_response_duplicate(RequestCodeClass::PullMessage);
        metrics.record_response_abandoned(ResponseAbandonedReason::Abandoned);
        metrics.record_request_latency(3);
        metrics.record_outbound_attempted_plaintext_bytes(256);
        metrics.record_outbound_accepted_plaintext_bytes(256);
        metrics.record_outbound_written_plaintext_bytes(256);
        metrics.record_inbound_decoded_plaintext_bytes(256);
        metrics.record_lifecycle_event("connected", "queued");
        metrics.record_deferred_terminal("pull_message", "owner_deadline");
        metrics.record_lifecycle_listener_latency(2, "connected");
        metrics.record_rpc_latency(5, 10, 0, false, RESULT_SUCCESS);
    }
}
