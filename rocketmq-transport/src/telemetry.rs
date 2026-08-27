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

#[cfg(any(test, feature = "observability", feature = "observability-traces"))]
#[inline]
fn request_identity_span(identity: crate::dispatch::OriginalRequestIdentity) -> tracing::Span {
    tracing::info_span!(
        "RocketMQ REMOTING REQUEST",
        rocketmq.request.code = identity.original_code(),
        rocketmq.request.owner_id = identity.request_id().owner_id(),
        rocketmq.request.sequence = identity.request_id().sequence(),
    )
}

#[cfg(test)]
type LegacyProcessorRequestCapture = std::sync::Arc<parking_lot::Mutex<Vec<(&'static str, i32)>>>;
#[cfg(test)]
type LifecycleEventCapture = std::sync::Arc<parking_lot::Mutex<Vec<(&'static str, &'static str)>>>;

/// Cloneable metrics and tracing capability for one transport composition.
///
/// A no-op value is explicit and never consults process-global OpenTelemetry state.
#[derive(Clone, Default)]
pub struct TransportTelemetry {
    #[cfg(feature = "observability")]
    remoting: rocketmq_observability::metrics::remoting::RemotingMetrics,
    #[cfg(feature = "observability")]
    client: rocketmq_observability::metrics::client::ClientMetrics,
    #[cfg(any(feature = "observability", feature = "observability-traces"))]
    handle: Option<rocketmq_observability::TelemetryHandle>,
    #[cfg(test)]
    legacy_processor_requests: Option<LegacyProcessorRequestCapture>,
    #[cfg(test)]
    lifecycle_events: Option<LifecycleEventCapture>,
}

impl TransportTelemetry {
    /// Creates a transport telemetry capability that never records.
    #[must_use]
    pub fn noop() -> Self {
        Self::default()
    }

    /// Binds transport metrics and tracing to one explicit telemetry runtime.
    #[cfg(any(feature = "observability", feature = "observability-traces"))]
    #[must_use]
    pub fn from_handle(telemetry: &rocketmq_observability::TelemetryHandle) -> Self {
        Self {
            #[cfg(feature = "observability")]
            remoting: rocketmq_observability::metrics::remoting::RemotingMetrics::from_handle(telemetry),
            #[cfg(feature = "observability")]
            client: rocketmq_observability::metrics::client::ClientMetrics::from_handle(telemetry),
            handle: Some(telemetry.clone()),
            #[cfg(test)]
            legacy_processor_requests: None,
            #[cfg(test)]
            lifecycle_events: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_legacy_processor_request_capture() -> (Self, LegacyProcessorRequestCapture) {
        let capture = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let telemetry = Self {
            #[cfg(feature = "observability")]
            remoting: Default::default(),
            #[cfg(feature = "observability")]
            client: Default::default(),
            #[cfg(any(feature = "observability", feature = "observability-traces"))]
            handle: None,
            legacy_processor_requests: Some(std::sync::Arc::clone(&capture)),
            lifecycle_events: None,
        };
        (telemetry, capture)
    }

    #[cfg(test)]
    pub(crate) fn with_lifecycle_event_capture() -> (Self, LifecycleEventCapture) {
        let capture = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let telemetry = Self {
            #[cfg(feature = "observability")]
            remoting: Default::default(),
            #[cfg(feature = "observability")]
            client: Default::default(),
            #[cfg(any(feature = "observability", feature = "observability-traces"))]
            handle: None,
            legacy_processor_requests: None,
            lifecycle_events: Some(std::sync::Arc::clone(&capture)),
        };
        (telemetry, capture)
    }

    #[inline]
    pub(crate) fn request_span(&self, identity: crate::dispatch::OriginalRequestIdentity) -> tracing::Span {
        #[cfg(any(feature = "observability", feature = "observability-traces"))]
        if self
            .handle
            .as_ref()
            .is_some_and(|handle| handle.is_active() && handle.trace_policy().enabled)
        {
            return request_identity_span(identity);
        }

        let _ = identity;
        tracing::Span::none()
    }

    #[inline]
    pub(crate) fn record_outbound_attempted_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_outbound_attempted_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_outbound_accepted_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_outbound_accepted_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_outbound_written_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_outbound_written_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_inbound_decoded_plaintext_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_inbound_decoded_plaintext_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_lifecycle_event(&self, event: &'static str, result: &'static str) {
        #[cfg(test)]
        if let Some(capture) = &self.lifecycle_events {
            capture.lock().push((event, result));
        }

        #[cfg(feature = "observability")]
        self.remoting.record_lifecycle_event(event, result);

        #[cfg(not(feature = "observability"))]
        let _ = (event, result);
    }

    #[inline]
    pub(crate) fn record_lifecycle_listener_latency(&self, latency: std::time::Duration, event: &'static str) {
        #[cfg(feature = "observability")]
        self.remoting
            .record_lifecycle_listener_latency(latency.as_millis().min(u128::from(u64::MAX)) as u64, event);

        #[cfg(not(feature = "observability"))]
        let _ = (latency, event);
    }

    #[inline]
    #[allow(
        dead_code,
        reason = "DSP-05 adapter metrics remain dormant until DSP-06 coexistence routing"
    )]
    pub(crate) fn record_legacy_processor_request(&self, processor: &'static str, request_code: i32) {
        #[cfg(test)]
        if let Some(capture) = &self.legacy_processor_requests {
            capture.lock().push((processor, request_code));
        }

        #[cfg(feature = "observability")]
        self.remoting.record_legacy_processor_request(processor, request_code);

        #[cfg(not(feature = "observability"))]
        let _ = (processor, request_code);
    }

    #[inline]
    pub(crate) fn record_nameserver_failover(&self, reason: TransportNameServerFailoverReason) {
        #[cfg(feature = "observability")]
        self.client.record_nameserver_failover(match reason {
            TransportNameServerFailoverReason::ConnectFailure => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::ConnectFailure
            }
            TransportNameServerFailoverReason::Unhealthy => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::Unhealthy
            }
            TransportNameServerFailoverReason::CircuitOpen => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::CircuitOpen
            }
            TransportNameServerFailoverReason::Draining => {
                rocketmq_observability::metrics::client::NameServerFailoverReason::Draining
            }
        });

        #[cfg(not(feature = "observability"))]
        let _ = reason;
    }

    #[inline]
    pub(crate) fn record_go_away(&self, outcome: TransportGoAwayOutcome) {
        self.record_lifecycle_event("go_away", outcome.as_str());
    }

    #[inline]
    pub(crate) fn request_guard(
        &self,
        request_code: i32,
        request_bytes: u64,
        is_long_polling: bool,
    ) -> TransportRequestMetricsGuard {
        #[cfg(feature = "observability")]
        {
            TransportRequestMetricsGuard {
                inner: rocketmq_observability::metrics::remoting::RequestMetricsGuard::start(
                    self.remoting.clone(),
                    request_code,
                    request_bytes,
                    is_long_polling,
                ),
            }
        }

        #[cfg(not(feature = "observability"))]
        {
            let _ = (request_code, request_bytes, is_long_polling);
            TransportRequestMetricsGuard {}
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TransportNameServerFailoverReason {
    ConnectFailure,
    Unhealthy,
    CircuitOpen,
    Draining,
}

#[derive(Clone, Copy)]
pub(crate) enum TransportGoAwayOutcome {
    Received,
    RetrySuccess,
    RetryFailed,
}

impl TransportGoAwayOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::RetrySuccess => "retry_success",
            Self::RetryFailed => "retry_failed",
        }
    }
}

pub(crate) struct TransportRequestMetricsGuard {
    #[cfg(feature = "observability")]
    inner: rocketmq_observability::metrics::remoting::RequestMetricsGuard,
}

impl TransportRequestMetricsGuard {
    #[inline]
    pub(crate) fn complete_response(&mut self, response_code: i32) {
        #[cfg(feature = "observability")]
        self.inner.complete_response(response_code);

        #[cfg(not(feature = "observability"))]
        let _ = response_code;
    }

    #[inline]
    pub(crate) fn complete_oneway(&mut self) {
        #[cfg(feature = "observability")]
        self.inner.complete_oneway();
    }

    #[inline]
    pub(crate) fn complete_legacy_ambiguous_none(&mut self) {
        #[cfg(feature = "observability")]
        self.inner.complete_legacy_ambiguous_none();
    }

    #[inline]
    pub(crate) fn complete_process_request_failed(&mut self, response_code: i32) {
        #[cfg(feature = "observability")]
        self.inner.complete_process_request_failed(response_code);

        #[cfg(not(feature = "observability"))]
        let _ = response_code;
    }

    #[inline]
    pub(crate) fn complete_write_channel_failed(&mut self, response_code: i32) {
        #[cfg(feature = "observability")]
        self.inner.complete_write_channel_failed(response_code);

        #[cfg(not(feature = "observability"))]
        let _ = response_code;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::sync::Mutex;

    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use tracing::field::Field;
    use tracing::field::Visit;
    use tracing::span::Attributes;
    use tracing::span::Id;
    use tracing::span::Record;
    use tracing::Event;
    use tracing::Metadata;
    use tracing::Subscriber;

    use super::request_identity_span;
    use super::TransportGoAwayOutcome;
    use super::TransportNameServerFailoverReason;
    use super::TransportTelemetry;

    struct FieldCapture<'a> {
        fields: &'a mut BTreeMap<String, String>,
    }

    impl Visit for FieldCapture<'_> {
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.fields.insert(field.name().to_owned(), value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields.insert(field.name().to_owned(), value.to_string());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.fields.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    struct SpanCapture {
        fields: Arc<Mutex<BTreeMap<String, String>>>,
    }

    impl Subscriber for SpanCapture {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attributes: &Attributes<'_>) -> Id {
            let mut fields = self.fields.lock().expect("span field capture lock");
            attributes.record(&mut FieldCapture { fields: &mut fields });
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {}

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    #[test]
    fn noop_transport_telemetry_covers_request_and_network_paths() {
        let telemetry = TransportTelemetry::noop();
        telemetry.record_outbound_attempted_plaintext_bytes(128);
        telemetry.record_outbound_accepted_plaintext_bytes(128);
        telemetry.record_outbound_written_plaintext_bytes(128);
        telemetry.record_lifecycle_event("connected", "queued");
        telemetry.record_lifecycle_listener_latency(std::time::Duration::from_millis(1), "connected");
        telemetry.record_legacy_processor_request("noop-legacy", 10);
        telemetry.record_nameserver_failover(TransportNameServerFailoverReason::ConnectFailure);
        telemetry.record_go_away(TransportGoAwayOutcome::Received);
        let identity = crate::dispatch::OriginalRequestIdentity::capture(
            1,
            &AtomicU64::new(1),
            &RemotingCommand::create_remoting_command(10).set_opaque(1),
        )
        .expect("test request identity should be allocated");
        assert!(telemetry.request_span(identity).is_disabled());
        let mut guard = telemetry.request_guard(10, 64, false);
        guard.complete_response(0);
        guard.complete_legacy_ambiguous_none();
    }

    #[test]
    fn request_span_records_only_original_low_cardinality_identity_fields() {
        let identity = crate::dispatch::OriginalRequestIdentity::capture(
            73,
            &AtomicU64::new(41),
            &RemotingCommand::create_remoting_command(-91_764).set_opaque(889),
        )
        .expect("test request identity should be allocated");
        let fields = Arc::new(Mutex::new(BTreeMap::new()));
        let subscriber = SpanCapture {
            fields: Arc::clone(&fields),
        };

        tracing::subscriber::with_default(subscriber, || {
            let span = request_identity_span(identity);
            assert!(!span.is_disabled());
        });

        let fields = fields.lock().expect("captured request span fields");
        assert_eq!(fields.get("rocketmq.request.code").map(String::as_str), Some("-91764"));
        assert_eq!(fields.get("rocketmq.request.owner_id").map(String::as_str), Some("73"));
        assert_eq!(fields.get("rocketmq.request.sequence").map(String::as_str), Some("41"));
        assert_eq!(fields.len(), 3, "request span fields: {fields:?}");
        assert!(fields.keys().all(|field| !field.contains("opaque")));
    }
}
