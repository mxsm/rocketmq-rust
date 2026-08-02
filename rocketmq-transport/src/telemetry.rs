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

/// Cloneable metrics and tracing capability for one transport composition.
///
/// A no-op value is explicit and never consults process-global OpenTelemetry state.
#[derive(Clone, Default)]
pub struct TransportTelemetry {
    #[cfg(feature = "observability")]
    remoting: rocketmq_observability::metrics::remoting::RemotingMetrics,
    #[cfg(any(feature = "observability", feature = "observability-traces"))]
    handle: Option<rocketmq_observability::TelemetryHandle>,
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
            handle: Some(telemetry.clone()),
        }
    }

    #[inline]
    pub(crate) fn request_span(&self, request_code: i32, request_opaque: i32) -> tracing::Span {
        #[cfg(any(feature = "observability", feature = "observability-traces"))]
        if self
            .handle
            .as_ref()
            .is_some_and(|handle| handle.is_active() && handle.trace_policy().enabled)
        {
            return tracing::info_span!(
                "RocketMQ REMOTING REQUEST",
                rocketmq.request.code = request_code,
                rocketmq.request.opaque = request_opaque,
            );
        }

        let _ = (request_code, request_opaque);
        tracing::Span::none()
    }

    #[inline]
    pub(crate) fn record_network_bytes(&self, bytes: usize) {
        #[cfg(feature = "observability")]
        self.remoting.record_network_bytes(bytes as u64);

        #[cfg(not(feature = "observability"))]
        let _ = bytes;
    }

    #[inline]
    pub(crate) fn record_lifecycle_event(&self, event: &'static str, result: &'static str) {
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
    pub(crate) fn complete_cancelled(&mut self) {
        #[cfg(feature = "observability")]
        self.inner.complete_cancelled();
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
    use super::TransportTelemetry;

    #[test]
    fn noop_transport_telemetry_covers_request_and_network_paths() {
        let telemetry = TransportTelemetry::noop();
        telemetry.record_network_bytes(128);
        telemetry.record_lifecycle_event("connected", "queued");
        telemetry.record_lifecycle_listener_latency(std::time::Duration::from_millis(1), "connected");
        assert!(telemetry.request_span(10, 1).is_disabled());
        let mut guard = telemetry.request_guard(10, 64, false);
        guard.complete_response(0);
        guard.complete_cancelled();
    }
}
