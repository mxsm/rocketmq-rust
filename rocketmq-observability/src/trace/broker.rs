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

pub use super::span_names::BROKER_RECEIVE_SEND;

/// Creates a Broker receive span only when the injected trace policy enables tracing.
#[must_use]
pub fn receive_send_span(telemetry: &crate::TelemetryHandle, request_code: i32, request_opaque: i32) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        if !telemetry.trace_policy().enabled {
            return tracing::Span::none();
        }
        tracing::info_span!(
            BROKER_RECEIVE_SEND,
            rocketmq.request.code = request_code,
            rocketmq.request.opaque = request_opaque,
            messaging.message.id = tracing::field::Empty,
            messaging.message.body.size = tracing::field::Empty,
            messaging.rocketmq.message.keys = tracing::field::Empty,
        )
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = (telemetry, request_code, request_opaque);
        tracing::Span::none()
    }
}
