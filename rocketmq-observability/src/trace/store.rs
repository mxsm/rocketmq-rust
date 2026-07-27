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

pub use super::span_names::STORE_APPEND;
pub use super::span_names::STORE_DISPATCH;
pub use super::span_names::STORE_FLUSH;

/// Creates an append span only when the explicit Store trace policy admits tracing.
#[must_use]
pub fn append_span(handle: &crate::TelemetryHandle) -> tracing::Span {
    if handle.trace_policy().enabled {
        tracing::info_span!(
            "RocketMQ STORE APPEND",
            messaging.message.id = tracing::field::Empty,
            messaging.message.body.size = tracing::field::Empty,
            messaging.rocketmq.message.keys = tracing::field::Empty,
        )
    } else {
        tracing::Span::none()
    }
}

/// Creates a flush span only when the explicit Store trace policy admits tracing.
#[must_use]
pub fn flush_span(handle: &crate::TelemetryHandle) -> tracing::Span {
    if handle.trace_policy().enabled {
        tracing::info_span!("RocketMQ STORE FLUSH")
    } else {
        tracing::Span::none()
    }
}

/// Creates a dispatch span only when the explicit Store trace policy admits tracing.
#[must_use]
pub fn dispatch_span(handle: &crate::TelemetryHandle) -> tracing::Span {
    if handle.trace_policy().enabled {
        tracing::info_span!("RocketMQ STORE DISPATCH")
    } else {
        tracing::Span::none()
    }
}
