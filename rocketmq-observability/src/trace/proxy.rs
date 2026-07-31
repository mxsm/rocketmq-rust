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

//! Bounded Proxy forwarding and authentication spans.
//!
//! The helpers accept only static RPC catalog names and bounded terminal
//! outcomes. Principals, client identifiers, addresses, credentials, request
//! payloads, and raw errors are deliberately absent from this contract.

pub use super::span_names::PROXY_AUTH;
pub use super::span_names::PROXY_FORWARD;

/// Stable terminal outcomes allowed on Proxy diagnostic spans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxySpanOutcome {
    Success,
    PayloadFailure,
    TransportFailure,
    Denied,
    Bypassed,
}

/// Creates a forwarding span as a child of the bounded Proxy RPC span.
pub fn forward_span(parent: &tracing::Span, rpc: &'static str) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        tracing::info_span!(
            parent: parent,
            PROXY_FORWARD,
            rpc,
            result = tracing::field::Empty,
        )
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = (parent, rpc);
        tracing::Span::none()
    }
}

/// Creates an authentication span as a child of the bounded Proxy RPC span.
pub fn auth_span(parent: &tracing::Span, rpc: &'static str) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        tracing::info_span!(
            parent: parent,
            PROXY_AUTH,
            rpc,
            result = tracing::field::Empty,
        )
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = (parent, rpc);
        tracing::Span::none()
    }
}

/// Records one stable terminal outcome on a Proxy diagnostic span.
pub fn record_outcome(span: &tracing::Span, outcome: ProxySpanOutcome) {
    let result = match outcome {
        ProxySpanOutcome::Success => "success",
        ProxySpanOutcome::PayloadFailure => "payload_failure",
        ProxySpanOutcome::TransportFailure => "transport_failure",
        ProxySpanOutcome::Denied => "denied",
        ProxySpanOutcome::Bypassed => "bypassed",
    };
    span.record(crate::semantic::labels::RESULT, result);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_spans_accept_only_bounded_contract_fields() {
        let parent = tracing::Span::none();
        let forward = forward_span(&parent, "SendMessage");
        let auth = auth_span(&parent, "SendMessage");

        record_outcome(&forward, ProxySpanOutcome::Success);
        record_outcome(&auth, ProxySpanOutcome::Bypassed);
    }
}
