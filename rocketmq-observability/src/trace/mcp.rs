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

//! Bounded spans for the MCP Tool and Resource execution surfaces.
//!
//! Callers must pass only static catalog operation identifiers. Request
//! arguments, Resource URIs, tenant or cluster identifiers, and raw errors are
//! deliberately absent from this contract.

pub use super::span_names::MCP_RESOURCE;
pub use super::span_names::MCP_TOOL;

use crate::metrics::mcp::McpOperationOutcome;

/// Creates a Tool execution span containing only bounded fields.
pub fn tool_span(operation: &'static str) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        tracing::info_span!(MCP_TOOL, operation, result = tracing::field::Empty,)
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = operation;
        tracing::Span::none()
    }
}

/// Creates a Resource execution span containing only bounded fields.
pub fn resource_span(operation: &'static str) -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        tracing::info_span!(MCP_RESOURCE, operation, result = tracing::field::Empty,)
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        let _ = operation;
        tracing::Span::none()
    }
}

/// Records one stable terminal outcome on an MCP span.
pub fn record_outcome(span: &tracing::Span, outcome: McpOperationOutcome) {
    let result = match outcome {
        McpOperationOutcome::Success => "success",
        McpOperationOutcome::Failure => "failure",
        McpOperationOutcome::Denied => "denied",
    };
    span.record(crate::semantic::labels::RESULT, result);
}
