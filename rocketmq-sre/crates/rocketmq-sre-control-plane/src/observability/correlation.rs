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

use rocketmq_sre_contracts::CorrelationId;

/// HTTP header used to propagate one operation identity through SRE, Connector,
/// MCP, evidence collection, diagnostics, and model invocation.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// Safe propagation context for a single logical SRE operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorrelationContext {
    id: CorrelationId,
}

impl CorrelationContext {
    #[must_use]
    pub const fn from_id(id: CorrelationId) -> Self {
        Self { id }
    }

    /// Uses a valid caller-provided UUID or generates a fresh correlation ID.
    ///
    /// The raw header value is never retained or recorded. Invalid input is
    /// replaced instead of being copied into an error or span.
    #[must_use]
    pub fn from_optional_header(value: Option<&str>) -> Self {
        let id = value
            .and_then(|candidate| candidate.parse::<CorrelationId>().ok())
            .unwrap_or_default();
        Self { id }
    }

    #[must_use]
    pub const fn id(self) -> CorrelationId {
        self.id
    }

    #[must_use]
    pub fn header_value(self) -> String {
        self.id.to_string()
    }
}

impl Default for CorrelationContext {
    fn default() -> Self {
        Self {
            id: CorrelationId::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_header_is_reused_across_operation_boundaries() {
        let original = CorrelationId::new();
        let context = CorrelationContext::from_optional_header(Some(original.to_string().as_str()));

        assert_eq!(context.id(), original);
        assert_eq!(context.header_value(), original.to_string());
    }

    #[test]
    fn invalid_header_is_not_retained() {
        let context = CorrelationContext::from_optional_header(Some("token=secret"));

        assert_ne!(context.header_value(), "token=secret");
        assert!(context.header_value().parse::<CorrelationId>().is_ok());
    }
}
