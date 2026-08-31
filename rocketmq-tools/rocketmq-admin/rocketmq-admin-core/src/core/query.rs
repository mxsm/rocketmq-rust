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

//! Bounded completeness evidence for multi-source admin queries.

use std::net::IpAddr;
use std::net::SocketAddr;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

use crate::core::AdminError;
use crate::core::AdminResult;

/// Maximum number of source failures exposed by one admin query.
pub const MAX_ADMIN_SOURCE_FAILURES: usize = 16;
/// Stable warning emitted whenever usable data is accompanied by source failures.
pub const SOURCE_FAILURE_WARNING: &str = "source_failures_present";
/// Stable warning emitted after source failures have been capped.
pub const SOURCE_FAILURE_OVERFLOW_WARNING: &str = "source_failures_truncated";

/// Allowlisted backend observation that can fail independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminQuerySource {
    BrokerRuntime,
    BrokerConfig,
    BrokerLogFilter,
    ConsumerStatistics,
    ConsumerConnection,
    ProducerConnection,
    TopicConfig,
    ConsumerGroupConfig,
    SubscriptionGroups,
}

/// Stable, backend-independent failure classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminQueryFailureCode {
    SourceUnavailable,
    Timeout,
    PermissionDenied,
    NotFound,
    RateLimited,
    InvalidResponse,
}

/// Sanitized evidence for one independently failed source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct AdminSourceFailure {
    source: AdminQuerySource,
    code: AdminQueryFailureCode,
    retryable: bool,
    logical_target: String,
}

impl AdminSourceFailure {
    /// Creates a failure while reducing the target to a bounded logical identifier.
    pub fn new(
        source: AdminQuerySource,
        code: AdminQueryFailureCode,
        retryable: bool,
        logical_target: impl AsRef<str>,
    ) -> Self {
        Self {
            source,
            code,
            retryable,
            logical_target: sanitize_logical_target(logical_target.as_ref()),
        }
    }

    pub const fn source(&self) -> AdminQuerySource {
        self.source
    }

    pub const fn code(&self) -> AdminQueryFailureCode {
        self.code
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    pub fn logical_target(&self) -> &str {
        &self.logical_target
    }
}

impl<'de> Deserialize<'de> for AdminSourceFailure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireFailure {
            source: AdminQuerySource,
            code: AdminQueryFailureCode,
            retryable: bool,
            logical_target: String,
        }

        let wire = WireFailure::deserialize(deserializer)?;
        Ok(Self::new(wire.source, wire.code, wire.retryable, wire.logical_target))
    }
}

/// Additive result returned by evidence-aware sibling query APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminQueryResult<T> {
    pub data: T,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub source_failures: Vec<AdminSourceFailure>,
}

impl<T> AdminQueryResult<T> {
    /// Wraps an authoritative complete result, including an empty result.
    pub fn complete(data: T) -> Self {
        Self {
            data,
            partial: false,
            warnings: Vec::new(),
            source_failures: Vec::new(),
        }
    }

    /// Builds a result from independently attempted required sources.
    ///
    /// A zero-source inventory is an authoritative empty result. If sources
    /// were attempted but none succeeded, the query returns a stable total
    /// error instead of a successful empty or zero-valued DTO.
    pub fn from_sources(
        data: T,
        successful_sources: usize,
        source_failures: Vec<AdminSourceFailure>,
    ) -> AdminResult<Self> {
        if successful_sources == 0 && !source_failures.is_empty() {
            let retryable = source_failures.iter().all(AdminSourceFailure::retryable);
            return Err(AdminError::backend_view(
                "admin_query_sources",
                "ADMIN_QUERY_ALL_SOURCES_FAILED",
                "All required admin query sources failed",
                None,
                503,
                retryable,
            ));
        }

        Ok(Self::from_partial_sources(data, source_failures))
    }

    /// Maps the data while preserving completeness evidence.
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> AdminQueryResult<U> {
        AdminQueryResult {
            data: map(self.data),
            partial: self.partial,
            warnings: self.warnings,
            source_failures: self.source_failures,
        }
    }

    fn from_partial_sources(data: T, mut source_failures: Vec<AdminSourceFailure>) -> Self {
        source_failures.sort();
        source_failures.dedup();
        let overflow = source_failures.len() > MAX_ADMIN_SOURCE_FAILURES;
        source_failures.truncate(MAX_ADMIN_SOURCE_FAILURES);
        let partial = !source_failures.is_empty();
        let mut warnings = Vec::new();
        if partial {
            warnings.push(SOURCE_FAILURE_WARNING.to_string());
        }
        if overflow {
            warnings.push(SOURCE_FAILURE_OVERFLOW_WARNING.to_string());
        }
        Self {
            data,
            partial,
            warnings,
            source_failures,
        }
    }
}

fn sanitize_logical_target(target: &str) -> String {
    const UNKNOWN_TARGET: &str = "unknown";
    const MAX_TARGET_BYTES: usize = 128;

    let target = target.trim();
    if target.is_empty()
        || target.len() > MAX_TARGET_BYTES
        || target.parse::<IpAddr>().is_ok()
        || target.parse::<SocketAddr>().is_ok()
        || target.contains([':', '/', '\\', '@', '=', '&', '?'])
        || target.chars().any(char::is_control)
    {
        return UNKNOWN_TARGET.to_string();
    }

    let sanitized = target
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        UNKNOWN_TARGET.to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(target: &str) -> AdminSourceFailure {
        AdminSourceFailure::new(
            AdminQuerySource::BrokerRuntime,
            AdminQueryFailureCode::SourceUnavailable,
            true,
            target,
        )
    }

    #[test]
    fn authoritative_empty_is_complete_and_failed_zero_is_an_error() {
        let empty = AdminQueryResult::from_sources(Vec::<u8>::new(), 0, Vec::new()).unwrap();
        assert!(!empty.partial);
        assert!(empty.source_failures.is_empty());

        let error = AdminQueryResult::from_sources(Vec::<u8>::new(), 0, vec![failure("broker-a")]).unwrap_err();
        assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
        assert!(error.is_retryable());
    }

    #[test]
    fn failures_are_sanitized_sorted_deduplicated_and_bounded() {
        let mut failures = (0..20)
            .rev()
            .map(|index| failure(&format!("broker-{index:02}")))
            .collect::<Vec<_>>();
        failures.push(failure("broker-00"));
        failures.push(failure("127.0.0.1:10911"));
        let result = AdminQueryResult::from_sources((), 1, failures).unwrap();

        assert!(result.partial);
        assert_eq!(result.source_failures.len(), MAX_ADMIN_SOURCE_FAILURES);
        assert_eq!(result.source_failures[0].logical_target(), "broker-00");
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning == SOURCE_FAILURE_OVERFLOW_WARNING));
        assert!(!serde_json::to_string(&result).unwrap().contains("127.0.0.1"));
    }

    #[test]
    fn deserialization_reapplies_logical_target_sanitization() {
        let failure: AdminSourceFailure = serde_json::from_value(serde_json::json!({
            "source": "consumer_connection",
            "code": "timeout",
            "retryable": true,
            "logical_target": "10.0.0.1:10911"
        }))
        .unwrap();
        assert_eq!(failure.logical_target(), "unknown");
    }
}
