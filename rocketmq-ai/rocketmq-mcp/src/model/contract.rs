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

use std::ops::Deref;

use chrono::SecondsFormat;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

pub const SCHEMA_VERSION: &str = "rocketmq-mcp.v2";
pub const DEFAULT_PAGE_LIMIT: u32 = 50;
pub const MAX_PAGE_LIMIT: u32 = 200;
pub const MAX_SOURCE_FAILURES: usize = 16;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum QuerySource {
    BrokerRuntime,
    ConsumerStatistics,
    ConsumerConnection,
    ProducerConnection,
    SubscriptionGroups,
    TopicRoute,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SourceFailureCode {
    SourceUnavailable,
    Timeout,
    PermissionDenied,
    NotFound,
    RateLimited,
    InvalidResponse,
}

#[derive(Debug, Clone, Serialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct SourceFailure {
    pub source: QuerySource,
    pub code: SourceFailureCode,
    pub retryable: bool,
    pub logical_target: String,
}

impl<'de> Deserialize<'de> for SourceFailure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireFailure {
            source: QuerySource,
            code: SourceFailureCode,
            retryable: bool,
            logical_target: String,
        }

        let failure = WireFailure::deserialize(deserializer)?;
        Ok(Self::new(
            failure.source,
            failure.code,
            failure.retryable,
            failure.logical_target,
        ))
    }
}

impl SourceFailure {
    pub(crate) fn new(
        source: QuerySource,
        code: SourceFailureCode,
        retryable: bool,
        logical_target: impl AsRef<str>,
    ) -> Self {
        Self {
            source,
            code,
            retryable,
            logical_target: safe_logical_target(logical_target.as_ref()),
        }
    }

    pub(crate) fn from_admin(failure: &rocketmq_admin_core::core::query::AdminSourceFailure) -> Self {
        use rocketmq_admin_core::core::query::AdminQueryFailureCode as AdminCode;
        use rocketmq_admin_core::core::query::AdminQuerySource as AdminSource;

        let source = match failure.source() {
            AdminSource::BrokerRuntime => QuerySource::BrokerRuntime,
            AdminSource::ConsumerStatistics => QuerySource::ConsumerStatistics,
            AdminSource::ConsumerConnection => QuerySource::ConsumerConnection,
            AdminSource::ProducerConnection => QuerySource::ProducerConnection,
            AdminSource::SubscriptionGroups => QuerySource::SubscriptionGroups,
        };
        let code = match failure.code() {
            AdminCode::SourceUnavailable => SourceFailureCode::SourceUnavailable,
            AdminCode::Timeout => SourceFailureCode::Timeout,
            AdminCode::PermissionDenied => SourceFailureCode::PermissionDenied,
            AdminCode::NotFound => SourceFailureCode::NotFound,
            AdminCode::RateLimited => SourceFailureCode::RateLimited,
            AdminCode::InvalidResponse => SourceFailureCode::InvalidResponse,
        };
        Self::new(source, code, failure.retryable(), failure.logical_target())
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct QueryPayload<T> {
    pub data: T,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub source_failures: Vec<SourceFailure>,
}

impl<T> QueryPayload<T> {
    pub(crate) fn complete(data: T) -> Self {
        Self {
            data,
            partial: false,
            warnings: Vec::new(),
            source_failures: Vec::new(),
        }
    }

    pub(crate) fn new(
        data: T,
        partial: bool,
        mut warnings: Vec<String>,
        mut source_failures: Vec<SourceFailure>,
    ) -> Self {
        source_failures.iter_mut().for_each(|failure| {
            failure.logical_target = safe_logical_target(&failure.logical_target);
        });
        source_failures.sort();
        source_failures.dedup();
        let overflow = source_failures.len() > MAX_SOURCE_FAILURES;
        source_failures.truncate(MAX_SOURCE_FAILURES);
        if !source_failures.is_empty() && !warnings.iter().any(|warning| warning == "source_failures_present") {
            warnings.push("source_failures_present".to_string());
        }
        if overflow && !warnings.iter().any(|warning| warning == "source_failures_truncated") {
            warnings.push("source_failures_truncated".to_string());
        }
        warnings.sort();
        warnings.dedup();
        Self {
            data,
            partial: partial || !source_failures.is_empty(),
            warnings,
            source_failures,
        }
    }

    pub(crate) fn from_admin(result: rocketmq_admin_core::core::query::AdminQueryResult<T>) -> Self {
        Self::new(
            result.data,
            result.partial,
            result.warnings,
            result.source_failures.iter().map(SourceFailure::from_admin).collect(),
        )
    }

    pub(crate) fn map<U>(self, map: impl FnOnce(T) -> U) -> QueryPayload<U> {
        QueryPayload {
            data: map(self.data),
            partial: self.partial,
            warnings: self.warnings,
            source_failures: self.source_failures,
        }
    }

    pub(crate) fn completeness(&self) -> QueryCompleteness {
        QueryCompleteness {
            partial: self.partial,
            warnings: self.warnings.clone(),
            source_failures: self.source_failures.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QueryCompleteness {
    pub partial: bool,
    pub warnings: Vec<String>,
    pub source_failures: Vec<SourceFailure>,
}

impl QueryCompleteness {
    pub(crate) fn merge(&mut self, other: QueryCompleteness) {
        self.partial |= other.partial;
        self.warnings.extend(other.warnings);
        self.source_failures.extend(other.source_failures);
    }

    pub(crate) fn wrap<T>(self, data: T) -> QueryPayload<T> {
        QueryPayload::new(data, self.partial, self.warnings, self.source_failures)
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    #[serde(default)]
    #[schemars(range(min = 1, max = 200))]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub count: usize,
    pub total_count: usize,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CacheStatus {
    Bypass,
    Hit,
    Miss,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryResult<T> {
    pub data: T,
    pub observed_at: String,
    pub freshness_ms: u64,
    pub cache_status: CacheStatus,
    pub partial: bool,
    pub warnings: Vec<String>,
    pub source_failures: Vec<SourceFailure>,
}

impl<T> QueryResult<T> {
    #[cfg(test)]
    pub(crate) fn bypass(data: T) -> Self {
        Self {
            data,
            observed_at: observed_at(),
            freshness_ms: 0,
            cache_status: CacheStatus::Bypass,
            partial: false,
            warnings: Vec::new(),
            source_failures: Vec::new(),
        }
    }

    pub(crate) fn from_payload(
        payload: QueryPayload<T>,
        observed_at: String,
        freshness_ms: u64,
        cache_status: CacheStatus,
    ) -> Self {
        Self {
            data: payload.data,
            observed_at,
            freshness_ms,
            cache_status,
            partial: payload.partial,
            warnings: payload.warnings,
            source_failures: payload.source_failures,
        }
    }
}

impl<T> Deref for QueryResult<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ToolResponse<T> {
    pub schema_version: String,
    pub request_id: String,
    pub cluster: String,
    pub observed_at: String,
    pub freshness_ms: u64,
    pub cache_status: CacheStatus,
    pub partial: bool,
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_failures: Option<Vec<SourceFailure>>,
    pub data: T,
}

impl<T> ToolResponse<T> {
    pub fn live(request_id: impl Into<String>, cluster: impl Into<String>, data: T) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            request_id: request_id.into(),
            cluster: cluster.into(),
            observed_at: observed_at(),
            freshness_ms: 0,
            cache_status: CacheStatus::Bypass,
            partial: false,
            warnings: Vec::new(),
            source_failures: None,
            data,
        }
    }

    pub(crate) fn from_query(
        request_id: impl Into<String>,
        cluster: impl Into<String>,
        result: QueryResult<T>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            request_id: request_id.into(),
            cluster: cluster.into(),
            observed_at: result.observed_at,
            freshness_ms: result.freshness_ms,
            cache_status: result.cache_status,
            partial: result.partial,
            warnings: result.warnings,
            source_failures: (!result.source_failures.is_empty()).then_some(result.source_failures),
            data: result.data,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PaginationError {
    #[error("limit must be between 1 and {MAX_PAGE_LIMIT}")]
    InvalidLimit,
    #[error("cursor is invalid or was created by an incompatible server version")]
    InvalidCursor,
}

pub fn paginate<T>(items: Vec<T>, request: &PageRequest) -> Result<Page<T>, PaginationError> {
    let limit = request.limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
        return Err(PaginationError::InvalidLimit);
    }

    let offset = request
        .cursor
        .as_deref()
        .map(decode_cursor)
        .transpose()?
        .unwrap_or_default();
    let total_count = items.len();
    if offset > total_count {
        return Err(PaginationError::InvalidCursor);
    }

    let end = offset.saturating_add(limit as usize).min(total_count);
    let items = items.into_iter().skip(offset).take(end - offset).collect::<Vec<_>>();
    let count = items.len();
    let has_more = end < total_count;
    let next_cursor = has_more.then(|| encode_cursor(end));

    Ok(Page {
        items,
        count,
        total_count,
        has_more,
        next_cursor,
    })
}

pub fn observed_at() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn observed_at_from_millis(timestamp_millis: i64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(timestamp_millis)
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn encode_cursor(offset: usize) -> String {
    format!("rmq-v1-{offset:x}")
}

fn decode_cursor(cursor: &str) -> Result<usize, PaginationError> {
    let offset = cursor
        .strip_prefix("rmq-v1-")
        .filter(|value| !value.is_empty())
        .ok_or(PaginationError::InvalidCursor)?;
    usize::from_str_radix(offset, 16).map_err(|_| PaginationError::InvalidCursor)
}

fn safe_logical_target(target: &str) -> String {
    const MAX_BYTES: usize = 128;
    let target = target.trim();
    if target.is_empty()
        || target.len() > MAX_BYTES
        || target.parse::<std::net::IpAddr>().is_ok()
        || target.parse::<std::net::SocketAddr>().is_ok()
        || target.contains([':', '/', '\\', '@', '=', '&', '?'])
        || target.chars().any(char::is_control)
    {
        return "unknown".to_string();
    }
    target
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_is_bounded_and_cursor_resumes() {
        let first = paginate(
            (0..5).collect(),
            &PageRequest {
                limit: Some(2),
                cursor: None,
            },
        )
        .unwrap();
        assert_eq!(first.items, [0, 1]);
        assert_eq!(first.total_count, 5);
        assert!(first.has_more);

        let second = paginate(
            (0..5).collect(),
            &PageRequest {
                limit: Some(2),
                cursor: first.next_cursor,
            },
        )
        .unwrap();
        assert_eq!(second.items, [2, 3]);
    }

    #[test]
    fn pagination_rejects_invalid_limits_and_cursors() {
        assert_eq!(
            paginate::<u8>(
                Vec::new(),
                &PageRequest {
                    limit: Some(0),
                    cursor: None,
                },
            ),
            Err(PaginationError::InvalidLimit)
        );
        assert_eq!(
            paginate::<u8>(
                Vec::new(),
                &PageRequest {
                    limit: None,
                    cursor: Some("2".to_string()),
                },
            ),
            Err(PaginationError::InvalidCursor)
        );
    }

    #[test]
    fn observed_timestamp_is_rfc3339() {
        let timestamp = observed_at();
        assert!(chrono::DateTime::parse_from_rfc3339(&timestamp).is_ok());
        assert_eq!(observed_at_from_millis(0).as_deref(), Some("1970-01-01T00:00:00.000Z"));
    }

    #[test]
    fn complete_tool_response_keeps_the_existing_wire_shape() {
        let response = ToolResponse::live("request-1", "primary", serde_json::json!({"brokers": []}));
        let value = serde_json::to_value(response).unwrap();

        assert_eq!(value["partial"], false);
        assert_eq!(value["warnings"], serde_json::json!([]));
        assert!(value.get("source_failures").is_none());
    }

    #[test]
    fn old_complete_tool_response_deserializes_without_source_failures() {
        let response: ToolResponse<serde_json::Value> = serde_json::from_value(serde_json::json!({
            "schema_version": "rocketmq-mcp.v2",
            "request_id": "request-1",
            "cluster": "primary",
            "observed_at": "2026-01-01T00:00:00.000Z",
            "freshness_ms": 0,
            "cache_status": "bypass",
            "partial": false,
            "warnings": [],
            "data": {"brokers": []}
        }))
        .unwrap();

        assert_eq!(response.source_failures, None);
    }

    #[test]
    fn tool_response_deserialization_reapplies_source_target_sanitization() {
        let response: ToolResponse<serde_json::Value> = serde_json::from_value(serde_json::json!({
            "schema_version": "rocketmq-mcp.v2",
            "request_id": "request-1",
            "cluster": "primary",
            "observed_at": "2026-01-01T00:00:00.000Z",
            "freshness_ms": 0,
            "cache_status": "bypass",
            "partial": true,
            "warnings": ["source_failures_present"],
            "source_failures": [{
                "source": "broker_runtime",
                "code": "source_unavailable",
                "retryable": true,
                "logical_target": "10.0.0.1:10911"
            }],
            "data": {"brokers": []}
        }))
        .unwrap();

        assert_eq!(response.source_failures.unwrap()[0].logical_target, "unknown");
    }

    #[test]
    fn query_payload_normalizes_failure_evidence_deterministically() {
        let failures = (0..20)
            .rev()
            .chain(std::iter::once(0))
            .map(|index| {
                SourceFailure::new(
                    QuerySource::BrokerRuntime,
                    SourceFailureCode::SourceUnavailable,
                    true,
                    format!("broker-{index:02}"),
                )
            })
            .chain(std::iter::once(SourceFailure::new(
                QuerySource::BrokerRuntime,
                SourceFailureCode::SourceUnavailable,
                true,
                "127.0.0.1:10911",
            )))
            .collect();
        let payload = QueryPayload::new((), false, Vec::new(), failures);

        assert!(payload.partial);
        assert_eq!(payload.source_failures.len(), MAX_SOURCE_FAILURES);
        assert_eq!(payload.source_failures[0].logical_target, "broker-00");
        assert!(payload
            .warnings
            .iter()
            .any(|warning| warning == "source_failures_present"));
        assert!(payload
            .warnings
            .iter()
            .any(|warning| warning == "source_failures_truncated"));
        assert!(!serde_json::to_string(&payload.source_failures)
            .unwrap()
            .contains("127.0.0.1"));
    }
}
