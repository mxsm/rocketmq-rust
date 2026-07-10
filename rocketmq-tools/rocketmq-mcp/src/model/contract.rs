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
}

impl<T> QueryResult<T> {
    #[cfg(test)]
    pub(crate) fn bypass(data: T) -> Self {
        Self {
            data,
            observed_at: observed_at(),
            freshness_ms: 0,
            cache_status: CacheStatus::Bypass,
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
            partial: false,
            warnings: Vec::new(),
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
}
