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
use crate::model::EnvironmentId;
use crate::model::StorageBackend;
use chrono::TimeZone;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardOverview {
    pub current_namesrv: Option<String>,
    pub broker_count: usize,
    pub topic_count: usize,
    pub consumer_group_count: usize,
    pub producer_count: usize,
    pub message_backlog: i64,
    pub system_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardTopicCurrent {
    pub total_topics: usize,
    pub top_topics: Vec<TopicCurrentMetric>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopicCurrentMetric {
    pub topic: String,
    pub total_msg: i64,
    pub in_tps: f64,
    pub out_tps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardHistoryQuery {
    pub date: String,
    pub topic_name: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardHistorySeries {
    pub date: String,
    pub metric: String,
    pub topic_name: Option<String>,
    pub collected: bool,
    pub points: Vec<DashboardHistoryPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub health: DashboardHistoryHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardHistoryPoint {
    pub timestamp: i64,
    pub value: f64,
}

/// Sanitized state of the persistent history collector.  It deliberately
/// excludes connection strings, lease holder identities, and sample values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DashboardHistoryHealth {
    pub backend: StorageBackend,
    pub connectivity: String,
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_collection_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_append_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_retention_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_error: Option<String>,
}

/// A normalized dimension carried by a persisted metric sample.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct MetricDimension {
    pub key: String,
    pub value: String,
}

/// A durable dashboard metric. The idempotency key is environment, metric,
/// bucket, and the normalized dimensions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetricSample {
    pub environment_id: EnvironmentId,
    pub metric: String,
    pub bucket_ms: i64,
    pub dimensions: Vec<MetricDimension>,
    pub value: f64,
}

impl MetricSample {
    pub const MAX_METRIC_LENGTH: usize = 64;
    pub const MAX_DIMENSIONS: usize = 16;
    pub const MAX_DIMENSION_KEY_LENGTH: usize = 64;
    pub const MAX_DIMENSION_VALUE_LENGTH: usize = 256;
    pub const MAX_DIMENSIONS_JSON_LENGTH: usize = 512;

    /// Validates the sample and sorts dimensions by their business key.
    pub fn normalize(&mut self) -> Result<(), String> {
        if self.environment_id.0.is_empty() || self.environment_id.0.len() > 36 {
            return Err("history sample environment is invalid".to_string());
        }
        if self.metric.is_empty() || self.metric.len() > Self::MAX_METRIC_LENGTH {
            return Err("history metric is invalid".to_string());
        }
        if self.bucket_ms < 0 || Utc.timestamp_millis_opt(self.bucket_ms).single().is_none() || !self.value.is_finite()
        {
            return Err("history sample value is invalid".to_string());
        }
        if self.value == 0.0 {
            self.value = 0.0;
        }
        if self.dimensions.len() > Self::MAX_DIMENSIONS {
            return Err("history sample has too many dimensions".to_string());
        }
        self.dimensions.sort_by(|left, right| left.key.cmp(&right.key));
        for dimension in &self.dimensions {
            if dimension.key.is_empty()
                || dimension.key.len() > Self::MAX_DIMENSION_KEY_LENGTH
                || dimension.value.len() > Self::MAX_DIMENSION_VALUE_LENGTH
            {
                return Err("history sample dimension is invalid".to_string());
            }
        }
        if self.dimensions.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err("history sample dimensions contain a duplicate key".to_string());
        }
        if self.dimensions_json()?.len() > Self::MAX_DIMENSIONS_JSON_LENGTH {
            return Err("history sample dimensions are too large".to_string());
        }
        Ok(())
    }

    /// Returns the deterministic JSON representation used by every backend.
    pub fn dimensions_json(&self) -> Result<String, String> {
        serde_json::to_string(&self.dimensions).map_err(|_| "history dimensions are invalid".to_string())
    }

    /// Returns dimensions in the form used by API filtering and cursor binding.
    pub fn dimensions_map(&self) -> BTreeMap<String, String> {
        self.dimensions
            .iter()
            .map(|dimension| (dimension.key.clone(), dimension.value.clone()))
            .collect()
    }
}
