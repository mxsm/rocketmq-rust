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

//! Typed, address-free Topic observation contracts.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::query::AdminQueryResult;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

/// Maximum logical Broker masters queried by one Topic observation.
pub const MAX_TOPIC_OBSERVATION_TARGETS: usize = 64;
/// Maximum queue rows retained by one Topic statistics observation.
pub const MAX_TOPIC_STATS_ROWS: usize = 10_000;
/// Stable warning emitted when queue rows are omitted by the retained-row cap.
pub const TOPIC_STATS_TRUNCATED_WARNING: &str = "topic_stats_rows_truncated";
/// Stable warning emitted when an aggregate exceeds the public integer range.
pub const TOPIC_STATS_TOTAL_SATURATED_WARNING: &str = "topic_stats_total_saturated";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTopicStatsRequest {
    pub cluster: String,
    pub topic: String,
    pub max_rows: usize,
}

impl QueryTopicStatsRequest {
    pub fn try_new(cluster: impl Into<String>, topic: impl Into<String>, max_rows: usize) -> AdminResult<Self> {
        if !(1..=MAX_TOPIC_STATS_ROWS).contains(&max_rows) {
            return Err(AdminError::invalid_argument(
                "max_rows",
                format!("must be between 1 and {MAX_TOPIC_STATS_ROWS}"),
            ));
        }
        Ok(Self {
            cluster: required("cluster", cluster)?,
            topic: required("topic", topic)?,
            max_rows,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicStatsQueueRow {
    pub broker_name: String,
    pub queue_id: i32,
    pub min_offset: i64,
    pub max_offset: i64,
    pub message_count: u64,
    pub last_update_timestamp: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTopicStatsResult {
    pub topic: String,
    pub total_message_count: u64,
    pub queue_count: usize,
    pub queues: Vec<TopicStatsQueueRow>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTopicConfigRequest {
    pub cluster: String,
    pub topic: String,
}

impl QueryTopicConfigRequest {
    pub fn try_new(cluster: impl Into<String>, topic: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            topic: required("topic", topic)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigObservationRow {
    pub broker_name: String,
    pub version: u64,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub order: bool,
    pub message_type: String,
}

/// Semantic Topic configuration fields compared across Brokers. Metadata
/// versions are observation evidence and are intentionally not differences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicConfigDifferenceField {
    ReadQueueNums,
    WriteQueueNums,
    Perm,
    Order,
    MessageType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTopicConfigResult {
    pub topic: String,
    pub brokers: Vec<TopicConfigObservationRow>,
    pub inconsistent_fields: Vec<TopicConfigDifferenceField>,
}

/// Evidence-aware read-only Topic observations.
pub trait TopicObservationQueryAdmin: Send {
    fn query_topic_stats<'a>(
        &'a mut self,
        request: &'a QueryTopicStatsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryTopicStatsResult>>;

    fn query_topic_config<'a>(
        &'a mut self,
        request: &'a QueryTopicConfigRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryTopicConfigResult>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_trim_required_values_and_bound_statistics_rows() {
        let stats = QueryTopicStatsRequest::try_new(" cluster-a ", " orders ", MAX_TOPIC_STATS_ROWS).unwrap();
        assert_eq!(stats.cluster, "cluster-a");
        assert_eq!(stats.topic, "orders");
        assert!(QueryTopicStatsRequest::try_new("cluster-a", "orders", 0).is_err());
        assert!(QueryTopicStatsRequest::try_new("cluster-a", "orders", MAX_TOPIC_STATS_ROWS + 1).is_err());

        let config = QueryTopicConfigRequest::try_new(" cluster-a ", " orders ").unwrap();
        assert_eq!(config.cluster, "cluster-a");
        assert_eq!(config.topic, "orders");
        assert!(QueryTopicConfigRequest::try_new("cluster-a", " ").is_err());
    }

    #[test]
    fn difference_fields_have_a_closed_stable_wire_order() {
        let fields = [
            TopicConfigDifferenceField::ReadQueueNums,
            TopicConfigDifferenceField::WriteQueueNums,
            TopicConfigDifferenceField::Perm,
            TopicConfigDifferenceField::Order,
            TopicConfigDifferenceField::MessageType,
        ];
        assert_eq!(
            serde_json::to_value(fields).unwrap(),
            serde_json::json!(["read_queue_nums", "write_queue_nums", "perm", "order", "message_type"])
        );
    }
}
