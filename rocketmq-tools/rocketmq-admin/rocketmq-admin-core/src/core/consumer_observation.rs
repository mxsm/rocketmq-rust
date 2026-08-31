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

//! Typed, address-free Consumer observation contracts.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::query::AdminQueryResult;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

pub const MAX_CONSUMER_OBSERVATION_TARGETS: usize = 64;
pub const MAX_CONSUMER_PROGRESS_ROWS: usize = 10_000;
pub const CONSUMER_DETAILS_TOTAL_SATURATED_WARNING: &str = "consumer_details_total_connections_saturated";
pub const CONSUMER_PROGRESS_TRUNCATED_WARNING: &str = "consumer_progress_rows_truncated";
pub const CONSUMER_PROGRESS_TOTAL_SATURATED_WARNING: &str = "consumer_progress_total_saturated";
pub const CONSUMER_PROGRESS_INVALID_TPS_WARNING: &str = "consumer_progress_invalid_tps_excluded";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerGroupDetailsRequest {
    pub cluster: String,
    pub consumer_group: String,
}

impl QueryConsumerGroupDetailsRequest {
    pub fn try_new(cluster: impl Into<String>, consumer_group: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            consumer_group: required("consumer_group", consumer_group)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerProgressRequest {
    pub cluster: String,
    pub consumer_group: String,
    pub max_rows: usize,
}

impl QueryConsumerProgressRequest {
    pub fn try_new(
        cluster: impl Into<String>,
        consumer_group: impl Into<String>,
        max_rows: usize,
    ) -> AdminResult<Self> {
        if !(1..=MAX_CONSUMER_PROGRESS_ROWS).contains(&max_rows) {
            return Err(AdminError::invalid_argument(
                "max_rows",
                format!("must be between 1 and {MAX_CONSUMER_PROGRESS_ROWS}"),
            ));
        }
        Ok(Self {
            cluster: required("cluster", cluster)?,
            consumer_group: required("consumer_group", consumer_group)?,
            max_rows,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerGroupConfigState {
    Present,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerConnectionState {
    Online,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerConsumeType {
    Pull,
    Push,
    Pop,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerMessageModel {
    Broadcasting,
    Clustering,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerConsumeFromWhere {
    LastOffset,
    LastOffsetAndMinFirst,
    MinOffset,
    MaxOffset,
    FirstOffset,
    Timestamp,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupDetailsBrokerRow {
    pub broker_name: String,
    pub config_state: ConsumerGroupConfigState,
    pub config_version: Option<u64>,
    pub consume_enable: Option<bool>,
    pub consume_from_min_enable: Option<bool>,
    pub consume_broadcast_enable: Option<bool>,
    pub consume_message_orderly: Option<bool>,
    pub retry_queue_nums: Option<i32>,
    pub retry_max_times: Option<i32>,
    pub notify_consumer_ids_changed_enable: Option<bool>,
    pub consume_timeout_minutes: Option<i32>,
    pub connection_state: Option<ConsumerConnectionState>,
    pub connection_count: u64,
    pub consume_type: Option<ConsumerConsumeType>,
    pub message_model: Option<ConsumerMessageModel>,
    pub consume_from_where: Option<ConsumerConsumeFromWhere>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerGroupDetailsResult {
    pub consumer_group: String,
    pub total_connection_count: u64,
    pub brokers: Vec<ConsumerGroupDetailsBrokerRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerProgressState {
    NoConsumption,
    Observed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerProgressQueueRow {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub pull_offset: i64,
    pub lag: u64,
    pub inflight: u64,
    pub last_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryConsumerProgressResult {
    pub consumer_group: String,
    pub state: ConsumerProgressState,
    pub topic_count: usize,
    pub queue_count: usize,
    pub total_lag: u64,
    pub max_queue_lag: u64,
    pub total_inflight: u64,
    pub consume_tps: f64,
    pub queues: Vec<ConsumerProgressQueueRow>,
    pub truncated: bool,
}

pub trait ConsumerObservationQueryAdmin: Send {
    fn query_consumer_group_details<'a>(
        &'a mut self,
        request: &'a QueryConsumerGroupDetailsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryConsumerGroupDetailsResult>>;

    fn query_consumer_progress<'a>(
        &'a mut self,
        request: &'a QueryConsumerProgressRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryConsumerProgressResult>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_are_required_and_progress_retention_is_bounded() {
        let details = QueryConsumerGroupDetailsRequest::try_new(" cluster-a ", " group-a ").unwrap();
        assert_eq!(details.cluster, "cluster-a");
        assert_eq!(details.consumer_group, "group-a");
        assert!(QueryConsumerGroupDetailsRequest::try_new("cluster-a", " ").is_err());
        assert!(QueryConsumerProgressRequest::try_new("cluster-a", "group-a", 0).is_err());
        assert!(QueryConsumerProgressRequest::try_new("cluster-a", "group-a", MAX_CONSUMER_PROGRESS_ROWS).is_ok());
        assert!(QueryConsumerProgressRequest::try_new("cluster-a", "group-a", MAX_CONSUMER_PROGRESS_ROWS + 1).is_err());
    }

    #[test]
    fn public_states_are_closed_snake_case_values() {
        assert_eq!(
            serde_json::to_value(ConsumerConnectionState::Offline).unwrap(),
            "offline"
        );
        assert_eq!(
            serde_json::to_value(ConsumerProgressState::NoConsumption).unwrap(),
            "no_consumption"
        );
        assert_eq!(serde_json::to_value(ConsumerConsumeType::Push).unwrap(), "push");
        assert_eq!(
            serde_json::to_value(ConsumerMessageModel::Clustering).unwrap(),
            "clustering"
        );
        assert_eq!(
            serde_json::to_value(ConsumerConsumeFromWhere::Timestamp).unwrap(),
            "timestamp"
        );
    }
}
