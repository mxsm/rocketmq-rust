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

//! Typed, address-free contract for the running state one consumer client reports.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::AdminFuture;
use crate::core::AdminResult;

/// Maximum queue rows retained from one consumer client.
pub const MAX_CONSUMER_CLIENT_QUEUES: usize = 10_000;
/// Maximum subscriptions and per-Topic status rows retained from one consumer client.
pub const MAX_CONSUMER_CLIENT_TOPICS: usize = 256;
/// Longest filter expression that is retained; a longer one is left out.
pub const MAX_CONSUMER_CLIENT_EXPRESSION_BYTES: usize = 1_024;

/// Selects one connected client of a Consumer Group by the logical Broker it is connected to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryConsumerClientRuntimeRequest {
    pub cluster: String,
    pub consumer_group: String,
    pub broker_name: String,
    pub client_id: String,
}

impl QueryConsumerClientRuntimeRequest {
    pub fn try_new(
        cluster: impl Into<String>,
        consumer_group: impl Into<String>,
        broker_name: impl Into<String>,
        client_id: impl Into<String>,
    ) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            consumer_group: required("consumer_group", consumer_group)?,
            broker_name: required("broker_name", broker_name)?,
            client_id: required("client_id", client_id)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerClientSubscription {
    pub topic: String,
    pub expression_type: String,
    /// The filter expression, or `None` when it is longer than
    /// [`MAX_CONSUMER_CLIENT_EXPRESSION_BYTES`].
    pub expression: Option<String>,
    pub sub_version: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConsumerClientTopicStatus {
    pub topic: String,
    pub pull_rt: f64,
    pub pull_tps: f64,
    pub consume_rt: f64,
    pub consume_ok_tps: f64,
    pub consume_failed_tps: f64,
    pub consume_failed_msgs: i64,
}

/// How a client takes messages from a queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerClientQueueMode {
    Pull,
    Pop,
}

/// What one client holds for one queue. A member that its mode does not have is `None`, and so is
/// a time the client never recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerClientQueueRuntime {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub mode: ConsumerClientQueueMode,
    pub dropped: bool,
    /// When the client last pulled or popped from the queue, in Unix milliseconds.
    pub last_pull_timestamp: Option<i64>,
    pub commit_offset: Option<i64>,
    pub cached_message_count: Option<u32>,
    pub cached_message_size_mib: Option<u32>,
    pub locked: Option<bool>,
    pub try_unlock_times: Option<u64>,
    pub last_lock_timestamp: Option<i64>,
    pub last_consume_timestamp: Option<i64>,
    pub wait_ack_count: Option<i32>,
}

/// The running state one consumer client reports, reduced to closed and bounded members.
///
/// The client's property table is never copied: of it, only the members named here are read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConsumerClientRuntime {
    pub consume_type: Option<String>,
    pub consume_orderly: Option<bool>,
    pub thread_pool_core_size: Option<u32>,
    pub client_version: Option<String>,
    pub started_timestamp: Option<i64>,
    pub subscriptions: Vec<ConsumerClientSubscription>,
    pub topic_status: Vec<ConsumerClientTopicStatus>,
    /// Queue rows in ascending order of Topic, Broker, queue and mode.
    pub queues: Vec<ConsumerClientQueueRuntime>,
    /// How many queues the client reported, before the retained-row cap.
    pub queue_count: usize,
    /// Whether rows beyond a retained-row cap were left out.
    pub truncated: bool,
}

/// Read of the running state of one connected consumer client.
///
/// A Broker the client is connected to relays the request, so the client answers for itself. The
/// thread dump of the client is never requested.
pub trait ConsumerRuntimeQueryAdmin: Send {
    fn query_consumer_client_runtime<'a>(
        &'a mut self,
        request: &'a QueryConsumerClientRuntimeRequest,
    ) -> AdminFuture<'a, ConsumerClientRuntime>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_names_a_cluster_a_group_a_broker_and_a_client() {
        let request =
            QueryConsumerClientRuntimeRequest::try_new(" cluster-a ", " workers ", " broker-a ", " c1 ").unwrap();
        assert_eq!(
            (
                request.cluster.as_str(),
                request.consumer_group.as_str(),
                request.broker_name.as_str(),
                request.client_id.as_str()
            ),
            ("cluster-a", "workers", "broker-a", "c1")
        );
        for blank in 0..4 {
            let part = |index: usize, value: &'static str| if index == blank { " " } else { value };
            assert!(QueryConsumerClientRuntimeRequest::try_new(
                part(0, "cluster-a"),
                part(1, "workers"),
                part(2, "broker-a"),
                part(3, "c1")
            )
            .is_err());
        }
    }
}
