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

//! Topic-related types and data structures

use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

/// Topic creation/update request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub topic_name: CheetahString,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_filter_type: Option<String>,
    pub topic_sys_flag: Option<i32>,
    pub order: bool,
}

/// Topic route information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRouteInfo {
    pub topic_name: CheetahString,
    pub broker_datas: Vec<BrokerData>,
    pub queue_datas: Vec<QueueData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerData {
    pub cluster: CheetahString,
    pub broker_name: CheetahString,
    pub broker_addrs: std::collections::HashMap<i64, CheetahString>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueData {
    pub broker_name: CheetahString,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
}

/// Topic cluster list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicClusterList {
    pub clusters: HashSet<CheetahString>,
}

/// Request model for querying the cluster list that owns a topic.
///
/// This type is intentionally independent from CLI argument structs so CLI,
/// TUI, or other callers can share the same core service boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicClusterQueryRequest {
    topic: CheetahString,
    namesrv_addr: Option<String>,
}

impl TopicClusterQueryRequest {
    pub fn try_new(topic: impl Into<String>) -> RocketMQResult<Self> {
        let topic = topic.into();
        let topic = topic.trim();
        if topic.is_empty() {
            return Err(ToolsError::validation_error("topic", "topic name must not be empty").into());
        }

        Ok(Self {
            topic: CheetahString::from(topic),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = namesrv_addr
            .map(|addr| addr.trim().to_string())
            .filter(|addr| !addr.is_empty());
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

/// Request model for querying topic route data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRouteQueryRequest {
    topic: CheetahString,
    namesrv_addr: Option<String>,
}

impl TopicRouteQueryRequest {
    pub fn try_new(topic: impl Into<String>) -> RocketMQResult<Self> {
        let topic = topic.into();
        let topic = topic.trim();
        if topic.is_empty() {
            return Err(ToolsError::validation_error("topic", "topic name must not be empty").into());
        }

        Ok(Self {
            topic: CheetahString::from(topic),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = namesrv_addr
            .map(|addr| addr.trim().to_string())
            .filter(|addr| !addr.is_empty());
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

/// Request model for querying topic status data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicStatusQueryRequest {
    topic: CheetahString,
    namesrv_addr: Option<String>,
    cluster_name: Option<CheetahString>,
}

impl TopicStatusQueryRequest {
    pub fn try_new(topic: impl Into<String>) -> RocketMQResult<Self> {
        let topic = topic.into();
        let topic = topic.trim();
        if topic.is_empty() {
            return Err(ToolsError::validation_error("topic", "topic name must not be empty").into());
        }

        Ok(Self {
            topic: CheetahString::from(topic),
            namesrv_addr: None,
            cluster_name: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = namesrv_addr
            .map(|addr| addr.trim().to_string())
            .filter(|addr| !addr.is_empty());
        self
    }

    pub fn with_optional_cluster_name(mut self, cluster_name: Option<String>) -> Self {
        self.cluster_name = cluster_name
            .map(|cluster| cluster.trim().to_string())
            .filter(|cluster| !cluster.is_empty())
            .map(CheetahString::from);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn cluster_name(&self) -> Option<&CheetahString> {
        self.cluster_name.as_ref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

/// Topic status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStatus {
    pub topic_name: CheetahString,
    pub broker_offset: i64,
    pub consumer_offset: i64,
    pub last_update_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatedMqQueryResult {
    pub topic: CheetahString,
    pub requested_ips: Vec<CheetahString>,
    pub route_found: bool,
    pub total_queues: usize,
    pub broker_names: Vec<CheetahString>,
}

/// Target for topic creation/update operations
#[derive(Debug, Clone)]
pub enum TopicTarget {
    /// Create/update on specific broker
    Broker(CheetahString),
    /// Create/update on all master brokers in cluster
    Cluster(CheetahString),
}

#[cfg(test)]
mod tests {
    use super::TopicClusterQueryRequest;
    use super::TopicRouteQueryRequest;
    use super::TopicStatusQueryRequest;

    #[test]
    fn topic_cluster_query_request_trims_topic_and_namesrv() {
        let request = TopicClusterQueryRequest::try_new("  TestTopic  ")
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn topic_cluster_query_request_rejects_blank_topic() {
        let err = TopicClusterQueryRequest::try_new("   ").unwrap_err();

        assert!(err.to_string().contains("topic name must not be empty"));
    }

    #[test]
    fn topic_route_query_request_trims_topic_and_namesrv() {
        let request = TopicRouteQueryRequest::try_new("  RouteTopic  ")
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

        assert_eq!(request.topic().as_str(), "RouteTopic");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn topic_route_query_request_rejects_blank_topic() {
        let err = TopicRouteQueryRequest::try_new("   ").unwrap_err();

        assert!(err.to_string().contains("topic name must not be empty"));
    }

    #[test]
    fn topic_status_query_request_trims_topic_namesrv_and_cluster() {
        let request = TopicStatusQueryRequest::try_new("  StatusTopic  ")
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()))
            .with_optional_cluster_name(Some(" DefaultCluster ".to_string()));

        assert_eq!(request.topic().as_str(), "StatusTopic");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(
            request.cluster_name().map(|value| value.as_str()),
            Some("DefaultCluster")
        );
    }

    #[test]
    fn topic_status_query_request_rejects_blank_topic() {
        let err = TopicStatusQueryRequest::try_new("   ").unwrap_err();

        assert!(err.to_string().contains("topic name must not be empty"));
    }
}
