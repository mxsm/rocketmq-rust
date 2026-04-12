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

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_required_cheetah(field: &'static str, value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(CheetahString::from(value))
}

/// Topic creation/update request
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Request model for listing topics, optionally scoped to a cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicListQueryRequest {
    namesrv_addr: Option<String>,
    cluster_name: Option<CheetahString>,
}

impl TopicListQueryRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn with_optional_cluster_name(mut self, cluster_name: Option<String>) -> Self {
        self.cluster_name = trim_optional_string(cluster_name).map(CheetahString::from);
        self
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicListItem {
    pub topic: CheetahString,
    pub cluster: Option<CheetahString>,
    pub consumer_group: Option<CheetahString>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicListResult {
    pub topics: Vec<TopicListItem>,
}

/// Request model for deleting a topic from a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicRequest {
    topic: CheetahString,
    cluster_name: CheetahString,
    namesrv_addr: Option<String>,
}

impl DeleteTopicRequest {
    pub fn try_new(topic: impl Into<String>, cluster_name: Option<String>) -> RocketMQResult<Self> {
        let topic = trim_required_cheetah("topic", topic)?;
        let cluster_name = trim_optional_string(cluster_name)
            .map(CheetahString::from)
            .ok_or_else(|| ToolsError::validation_error("clusterName", "clusterName must be provided"))?;

        Ok(Self {
            topic,
            cluster_name,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn cluster_name(&self) -> &CheetahString {
        &self.cluster_name
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTopicResult {
    pub topic: CheetahString,
    pub cluster_name: CheetahString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderConfMethod {
    Put,
    Get,
    Delete,
}

impl OrderConfMethod {
    pub fn parse(value: impl AsRef<str>) -> RocketMQResult<Self> {
        match value.as_ref().trim().to_ascii_lowercase().as_str() {
            "put" => Ok(Self::Put),
            "get" => Ok(Self::Get),
            "delete" => Ok(Self::Delete),
            method => Err(ToolsError::validation_error(
                "method",
                format!("invalid method '{method}', allowed values: put, get, delete"),
            )
            .into()),
        }
    }
}

/// Request model for getting, updating, or deleting order topic configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderConfRequest {
    topic: CheetahString,
    method: OrderConfMethod,
    order_conf: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl OrderConfRequest {
    pub fn try_new(
        topic: impl Into<String>,
        method: impl AsRef<str>,
        order_conf: Option<String>,
    ) -> RocketMQResult<Self> {
        let topic = trim_required_cheetah("topic", topic)?;
        let method = OrderConfMethod::parse(method)?;
        let order_conf = trim_optional_string(order_conf).map(CheetahString::from);
        if matches!(method, OrderConfMethod::Put) && order_conf.is_none() {
            return Err(ToolsError::validation_error("orderConf", "orderConf must be provided for put method").into());
        }

        Ok(Self {
            topic,
            method,
            order_conf,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn method(&self) -> OrderConfMethod {
        self.method
    }

    pub fn order_conf(&self) -> Option<&str> {
        self.order_conf.as_ref().map(|value| value.as_str())
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderConfResult {
    pub topic: CheetahString,
    pub method: OrderConfMethod,
    pub order_conf: Option<CheetahString>,
}

/// Request model for querying message queue allocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocateMqQueryRequest {
    topic: CheetahString,
    ip_list: CheetahString,
    namesrv_addr: Option<String>,
}

impl AllocateMqQueryRequest {
    pub fn try_new(topic: impl Into<String>, ip_list: impl Into<String>) -> RocketMQResult<Self> {
        let topic = trim_required_cheetah("topic", topic)?;
        let ip_list = trim_required_cheetah("ipList", ip_list)?;
        Ok(Self {
            topic,
            ip_list,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn ip_list(&self) -> &CheetahString {
        &self.ip_list
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicTarget {
    /// Create/update on specific broker
    Broker(CheetahString),
    /// Create/update on all master brokers in cluster
    Cluster(CheetahString),
}

/// Request model for applying a batch of topic configs to a broker or cluster.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateTopicListRequest {
    target: TopicTarget,
    topic_configs: Vec<rocketmq_common::common::config::TopicConfig>,
    namesrv_addr: Option<String>,
}

impl UpdateTopicListRequest {
    pub fn try_new(
        target: TopicTarget,
        topic_configs: Vec<rocketmq_common::common::config::TopicConfig>,
    ) -> RocketMQResult<Self> {
        if topic_configs.is_empty() {
            return Err(ToolsError::validation_error("topicConfigs", "topicConfigs must not be empty").into());
        }

        Ok(Self {
            target,
            topic_configs,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &TopicTarget {
        &self.target
    }

    pub fn topic_configs(&self) -> &[rocketmq_common::common::config::TopicConfig] {
        &self.topic_configs
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateTopicListResult {
    pub target: TopicTarget,
    pub broker_addrs: Vec<CheetahString>,
}

/// Request model for creating or updating a topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateTopicRequest {
    config: TopicConfig,
    target: TopicTarget,
    namesrv_addr: Option<String>,
}

impl UpdateTopicRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        topic: impl Into<String>,
        target: TopicTarget,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: Option<u32>,
        order: Option<bool>,
        unit: Option<bool>,
        has_unit_sub: Option<bool>,
    ) -> RocketMQResult<Self> {
        let topic = trim_required_cheetah("topic", topic)?;
        let perm = perm.unwrap_or(6);
        if !matches!(perm, 2 | 4 | 6) {
            return Err(ToolsError::validation_error("perm", "perm must be 2, 4, or 6").into());
        }
        if read_queue_nums == 0 || write_queue_nums == 0 {
            return Err(ToolsError::validation_error("queueNums", "queue nums must be greater than 0").into());
        }

        let topic_sys_flag =
            rocketmq_common::common::TopicSysFlag::build_sys_flag(unit.unwrap_or(false), has_unit_sub.unwrap_or(false));

        Ok(Self {
            config: TopicConfig {
                topic_name: topic,
                read_queue_nums: read_queue_nums as i32,
                write_queue_nums: write_queue_nums as i32,
                perm: perm as i32,
                topic_filter_type: None,
                topic_sys_flag: Some(topic_sys_flag as i32),
                order: order.unwrap_or(false),
            },
            target,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn config(&self) -> &TopicConfig {
        &self.config
    }

    pub fn target(&self) -> &TopicTarget {
        &self.target
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTopicResult {
    pub config: TopicConfig,
    pub target: TopicTarget,
    pub order_warning: bool,
}

/// Request model for updating topic permissions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateTopicPermRequest {
    topic: CheetahString,
    target: TopicTarget,
    perm: i32,
    namesrv_addr: Option<String>,
}

impl UpdateTopicPermRequest {
    pub fn try_new(topic: impl Into<String>, target: TopicTarget, perm: i32) -> RocketMQResult<Self> {
        let topic = trim_required_cheetah("topic", topic)?;
        if !matches!(perm, 2 | 4 | 6) {
            return Err(ToolsError::validation_error("perm", "perm must be 2, 4, or 6").into());
        }

        Ok(Self {
            topic,
            target,
            perm,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn target(&self) -> &TopicTarget {
        &self.target
    }

    pub fn perm(&self) -> i32 {
        self.perm
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTopicPermResult {
    pub topic: CheetahString,
    pub target: TopicTarget,
    pub perm: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn topic_list_query_request_trims_namesrv_and_cluster() {
        let request = TopicListQueryRequest::new()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()))
            .with_optional_cluster_name(Some(" DefaultCluster ".to_string()));

        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(
            request.cluster_name().map(|value| value.as_str()),
            Some("DefaultCluster")
        );
    }

    #[test]
    fn delete_topic_request_requires_cluster_and_trims_fields() {
        let request = DeleteTopicRequest::try_new(" TestTopic ", Some(" DefaultCluster ".to_string()))
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(DeleteTopicRequest::try_new("TestTopic", None).is_err());
    }

    #[test]
    fn order_conf_request_validates_method_and_value() {
        let put =
            OrderConfRequest::try_new(" TestTopic ", " put ", Some(" broker-a:4;broker-b:4 ".to_string())).unwrap();

        assert_eq!(put.topic().as_str(), "TestTopic");
        assert!(matches!(put.method(), OrderConfMethod::Put));
        assert_eq!(put.order_conf(), Some("broker-a:4;broker-b:4"));
        assert!(OrderConfRequest::try_new("TestTopic", "put", None).is_err());
        assert!(OrderConfRequest::try_new("TestTopic", "invalid", None).is_err());
    }

    #[test]
    fn allocate_mq_query_request_trims_topic_and_ip_list() {
        let request = AllocateMqQueryRequest::try_new(" TestTopic ", " 192.168.1.1, 192.168.1.2 ")
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.ip_list().as_str(), "192.168.1.1, 192.168.1.2");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
        assert!(AllocateMqQueryRequest::try_new("TestTopic", "   ").is_err());
    }

    #[test]
    fn update_topic_request_builds_config_and_target() {
        let request = UpdateTopicRequest::try_new(
            " TestTopic ",
            TopicTarget::Broker("127.0.0.1:10911".into()),
            4,
            5,
            Some(6),
            Some(true),
            Some(true),
            Some(false),
        )
        .unwrap();

        assert_eq!(request.config().topic_name.as_str(), "TestTopic");
        assert_eq!(request.config().read_queue_nums, 4);
        assert_eq!(request.config().write_queue_nums, 5);
        assert_eq!(request.config().perm, 6);
        assert!(request.config().order);
    }

    #[test]
    fn update_topic_perm_request_validates_target_and_perm() {
        let request =
            UpdateTopicPermRequest::try_new(" TestTopic ", TopicTarget::Cluster("DefaultCluster".into()), 4).unwrap();

        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.perm(), 4);
        assert!(
            UpdateTopicPermRequest::try_new("TestTopic", TopicTarget::Cluster("DefaultCluster".into()), 1).is_err()
        );
    }
}
