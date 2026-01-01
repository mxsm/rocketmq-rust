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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicFromNamesrvRequestHeader {
    #[required]
    pub topic: CheetahString,
    pub cluster_name: Option<CheetahString>,
}

impl DeleteTopicFromNamesrvRequestHeader {
    pub fn new(topic: impl Into<CheetahString>, cluster_name: Option<impl Into<CheetahString>>) -> Self {
        Self {
            topic: topic.into(),
            cluster_name: cluster_name.map(|s| s.into()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct RegisterTopicRequestHeader {
    #[required]
    pub topic: CheetahString,
    #[serde(flatten)]
    pub topic_request: Option<TopicRequestHeader>,
}

impl RegisterTopicRequestHeader {
    pub fn new(topic: impl Into<CheetahString>) -> Self {
        Self {
            topic: topic.into(),
            topic_request: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
pub struct GetTopicsByClusterRequestHeader {
    #[required]
    pub cluster: CheetahString,
}

impl GetTopicsByClusterRequestHeader {
    pub fn new(cluster: impl Into<CheetahString>) -> Self {
        Self {
            cluster: cluster.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct TopicRequestHeader {
    pub lo: Option<bool>,
    #[serde(flatten)]
    pub rpc: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn delete_topic_from_namesrv_request_header_new() {
        let header = DeleteTopicFromNamesrvRequestHeader::new("topic1", Some("cluster1"));
        assert_eq!(header.topic, CheetahString::from("topic1"));
        assert_eq!(header.cluster_name, Some(CheetahString::from("cluster1")));
    }

    #[test]
    fn delete_topic_from_namesrv_request_header_serialization() {
        let header = DeleteTopicFromNamesrvRequestHeader::new("topic1", Some("cluster1"));
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"topic":"topic1","clusterName":"cluster1"}"#);
    }

    #[test]
    fn delete_topic_from_namesrv_request_header_deserialization() {
        let json = r#"{"topic":"topic1","clusterName":"cluster1"}"#;
        let deserialized: DeleteTopicFromNamesrvRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.topic, CheetahString::from("topic1"));
        assert_eq!(deserialized.cluster_name, Some(CheetahString::from("cluster1")));
    }

    #[test]
    fn register_topic_request_header_new() {
        let header = RegisterTopicRequestHeader::new("topic1");
        assert_eq!(header.topic, CheetahString::from("topic1"));
        assert!(header.topic_request.is_none());
    }

    #[test]
    fn register_topic_request_header_serialization() {
        let header = RegisterTopicRequestHeader::new("topic1");
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"topic":"topic1"}"#);
    }

    #[test]
    fn get_topics_by_cluster_request_header_new() {
        let header = GetTopicsByClusterRequestHeader::new("cluster1");
        assert_eq!(header.cluster, CheetahString::from("cluster1"));
    }

    #[test]
    fn get_topics_by_cluster_request_header_serialization() {
        let header = GetTopicsByClusterRequestHeader::new("cluster1");
        let serialized = serde_json::to_string(&header).unwrap();
        assert_eq!(serialized, r#"{"cluster":"cluster1"}"#);
    }

    #[test]
    fn get_topics_by_cluster_request_header_deserialization() {
        let json = r#"{"cluster":"cluster1"}"#;
        let deserialized: GetTopicsByClusterRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.cluster, CheetahString::from("cluster1"));
    }
}
