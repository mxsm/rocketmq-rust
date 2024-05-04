/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    protocol::command_custom_header::{CommandCustomHeader, FromMap},
    rpc::rpc_request_header::RpcRequestHeader,
};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicFromNamesrvRequestHeader {
    pub topic: String,
    pub cluster_name: Option<String>,
}

impl DeleteTopicFromNamesrvRequestHeader {
    const TOPIC: &'static str = "topic";
    const CLUSTER_NAME: &'static str = "clusterName";
    pub fn new(topic: impl Into<String>, cluster_name: Option<impl Into<String>>) -> Self {
        Self {
            topic: topic.into(),
            cluster_name: cluster_name.map(|s| s.into()),
        }
    }
}

impl CommandCustomHeader for DeleteTopicFromNamesrvRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::from([(Self::TOPIC.to_string(), self.topic.clone())]);
        if let Some(ref cluster_name) = self.cluster_name {
            map.insert(Self::CLUSTER_NAME.to_string(), cluster_name.clone());
        }
        Some(map)
    }
}

impl FromMap for DeleteTopicFromNamesrvRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(DeleteTopicFromNamesrvRequestHeader {
            topic: map.get(Self::TOPIC).cloned().unwrap_or_default(),
            cluster_name: map.get(Self::CLUSTER_NAME).map(|s| s.into()),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterTopicRequestHeader {
    pub topic: String,
    #[serde(flatten)]
    pub topic_request: Option<TopicRequestHeader>,
}

impl RegisterTopicRequestHeader {
    const TOPIC: &'static str = "topic";
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            topic_request: None,
        }
    }
}

impl CommandCustomHeader for RegisterTopicRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::from([(Self::TOPIC.to_string(), self.topic.clone())]);
        if let Some(ref request) = self.topic_request {
            if let Some(val) = request.to_map() {
                map.extend(val);
            }
        }
        Some(map)
    }
}
impl FromMap for RegisterTopicRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(RegisterTopicRequestHeader {
            topic: map.get(Self::TOPIC).cloned().unwrap_or_default(),
            topic_request: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetTopicsByClusterRequestHeader {
    pub cluster: String,
}

impl GetTopicsByClusterRequestHeader {
    const CLUSTER: &'static str = "cluster";
    pub fn new(cluster: impl Into<String>) -> Self {
        Self {
            cluster: cluster.into(),
        }
    }
}

impl CommandCustomHeader for GetTopicsByClusterRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let map = HashMap::from([(Self::CLUSTER.to_string(), self.cluster.clone())]);
        Some(map)
    }
}

impl FromMap for GetTopicsByClusterRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(GetTopicsByClusterRequestHeader {
            cluster: map.get(Self::CLUSTER).cloned().unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRequestHeader {
    pub lo: Option<bool>,
    #[serde(flatten)]
    pub rpc: Option<RpcRequestHeader>,
}

impl TopicRequestHeader {
    const LO: &'static str = "lo";
}

impl CommandCustomHeader for TopicRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::new();
        if let Some(lo) = self.lo {
            map.insert(Self::LO.to_string(), lo.to_string());
        }
        if let Some(ref rpc) = self.rpc {
            if let Some(rpc_map) = rpc.to_map() {
                map.extend(rpc_map);
            }
        }
        Some(map)
    }
}

impl FromMap for TopicRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(TopicRequestHeader {
            lo: map.get(Self::LO).and_then(|s| s.parse::<bool>().ok()),
            rpc: <RpcRequestHeader as FromMap>::from(map),
        })
    }
}
