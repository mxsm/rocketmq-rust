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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodec)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTopicFromNamesrvRequestHeader {
    #[required]
    pub topic: CheetahString,
    pub cluster_name: Option<CheetahString>,
}

impl DeleteTopicFromNamesrvRequestHeader {
    pub fn new(
        topic: impl Into<CheetahString>,
        cluster_name: Option<impl Into<CheetahString>>,
    ) -> Self {
        Self {
            topic: topic.into(),
            cluster_name: cluster_name.map(|s| s.into()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodec)]
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

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetTopicsByClusterRequestHeader {
    pub cluster: CheetahString,
}

impl GetTopicsByClusterRequestHeader {
    const CLUSTER: &'static str = "cluster";

    pub fn new(cluster: impl Into<CheetahString>) -> Self {
        Self {
            cluster: cluster.into(),
        }
    }
}

impl CommandCustomHeader for GetTopicsByClusterRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let map = HashMap::from([(
            CheetahString::from_static_str(Self::CLUSTER),
            self.cluster.clone(),
        )]);
        Some(map)
    }
}

impl FromMap for GetTopicsByClusterRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(GetTopicsByClusterRequestHeader {
            cluster: map
                .get(&CheetahString::from_static_str(Self::CLUSTER))
                .cloned()
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
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
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();
        if let Some(lo) = self.lo {
            map.insert(
                CheetahString::from_static_str(Self::LO),
                CheetahString::from_string(lo.to_string()),
            );
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
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(TopicRequestHeader {
            lo: map
                .get(&CheetahString::from_static_str(Self::LO))
                .and_then(|s| s.parse::<bool>().ok()),
            rpc: Some(<RpcRequestHeader as FromMap>::from(map)?),
        })
    }
}
