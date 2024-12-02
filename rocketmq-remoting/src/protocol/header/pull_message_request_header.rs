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

use bytes::BytesMut;
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PullMessageRequestHeader {
    pub consumer_group: CheetahString,
    pub topic: CheetahString,
    pub queue_id: Option<i32>,
    pub queue_offset: i64,
    pub max_msg_nums: i32,
    pub sys_flag: i32,
    pub commit_offset: i64,
    pub suspend_timeout_millis: u64,
    pub subscription: Option<CheetahString>,
    pub sub_version: i64,
    pub expression_type: Option<CheetahString>,
    pub max_msg_bytes: Option<i32>,
    pub request_source: Option<i32>,
    pub proxy_forward_client_id: Option<CheetahString>,
    #[serde(flatten)]
    pub topic_request: Option<TopicRequestHeader>,
}

impl PullMessageRequestHeader {
    const COMMIT_OFFSET: &'static str = "commitOffset";
    const CONSUMER_GROUP: &'static str = "consumerGroup";
    const EXPRESSION_TYPE: &'static str = "expressionType";
    const MAX_MSG_BYTES: &'static str = "maxMsgBytes";
    const MAX_MSG_NUMS: &'static str = "maxMsgNums";
    const PROXY_FORWARD_CLIENT_ID: &'static str = "proxyForwardClientId";
    const QUEUE_ID: &'static str = "queueId";
    const QUEUE_OFFSET: &'static str = "queueOffset";
    const REQUEST_SOURCE: &'static str = "requestSource";
    const SUBSCRIPTION: &'static str = "subscription";
    const SUB_VERSION: &'static str = "subVersion";
    const SUSPEND_TIMEOUT_MILLIS: &'static str = "suspendTimeoutMillis";
    const SYS_FLAG: &'static str = "sysFlag";
    const TOPIC: &'static str = "topic";
}

impl CommandCustomHeader for PullMessageRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();

        map.insert(
            CheetahString::from_static_str(Self::CONSUMER_GROUP),
            self.consumer_group.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::TOPIC),
            self.topic.clone(),
        );
        if let Some(value) = self.queue_id {
            map.insert(
                CheetahString::from_static_str(Self::QUEUE_ID),
                CheetahString::from_string(value.to_string()),
            );
        }

        map.insert(
            CheetahString::from_static_str(Self::QUEUE_OFFSET),
            CheetahString::from_string(self.queue_offset.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::MAX_MSG_NUMS),
            CheetahString::from_string(self.max_msg_nums.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::SYS_FLAG),
            CheetahString::from_string(self.sys_flag.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::COMMIT_OFFSET),
            CheetahString::from_string(self.commit_offset.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::SUSPEND_TIMEOUT_MILLIS),
            CheetahString::from_string(self.suspend_timeout_millis.to_string()),
        );

        if let Some(ref value) = self.subscription {
            map.insert(
                CheetahString::from_static_str(Self::SUBSCRIPTION),
                value.clone(),
            );
        }

        map.insert(
            CheetahString::from_static_str(Self::SUB_VERSION),
            CheetahString::from_string(self.sub_version.to_string()),
        );

        if let Some(ref value) = self.expression_type {
            map.insert(
                CheetahString::from_static_str(Self::EXPRESSION_TYPE),
                value.clone(),
            );
        }

        if let Some(value) = self.max_msg_bytes {
            map.insert(
                CheetahString::from_static_str(Self::MAX_MSG_BYTES),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.request_source {
            map.insert(
                CheetahString::from_static_str(Self::REQUEST_SOURCE),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(ref value) = self.proxy_forward_client_id {
            map.insert(
                CheetahString::from_static_str(Self::PROXY_FORWARD_CLIENT_ID),
                value.clone(),
            );
        }

        if let Some(ref rpc) = self.topic_request {
            if let Some(rpc_map) = rpc.to_map() {
                map.extend(rpc_map);
            }
        }
        Some(map)
    }

    fn encode_fast(&mut self, out: &mut BytesMut) {
        self.write_if_not_null(out, "consumerGroup", self.consumer_group.as_str());
        self.write_if_not_null(out, "topic", self.topic.as_str());
        if let Some(value) = self.queue_id {
            self.write_if_not_null(out, "queueId", value.to_string().as_str());
        }

        self.write_if_not_null(out, "queueOffset", self.queue_offset.to_string().as_str());
        self.write_if_not_null(out, "maxMsgNums", self.max_msg_nums.to_string().as_str());
        self.write_if_not_null(out, "sysFlag", self.sys_flag.to_string().as_str());
        self.write_if_not_null(out, "commitOffset", self.commit_offset.to_string().as_str());
        self.write_if_not_null(
            out,
            "suspendTimeoutMillis",
            self.suspend_timeout_millis.to_string().as_str(),
        );
        if let Some(ref value) = self.subscription {
            self.write_if_not_null(out, "subscription", value.as_str());
        }

        self.write_if_not_null(out, "subVersion", self.sub_version.to_string().as_str());
        if let Some(ref value) = self.expression_type {
            self.write_if_not_null(out, "expressionType", value.as_str());
        }
        if let Some(value) = self.max_msg_bytes {
            self.write_if_not_null(out, "maxMsgBytes", value.to_string().as_str());
        }

        if let Some(value) = self.request_source {
            self.write_if_not_null(out, "requestSource", value.to_string().as_str());
        }
        if let Some(ref value) = self.proxy_forward_client_id {
            self.write_if_not_null(out, "proxyFrowardClientId", value.as_str());
        }

        // Assuming "lo", "ns", "nsd", "bname", "oway" are other fields in the struct
        if let Some(ref value) = self.topic_request {
            if let Some(lo) = value.lo {
                self.write_if_not_null(out, "lo", lo.to_string().as_str());
            }
        }
        if let Some(ref topic_request) = self.topic_request {
            if let Some(ref rpc) = topic_request.rpc {
                if let Some(ref np) = rpc.namespace {
                    self.write_if_not_null(out, "ns", np.as_str());
                }
                if let Some(ref nsd) = rpc.namespaced {
                    self.write_if_not_null(out, "nsd", nsd.to_string().as_str());
                }
                if let Some(ref bname) = rpc.broker_name {
                    self.write_if_not_null(out, "bname", bname.as_str());
                }
                if let Some(ref oway) = rpc.namespace {
                    self.write_if_not_null(out, "oway", oway.as_str());
                }
            }
        }
    }

    fn decode_fast(&mut self, fields: &HashMap<CheetahString, CheetahString>) -> crate::Result<()> {
        if let Some(str) = fields.get(&CheetahString::from_slice("consumerGroup")) {
            self.consumer_group = CheetahString::from_slice(str);
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("topic")) {
            self.topic = CheetahString::from_slice(str);
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("queueId")) {
            self.queue_id = str.parse::<i32>().ok();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("queueOffset")) {
            self.queue_offset = str.parse::<i64>().unwrap();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("maxMsgNums")) {
            self.max_msg_nums = str.parse::<i32>().unwrap();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("sysFlag")) {
            self.sys_flag = str.parse::<i32>().unwrap();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("commitOffset")) {
            self.commit_offset = str.parse::<i64>().unwrap();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("suspendTimeoutMillis")) {
            self.suspend_timeout_millis = str.parse::<u64>().unwrap();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("subscription")) {
            self.subscription = Some(CheetahString::from_slice(str));
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("subVersion")) {
            self.sub_version = str.parse::<i64>().unwrap();
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("expressionType")) {
            self.expression_type = Some(CheetahString::from_slice(str));
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("maxMsgBytes")) {
            self.max_msg_bytes = Some(str.parse::<i32>().unwrap());
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("requestSource")) {
            self.request_source = Some(str.parse::<i32>().unwrap());
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("proxyForwardClientId")) {
            self.proxy_forward_client_id = Some(CheetahString::from_slice(str));
        }

        self.topic_request = Some(TopicRequestHeader {
            rpc: Some(RpcRequestHeader::default()),
            ..TopicRequestHeader::default()
        });

        if let Some(str) = fields.get(&CheetahString::from_slice("lo")) {
            self.topic_request.as_mut().unwrap().lo = Some(str.parse::<bool>().unwrap());
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("ns")) {
            self.topic_request
                .as_mut()
                .unwrap()
                .rpc
                .as_mut()
                .unwrap()
                .namespace = Some(CheetahString::from_slice(str));
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("nsd")) {
            self.topic_request
                .as_mut()
                .unwrap()
                .rpc
                .as_mut()
                .unwrap()
                .namespaced = Some(str.parse::<bool>().unwrap());
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("bname")) {
            self.topic_request
                .as_mut()
                .unwrap()
                .rpc
                .as_mut()
                .unwrap()
                .broker_name = Some(CheetahString::from_slice(str));
        }

        if let Some(str) = fields.get(&CheetahString::from_slice("oway")) {
            self.topic_request
                .as_mut()
                .unwrap()
                .rpc
                .as_mut()
                .unwrap()
                .oneway = Some(str.parse::<bool>().unwrap());
        }
        Ok(())
    }

    fn support_fast_codec(&self) -> bool {
        true
    }
}

impl FromMap for PullMessageRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(Self {
            consumer_group: map
                .get(&CheetahString::from_static_str(Self::CONSUMER_GROUP))
                .cloned()
                .unwrap_or_default(),
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))
                .cloned()
                .unwrap_or_default(),
            queue_id: map
                .get(&CheetahString::from_static_str(Self::QUEUE_ID))
                .and_then(|value| value.parse::<i32>().ok()),
            queue_offset: map
                .get(&CheetahString::from_static_str(Self::QUEUE_OFFSET))
                .map_or(0, |value| value.parse().unwrap_or_default()),
            max_msg_nums: map
                .get(&CheetahString::from_static_str(Self::MAX_MSG_NUMS))
                .map_or(0, |value| value.parse().unwrap_or_default()),
            sys_flag: map
                .get(&CheetahString::from_static_str(Self::SYS_FLAG))
                .map_or(0, |value| value.parse().unwrap_or_default()),
            commit_offset: map
                .get(&CheetahString::from_static_str(Self::COMMIT_OFFSET))
                .map_or(0, |value| value.parse().unwrap_or_default()),
            suspend_timeout_millis: map
                .get(&CheetahString::from_static_str(
                    Self::SUSPEND_TIMEOUT_MILLIS,
                ))
                .map_or(0, |value| value.parse().unwrap_or_default()),
            subscription: map
                .get(&CheetahString::from_static_str(Self::SUBSCRIPTION))
                .cloned(),
            sub_version: map
                .get(&CheetahString::from_static_str(Self::SUB_VERSION))
                .map_or(0, |value| value.parse().unwrap_or_default()),
            expression_type: map
                .get(&CheetahString::from_static_str(Self::EXPRESSION_TYPE))
                .cloned(),
            max_msg_bytes: map
                .get(&CheetahString::from_static_str(Self::MAX_MSG_BYTES))
                .and_then(|value| value.parse::<i32>().ok()),
            request_source: map
                .get(&CheetahString::from_static_str(Self::REQUEST_SOURCE))
                .and_then(|value| value.parse::<i32>().ok()),
            proxy_forward_client_id: map
                .get(&CheetahString::from_static_str(
                    Self::PROXY_FORWARD_CLIENT_ID,
                ))
                .cloned(),
            topic_request: Some(<TopicRequestHeader as FromMap>::from(map)?),
        })
    }
}

impl TopicRequestHeaderTrait for PullMessageRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request.as_ref().unwrap().lo
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    fn topic(&self) -> &CheetahString {
        &self.topic
    }

    fn broker_name(&self) -> Option<&CheetahString> {
        self.topic_request
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .broker_name
            .as_ref()
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.topic_request
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .namespace
            .as_deref()
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        self.topic_request
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .namespace = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .namespaced
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        self.topic_request
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .namespaced = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .oneway
    }

    fn set_oneway(&mut self, oneway: bool) {
        self.topic_request
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .oneway = Some(oneway);
    }

    fn queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.queue_id = queue_id;
    }
}
