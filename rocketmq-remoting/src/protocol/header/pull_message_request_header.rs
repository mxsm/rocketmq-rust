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
use rocketmq_error::RocketmqError::DeserializeHeaderError;
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

    pub queue_id: i32,

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

        map.insert(
            CheetahString::from_static_str(Self::QUEUE_ID),
            CheetahString::from_string(self.queue_id.to_string()),
        );

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
        self.write_if_not_null(out, Self::CONSUMER_GROUP, self.consumer_group.as_str());
        self.write_if_not_null(out, Self::TOPIC, self.topic.as_str());

        self.write_if_not_null(out, Self::QUEUE_ID, self.queue_id.to_string().as_str());

        self.write_if_not_null(
            out,
            Self::QUEUE_OFFSET,
            self.queue_offset.to_string().as_str(),
        );
        self.write_if_not_null(
            out,
            Self::MAX_MSG_NUMS,
            self.max_msg_nums.to_string().as_str(),
        );
        self.write_if_not_null(out, Self::SYS_FLAG, self.sys_flag.to_string().as_str());
        self.write_if_not_null(
            out,
            Self::COMMIT_OFFSET,
            self.commit_offset.to_string().as_str(),
        );
        self.write_if_not_null(
            out,
            Self::SUSPEND_TIMEOUT_MILLIS,
            self.suspend_timeout_millis.to_string().as_str(),
        );
        if let Some(ref value) = self.subscription {
            self.write_if_not_null(out, Self::SUBSCRIPTION, value.as_str());
        }

        self.write_if_not_null(
            out,
            Self::SUB_VERSION,
            self.sub_version.to_string().as_str(),
        );
        if let Some(ref value) = self.expression_type {
            self.write_if_not_null(out, Self::EXPRESSION_TYPE, value.as_str());
        }
        if let Some(value) = self.max_msg_bytes {
            self.write_if_not_null(out, Self::MAX_MSG_BYTES, value.to_string().as_str());
        }

        if let Some(value) = self.request_source {
            self.write_if_not_null(out, Self::REQUEST_SOURCE, value.to_string().as_str());
        }
        if let Some(ref value) = self.proxy_forward_client_id {
            self.write_if_not_null(out, Self::PROXY_FORWARD_CLIENT_ID, value.as_str());
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

    fn decode_fast(
        &mut self,
        fields: &HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.consumer_group = self.get_and_check_not_none(
            fields,
            &CheetahString::from_static_str(Self::CONSUMER_GROUP),
        )?;
        self.topic =
            self.get_and_check_not_none(fields, &CheetahString::from_static_str(Self::TOPIC))?;
        self.queue_id = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::QUEUE_ID))?
            .parse()
            .map_err(|_| DeserializeHeaderError(format!("Parse field {} error", Self::QUEUE_ID)))?;
        self.queue_offset = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::QUEUE_OFFSET))?
            .parse()
            .map_err(|_| {
                DeserializeHeaderError(format!("Parse field {} error", Self::QUEUE_OFFSET))
            })?;
        self.max_msg_nums = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::MAX_MSG_NUMS))?
            .parse()
            .map_err(|_| {
                DeserializeHeaderError(format!("Parse field {} error", Self::MAX_MSG_NUMS))
            })?;
        self.sys_flag = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::SYS_FLAG))?
            .parse()
            .map_err(|_| DeserializeHeaderError(format!("Parse field {} error", Self::SYS_FLAG)))?;
        self.commit_offset = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::COMMIT_OFFSET))?
            .parse()
            .map_err(|_| {
                DeserializeHeaderError(format!("Parse field {} error", Self::COMMIT_OFFSET))
            })?;
        self.suspend_timeout_millis = self
            .get_and_check_not_none(
                fields,
                &CheetahString::from_static_str(Self::SUSPEND_TIMEOUT_MILLIS),
            )?
            .parse()
            .map_err(|_| {
                DeserializeHeaderError(format!(
                    "Parse field {} error",
                    Self::SUSPEND_TIMEOUT_MILLIS
                ))
            })?;
        self.consumer_group = self.get_and_check_not_none(
            fields,
            &CheetahString::from_static_str(Self::CONSUMER_GROUP),
        )?;

        self.subscription = fields
            .get(&CheetahString::from_static_str(Self::SUBSCRIPTION))
            .cloned();

        self.sub_version = self
            .get_and_check_not_none(fields, &CheetahString::from_static_str(Self::SUB_VERSION))?
            .parse()
            .map_err(|_| DeserializeHeaderError(format!("Parse field {} error", Self::QUEUE_ID)))?;

        self.expression_type = fields
            .get(&CheetahString::from_static_str(Self::EXPRESSION_TYPE))
            .cloned();

        self.max_msg_bytes = fields
            .get(&CheetahString::from_static_str(Self::MAX_MSG_BYTES))
            .and_then(|value| value.parse::<i32>().ok());

        self.request_source = fields
            .get(&CheetahString::from_static_str(Self::REQUEST_SOURCE))
            .and_then(|value| value.parse::<i32>().ok());

        self.proxy_forward_client_id = fields
            .get(&CheetahString::from_static_str(
                Self::PROXY_FORWARD_CLIENT_ID,
            ))
            .cloned();

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
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(Self {
            consumer_group: map
                .get(&CheetahString::from_static_str(Self::CONSUMER_GROUP))
                .cloned()
                .ok_or(DeserializeHeaderError(
                    "Missing field consumerGroup".to_string(),
                ))?,
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))
                .cloned()
                .ok_or(DeserializeHeaderError("Missing field topic".to_string()))?,
            queue_id: map
                .get(&CheetahString::from_static_str(Self::QUEUE_ID))
                .and_then(|value| value.parse::<i32>().ok())
                .ok_or(DeserializeHeaderError("Missing field queueId".to_string()))?,
            queue_offset: map
                .get(&CheetahString::from_static_str(Self::QUEUE_OFFSET))
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or(DeserializeHeaderError(
                    "Missing field queueOffset".to_string(),
                ))?,
            max_msg_nums: map
                .get(&CheetahString::from_static_str(Self::MAX_MSG_NUMS))
                .and_then(|value| value.parse::<i32>().ok())
                .ok_or(DeserializeHeaderError(
                    "Missing field maxMsgNums".to_string(),
                ))?,
            sys_flag: map
                .get(&CheetahString::from_static_str(Self::SYS_FLAG))
                .and_then(|value| value.parse::<i32>().ok())
                .ok_or(DeserializeHeaderError("Missing field sysFlag".to_string()))?,
            commit_offset: map
                .get(&CheetahString::from_static_str(Self::COMMIT_OFFSET))
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or(DeserializeHeaderError(
                    "Missing field commitOffset".to_string(),
                ))?,
            suspend_timeout_millis: map
                .get(&CheetahString::from_static_str(
                    Self::SUSPEND_TIMEOUT_MILLIS,
                ))
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(DeserializeHeaderError(
                    "Missing field suspendTimeoutMillis".to_string(),
                ))?,
            subscription: map
                .get(&CheetahString::from_static_str(Self::SUBSCRIPTION))
                .cloned(),
            sub_version: map
                .get(&CheetahString::from_static_str(Self::SUB_VERSION))
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or(DeserializeHeaderError(
                    "Missing field subVersion".to_string(),
                ))?,
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

    fn queue_id(&self) -> i32 {
        self.queue_id
    }

    fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn pull_message_request_header_serializes_correctly() {
        let header = PullMessageRequestHeader {
            consumer_group: CheetahString::from_static_str("test_consumer_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            queue_offset: 100,
            max_msg_nums: 10,
            sys_flag: 0,
            commit_offset: 50,
            suspend_timeout_millis: 3000,
            subscription: Some(CheetahString::from_static_str("test_subscription")),
            sub_version: 1,
            expression_type: Some(CheetahString::from_static_str("test_expression")),
            max_msg_bytes: Some(1024),
            request_source: Some(1),
            proxy_forward_client_id: Some(CheetahString::from_static_str("test_client_id")),
            topic_request: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("consumerGroup"))
                .unwrap(),
            "test_consumer_group"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("topic")).unwrap(),
            "test_topic"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("queueId")).unwrap(),
            "1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("queueOffset"))
                .unwrap(),
            "100"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("maxMsgNums"))
                .unwrap(),
            "10"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("sysFlag")).unwrap(),
            "0"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("commitOffset"))
                .unwrap(),
            "50"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("suspendTimeoutMillis"))
                .unwrap(),
            "3000"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("subscription"))
                .unwrap(),
            "test_subscription"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("subVersion"))
                .unwrap(),
            "1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("expressionType"))
                .unwrap(),
            "test_expression"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("maxMsgBytes"))
                .unwrap(),
            "1024"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("requestSource"))
                .unwrap(),
            "1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("proxyForwardClientId"))
                .unwrap(),
            "test_client_id"
        );
    }

    #[test]
    fn pull_message_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("test_consumer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("queueOffset"),
            CheetahString::from_static_str("100"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgNums"),
            CheetahString::from_static_str("10"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("50"),
        );
        map.insert(
            CheetahString::from_static_str("suspendTimeoutMillis"),
            CheetahString::from_static_str("3000"),
        );
        map.insert(
            CheetahString::from_static_str("subscription"),
            CheetahString::from_static_str("test_subscription"),
        );
        map.insert(
            CheetahString::from_static_str("subVersion"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("expressionType"),
            CheetahString::from_static_str("test_expression"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgBytes"),
            CheetahString::from_static_str("1024"),
        );
        map.insert(
            CheetahString::from_static_str("requestSource"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("proxyForwardClientId"),
            CheetahString::from_static_str("test_client_id"),
        );

        let header = <PullMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_consumer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.queue_offset, 100);
        assert_eq!(header.max_msg_nums, 10);
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.commit_offset, 50);
        assert_eq!(header.suspend_timeout_millis, 3000);
        assert_eq!(header.subscription.unwrap(), "test_subscription");
        assert_eq!(header.sub_version, 1);
        assert_eq!(header.expression_type.unwrap(), "test_expression");
        assert_eq!(header.max_msg_bytes.unwrap(), 1024);
        assert_eq!(header.request_source.unwrap(), 1);
        assert_eq!(header.proxy_forward_client_id.unwrap(), "test_client_id");
    }

    #[test]
    fn pull_message_request_header_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("test_consumer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("queueOffset"),
            CheetahString::from_static_str("100"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgNums"),
            CheetahString::from_static_str("10"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("50"),
        );
        map.insert(
            CheetahString::from_static_str("suspendTimeoutMillis"),
            CheetahString::from_static_str("3000"),
        );
        map.insert(
            CheetahString::from_static_str("subVersion"),
            CheetahString::from_static_str("1"),
        );

        let header = <PullMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_consumer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.queue_offset, 100);
        assert_eq!(header.max_msg_nums, 10);
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.commit_offset, 50);
        assert_eq!(header.suspend_timeout_millis, 3000);
        assert_eq!(header.sub_version, 1);
        assert!(header.subscription.is_none());
        assert!(header.expression_type.is_none());
        assert!(header.max_msg_bytes.is_none());
        assert!(header.request_source.is_none());
        assert!(header.proxy_forward_client_id.is_none());
    }

    #[test]
    fn pull_message_request_header_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("test_consumer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("queueOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgNums"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("suspendTimeoutMillis"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("subVersion"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <PullMessageRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
