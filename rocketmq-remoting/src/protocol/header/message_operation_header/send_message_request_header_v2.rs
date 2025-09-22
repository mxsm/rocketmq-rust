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
use crate::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequestHeaderV2 {
    pub a: CheetahString,         // producerGroup
    pub b: CheetahString,         // topic
    pub c: CheetahString,         // defaultTopic
    pub d: i32,                   // defaultTopicQueueNums
    pub e: i32,                   // queueId
    pub f: i32,                   // sysFlag
    pub g: i64,                   // bornTimestamp
    pub h: i32,                   // flag
    pub i: Option<CheetahString>, // properties
    pub j: Option<i32>,           // reconsumeTimes
    pub k: Option<bool>,          // unitMode
    pub l: Option<i32>,           // consumeRetryTimes
    pub m: Option<bool>,          // batch
    pub n: Option<CheetahString>, // brokerName

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl CommandCustomHeader for SendMessageRequestHeaderV2 {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();
        map.insert(CheetahString::from_slice("a"), self.a.clone());
        map.insert(CheetahString::from_slice("b"), self.b.clone());
        map.insert(CheetahString::from_slice("c"), self.c.clone());
        map.insert(
            CheetahString::from_slice("d"),
            CheetahString::from_string(self.d.to_string()),
        );
        map.insert(
            CheetahString::from_slice("e"),
            CheetahString::from_string(self.e.to_string()),
        );
        map.insert(
            CheetahString::from_slice("f"),
            CheetahString::from_string(self.f.to_string()),
        );
        map.insert(
            CheetahString::from_slice("g"),
            CheetahString::from_string(self.g.to_string()),
        );
        map.insert(
            CheetahString::from_slice("h"),
            CheetahString::from_string(self.h.to_string()),
        );
        if let Some(ref value) = self.i {
            map.insert(
                CheetahString::from_slice("i"),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.j {
            map.insert(
                CheetahString::from_slice("j"),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.k {
            map.insert(
                CheetahString::from_slice("k"),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.l {
            map.insert(
                CheetahString::from_slice("l"),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.m {
            map.insert(
                CheetahString::from_slice("m"),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(ref value) = self.n {
            map.insert(CheetahString::from_slice("n"), value.clone());
        }
        if let Some(ref value) = self.topic_request_header {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }

    fn encode_fast(&mut self, out: &mut BytesMut) {
        self.write_if_not_null(out, "a", self.a.as_str());
        self.write_if_not_null(out, "b", self.b.as_str());
        self.write_if_not_null(out, "c", self.c.as_str());
        self.write_if_not_null(out, "d", self.d.to_string().as_str());
        self.write_if_not_null(out, "e", self.e.to_string().as_str());
        self.write_if_not_null(out, "f", self.f.to_string().as_str());
        self.write_if_not_null(out, "g", self.g.to_string().as_str());
        self.write_if_not_null(out, "h", self.h.to_string().as_str());
        if let Some(ref value) = self.i {
            self.write_if_not_null(out, "i", value.as_str());
        }
        if let Some(value) = self.j {
            self.write_if_not_null(out, "j", value.to_string().as_str());
        }
        if let Some(value) = self.k {
            self.write_if_not_null(out, "k", value.to_string().as_str());
        }
        if let Some(value) = self.l {
            self.write_if_not_null(out, "l", value.to_string().as_str());
        }
        if let Some(value) = self.m {
            self.write_if_not_null(out, "m", value.to_string().as_str());
        }
        if let Some(ref value) = self.n {
            self.write_if_not_null(out, "n", value.as_str());
        }
    }

    fn decode_fast(
        &mut self,
        fields: &HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.a = self.get_and_check_not_none(fields, &CheetahString::from_slice("a"))?; //producerGroup
        self.b = self.get_and_check_not_none(fields, &CheetahString::from_slice("b"))?; //topic
        self.c = self.get_and_check_not_none(fields, &CheetahString::from_slice("c"))?; //defaultTopic
        self.d = self
            .get_and_check_not_none(fields, &CheetahString::from_slice("d"))?
            .parse()
            .map_err(|_| DeserializeHeaderError("Parse field d error".to_string()))?; //defaultTopicQueueNums
        self.e = self
            .get_and_check_not_none(fields, &CheetahString::from_slice("e"))?
            .parse()
            .map_err(|_| DeserializeHeaderError("Parse field e error".to_string()))?; //queueId
        self.f = self
            .get_and_check_not_none(fields, &CheetahString::from_slice("f"))?
            .parse()
            .map_err(|_| DeserializeHeaderError("Parse field f error".to_string()))?; //sysFlag
        self.g = self
            .get_and_check_not_none(fields, &CheetahString::from_slice("g"))?
            .parse()
            .map_err(|_| DeserializeHeaderError("Parse field g error".to_string()))?; //bornTimestamp
        self.h = self
            .get_and_check_not_none(fields, &CheetahString::from_slice("h"))?
            .parse()
            .map_err(|_| DeserializeHeaderError("Parse field h error".to_string()))?; //flag

        if let Some(v) = fields.get(&CheetahString::from_slice("i")) {
            self.i = Some(v.clone());
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("j")) {
            self.j = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("k")) {
            self.k = Some(
                v.parse()
                    .map_err(|_| DeserializeHeaderError("Parse field k error".to_string()))?,
            );
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("l")) {
            self.l = Some(
                v.parse()
                    .map_err(|_| DeserializeHeaderError("Parse field l error".to_string()))?,
            );
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("m")) {
            self.m = Some(
                v.parse()
                    .map_err(|_| DeserializeHeaderError("Parse field m error".to_string()))?,
            );
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("n")) {
            self.n = Some(v.clone());
        }
        Ok(())
    }

    fn support_fast_codec(&self) -> bool {
        true
    }
}

impl FromMap for SendMessageRequestHeaderV2 {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(SendMessageRequestHeaderV2 {
            a: map.get(&CheetahString::from_slice("a")).cloned().ok_or(
                Self::Error::DeserializeHeaderError("Miss a field".to_string()),
            )?,
            b: map.get(&CheetahString::from_slice("b")).cloned().ok_or(
                Self::Error::DeserializeHeaderError("Miss b field".to_string()),
            )?,
            c: map.get(&CheetahString::from_slice("c")).cloned().ok_or(
                Self::Error::DeserializeHeaderError("Miss c field".to_string()),
            )?,
            d: map
                .get(&CheetahString::from_slice("d"))
                .cloned()
                .ok_or(Self::Error::DeserializeHeaderError(
                    "Miss d field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::DeserializeHeaderError("Parse d field error".to_string())
                })?,
            e: map
                .get(&CheetahString::from_slice("e"))
                .cloned()
                .ok_or(Self::Error::DeserializeHeaderError(
                    "Miss e field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::DeserializeHeaderError("Parse e field error".to_string())
                })?,
            f: map
                .get(&CheetahString::from_slice("f"))
                .cloned()
                .ok_or(Self::Error::DeserializeHeaderError(
                    "Miss f field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::DeserializeHeaderError("Parse f field error".to_string())
                })?,
            g: map
                .get(&CheetahString::from_slice("g"))
                .ok_or(Self::Error::DeserializeHeaderError(
                    "Miss g field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::DeserializeHeaderError("Parse g field error".to_string())
                })?,
            h: map
                .get(&CheetahString::from_slice("h"))
                .ok_or(Self::Error::DeserializeHeaderError(
                    "Miss h field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::DeserializeHeaderError("Parse h field error".to_string())
                })?,
            i: map.get(&CheetahString::from_slice("i")).cloned(),
            j: map
                .get(&CheetahString::from_slice("j"))
                .and_then(|v| v.parse().ok()),
            k: map
                .get(&CheetahString::from_slice("k"))
                .and_then(|v| v.parse().ok()),
            l: map
                .get(&CheetahString::from_slice("l"))
                .and_then(|v| v.parse().ok()),
            m: map
                .get(&CheetahString::from_slice("m"))
                .and_then(|v| v.parse().ok()),
            n: map.get(&CheetahString::from_slice("n")).cloned(),
            topic_request_header: Some(<TopicRequestHeader as FromMap>::from(map)?),
        })
    }
}

impl SendMessageRequestHeaderV2 {
    pub fn create_send_message_request_header_v1(this: &Self) -> SendMessageRequestHeader {
        SendMessageRequestHeader {
            producer_group: this.a.clone(),
            topic: this.b.clone(),
            default_topic: this.c.clone(),
            default_topic_queue_nums: this.d,
            queue_id: this.e,
            sys_flag: this.f,
            born_timestamp: this.g,
            flag: this.h,
            properties: this.i.as_ref().cloned(),
            reconsume_times: this.j,
            unit_mode: this.k,
            batch: this.m,
            max_reconsume_times: this.l,
            topic_request_header: None,
        }
    }

    pub fn create_send_message_request_header_v2(
        v1: &SendMessageRequestHeader,
    ) -> SendMessageRequestHeaderV2 {
        SendMessageRequestHeaderV2 {
            a: v1.producer_group.clone(),
            b: v1.topic.clone(),
            c: v1.default_topic.clone(),
            d: v1.default_topic_queue_nums,
            e: v1.queue_id,
            f: v1.sys_flag,
            g: v1.born_timestamp,
            h: v1.flag,
            i: v1.properties.clone(),
            j: v1.reconsume_times,
            k: v1.unit_mode,
            l: v1.max_reconsume_times,
            m: v1.batch,
            n: v1
                .topic_request_header
                .as_ref()
                .and_then(|v| v.get_broker_name().cloned()),
            topic_request_header: v1.topic_request_header.clone(),
        }
    }

    pub fn create_send_message_request_header_v2_with_move(
        v1: SendMessageRequestHeader,
    ) -> SendMessageRequestHeaderV2 {
        SendMessageRequestHeaderV2 {
            a: v1.producer_group,
            b: v1.topic,
            c: v1.default_topic,
            d: v1.default_topic_queue_nums,
            e: v1.queue_id,
            f: v1.sys_flag,
            g: v1.born_timestamp,
            h: v1.flag,
            i: v1.properties,
            j: v1.reconsume_times,
            k: v1.unit_mode,
            l: v1.max_reconsume_times,
            m: v1.batch,
            n: v1
                .topic_request_header
                .as_ref()
                .and_then(|v| v.get_broker_name().cloned()),
            topic_request_header: v1.topic_request_header,
        }
    }
}

impl TopicRequestHeaderTrait for SendMessageRequestHeaderV2 {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        match self.topic_request_header {
            None => None,
            Some(ref value) => value.lo,
        }
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.b = topic;
    }

    fn topic(&self) -> &CheetahString {
        &self.b
    }

    fn broker_name(&self) -> Option<&CheetahString> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .broker_name
            .as_ref()
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .namespace
            .as_deref()
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .namespace = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .namespaced
            .as_ref()
            .cloned()
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .namespaced = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .oneway
            .as_ref()
            .cloned()
    }

    fn set_oneway(&mut self, oneway: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .namespaced = Some(oneway);
    }

    fn queue_id(&self) -> i32 {
        self.e
    }

    fn set_queue_id(&mut self, queue_id: i32) {
        self.e = queue_id;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn send_message_request_header_v2_serializes_correctly() {
        let header = SendMessageRequestHeaderV2 {
            a: CheetahString::from_static_str("test_producer_group"),
            b: CheetahString::from_static_str("test_topic"),
            c: CheetahString::from_static_str("test_default_topic"),
            d: 8,
            e: 1,
            f: 0,
            g: 1622547800000,
            h: 0,
            i: Some(CheetahString::from_static_str("test_properties")),
            j: Some(3),
            k: Some(true),
            l: Some(5),
            m: Some(false),
            n: Some(CheetahString::from_static_str("test_broker_name")),
            topic_request_header: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("a")).unwrap(),
            "test_producer_group"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("b")).unwrap(),
            "test_topic"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("c")).unwrap(),
            "test_default_topic"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("d")).unwrap(), "8");
        assert_eq!(map.get(&CheetahString::from_static_str("e")).unwrap(), "1");
        assert_eq!(map.get(&CheetahString::from_static_str("f")).unwrap(), "0");
        assert_eq!(
            map.get(&CheetahString::from_static_str("g")).unwrap(),
            "1622547800000"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("h")).unwrap(), "0");
        assert_eq!(
            map.get(&CheetahString::from_static_str("i")).unwrap(),
            "test_properties"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("j")).unwrap(), "3");
        assert_eq!(
            map.get(&CheetahString::from_static_str("k")).unwrap(),
            "true"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("l")).unwrap(), "5");
        assert_eq!(
            map.get(&CheetahString::from_static_str("m")).unwrap(),
            "false"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("n")).unwrap(),
            "test_broker_name"
        );
    }

    #[test]
    fn send_message_request_header_v2_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("a"),
            CheetahString::from_static_str("test_producer_group"),
        );
        map.insert(
            CheetahString::from_static_str("b"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("c"),
            CheetahString::from_static_str("test_default_topic"),
        );
        map.insert(
            CheetahString::from_static_str("d"),
            CheetahString::from_static_str("8"),
        );
        map.insert(
            CheetahString::from_static_str("e"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("f"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("g"),
            CheetahString::from_static_str("1622547800000"),
        );
        map.insert(
            CheetahString::from_static_str("h"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("i"),
            CheetahString::from_static_str("test_properties"),
        );
        map.insert(
            CheetahString::from_static_str("j"),
            CheetahString::from_static_str("3"),
        );
        map.insert(
            CheetahString::from_static_str("k"),
            CheetahString::from_static_str("true"),
        );
        map.insert(
            CheetahString::from_static_str("l"),
            CheetahString::from_static_str("5"),
        );
        map.insert(
            CheetahString::from_static_str("m"),
            CheetahString::from_static_str("false"),
        );
        map.insert(
            CheetahString::from_static_str("n"),
            CheetahString::from_static_str("test_broker_name"),
        );

        let header = <SendMessageRequestHeaderV2 as FromMap>::from(&map).unwrap();
        assert_eq!(header.a, "test_producer_group");
        assert_eq!(header.b, "test_topic");
        assert_eq!(header.c, "test_default_topic");
        assert_eq!(header.d, 8);
        assert_eq!(header.e, 1);
        assert_eq!(header.f, 0);
        assert_eq!(header.g, 1622547800000);
        assert_eq!(header.h, 0);
        assert_eq!(header.i.unwrap(), "test_properties");
        assert_eq!(header.j.unwrap(), 3);
        assert!(header.k.unwrap());
        assert_eq!(header.l.unwrap(), 5);
        assert!(!header.m.unwrap());
        assert_eq!(header.n.unwrap(), "test_broker_name");
    }

    #[test]
    fn send_message_request_header_v2_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("a"),
            CheetahString::from_static_str("test_producer_group"),
        );
        map.insert(
            CheetahString::from_static_str("b"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("c"),
            CheetahString::from_static_str("test_default_topic"),
        );
        map.insert(
            CheetahString::from_static_str("d"),
            CheetahString::from_static_str("8"),
        );
        map.insert(
            CheetahString::from_static_str("e"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("f"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("g"),
            CheetahString::from_static_str("1622547800000"),
        );
        map.insert(
            CheetahString::from_static_str("h"),
            CheetahString::from_static_str("0"),
        );

        let header = <SendMessageRequestHeaderV2 as FromMap>::from(&map).unwrap();
        assert_eq!(header.a, "test_producer_group");
        assert_eq!(header.b, "test_topic");
        assert_eq!(header.c, "test_default_topic");
        assert_eq!(header.d, 8);
        assert_eq!(header.e, 1);
        assert_eq!(header.f, 0);
        assert_eq!(header.g, 1622547800000);
        assert_eq!(header.h, 0);
        assert!(header.i.is_none());
        assert!(header.j.is_none());
        assert!(header.k.is_none());
        assert!(header.l.is_none());
        assert!(header.m.is_none());
        assert!(header.n.is_none());
    }

    #[test]
    fn send_message_request_header_v2_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("a"),
            CheetahString::from_static_str("test_producer_group"),
        );
        map.insert(
            CheetahString::from_static_str("b"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("c"),
            CheetahString::from_static_str("test_default_topic"),
        );
        map.insert(
            CheetahString::from_static_str("d"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("e"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("f"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("g"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("h"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <SendMessageRequestHeaderV2 as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
