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

use std::collections::HashMap;
use std::str::FromStr;

use bytes::BytesMut;
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::rpc::topic_request_header::TopicRequestHeader;

// Field key constants to avoid repeated allocations
const FIELD_A: &str = "a";
const FIELD_B: &str = "b";
const FIELD_C: &str = "c";
const FIELD_D: &str = "d";
const FIELD_E: &str = "e";
const FIELD_F: &str = "f";
const FIELD_G: &str = "g";
const FIELD_H: &str = "h";
const FIELD_I: &str = "i";
const FIELD_J: &str = "j";
const FIELD_K: &str = "k";
const FIELD_L: &str = "l";
const FIELD_M: &str = "m";
const FIELD_N: &str = "n";

const KEY_A: CheetahString = CheetahString::from_static_str(FIELD_A);
const KEY_B: CheetahString = CheetahString::from_static_str(FIELD_B);
const KEY_C: CheetahString = CheetahString::from_static_str(FIELD_C);
const KEY_D: CheetahString = CheetahString::from_static_str(FIELD_D);
const KEY_E: CheetahString = CheetahString::from_static_str(FIELD_E);
const KEY_F: CheetahString = CheetahString::from_static_str(FIELD_F);
const KEY_G: CheetahString = CheetahString::from_static_str(FIELD_G);
const KEY_H: CheetahString = CheetahString::from_static_str(FIELD_H);
const KEY_I: CheetahString = CheetahString::from_static_str(FIELD_I);
const KEY_J: CheetahString = CheetahString::from_static_str(FIELD_J);
const KEY_K: CheetahString = CheetahString::from_static_str(FIELD_K);
const KEY_L: CheetahString = CheetahString::from_static_str(FIELD_L);
const KEY_M: CheetahString = CheetahString::from_static_str(FIELD_M);
const KEY_N: CheetahString = CheetahString::from_static_str(FIELD_N);

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
        let mut map = HashMap::with_capacity(14);
        map.insert(CheetahString::from_static_str(FIELD_A), self.a.clone());
        map.insert(CheetahString::from_static_str(FIELD_B), self.b.clone());
        map.insert(CheetahString::from_static_str(FIELD_C), self.c.clone());
        map.insert(
            CheetahString::from_static_str(FIELD_D),
            CheetahString::from_string(self.d.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(FIELD_E),
            CheetahString::from_string(self.e.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(FIELD_F),
            CheetahString::from_string(self.f.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(FIELD_G),
            CheetahString::from_string(self.g.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(FIELD_H),
            CheetahString::from_string(self.h.to_string()),
        );
        if let Some(ref value) = self.i {
            map.insert(CheetahString::from_static_str(FIELD_I), value.clone());
        }
        if let Some(value) = self.j {
            map.insert(
                CheetahString::from_static_str(FIELD_J),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.k {
            map.insert(
                CheetahString::from_static_str(FIELD_K),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.l {
            map.insert(
                CheetahString::from_static_str(FIELD_L),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.m {
            map.insert(
                CheetahString::from_static_str(FIELD_M),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(ref value) = self.n {
            map.insert(CheetahString::from_static_str(FIELD_N), value.clone());
        }
        if let Some(ref value) = self.topic_request_header {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }

    fn encode_fast(&mut self, out: &mut BytesMut) {
        self.write_if_not_null(out, FIELD_A, self.a.as_str());
        self.write_if_not_null(out, FIELD_B, self.b.as_str());
        self.write_if_not_null(out, FIELD_C, self.c.as_str());

        // Reduce allocations by avoiding format! macro
        let d_str = self.d.to_string();
        self.write_if_not_null(out, FIELD_D, &d_str);
        let e_str = self.e.to_string();
        self.write_if_not_null(out, FIELD_E, &e_str);
        let f_str = self.f.to_string();
        self.write_if_not_null(out, FIELD_F, &f_str);
        let g_str = self.g.to_string();
        self.write_if_not_null(out, FIELD_G, &g_str);
        let h_str = self.h.to_string();
        self.write_if_not_null(out, FIELD_H, &h_str);

        if let Some(ref value) = self.i {
            self.write_if_not_null(out, FIELD_I, value.as_str());
        }
        if let Some(value) = self.j {
            let j_str = value.to_string();
            self.write_if_not_null(out, FIELD_J, &j_str);
        }
        if let Some(value) = self.k {
            self.write_if_not_null(out, FIELD_K, if value { "true" } else { "false" });
        }
        if let Some(value) = self.l {
            let l_str = value.to_string();
            self.write_if_not_null(out, FIELD_L, &l_str);
        }
        if let Some(value) = self.m {
            self.write_if_not_null(out, FIELD_M, if value { "true" } else { "false" });
        }
        if let Some(ref value) = self.n {
            self.write_if_not_null(out, FIELD_N, value.as_str());
        }
    }

    fn decode_fast(&mut self, fields: &HashMap<CheetahString, CheetahString>) -> rocketmq_error::RocketMQResult<()> {
        // Use static keys to avoid repeated allocations

        self.a = self.get_and_check_not_none(fields, &KEY_A)?; //producerGroup
        self.b = self.get_and_check_not_none(fields, &KEY_B)?; //topic
        self.c = self.get_and_check_not_none(fields, &KEY_C)?; //defaultTopic
        self.d = self.get_and_check_not_none(fields, &KEY_D)?.parse().map_err(|_| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "header",
                message: "Parse field d error".to_string(),
            })
        })?; //defaultTopicQueueNums
        self.e = self.get_and_check_not_none(fields, &KEY_E)?.parse().map_err(|_| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "header",
                message: "Parse field e error".to_string(),
            })
        })?; //queueId
        self.f = self.get_and_check_not_none(fields, &KEY_F)?.parse().map_err(|_| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "header",
                message: "Parse field f error".to_string(),
            })
        })?; //sysFlag
        self.g = self.get_and_check_not_none(fields, &KEY_G)?.parse().map_err(|_| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "header",
                message: "Parse field g error".to_string(),
            })
        })?; //bornTimestamp
        self.h = self.get_and_check_not_none(fields, &KEY_H)?.parse().map_err(|_| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "header",
                message: "Parse field h error".to_string(),
            })
        })?; //flag

        if let Some(v) = fields.get(&CheetahString::from_static_str(FIELD_I)) {
            self.i = Some(v.clone());
        }

        if let Some(v) = fields.get(&CheetahString::from_static_str(FIELD_J)) {
            self.j = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get(&CheetahString::from_static_str(FIELD_K)) {
            self.k = Some(v.parse().map_err(|_| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "Parse field k error".to_string(),
                })
            })?);
        }

        if let Some(v) = fields.get(&CheetahString::from_static_str(FIELD_L)) {
            self.l = Some(v.parse().map_err(|_| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "Parse field l error".to_string(),
                })
            })?);
        }

        if let Some(v) = fields.get(&CheetahString::from_static_str(FIELD_M)) {
            self.m = Some(v.parse().map_err(|_| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "Parse field m error".to_string(),
                })
            })?);
        }

        if let Some(v) = fields.get(&CheetahString::from_static_str(FIELD_N)) {
            self.n = Some(v.clone());
        }
        Ok(())
    }

    fn support_fast_codec(&self) -> bool {
        true
    }
}

impl FromMap for SendMessageRequestHeaderV2 {
    type Error = rocketmq_error::RocketMQError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        #[inline(always)]
        fn get_required<'a>(
            map: &'a HashMap<CheetahString, CheetahString>,
            key: &CheetahString,
            field: &'static str,
        ) -> Result<&'a CheetahString, rocketmq_error::RocketMQError> {
            map.get(key).ok_or_else(|| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: format!("Missing field: {}", field),
                })
            })
        }

        #[inline(always)]
        fn parse_required<T: FromStr>(
            map: &HashMap<CheetahString, CheetahString>,
            key: &CheetahString,
            field: &'static str,
        ) -> Result<T, rocketmq_error::RocketMQError> {
            get_required(map, key, field)?.as_str().parse::<T>().map_err(|_| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: format!("Parse {} field error", field),
                })
            })
        }

        #[inline(always)]
        fn optional_parse<T: FromStr>(map: &HashMap<CheetahString, CheetahString>, key: &CheetahString) -> Option<T> {
            map.get(key)?.as_str().parse::<T>().ok()
        }

        Ok(SendMessageRequestHeaderV2 {
            a: get_required(map, &KEY_A, FIELD_A)?.clone(),
            b: get_required(map, &KEY_B, FIELD_B)?.clone(),
            c: get_required(map, &KEY_C, FIELD_C)?.clone(),

            d: parse_required::<i32>(map, &KEY_D, FIELD_D)?,
            e: parse_required::<i32>(map, &KEY_E, FIELD_E)?,
            f: parse_required::<i32>(map, &KEY_F, FIELD_F)?,
            g: parse_required::<i64>(map, &KEY_G, FIELD_G)?,
            h: parse_required::<i32>(map, &KEY_H, FIELD_H)?,

            i: map.get(&KEY_I).cloned(),
            j: optional_parse::<i32>(map, &KEY_J),
            k: optional_parse::<bool>(map, &KEY_K),
            l: optional_parse::<i32>(map, &KEY_L),
            m: optional_parse::<bool>(map, &KEY_M),
            n: map.get(&KEY_N).cloned(),
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

    pub fn create_send_message_request_header_v2(v1: &SendMessageRequestHeader) -> SendMessageRequestHeaderV2 {
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

    pub fn create_send_message_request_header_v2_with_move(v1: SendMessageRequestHeader) -> SendMessageRequestHeaderV2 {
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
        if let Some(header) = self.topic_request_header.as_mut() {
            header.lo = lo;
        }
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request_header.as_ref().and_then(|h| h.lo)
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
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.broker_name = Some(broker_name);
            }
        }
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
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.namespace = Some(namespace);
            }
        }
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
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.namespaced = Some(namespaced);
            }
        }
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
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.oneway = Some(oneway);
            }
        }
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
        assert_eq!(map.get(&CheetahString::from_static_str("b")).unwrap(), "test_topic");
        assert_eq!(
            map.get(&CheetahString::from_static_str("c")).unwrap(),
            "test_default_topic"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("d")).unwrap(), "8");
        assert_eq!(map.get(&CheetahString::from_static_str("e")).unwrap(), "1");
        assert_eq!(map.get(&CheetahString::from_static_str("f")).unwrap(), "0");
        assert_eq!(map.get(&CheetahString::from_static_str("g")).unwrap(), "1622547800000");
        assert_eq!(map.get(&CheetahString::from_static_str("h")).unwrap(), "0");
        assert_eq!(
            map.get(&CheetahString::from_static_str("i")).unwrap(),
            "test_properties"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("j")).unwrap(), "3");
        assert_eq!(map.get(&CheetahString::from_static_str("k")).unwrap(), "true");
        assert_eq!(map.get(&CheetahString::from_static_str("l")).unwrap(), "5");
        assert_eq!(map.get(&CheetahString::from_static_str("m")).unwrap(), "false");
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
        map.insert(CheetahString::from_static_str("d"), CheetahString::from_static_str("8"));
        map.insert(CheetahString::from_static_str("e"), CheetahString::from_static_str("1"));
        map.insert(CheetahString::from_static_str("f"), CheetahString::from_static_str("0"));
        map.insert(
            CheetahString::from_static_str("g"),
            CheetahString::from_static_str("1622547800000"),
        );
        map.insert(CheetahString::from_static_str("h"), CheetahString::from_static_str("0"));
        map.insert(
            CheetahString::from_static_str("i"),
            CheetahString::from_static_str("test_properties"),
        );
        map.insert(CheetahString::from_static_str("j"), CheetahString::from_static_str("3"));
        map.insert(
            CheetahString::from_static_str("k"),
            CheetahString::from_static_str("true"),
        );
        map.insert(CheetahString::from_static_str("l"), CheetahString::from_static_str("5"));
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
        map.insert(CheetahString::from_static_str("d"), CheetahString::from_static_str("8"));
        map.insert(CheetahString::from_static_str("e"), CheetahString::from_static_str("1"));
        map.insert(CheetahString::from_static_str("f"), CheetahString::from_static_str("0"));
        map.insert(
            CheetahString::from_static_str("g"),
            CheetahString::from_static_str("1622547800000"),
        );
        map.insert(CheetahString::from_static_str("h"), CheetahString::from_static_str("0"));

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
