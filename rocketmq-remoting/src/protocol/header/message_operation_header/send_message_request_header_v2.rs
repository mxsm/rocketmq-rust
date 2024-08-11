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
    pub a: String,         // producerGroup
    pub b: String,         // topic
    pub c: String,         // defaultTopic
    pub d: i32,            // defaultTopicQueueNums
    pub e: i32,            // queueId
    pub f: i32,            // sysFlag
    pub g: i64,            // bornTimestamp
    pub h: i32,            // flag
    pub i: Option<String>, // properties
    pub j: Option<i32>,    // reconsumeTimes
    pub k: Option<bool>,   // unitMode
    pub l: Option<i32>,    // consumeRetryTimes
    pub m: Option<bool>,   // batch
    pub n: Option<String>, // brokerName

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl CommandCustomHeader for SendMessageRequestHeaderV2 {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::new();
        map.insert("a".to_string(), self.a.clone());
        map.insert("b".to_string(), self.b.clone());
        map.insert("c".to_string(), self.c.clone());
        map.insert("d".to_string(), self.d.to_string());
        map.insert("e".to_string(), self.e.to_string());
        map.insert("f".to_string(), self.f.to_string());
        map.insert("g".to_string(), self.g.to_string());
        map.insert("h".to_string(), self.h.to_string());
        if let Some(ref value) = self.i {
            map.insert("i".to_string(), value.clone());
        }
        if let Some(value) = self.j {
            map.insert("j".to_string(), value.to_string());
        }
        if let Some(value) = self.k {
            map.insert("k".to_string(), value.to_string());
        }
        if let Some(value) = self.l {
            map.insert("l".to_string(), value.to_string());
        }
        if let Some(value) = self.m {
            map.insert("m".to_string(), value.to_string());
        }
        if let Some(ref value) = self.n {
            map.insert("n".to_string(), value.clone());
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

    fn decode_fast(&mut self, fields: &HashMap<String, String>) {
        if let Some(v) = fields.get("a") {
            self.a.clone_from(v)
        }
        if let Some(v) = fields.get("b") {
            self.b.clone_from(v)
        }
        if let Some(v) = fields.get("c") {
            self.c.clone_from(v)
        }

        if let Some(v) = fields.get("d") {
            self.d = v.parse().unwrap();
        }
        if let Some(v) = fields.get("e") {
            self.e = v.parse().unwrap();
        }
        if let Some(v) = fields.get("f") {
            self.f = v.parse().unwrap();
        }
        if let Some(v) = fields.get("g") {
            self.g = v.parse().unwrap();
        }

        if let Some(v) = fields.get("h") {
            self.h = v.parse().unwrap();
        }

        if let Some(v) = fields.get("i") {
            self.i = Some(v.clone());
        }

        if let Some(v) = fields.get("j") {
            self.j = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get("k") {
            self.k = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get("l") {
            self.l = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get("m") {
            self.m = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get("n") {
            self.n = Some(v.clone());
        }
    }

    fn support_fast_codec(&self) -> bool {
        true
    }
}

impl FromMap for SendMessageRequestHeaderV2 {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(SendMessageRequestHeaderV2 {
            a: map.get("a")?.clone(),
            b: map.get("b")?.clone(),
            c: map.get("c")?.clone(),
            d: map.get("d")?.parse().ok()?,
            e: map.get("e")?.parse().ok()?,
            f: map.get("f")?.parse().ok()?,
            g: map.get("g")?.parse().ok()?,
            h: map.get("h")?.parse().ok()?,
            i: map.get("i").cloned(),
            j: map.get("j").and_then(|v| v.parse().ok()),
            k: map.get("k").and_then(|v| v.parse().ok()),
            l: map.get("l").and_then(|v| v.parse().ok()),
            m: map.get("m").and_then(|v| v.parse().ok()),
            n: map.get("n").cloned(),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
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
            queue_id: Some(this.e),
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
            e: v1.queue_id.unwrap(),
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
                .and_then(|v| v.get_broker_name().map(|v| v.to_string())),
            topic_request_header: v1.topic_request_header.clone(),
        }
    }
}

impl TopicRequestHeaderTrait for SendMessageRequestHeaderV2 {
    fn with_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        match self.topic_request_header {
            None => None,
            Some(ref value) => value.lo,
        }
    }

    fn with_topic(&mut self, topic: String) {
        self.b = topic;
    }

    fn topic(&self) -> &str {
        self.b.as_str()
    }

    fn broker_name(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()?
            .rpc_request_header
            .as_ref()?
            .broker_name
            .as_deref()
    }

    fn with_broker_name(&mut self, broker_name: String) {
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

    fn with_namespace(&mut self, namespace: String) {
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

    fn with_namespaced(&mut self, namespaced: bool) {
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

    fn with_oneway(&mut self, oneway: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc_request_header
            .as_mut()
            .unwrap()
            .namespaced = Some(oneway);
    }

    fn queue_id(&self) -> Option<i32> {
        Some(self.e)
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.e = queue_id.unwrap();
    }
}

#[cfg(test)]
mod send_message_request_header_v2_tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn from_map_creates_instance_with_all_fields() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), "ProducerGroup".to_string());
        map.insert("b".to_string(), "TopicName".to_string());
        map.insert("c".to_string(), "DefaultTopic".to_string());
        map.insert("d".to_string(), "4".to_string());
        map.insert("e".to_string(), "1".to_string());
        map.insert("f".to_string(), "0".to_string());
        map.insert("g".to_string(), "1622547600000".to_string());
        map.insert("h".to_string(), "0".to_string());
        map.insert("i".to_string(), "key:value".to_string());
        map.insert("j".to_string(), "0".to_string());
        map.insert("k".to_string(), "false".to_string());
        map.insert("l".to_string(), "3".to_string());
        map.insert("m".to_string(), "false".to_string());
        map.insert("n".to_string(), "BrokerName".to_string());

        let header = <SendMessageRequestHeaderV2 as FromMap>::from(&map).unwrap();

        assert_eq!(header.a, "ProducerGroup");
        assert_eq!(header.b, "TopicName");
        assert_eq!(header.c, "DefaultTopic");
        assert_eq!(header.d, 4);
        assert_eq!(header.e, 1);
        assert_eq!(header.f, 0);
        assert_eq!(header.g, 1622547600000);
        assert_eq!(header.h, 0);
        assert_eq!(header.i, Some("key:value".to_string()));
        assert_eq!(header.j, Some(0));
        assert_eq!(header.k, Some(false));
        assert_eq!(header.l, Some(3));
        assert_eq!(header.m, Some(false));
        assert_eq!(header.n, Some("BrokerName".to_string()));
    }

    #[test]
    fn to_map_returns_map_with_all_fields() {
        let header = SendMessageRequestHeaderV2 {
            a: "ProducerGroup".to_string(),
            b: "TopicName".to_string(),
            c: "DefaultTopic".to_string(),
            d: 4,
            e: 1,
            f: 0,
            g: 1622547600000,
            h: 0,
            i: Some("key:value".to_string()),
            j: Some(0),
            k: Some(false),
            l: Some(3),
            m: Some(false),
            n: Some("BrokerName".to_string()),
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();

        assert_eq!(map.get("a").unwrap(), "ProducerGroup");
        assert_eq!(map.get("b").unwrap(), "TopicName");
        assert_eq!(map.get("c").unwrap(), "DefaultTopic");
        assert_eq!(map.get("d").unwrap(), "4");
        assert_eq!(map.get("e").unwrap(), "1");
        assert_eq!(map.get("f").unwrap(), "0");
        assert_eq!(map.get("g").unwrap(), "1622547600000");
        assert_eq!(map.get("h").unwrap(), "0");
        assert_eq!(map.get("i").unwrap(), "key:value");
        assert_eq!(map.get("j").unwrap(), "0");
        assert_eq!(map.get("k").unwrap(), "false");
        assert_eq!(map.get("l").unwrap(), "3");
        assert_eq!(map.get("m").unwrap(), "false");
        assert_eq!(map.get("n").unwrap(), "BrokerName");
    }

    #[test]
    fn from_map_with_missing_fields_returns_none() {
        let map = HashMap::new(); // Empty map

        let header = <SendMessageRequestHeaderV2 as FromMap>::from(&map);

        assert!(header.is_none());
    }

    #[test]
    fn from_map_with_invalid_values_returns_none() {
        let mut map = HashMap::new();
        map.insert("d".to_string(), "invalid".to_string()); // Invalid integer

        let header = <SendMessageRequestHeaderV2 as FromMap>::from(&map);

        assert!(header.is_none());
    }

    #[test]
    fn create_send_message_request_header_v1_correctly_transforms_fields() {
        let header_v2 = SendMessageRequestHeaderV2 {
            a: "ProducerGroup".to_string(),
            b: "TopicName".to_string(),
            c: "DefaultTopic".to_string(),
            d: 4,
            e: 1,
            f: 0,
            g: 1622547600000,
            h: 0,
            i: Some("key:value".to_string()),
            j: Some(0),
            k: Some(false),
            l: Some(3),
            m: Some(false),
            n: Some("BrokerName".to_string()),
            topic_request_header: None,
        };

        let header_v1 =
            SendMessageRequestHeaderV2::create_send_message_request_header_v1(&header_v2);

        assert_eq!(header_v1.producer_group, "ProducerGroup");
        assert_eq!(header_v1.topic, "TopicName");
        assert_eq!(header_v1.default_topic, "DefaultTopic");
        assert_eq!(header_v1.default_topic_queue_nums, 4);
        assert_eq!(header_v1.queue_id, Some(1));
        assert_eq!(header_v1.sys_flag, 0);
        assert_eq!(header_v1.born_timestamp, 1622547600000);
        assert_eq!(header_v1.flag, 0);
        assert_eq!(header_v1.properties, Some("key:value".to_string()));
        assert_eq!(header_v1.reconsume_times, Some(0));
        assert_eq!(header_v1.unit_mode, Some(false));
        assert_eq!(header_v1.batch, Some(false));
        assert_eq!(header_v1.max_reconsume_times, Some(3));
    }
}
