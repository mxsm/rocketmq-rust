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

    fn decode_fast(&mut self, fields: &HashMap<CheetahString, CheetahString>) {
        if let Some(v) = fields.get(&CheetahString::from_slice("a")) {
            self.a = v.clone();
        }
        if let Some(v) = fields.get(&CheetahString::from_slice("b")) {
            self.b = v.clone();
        }
        if let Some(v) = fields.get(&CheetahString::from_slice("c")) {
            self.c = v.clone();
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("d")) {
            self.d = v.parse().unwrap();
        }
        if let Some(v) = fields.get(&CheetahString::from_slice("e")) {
            self.e = v.parse().unwrap();
        }
        if let Some(v) = fields.get(&CheetahString::from_slice("f")) {
            self.f = v.parse().unwrap();
        }
        if let Some(v) = fields.get(&CheetahString::from_slice("g")) {
            self.g = v.parse().unwrap();
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("h")) {
            self.h = v.parse().unwrap();
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("i")) {
            self.i = Some(v.clone());
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("j")) {
            self.j = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("k")) {
            self.k = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("l")) {
            self.l = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("m")) {
            self.m = Some(v.parse().unwrap());
        }

        if let Some(v) = fields.get(&CheetahString::from_slice("n")) {
            self.n = Some(v.clone());
        }
    }

    fn support_fast_codec(&self) -> bool {
        true
    }
}

impl FromMap for SendMessageRequestHeaderV2 {
    type Error = crate::remoting_error::RemotingError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(SendMessageRequestHeaderV2 {
            a: map.get(&CheetahString::from_slice("a")).cloned().ok_or(
                Self::Error::RemotingCommandError("Miss a field".to_string()),
            )?,
            b: map.get(&CheetahString::from_slice("b")).cloned().ok_or(
                Self::Error::RemotingCommandError("Miss b field".to_string()),
            )?,
            c: map.get(&CheetahString::from_slice("c")).cloned().ok_or(
                Self::Error::RemotingCommandError("Miss c field".to_string()),
            )?,
            d: map
                .get(&CheetahString::from_slice("d"))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss d field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::RemotingCommandError("Parse d field error".to_string())
                })?,
            e: map
                .get(&CheetahString::from_slice("e"))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss e field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::RemotingCommandError("Parse e field error".to_string())
                })?,
            f: map
                .get(&CheetahString::from_slice("f"))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss f field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::RemotingCommandError("Parse f field error".to_string())
                })?,
            g: map
                .get(&CheetahString::from_slice("g"))
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss g field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::RemotingCommandError("Parse g field error".to_string())
                })?,
            h: map
                .get(&CheetahString::from_slice("h"))
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss h field".to_string(),
                ))?
                .parse()
                .map_err(|_| {
                    Self::Error::RemotingCommandError("Parse h field error".to_string())
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
                .and_then(|v| v.get_broker_name().cloned()),
            topic_request_header: v1.topic_request_header.clone(),
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

    fn queue_id(&self) -> Option<i32> {
        Some(self.e)
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.e = queue_id.unwrap();
    }
}
