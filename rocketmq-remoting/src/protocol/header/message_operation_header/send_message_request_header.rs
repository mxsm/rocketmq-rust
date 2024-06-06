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
use rocketmq_macros::{RemotingSerializable, RequestHeaderCodec};
use serde::{Deserialize, Serialize};

use crate::{
    code::request_code::RequestCode,
    protocol::{
        header::message_operation_header::TopicRequestHeaderTrait,
        remoting_command::RemotingCommand,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, RemotingSerializable, RequestHeaderCodec)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequestHeader {
    // the namespace name
    pub ns: Option<String>,
    // if the data has been namespaced
    pub nsd: Option<bool>,
    // the abstract remote addr name, usually the physical broker name
    pub bname: Option<String>,
    // oneway
    pub oway: Option<bool>,

    pub lo: Option<bool>,

    pub producer_group: String,
    pub topic: String,
    pub default_topic: String,
    pub default_topic_queue_nums: i32,
    pub queue_id: Option<i32>,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: Option<String>,
    pub reconsume_times: Option<i32>,
    pub unit_mode: Option<bool>,
    pub batch: Option<bool>,
    pub max_reconsume_times: Option<i32>,
}

impl SendMessageRequestHeader {
    pub fn new(
        producer_group: String,
        topic: String,
        default_topic: String,
        default_topic_queue_nums: i32,
        queue_id: Option<i32>,
        sys_flag: i32,
        born_timestamp: i64,
        flag: i32,
    ) -> Self {
        SendMessageRequestHeader {
            ns: None,
            nsd: None,
            bname: None,
            oway: None,
            lo: None,
            producer_group,
            topic,
            default_topic,
            default_topic_queue_nums,
            queue_id,
            sys_flag,
            born_timestamp,
            flag,
            properties: None,
            reconsume_times: None,
            unit_mode: None,
            batch: None,
            max_reconsume_times: None,
        }
    }
}

impl TopicRequestHeaderTrait for SendMessageRequestHeader {
    fn with_lo(&mut self, lo: Option<bool>) {
        self.lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.lo
    }

    fn with_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    fn topic(&self) -> String {
        self.topic.clone()
    }

    fn broker_name(&self) -> Option<String> {
        self.bname.clone()
    }

    fn with_broker_name(&mut self, broker_name: String) {
        self.bname = Some(broker_name);
    }

    fn namespace(&self) -> Option<String> {
        self.ns.clone()
    }

    fn with_namespace(&mut self, namespace: String) {
        self.ns = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.nsd
    }

    fn with_namespaced(&mut self, namespaced: bool) {
        self.nsd = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.oway
    }

    fn with_oneway(&mut self, oneway: bool) {
        self.oway = Some(oneway);
    }

    fn queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.queue_id = queue_id;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, RemotingSerializable, RequestHeaderCodec)]
#[serde(rename_all = "camelCase")]
pub struct SendMessageRequestHeaderV2 {
    // the namespace name
    pub ns: Option<String>,
    // if the data has been namespaced
    pub nsd: Option<bool>,
    // the abstract remote addr name, usually the physical broker name
    pub bname: Option<String>,
    // oneway
    pub oway: Option<bool>,

    pub lo: Option<bool>,

    // producerGroup
    pub a: String,

    // topic
    pub b: String,

    // defaultTopic
    pub c: String,

    // defaultTopicQueueNums
    pub d: i32,

    // queueId
    pub e: Option<i32>,

    //sysFlag
    pub f: i32,

    // bornTimestamp
    pub g: i64,

    // flag
    pub h: i32,

    // properties
    pub i: Option<String>,

    // reconsumeTimes
    pub j: Option<i32>,

    // unitMode
    pub k: Option<bool>,

    // consumeRetryTimes
    pub l: Option<i32>,

    // batch
    pub m: Option<bool>,

    // brokerName
    pub n: Option<String>,
}

impl SendMessageRequestHeaderV2 {
    pub fn new(
        a: String,
        b: String,
        c: String,
        d: i32,
        e: Option<i32>,
        f: i32,
        g: i64,
        h: i32,
    ) -> Self {
        SendMessageRequestHeaderV2 {
            ns: None,
            nsd: None,
            bname: None,
            oway: None,
            lo: None,
            a,
            b,
            c,
            d,
            e,
            f,
            g,
            h,
            i: None,
            j: None,
            k: None,
            l: None,
            m: None,
            n: None,
        }
    }

    pub fn create_send_message_request_header_v1(&self) -> SendMessageRequestHeader {
        SendMessageRequestHeader {
            ns: self.ns.as_ref().cloned(),
            nsd: self.nsd.as_ref().cloned(),
            bname: self.n.as_ref().cloned(),
            oway: self.oway.as_ref().cloned(),
            lo: self.lo.as_ref().cloned(),
            producer_group: self.a.clone(),
            topic: self.b.clone(),
            default_topic: self.c.clone(),
            default_topic_queue_nums: self.d,
            queue_id: self.e,
            sys_flag: self.f,
            born_timestamp: self.g,
            flag: self.h,
            properties: self.i.as_ref().cloned(),
            reconsume_times: self.j,
            unit_mode: self.k,
            batch: self.m,
            max_reconsume_times: self.l,
        }
    }
}

impl TopicRequestHeaderTrait for SendMessageRequestHeaderV2 {
    fn with_lo(&mut self, lo: Option<bool>) {
        self.lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.lo
    }

    fn with_topic(&mut self, topic: String) {
        self.b = topic;
    }

    fn topic(&self) -> String {
        self.b.clone()
    }

    fn broker_name(&self) -> Option<String> {
        self.bname.clone()
    }

    fn with_broker_name(&mut self, broker_name: String) {
        self.bname = Some(broker_name);
    }

    fn namespace(&self) -> Option<String> {
        self.ns.clone()
    }

    fn with_namespace(&mut self, namespace: String) {
        self.ns = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.nsd
    }

    fn with_namespaced(&mut self, namespaced: bool) {
        self.nsd = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.oway
    }

    fn with_oneway(&mut self, oneway: bool) {
        self.oway = Some(oneway);
    }

    fn queue_id(&self) -> Option<i32> {
        self.e
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.e = queue_id;
    }
}

pub fn parse_request_header(request: &RemotingCommand) -> Option<SendMessageRequestHeader> {
    let mut request_header_v2 = None;
    if RequestCode::SendMessageV2.to_i32() == request.code()
        || RequestCode::SendBatchMessage.to_i32() == request.code()
    {
        request_header_v2 = request.decode_command_custom_header::<SendMessageRequestHeaderV2>();
    }

    match request_header_v2 {
        Some(header) => Some(header.create_send_message_request_header_v1()),
        None => request.decode_command_custom_header::<SendMessageRequestHeader>(),
    }
}

#[cfg(test)]
mod tests {
    use RemotingCommand;

    use super::*;

    #[test]
    fn test_send_message_request_header_new() {
        let header = SendMessageRequestHeader::new(
            String::from("group"),
            String::from("topic"),
            String::from("default_topic"),
            1,
            Some(0),
            0,
            0,
            0,
        );
        assert_eq!(header.producer_group, "group");
        assert_eq!(header.topic, "topic");
        assert_eq!(header.default_topic, "default_topic");
        assert_eq!(header.default_topic_queue_nums, 1);
        assert_eq!(header.queue_id, Some(0));
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.born_timestamp, 0);
        assert_eq!(header.flag, 0);
    }

    #[test]
    fn test_send_message_request_header_v2_new() {
        let header_v2 = SendMessageRequestHeaderV2::new(
            String::from("group"),
            String::from("topic"),
            String::from("default_topic"),
            1,
            Some(0),
            0,
            0,
            0,
        );
        assert_eq!(header_v2.a, "group");
        assert_eq!(header_v2.b, "topic");
        assert_eq!(header_v2.c, "default_topic");
        assert_eq!(header_v2.d, 1);
        assert_eq!(header_v2.e, Some(0));
        assert_eq!(header_v2.f, 0);
        assert_eq!(header_v2.g, 0);
        assert_eq!(header_v2.h, 0);
    }

    #[test]
    fn test_send_message_request_header_v2_create_v1() {
        let header_v2 = SendMessageRequestHeaderV2::new(
            String::from("group"),
            String::from("topic"),
            String::from("default_topic"),
            1,
            Some(0),
            0,
            0,
            0,
        );
        let header_v1 = header_v2.create_send_message_request_header_v1();
        assert_eq!(header_v1.producer_group, "group");
        assert_eq!(header_v1.topic, "topic");
        assert_eq!(header_v1.default_topic, "default_topic");
        assert_eq!(header_v1.default_topic_queue_nums, 1);
        assert_eq!(header_v1.queue_id, Some(0));
        assert_eq!(header_v1.sys_flag, 0);
        assert_eq!(header_v1.born_timestamp, 0);
        assert_eq!(header_v1.flag, 0);
    }

    #[test]
    fn test_parse_request_header_v1() {
        let mut request = RemotingCommand::create_response_command();
        request = request.set_code(RequestCode::SendMessage.to_i32());
        let header = parse_request_header(&request);
        assert_eq!(header.is_none(), true);
    }

    #[test]
    fn test_parse_request_header_v2() {
        let mut request = RemotingCommand::create_response_command();
        request = request.set_code(RequestCode::SendMessage.to_i32());
        let header = parse_request_header(&request);
        assert_eq!(header.is_none(), true);
    }
}
