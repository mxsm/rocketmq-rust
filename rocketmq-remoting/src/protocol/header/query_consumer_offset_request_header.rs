//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumerOffsetRequestHeader {
    pub consumer_group: CheetahString,
    pub topic: CheetahString,
    pub queue_id: i32,

    pub set_zero_if_not_found: Option<bool>,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl TopicRequestHeaderTrait for QueryConsumerOffsetRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request_header.as_ref().unwrap().lo
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    fn topic(&self) -> &CheetahString {
        &self.topic
    }

    fn broker_name(&self) -> Option<&CheetahString> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .broker_name
            .as_ref()
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .namespace
            .as_deref()
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .namespace = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .namespaced
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .namespaced = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .oneway
    }

    fn set_oneway(&mut self, oneway: bool) {
        self.topic_request_header
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
