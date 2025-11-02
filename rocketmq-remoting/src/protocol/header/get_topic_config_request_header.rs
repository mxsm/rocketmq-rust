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
use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodec)]
pub struct GetTopicConfigRequestHeader {
    #[required]
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetTopicConfigRequestHeader {
    pub fn get_topic(&self) -> &CheetahString {
        &self.topic
    }
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }
}

