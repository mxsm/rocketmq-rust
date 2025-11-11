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
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
pub struct GetConsumeStatsRequestHeader {
    #[serde(rename = "consumerGroup")]
    pub consumer_group: CheetahString,
    #[serde(rename = "topic")]
    pub topic: CheetahString,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetConsumeStatsRequestHeader {
    // pub const CONSUMER_GROUP: &'static str = "consumerGroup";
    // pub const TOPIC: &'static str = "topic";

    pub fn get_consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }
    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = consumer_group;
    }

    pub fn get_topic(&self) -> &CheetahString {
        &self.topic
    }
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }
}

// impl CommandCustomHeader for GetConsumeStatsRequestHeader {
//     fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
//         let mut map = std::collections::HashMap::new();

//         map.insert(
//             CheetahString::from_static_str(Self::CONSUMER_GROUP),
//             self.consumer_group.clone(),
//         );
//         map.insert(
//             CheetahString::from_static_str(Self::TOPIC),
//             self.topic.clone(),
//         );
//         if let Some(value) = self.topic_request_header.as_ref() {
//             if let Some(value) = value.to_map() {
//                 map.extend(value);
//             }
//         }
//         Some(map)
//     }
// }

// impl FromMap for GetConsumeStatsRequestHeader {
//     type Error = rocketmq_error::RocketmqError;

//     type Target = Self;

//     fn from(
//         map: &std::collections::HashMap<CheetahString, CheetahString>,
//     ) -> Result<Self::Target, Self::Error> {
//         Ok(GetConsumeStatsRequestHeader {
//             consumer_group: map
//                 .get(&CheetahString::from_static_str(Self::CONSUMER_GROUP))
//                 .cloned()
//                 .unwrap_or_default(),
//             topic: map
//                 .get(&CheetahString::from_static_str(Self::TOPIC))
//                 .cloned()
//                 .unwrap_or_default(),
//             topic_request_header: Some(<TopicRequestHeader as FromMap>::from(map)?),
//         })
//     }
//}
