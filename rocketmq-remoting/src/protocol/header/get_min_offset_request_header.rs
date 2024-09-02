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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetMinOffsetRequestHeader {
    pub topic: String,

    pub queue_id: i32,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetMinOffsetRequestHeader {
    pub const TOPIC: &'static str = "topic";
    pub const QUEUE_ID: &'static str = "queueId";
}

impl CommandCustomHeader for GetMinOffsetRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::new();
        map.insert(Self::TOPIC.to_string(), self.topic.clone());
        map.insert(Self::QUEUE_ID.to_string(), self.queue_id.to_string());
        if let Some(topic_request_header) = &self.topic_request_header {
            if let Some(topic_request_header_map) = topic_request_header.to_map() {
                map.extend(topic_request_header_map);
            }
        }
        Some(map)
    }
}

impl FromMap for GetMinOffsetRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(GetMinOffsetRequestHeader {
            topic: map
                .get(GetMinOffsetRequestHeader::TOPIC)
                .map(|s| s.to_string())
                .unwrap_or_default(),
            queue_id: map
                .get(GetMinOffsetRequestHeader::QUEUE_ID)
                .map(|s| s.parse().unwrap())
                .unwrap_or_default(),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}
