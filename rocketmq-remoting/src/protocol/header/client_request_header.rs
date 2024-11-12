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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct GetRouteInfoRequestHeader {
    pub topic: CheetahString,

    #[serde(rename = "acceptStandardJsonOnly")]
    pub accept_standard_json_only: Option<bool>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetRouteInfoRequestHeader {
    const ACCEPT_STANDARD_JSON_ONLY: &'static str = "acceptStandardJsonOnly";
    const TOPIC: &'static str = "topic";

    pub fn new(topic: impl Into<CheetahString>, accept_standard_json_only: Option<bool>) -> Self {
        GetRouteInfoRequestHeader {
            topic: topic.into(),
            accept_standard_json_only,
            ..Default::default()
        }
    }
}

impl FromMap for GetRouteInfoRequestHeader {
    type Target = GetRouteInfoRequestHeader;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(GetRouteInfoRequestHeader {
            topic: map
                .get(&CheetahString::from_static_str(
                    GetRouteInfoRequestHeader::TOPIC,
                ))
                .cloned()
                .unwrap_or_default(),
            accept_standard_json_only: map
                .get(&CheetahString::from_static_str(
                    GetRouteInfoRequestHeader::ACCEPT_STANDARD_JSON_ONLY,
                ))
                .and_then(|s| s.parse::<bool>().ok()),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}

impl CommandCustomHeader for GetRouteInfoRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::with_capacity(2);
        map.insert(
            CheetahString::from_static_str(Self::TOPIC),
            self.topic.clone(),
        );
        match self.accept_standard_json_only {
            None => {
                map.insert(
                    CheetahString::from_slice("acceptStandardJsonOnly"),
                    CheetahString::from_slice("false"),
                );
            }
            Some(val) => {
                map.insert(
                    CheetahString::from_slice("acceptStandardJsonOnly"),
                    CheetahString::from_slice(if val { "true" } else { "false" }),
                );
            }
        }
        if let Some(topic_request_header) = &self.topic_request_header {
            if let Some(topic_request_header_map) = topic_request_header.to_map() {
                map.extend(topic_request_header_map);
            }
        }
        Some(map)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::GetRouteInfoRequestHeader;
    use crate::protocol::command_custom_header::CommandCustomHeader;

    #[test]
    fn test_to_map_no_accept_standard_json_only() {
        let request_header = GetRouteInfoRequestHeader {
            topic: "test".into(),
            accept_standard_json_only: None,
            topic_request_header: None,
        };

        let result: Option<HashMap<CheetahString, CheetahString>> = request_header.to_map();
        assert_eq!(
            result,
            Some(HashMap::from([
                (CheetahString::from("topic"), CheetahString::from("test")),
                (
                    CheetahString::from("acceptStandardJsonOnly"),
                    CheetahString::from("false")
                )
            ]))
        );
    }
}
