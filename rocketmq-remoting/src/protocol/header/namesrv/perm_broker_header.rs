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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct WipeWritePermOfBrokerRequestHeader {
    #[required]
    pub broker_name: CheetahString,
}

impl WipeWritePermOfBrokerRequestHeader {
    pub fn new(broker_name: impl Into<CheetahString>) -> Self {
        Self {
            broker_name: broker_name.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct WipeWritePermOfBrokerResponseHeader {
    pub wipe_topic_count: i32,
}

impl WipeWritePermOfBrokerResponseHeader {
    pub fn new(wipe_topic_count: i32) -> Self {
        Self { wipe_topic_count }
    }

    pub fn get_wipe_topic_count(&self) -> i32 {
        self.wipe_topic_count
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct AddWritePermOfBrokerRequestHeader {
    pub broker_name: CheetahString,
}

impl AddWritePermOfBrokerRequestHeader {
    pub fn new(broker_name: impl Into<CheetahString>) -> Self {
        Self {
            broker_name: broker_name.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct AddWritePermOfBrokerResponseHeader {
    pub add_topic_count: i32,
}

impl AddWritePermOfBrokerResponseHeader {
    //const ADD_TOPIC_COUNT: &'static str = "addTopicCount";

    pub fn new(add_topic_count: i32) -> Self {
        Self { add_topic_count }
    }
}

impl AddWritePermOfBrokerResponseHeader {
    pub fn get_add_topic_count(&self) -> i32 {
        self.add_topic_count
    }
}

// impl CommandCustomHeader for AddWritePermOfBrokerResponseHeader {
//     fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
//         Some(HashMap::from([(
//             CheetahString::from_static_str(Self::ADD_TOPIC_COUNT),
//             CheetahString::from_string(self.add_topic_count.to_string()),
//         )]))
//     }
// }

// impl FromMap for AddWritePermOfBrokerResponseHeader {
//     type Error = rocketmq_error::RocketmqError;

//     type Target = Self;

//     fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
//         Ok(AddWritePermOfBrokerResponseHeader {
//             add_topic_count: map
//                 .get(&CheetahString::from_static_str(
//                     AddWritePermOfBrokerResponseHeader::ADD_TOPIC_COUNT,
//                 ))
//                 .and_then(|s| s.parse::<i32>().ok())
//                 .unwrap_or(0),
//         })
//     }
// }
