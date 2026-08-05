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
    pub fn new(add_topic_count: i32) -> Self {
        Self { add_topic_count }
    }

    pub fn get_add_topic_count(&self) -> i32 {
        self.add_topic_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wipe_write_perm_of_broker_request_header_new() {
        let header = WipeWritePermOfBrokerRequestHeader::new("broker1");
        assert_eq!(header.broker_name, CheetahString::from("broker1"));
    }

    #[test]
    fn wipe_write_perm_of_broker_response_header_new_and_getters() {
        let header = WipeWritePermOfBrokerResponseHeader::new(10);
        assert_eq!(header.get_wipe_topic_count(), 10);
    }

    #[test]
    fn add_write_perm_of_broker_request_header_new() {
        let header = AddWritePermOfBrokerRequestHeader::new("broker1");
        assert_eq!(header.broker_name, CheetahString::from("broker1"));
    }

    #[test]
    fn add_write_perm_of_broker_response_header_new_and_getters() {
        let header = AddWritePermOfBrokerResponseHeader::new(20);
        assert_eq!(header.get_add_topic_count(), 20);
    }
}
