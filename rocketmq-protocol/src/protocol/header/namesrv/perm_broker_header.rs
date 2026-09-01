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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader"
)]
pub struct WipeWritePermOfBrokerRequestHeader {
    #[header(required)]
    pub broker_name: CheetahString,
}

impl WipeWritePermOfBrokerRequestHeader {
    pub fn new(broker_name: impl Into<CheetahString>) -> Self {
        Self {
            broker_name: broker_name.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader"
)]
pub struct WipeWritePermOfBrokerResponseHeader {
    #[header(required)]
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

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader"
)]
pub struct AddWritePermOfBrokerRequestHeader {
    #[header(required)]
    pub broker_name: CheetahString,
}

impl AddWritePermOfBrokerRequestHeader {
    pub fn new(broker_name: impl Into<CheetahString>) -> Self {
        Self {
            broker_name: broker_name.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader"
)]
pub struct AddWritePermOfBrokerResponseHeader {
    #[header(required)]
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
    fn permission_headers_use_java_wire_names() {
        let wipe_request = WipeWritePermOfBrokerRequestHeader::new("broker-a");
        let wipe_response = WipeWritePermOfBrokerResponseHeader::new(3);
        let add_request = AddWritePermOfBrokerRequestHeader::new("broker-b");
        let add_response = AddWritePermOfBrokerResponseHeader::new(5);

        assert_eq!(
            serde_json::to_string(&wipe_request).unwrap(),
            r#"{"brokerName":"broker-a"}"#
        );
        assert_eq!(
            serde_json::to_string(&wipe_response).unwrap(),
            r#"{"wipeTopicCount":3}"#
        );
        assert_eq!(
            serde_json::to_string(&add_request).unwrap(),
            r#"{"brokerName":"broker-b"}"#
        );
        assert_eq!(serde_json::to_string(&add_response).unwrap(), r#"{"addTopicCount":5}"#);

        let wipe_response: WipeWritePermOfBrokerResponseHeader =
            serde_json::from_str(r#"{"wipeTopicCount":3}"#).unwrap();
        let add_response: AddWritePermOfBrokerResponseHeader = serde_json::from_str(r#"{"addTopicCount":5}"#).unwrap();
        assert_eq!(wipe_response.get_wipe_topic_count(), 3);
        assert_eq!(add_response.get_add_topic_count(), 5);
    }
}
