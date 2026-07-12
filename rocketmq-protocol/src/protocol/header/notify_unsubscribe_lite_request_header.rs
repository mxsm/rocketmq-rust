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

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct NotifyUnsubscribeLiteRequestHeader {
    #[required]
    pub lite_topic: CheetahString,

    #[required]
    pub consumer_group: CheetahString,

    #[required]
    pub client_id: CheetahString,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn notify_unsubscribe_lite_request_header_round_trips_ext_fields() {
        let header = NotifyUnsubscribeLiteRequestHeader {
            lite_topic: CheetahString::from_static_str("lite-topic"),
            consumer_group: CheetahString::from_static_str("consumer-group"),
            client_id: CheetahString::from_static_str("client-id"),
            rpc_request_header: None,
        };

        let map = header.to_map().expect("header should encode to ext fields");
        assert_eq!(
            map.get(&CheetahString::from_static_str("liteTopic")),
            Some(&CheetahString::from_static_str("lite-topic"))
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("consumerGroup")),
            Some(&CheetahString::from_static_str("consumer-group"))
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("clientId")),
            Some(&CheetahString::from_static_str("client-id"))
        );

        let decoded =
            <NotifyUnsubscribeLiteRequestHeader as FromMap>::from(&map).expect("header should decode from ext fields");
        assert_eq!(decoded.lite_topic, "lite-topic");
        assert_eq!(decoded.consumer_group, "consumer-group");
        assert_eq!(decoded.client_id, "client-id");
    }

    #[test]
    fn notify_unsubscribe_lite_request_header_requires_java_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("liteTopic"),
            CheetahString::from_static_str("lite-topic"),
        );
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("consumer-group"),
        );

        assert!(
            <NotifyUnsubscribeLiteRequestHeader as FromMap>::from(&map).is_err(),
            "clientId is required by the Java header contract"
        );
    }
}
