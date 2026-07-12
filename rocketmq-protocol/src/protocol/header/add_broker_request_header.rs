// Copyright 2026 The RocketMQ Rust Authors
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct AddBrokerRequestHeader {
    pub config_path: Option<CheetahString>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn add_broker_request_header_serializes_config_path() {
        let header = AddBrokerRequestHeader {
            config_path: Some(CheetahString::from_static_str("/tmp/broker.conf")),
        };

        let map = header.to_map().expect("header should encode");
        assert_eq!(
            map.get(&CheetahString::from_static_str("configPath")),
            Some(&CheetahString::from_static_str("/tmp/broker.conf"))
        );
    }

    #[test]
    fn add_broker_request_header_deserializes_config_path() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("configPath"),
            CheetahString::from_static_str("/tmp/broker.conf"),
        );

        let header = <AddBrokerRequestHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(
            header.config_path,
            Some(CheetahString::from_static_str("/tmp/broker.conf"))
        );
    }
}
