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

/// Generation bound to the exact Broker configuration response body.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetBrokerConfigResponseHeader {
    pub version: Option<CheetahString>,
    pub config_generation: u64,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::GetBrokerConfigResponseHeader;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn config_generation_round_trips_through_wire_map() {
        let header = GetBrokerConfigResponseHeader {
            version: Some("{\"stateVersion\":0,\"timestamp\":42,\"counter\":43}".into()),
            config_generation: 43,
        };
        let map = header.to_map().expect("header should encode");
        assert_eq!(map.get("configGeneration").map(CheetahString::as_str), Some("43"));
        assert_eq!(
            map.get("version").map(CheetahString::as_str),
            Some("{\"stateVersion\":0,\"timestamp\":42,\"counter\":43}")
        );

        let decoded = <GetBrokerConfigResponseHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(decoded, header);
    }
}
