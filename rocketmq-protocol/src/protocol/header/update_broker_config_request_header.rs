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

use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

/// Compare-and-set precondition for `UpdateBrokerConfigCas`.
///
/// Legacy `UpdateBrokerConfig` requests do not use this header. Keeping CAS on
/// a distinct request code ensures an older broker rejects the operation
/// instead of ignoring an unknown header and applying it unconditionally.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::update_broker_config_request_header::UpdateBrokerConfigRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct UpdateBrokerConfigRequestHeader {
    #[header(default, default_semantic = "literal:0")]
    pub expected_generation: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::UpdateBrokerConfigRequestHeader;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn expected_generation_round_trips_through_wire_map() {
        let header = UpdateBrokerConfigRequestHeader {
            expected_generation: 42,
        };
        let map = header.to_map().expect("header should encode");
        assert_eq!(map.get("expectedGeneration").map(CheetahString::as_str), Some("42"));

        let decoded = <UpdateBrokerConfigRequestHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(decoded, header);
    }

    #[test]
    fn missing_expected_generation_decodes_to_invalid_zero() {
        let map = HashMap::new();
        let decoded = <UpdateBrokerConfigRequestHeader as FromMap>::from(&map).expect("wire decoder uses the default");
        assert_eq!(decoded.expected_generation, 0);
    }
}
