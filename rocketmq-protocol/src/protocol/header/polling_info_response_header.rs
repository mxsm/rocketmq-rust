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

use rocketmq_macros::RequestHeaderCodecV3;
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::polling_info_response_header::PollingInfoResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.PollingInfoResponseHeader"
)]
pub struct PollingInfoResponseHeader {
    #[header(required)]
    pub polling_num: i32,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};

    #[test]
    fn serde_uses_the_java_field_name_and_requires_the_value() {
        let header = PollingInfoResponseHeader { polling_num: -50 };
        let json = serde_json::to_string(&header).unwrap();

        assert_eq!(json, r#"{"pollingNum":-50}"#);
        assert_eq!(
            serde_json::from_str::<PollingInfoResponseHeader>(&json).unwrap(),
            header
        );
        assert!(serde_json::from_str::<PollingInfoResponseHeader>("{}").is_err());
        assert!(serde_json::from_str::<PollingInfoResponseHeader>(r#"{"pollingNum":"invalid"}"#).is_err());
    }

    #[test]
    fn v3_codec_round_trips_signed_boundaries_and_requires_the_value() {
        for polling_num in [i32::MIN, 0, i32::MAX] {
            let header = PollingInfoResponseHeader { polling_num };
            let map = header.to_map().unwrap();
            let decoded = <PollingInfoResponseHeader as FromMap>::from(&map).unwrap();

            assert_eq!(decoded, header);
        }

        assert!(<PollingInfoResponseHeader as FromMap>::from(&HashMap::new()).is_err());
    }
}
