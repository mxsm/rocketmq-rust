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
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader"
)]
pub struct GetMinOffsetResponseHeader {
    #[header(required)]
    pub offset: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};

    #[test]
    fn serde_preserves_the_offset_and_requires_a_number() {
        let header = GetMinOffsetResponseHeader { offset: -1 };
        let json = serde_json::to_string(&header).unwrap();

        assert_eq!(json, r#"{"offset":-1}"#);
        assert_eq!(
            serde_json::from_str::<GetMinOffsetResponseHeader>(&json)
                .unwrap()
                .offset,
            -1
        );
        assert!(serde_json::from_str::<GetMinOffsetResponseHeader>("{}").is_err());
        assert!(serde_json::from_str::<GetMinOffsetResponseHeader>(r#"{"offset":"invalid"}"#).is_err());
    }

    #[test]
    fn v3_codec_round_trips_signed_boundaries_and_rejects_missing_or_invalid_values() {
        for offset in [i64::MIN, 0, i64::MAX] {
            let map = GetMinOffsetResponseHeader { offset }.to_map().unwrap();
            let decoded = <GetMinOffsetResponseHeader as FromMap>::from(&map).unwrap();

            assert_eq!(decoded.offset, offset);
        }

        assert!(<GetMinOffsetResponseHeader as FromMap>::from(&HashMap::new()).is_err());

        let invalid = HashMap::from([("offset".into(), "invalid".into())]);
        assert!(<GetMinOffsetResponseHeader as FromMap>::from(&invalid).is_err());
    }
}
