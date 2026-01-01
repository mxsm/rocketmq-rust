use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
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
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeHaInfoResponseHeader {
    pub master_ha_address: Option<CheetahString>,
    pub master_flush_offset: Option<i64>,
    pub master_address: Option<CheetahString>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    fn create_cheetah_string(value: &str) -> Option<CheetahString> {
        Some(CheetahString::from(value))
    }

    #[test]
    fn serialize_with_all_fields_set() {
        let header = ExchangeHaInfoResponseHeader {
            master_ha_address: create_cheetah_string("127.0.0.1:10911"),
            master_flush_offset: Some(1024),
            master_address: create_cheetah_string("127.0.0.1"),
        };

        let serialized = serde_json::to_string(&header).unwrap();
        assert!(serialized.contains("\"masterHaAddress\":\"127.0.0.1:10911\""));
        assert!(serialized.contains("\"masterFlushOffset\":1024"));
        assert!(serialized.contains("\"masterAddress\":\"127.0.0.1\""));
    }
}
