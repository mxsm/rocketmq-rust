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

use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ResetMasterFlushOffsetHeader {
    pub master_flush_offset: Option<i64>,
}

#[cfg(test)]
mod tests {
    use crate::protocol::header::reset_master_flush_offset_header::ResetMasterFlushOffsetHeader;

    #[test]
    fn reset_master_flush_offset_header_serializes_correctly() {
        let header = ResetMasterFlushOffsetHeader {
            master_flush_offset: Some(4231),
        };

        let serialized = serde_json::to_string(&header).unwrap();
        let expected = r#"{"masterFlushOffset":4231}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn reset_master_flush_offset_header_deserializes_correctly() {
        let data = r#"{"masterFlushOffset":9527}"#;
        let header: ResetMasterFlushOffsetHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.master_flush_offset, Some(9527));
    }
}
