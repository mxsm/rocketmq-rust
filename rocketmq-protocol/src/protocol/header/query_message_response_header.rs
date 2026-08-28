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

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV3, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::query_message_response_header::QueryMessageResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader"
)]
pub struct QueryMessageResponseHeader {
    #[header(required)]
    pub index_last_update_timestamp: i64,
    #[header(required)]
    pub index_last_update_phyoffset: i64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};
    use crate::protocol::remoting_command::RemotingCommand;

    #[test]
    fn serde_uses_java_field_names_and_requires_both_values() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 131_415,
            index_last_update_phyoffset: 131_496,
        };
        let json = serde_json::to_string(&header).unwrap();

        assert_eq!(
            json,
            r#"{"indexLastUpdateTimestamp":131415,"indexLastUpdatePhyoffset":131496}"#
        );
        assert_eq!(
            serde_json::from_str::<QueryMessageResponseHeader>(&json).unwrap(),
            header
        );
        assert!(serde_json::from_str::<QueryMessageResponseHeader>("{}").is_err());
        assert!(serde_json::from_str::<QueryMessageResponseHeader>(
            r#"{"indexLastUpdateTimestamp":1,"indexLastUpdatePhyoffset":"invalid"}"#
        )
        .is_err());
    }

    #[test]
    fn v3_codec_round_trips_signed_boundaries_and_requires_both_values() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: i64::MIN,
            index_last_update_phyoffset: i64::MAX,
        };
        let map = header.to_map().unwrap();
        let decoded = <QueryMessageResponseHeader as FromMap>::from(&map).unwrap();

        assert_eq!(decoded, header);
        assert!(<QueryMessageResponseHeader as FromMap>::from(&HashMap::new()).is_err());
    }

    #[test]
    fn remoting_command_decodes_the_same_header_with_both_codec_paths() {
        let header = QueryMessageResponseHeader {
            index_last_update_timestamp: 123_456_789,
            index_last_update_phyoffset: 987_654_321,
        };
        let mut command = RemotingCommand::create_success_response_command_with_header(header);
        command.make_custom_header_to_net();

        let normal: QueryMessageResponseHeader = command.decode_command_custom_header().unwrap();
        let fast: QueryMessageResponseHeader = command.decode_command_custom_header_fast().unwrap();

        assert_eq!(normal, fast);
        assert_eq!(normal.index_last_update_timestamp, 123_456_789);
        assert_eq!(normal.index_last_update_phyoffset, 987_654_321);
    }
}
