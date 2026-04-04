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

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ResumeCheckHalfMessageRequestHeader {
    #[required]
    pub topic: CheetahString,

    pub msg_id: Option<CheetahString>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::ResumeCheckHalfMessageRequestHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn resume_check_half_message_request_header_deserializes_from_map() {
        let mut fields = HashMap::new();
        fields.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("half-topic"),
        );
        fields.insert(
            CheetahString::from_static_str("msgId"),
            CheetahString::from_static_str("msg-id"),
        );

        let header = <ResumeCheckHalfMessageRequestHeader as FromMap>::from(&fields)
            .expect("header should decode from ext fields");

        assert_eq!(header.topic, CheetahString::from_static_str("half-topic"));
        assert_eq!(header.msg_id, Some(CheetahString::from_static_str("msg-id")));
    }
}
