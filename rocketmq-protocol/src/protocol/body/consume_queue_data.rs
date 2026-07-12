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

use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeQueueData {
    pub physic_offset: i64,
    pub physic_size: i32,
    pub tags_code: i64,
    pub extend_data_json: Option<CheetahString>,
    pub bit_map: Option<CheetahString>,
    pub eval: bool,
    pub msg: Option<CheetahString>,
}

impl Display for ConsumeQueueData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConsumeQueueData [physic_offset={}, physic_size={}, tags_code={}, extend_data_json={:?}, bit_map={:?}, \
             eval={}, msg={:?}]",
            self.physic_offset,
            self.physic_size,
            self.tags_code,
            self.extend_data_json,
            self.bit_map,
            self.eval,
            self.msg
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn consume_queue_data_display_format() {
        let data = ConsumeQueueData {
            physic_offset: 123,
            physic_size: 456,
            tags_code: 789,
            extend_data_json: Some(CheetahString::from("extend_data")),
            bit_map: Some(CheetahString::from("bit_map")),
            eval: true,
            msg: Some(CheetahString::from("message")),
        };
        let display = format!("{}", data);
        assert_eq!(
            display,
            "ConsumeQueueData [physic_offset=123, physic_size=456, tags_code=789, \
             extend_data_json=Some(\"extend_data\"), bit_map=Some(\"bit_map\"), eval=true, msg=Some(\"message\")]"
        );
    }

    #[test]
    fn consume_queue_data_default_values() {
        let data: ConsumeQueueData = Default::default();
        assert_eq!(data.physic_offset, 0);
        assert_eq!(data.physic_size, 0);
        assert_eq!(data.tags_code, 0);
        assert!(data.extend_data_json.is_none());
        assert!(data.bit_map.is_none());
        assert!(!data.eval);
        assert!(data.msg.is_none());
    }

    #[test]
    fn consume_queue_data_serialization() {
        let data = ConsumeQueueData {
            physic_offset: 123,
            physic_size: 456,
            tags_code: 789,
            extend_data_json: Some(CheetahString::from("extend_data")),
            bit_map: Some(CheetahString::from("bit_map")),
            eval: true,
            msg: Some(CheetahString::from("message")),
        };
        let serialized = serde_json::to_string(&data).unwrap();
        assert_eq!(
            serialized,
            r#"{"physicOffset":123,"physicSize":456,"tagsCode":789,"extendDataJson":"extend_data","bitMap":"bit_map","eval":true,"msg":"message"}"#
        );
    }

    #[test]
    fn consume_queue_data_deserialization() {
        let json = r#"{"physicOffset":123,"physicSize":456,"tagsCode":789,"extendDataJson":"extend_data","bitMap":"bit_map","eval":true,"msg":"message"}"#;
        let deserialized: ConsumeQueueData = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.physic_offset, 123);
        assert_eq!(deserialized.physic_size, 456);
        assert_eq!(deserialized.tags_code, 789);
        assert_eq!(
            deserialized.extend_data_json.unwrap(),
            CheetahString::from("extend_data")
        );
        assert_eq!(deserialized.bit_map.unwrap(), CheetahString::from("bit_map"));
        assert!(deserialized.eval);
        assert_eq!(deserialized.msg.unwrap(), CheetahString::from("message"));
    }

    #[test]
    fn consume_queue_data_deserialization_missing_fields() {
        let json = r#"{"physicOffset":123,"physicSize":456,"tagsCode":789,"eval":false}"#;
        let deserialized: ConsumeQueueData = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.physic_offset, 123);
        assert_eq!(deserialized.physic_size, 456);
        assert_eq!(deserialized.tags_code, 789);
        assert!(deserialized.extend_data_json.is_none());
        assert!(deserialized.bit_map.is_none());
        assert!(!deserialized.eval);
        assert!(deserialized.msg.is_none());
    }
}
