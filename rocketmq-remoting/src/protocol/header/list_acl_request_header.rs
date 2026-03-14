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

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ListAclRequestHeader {
    pub subject_filter: CheetahString,
    pub resource_filter: CheetahString,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cheetah_string::CheetahString;

    #[test]
    fn list_acl_request_header_default() {
        let header: ListAclRequestHeader = Default::default();
        assert_eq!(header.subject_filter, CheetahString::default());
        assert_eq!(header.resource_filter, CheetahString::default());
    }

    #[test]
    fn list_acl_request_header_serialize() {
        let header = ListAclRequestHeader {
            subject_filter: CheetahString::from("subjectFilterValue"),
            resource_filter: CheetahString::from("resourceFilterValue"),
        };

        let json_str = serde_json::to_string(&header).unwrap();

        // Verify the serialized JSON contains expected fields with camelCase naming
        assert!(json_str.contains("\"subjectFilter\":\"subjectFilterValue\""));
        assert!(json_str.contains("\"resourceFilter\":\"resourceFilterValue\""));
    }

    #[test]
    fn list_acl_request_header_deserialize() {
        let json_str = r#"{
            "subjectFilter": "subjectFilterValue",
            "resourceFilter": "resourceFilterValue"
        }"#;

        let header: ListAclRequestHeader = serde_json::from_str(json_str).unwrap();

        assert_eq!(header.subject_filter, CheetahString::from("subjectFilterValue"));
        assert_eq!(header.resource_filter, CheetahString::from("resourceFilterValue"));
    }

    #[test]
    fn list_acl_request_header_clone() {
        let header = ListAclRequestHeader {
            subject_filter: CheetahString::from("subjectFilterValue"),
            resource_filter: CheetahString::from("resourceFilterValue"),
        };

        let cloned_header = header.clone();

        assert_eq!(header.subject_filter, cloned_header.subject_filter);
        assert_eq!(header.resource_filter, cloned_header.resource_filter);
    }
}
