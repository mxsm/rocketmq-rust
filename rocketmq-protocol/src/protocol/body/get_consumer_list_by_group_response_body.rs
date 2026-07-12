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
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerListByGroupResponseBody {
    pub consumer_id_list: Vec<CheetahString>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_consumer_list_by_group_response_body_serialization() {
        let body = GetConsumerListByGroupResponseBody {
            consumer_id_list: vec![CheetahString::from("id1"), CheetahString::from("id2")],
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"consumerIdList\":[\"id1\",\"id2\"]"));
    }

    #[test]
    fn get_consumer_list_by_group_response_body_deserialization() {
        let json = r#"{"consumerIdList":["id1","id2"]}"#;
        let body: GetConsumerListByGroupResponseBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.consumer_id_list.len(), 2);
        assert_eq!(body.consumer_id_list[0], "id1");
        assert_eq!(body.consumer_id_list[1], "id2");
    }
}
