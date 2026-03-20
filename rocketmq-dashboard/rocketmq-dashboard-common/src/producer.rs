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

//! Shared Producer-domain request models for dashboard implementations.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProducerConnectionQueryRequest {
    pub topic: String,
    pub producer_group: String,
}

#[cfg(test)]
mod tests {
    use super::ProducerConnectionQueryRequest;

    #[test]
    fn producer_connection_query_request_uses_expected_field_names() {
        let request = ProducerConnectionQueryRequest {
            topic: "TopicTest".to_string(),
            producer_group: "producer_group".to_string(),
        };
        let json = serde_json::to_string(&request).expect("serialize producer request");

        assert!(json.contains("\"topic\""));
        assert!(json.contains("\"producerGroup\""));
    }
}
