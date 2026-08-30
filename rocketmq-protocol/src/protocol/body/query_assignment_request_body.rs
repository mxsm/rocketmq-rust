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

use crate::protocol::heartbeat::message_model::MessageModel;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryAssignmentRequestBody {
    pub topic: CheetahString,
    pub consumer_group: CheetahString,
    pub client_id: CheetahString,
    pub strategy_name: CheetahString,
    pub message_model: MessageModel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_contract_uses_java_field_names_and_round_trips_all_fields() {
        let body = QueryAssignmentRequestBody {
            topic: CheetahString::from_static_str("topic-a"),
            consumer_group: CheetahString::from_static_str("group-a"),
            client_id: CheetahString::from_static_str("client-a"),
            strategy_name: CheetahString::from_static_str("AVG"),
            message_model: MessageModel::Broadcasting,
        };

        let value = serde_json::to_value(&body).expect("serialize query assignment request");
        assert_eq!(
            value,
            serde_json::json!({
                "topic": "topic-a",
                "consumerGroup": "group-a",
                "clientId": "client-a",
                "strategyName": "AVG",
                "messageModel": "BROADCASTING"
            })
        );

        let decoded: QueryAssignmentRequestBody =
            serde_json::from_value(value).expect("deserialize query assignment request");
        assert_eq!(decoded.topic, body.topic);
        assert_eq!(decoded.consumer_group, body.consumer_group);
        assert_eq!(decoded.client_id, body.client_id);
        assert_eq!(decoded.strategy_name, body.strategy_name);
        assert_eq!(decoded.message_model, body.message_model);
    }

    #[test]
    fn serde_contract_rejects_unknown_message_model() {
        let value = serde_json::json!({
            "topic": "topic-a",
            "consumerGroup": "group-a",
            "clientId": "client-a",
            "strategyName": "AVG",
            "messageModel": "UNKNOWN"
        });

        assert!(serde_json::from_value::<QueryAssignmentRequestBody>(value).is_err());
    }
}
