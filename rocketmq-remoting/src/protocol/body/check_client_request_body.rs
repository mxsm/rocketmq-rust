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

use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CheckClientRequestBody {
    pub client_id: String,
    pub group: String,
    pub subscription_data: SubscriptionData,
    pub namespace: Option<String>,
}

impl CheckClientRequestBody {
    pub fn new(client_id: String, group: String, subscription_data: SubscriptionData) -> Self {
        Self {
            client_id,
            group,
            subscription_data,
            namespace: None,
        }
    }

    pub fn get_client_id(&self) -> &String {
        &self.client_id
    }

    pub fn set_client_id(&mut self, client_id: String) {
        self.client_id = client_id;
    }

    pub fn get_group(&self) -> &String {
        &self.group
    }

    pub fn set_group(&mut self, group: String) {
        self.group = group;
    }

    pub fn get_subscription_data(&self) -> &SubscriptionData {
        &self.subscription_data
    }

    pub fn set_subscription_data(&mut self, subscription_data: SubscriptionData) {
        self.subscription_data = subscription_data;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_mock_subscription() -> SubscriptionData {
        SubscriptionData {
            topic: "test-topic".into(),
            sub_string: "*".into(),
            ..Default::default()
        }
    }

    #[test]
    fn test_new_and_getters() {
        let sub_data = create_mock_subscription();
        let body = CheckClientRequestBody::new(
            "some_client-123".to_string(),
            "some_group-abc".to_string(),
            sub_data.clone(),
        );

        assert_eq!(body.get_client_id(), "some_client-123");
        assert_eq!(body.get_group(), "some_group-abc");
        assert_eq!(body.get_subscription_data().topic, "test-topic");
        assert!(body.namespace.is_none());
    }

    #[test]
    fn test_setters() {
        let mut body = CheckClientRequestBody::new("old_id".into(), "old_group".into(), create_mock_subscription());

        body.set_client_id("new_id".into());
        body.set_group("new_group".into());

        assert_eq!(body.client_id, "new_id");
        assert_eq!(body.group, "new_group");
    }

    #[test]
    fn test_serialization_camel_case() {
        let sub_data = create_mock_subscription();
        let body = CheckClientRequestBody::new("c1".into(), "g1".into(), sub_data);

        let json = serde_json::to_string(&body).unwrap();

        assert!(json.contains("\"clientId\":\"c1\""));
        assert!(json.contains("\"subscriptionData\":"));
    }

    #[test]
    fn test_deserialization() {
        let json = r#"{
            "clientId": "c2",
            "group": "g2",
            "subscriptionData": {
                "classFilterMode": false,
                "topic": "top",
                "subString": "*",
                "tagsSet": [],
                "codeSet": [],
                "subVersion": 1,
                "expressionType": "TAG"
            }
        }"#;

        let decoded: CheckClientRequestBody = serde_json::from_str(json).expect("Failed to decode");

        assert_eq!(decoded.client_id, "c2");
        assert_eq!(decoded.subscription_data.sub_version, 1);
    }
}
