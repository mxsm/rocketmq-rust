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

use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QuerySubscriptionResponseBody {
    pub subscription_data: Option<SubscriptionData>,
    pub group: CheetahString,
    pub topic: CheetahString,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_subscription_response_body_serializes_camel_case_fields() {
        let body = QuerySubscriptionResponseBody {
            subscription_data: Some(SubscriptionData {
                topic: CheetahString::from_static_str("topic-a"),
                sub_string: CheetahString::from_static_str("*"),
                ..Default::default()
            }),
            group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("topic-a"),
        };

        let json = serde_json::to_string(&body).expect("serialize query subscription response body");
        assert!(json.contains("\"subscriptionData\""));
        assert!(json.contains("\"group\":\"group-a\""));
        assert!(json.contains("\"topic\":\"topic-a\""));
    }

    #[test]
    fn query_subscription_response_body_allows_missing_subscription_data() {
        let json = r#"{"group":"group-a","topic":"topic-a","subscriptionData":null}"#;
        let body: QuerySubscriptionResponseBody =
            serde_json::from_str(json).expect("deserialize query subscription response body");

        assert!(body.subscription_data.is_none());
        assert_eq!(body.group, "group-a");
        assert_eq!(body.topic, "topic-a");
    }
}
