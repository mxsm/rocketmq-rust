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

use crate::protocol::body::consume_queue_data::ConsumeQueueData;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumeQueueResponseBody {
    pub subscription_data: Option<SubscriptionData>,
    pub filter_data: Option<CheetahString>,
    pub queue_data: Option<Vec<ConsumeQueueData>>,
    pub max_queue_index: i64,
    pub min_queue_index: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn populated_response_preserves_java_field_names_and_signed_indexes() {
        let body = QueryConsumeQueueResponseBody {
            subscription_data: Some(SubscriptionData::default()),
            filter_data: Some("filter expression".into()),
            queue_data: Some(vec![ConsumeQueueData::default()]),
            max_queue_index: i64::MAX,
            min_queue_index: -1,
        };
        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "subscriptionData": body.subscription_data,
                "filterData": "filter expression",
                "queueData": body.queue_data,
                "maxQueueIndex": i64::MAX,
                "minQueueIndex": -1
            })
        );
        let decoded: QueryConsumeQueueResponseBody = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(decoded.max_queue_index, i64::MAX);
        assert_eq!(decoded.min_queue_index, -1);
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }

    #[test]
    fn null_optionals_remain_distinct_from_an_empty_queue_array() {
        let value = serde_json::json!({
            "subscriptionData": null, "filterData": null, "queueData": null,
            "maxQueueIndex": 0, "minQueueIndex": 0
        });
        assert_eq!(
            serde_json::to_value(QueryConsumeQueueResponseBody::default()).unwrap(),
            value
        );
        let mut decoded: QueryConsumeQueueResponseBody = serde_json::from_value(value).unwrap();
        assert!(decoded.subscription_data.is_none());
        assert!(decoded.filter_data.is_none());
        assert!(decoded.queue_data.is_none());
        decoded.queue_data = Some(Vec::new());
        let value = serde_json::to_value(decoded).unwrap();
        assert_eq!(value["queueData"], serde_json::json!([]));
        let decoded: QueryConsumeQueueResponseBody = serde_json::from_value(value).unwrap();
        assert!(decoded.queue_data.unwrap().is_empty());
    }
}
