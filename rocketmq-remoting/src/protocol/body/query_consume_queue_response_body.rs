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
    subscription_data: SubscriptionData,
    filter_data: CheetahString,
    queue_data: Vec<ConsumeQueueData>,
    max_queue_index: i64,
    min_queue_index: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_consume_queue_response_body_default_values() {
        let response_body: QueryConsumeQueueResponseBody = Default::default();
        //assert_eq!(response_body.subscription_data, SubscriptionData::default());
        assert_eq!(response_body.filter_data, "");
        assert!(response_body.queue_data.is_empty());
        assert_eq!(response_body.max_queue_index, 0);
        assert_eq!(response_body.min_queue_index, 0);
    }
}
