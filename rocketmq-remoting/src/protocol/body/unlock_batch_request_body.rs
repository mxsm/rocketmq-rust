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

use std::collections::HashSet;
use std::fmt::Display;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnlockBatchRequestBody {
    pub consumer_group: Option<CheetahString>,
    pub client_id: Option<CheetahString>,
    pub only_this_broker: bool,
    pub mq_set: HashSet<MessageQueue>,
}

impl Display for UnlockBatchRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnlockBatchRequestBody [consumer_group={}, client_id={}, only_this_broker={}, mq_set={:?}]",
            self.consumer_group.as_ref().unwrap_or(&CheetahString::new()),
            self.client_id.as_ref().unwrap_or(&CheetahString::new()),
            self.only_this_broker,
            self.mq_set
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlock_batch_request_body_default() {
        let body = UnlockBatchRequestBody::default();
        assert!(body.consumer_group.is_none());
        assert!(body.client_id.is_none());
        assert!(!body.only_this_broker);
        assert!(body.mq_set.is_empty());
    }

    #[test]
    fn unlock_batch_request_body_display() {
        let body = UnlockBatchRequestBody {
            consumer_group: Some(CheetahString::from("group")),
            client_id: Some(CheetahString::from("client")),
            only_this_broker: false,
            ..Default::default()
        };
        let display = format!("{}", body);
        assert!(display.contains("consumer_group=group"));
        assert!(display.contains("client_id=client"));
        assert!(display.contains("only_this_broker=false"));
    }

    #[test]
    fn unlock_batch_request_body_serialization() {
        let mut mq_set = HashSet::new();
        mq_set.insert(MessageQueue::from_parts("topic", "broker", 1));
        let body = UnlockBatchRequestBody {
            consumer_group: Some(CheetahString::from("group")),
            client_id: Some(CheetahString::from("client")),
            only_this_broker: true,
            mq_set,
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"consumerGroup\":\"group\""));
        assert!(json.contains("\"clientId\":\"client\""));
        assert!(json.contains("\"onlyThisBroker\":true"));
        assert!(json.contains("\"mqSet\""));
    }

    #[test]
    fn unlock_batch_request_body_deserialization() {
        let json = r#"{"consumerGroup":"group","clientId":"client","onlyThisBroker":true,"mqSet":[{"topic":"topic","brokerName":"broker","queueId":1}]}"#;
        let body: UnlockBatchRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.consumer_group, Some(CheetahString::from("group")));
        assert_eq!(body.client_id, Some(CheetahString::from("client")));
        assert!(body.only_this_broker);
        assert_eq!(body.mq_set.len(), 1);
        let mq = body.mq_set.iter().next().unwrap();
        assert_eq!(mq, &MessageQueue::from_parts("topic", "broker", 1));
    }
}
//
