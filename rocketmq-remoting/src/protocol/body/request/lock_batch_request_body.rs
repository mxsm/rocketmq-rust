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

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchRequestBody {
    pub consumer_group: Option<CheetahString>,
    pub client_id: Option<CheetahString>,
    pub only_this_broker: bool,
    pub mq_set: HashSet<MessageQueue>,
}

impl Display for LockBatchRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LockBatchRequestBody [consumer_group={}, client_id={}, only_this_broker={}, mq_set={:?}]",
            self.consumer_group.as_ref().unwrap_or(&"".into()),
            self.client_id.as_ref().unwrap_or(&"".into()),
            self.only_this_broker,
            self.mq_set
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_batch_request_body_default() {
        let body = LockBatchRequestBody::default();
        assert!(body.consumer_group.is_none());
        assert!(body.client_id.is_none());
        assert!(!body.only_this_broker);
        assert!(body.mq_set.is_empty());
    }

    #[test]
    fn test_lock_batch_request_body_serialization() {
        let mut mq_set = HashSet::new();
        mq_set.insert(MessageQueue::from_parts("topic", "broker", 1));
        let body = LockBatchRequestBody {
            consumer_group: Some(CheetahString::from("group")),
            client_id: Some(CheetahString::from("client")),
            only_this_broker: true,
            mq_set,
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("consumerGroup"));
        assert!(json.contains("clientId"));
        assert!(json.contains("onlyThisBroker"));
        assert!(json.contains("mqSet"));
    }

    #[test]
    fn test_lock_batch_request_body_deserialization() {
        let json = r#"{"consumerGroup":"group","clientId":"client","onlyThisBroker":true,"mqSet":[{"topic":"topic","brokerName":"broker","queueId":1}]}"#;
        let body: LockBatchRequestBody = serde_json::from_str(json).unwrap();
        assert_eq!(body.consumer_group.unwrap(), "group");
        assert_eq!(body.client_id.unwrap(), "client");
        assert!(body.only_this_broker);
        assert_eq!(body.mq_set.len(), 1);
    }

    #[test]
    fn test_lock_batch_request_body_display() {
        let body = LockBatchRequestBody {
            consumer_group: Some(CheetahString::from("group")),
            client_id: Some(CheetahString::from("client")),
            only_this_broker: true,
            mq_set: HashSet::new(),
        };
        let display = format!("{}", body);
        let expected =
            "LockBatchRequestBody [consumer_group=group, client_id=client, only_this_broker=true, mq_set={}]";
        assert_eq!(display, expected);
    }
}
