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

use std::fmt::Display;

use serde::Deserialize;
use serde::Serialize;

use crate::producer::local_transaction_state::LocalTransactionState;
use crate::producer::send_result::SendResult;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TransactionSendResult {
    pub local_transaction_state: Option<LocalTransactionState>,

    #[serde(flatten)]
    pub send_result: Option<SendResult>,
}

impl Display for TransactionSendResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.send_result.as_ref() {
            Some(send_result) => write!(f, "{send_result}"),
            None => write!(
                f,
                "SendResult [sendStatus=null, msgId=null, offsetMsgId=null, messageQueue=null, queueOffset=0, \
                 recallHandle=null]"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::*;
    use crate::producer::send_status::SendStatus;

    #[test]
    fn transaction_send_result_display_delegates_to_send_result_like_java() {
        let mut send_result = SendResult::new(
            SendStatus::SendOk,
            Some(CheetahString::from("msg-a")),
            Some("offset-a".to_string()),
            Some(MessageQueue::from_parts("TopicA", "BrokerA", 1)),
            9,
        );
        send_result.set_recall_handle("recall-a".to_string());

        let transaction_result = TransactionSendResult {
            local_transaction_state: Some(LocalTransactionState::CommitMessage),
            send_result: Some(send_result),
        };

        assert_eq!(
            transaction_result.to_string(),
            "SendResult [sendStatus=SEND_OK, msgId=msg-a, offsetMsgId=offset-a, messageQueue=MessageQueue \
             [topic=TopicA, brokerName=BrokerA, queueId=1], queueOffset=9, recallHandle=recall-a]"
        );
    }

    #[test]
    fn empty_transaction_send_result_display_matches_java_empty_parent_result() {
        let transaction_result = TransactionSendResult {
            local_transaction_state: None,
            send_result: None,
        };

        assert_eq!(
            transaction_result.to_string(),
            "SendResult [sendStatus=null, msgId=null, offsetMsgId=null, messageQueue=null, queueOffset=0, \
             recallHandle=null]"
        );
    }
}
