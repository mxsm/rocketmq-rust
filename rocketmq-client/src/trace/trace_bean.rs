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

use std::sync::LazyLock;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::utils::util_all;

use crate::producer::local_transaction_state::LocalTransactionState;

static LOCAL_ADDRESS: LazyLock<CheetahString> = LazyLock::new(util_all::get_ip_str);

#[derive(Debug, Clone)]
pub struct TraceBean {
    pub topic: CheetahString,
    pub msg_id: CheetahString,
    pub offset_msg_id: CheetahString,
    pub tags: CheetahString,
    pub keys: CheetahString,
    pub store_host: CheetahString,
    pub client_host: CheetahString,
    pub store_time: i64,
    pub retry_times: i32,
    pub body_length: i32,
    pub msg_type: Option<MessageType>,
    pub transaction_state: Option<LocalTransactionState>,
    pub transaction_id: Option<CheetahString>,
    pub from_transaction_check: bool,
}

impl Default for TraceBean {
    fn default() -> Self {
        TraceBean {
            topic: CheetahString::default(),
            msg_id: CheetahString::default(),
            offset_msg_id: CheetahString::default(),
            tags: CheetahString::default(),
            keys: CheetahString::default(),
            store_host: LOCAL_ADDRESS.clone(),
            client_host: LOCAL_ADDRESS.clone(),
            store_time: 0,
            retry_times: 0,
            body_length: 0,
            msg_type: None,
            transaction_state: None,
            transaction_id: None,
            from_transaction_check: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn trace_bean_default_values() {
        let trace_bean = TraceBean::default();
        assert_eq!(trace_bean.topic, CheetahString::default());
        assert_eq!(trace_bean.msg_id, CheetahString::default());
        assert_eq!(trace_bean.offset_msg_id, CheetahString::default());
        assert_eq!(trace_bean.tags, CheetahString::default());
        assert_eq!(trace_bean.keys, CheetahString::default());
        assert_eq!(trace_bean.store_host, LOCAL_ADDRESS.clone());
        assert_eq!(trace_bean.client_host, LOCAL_ADDRESS.clone());
        assert_eq!(trace_bean.store_time, 0);
        assert_eq!(trace_bean.retry_times, 0);
        assert_eq!(trace_bean.body_length, 0);
        assert!(trace_bean.msg_type.is_none());
        assert!(trace_bean.transaction_state.is_none());
        assert!(trace_bean.transaction_id.is_none());
        assert!(!trace_bean.from_transaction_check);
    }

    #[test]
    fn trace_bean_with_values() {
        let trace_bean = TraceBean {
            topic: CheetahString::from("topic"),
            msg_id: CheetahString::from("msg_id"),
            offset_msg_id: CheetahString::from("offset_msg_id"),
            tags: CheetahString::from("tags"),
            keys: CheetahString::from("keys"),
            store_host: CheetahString::from("store_host"),
            client_host: CheetahString::from("client_host"),
            store_time: 123456789,
            retry_times: 3,
            body_length: 1024,
            msg_type: Some(MessageType::NormalMsg),
            transaction_state: Some(LocalTransactionState::CommitMessage),
            transaction_id: Some(CheetahString::from("transaction_id")),
            from_transaction_check: true,
        };
        assert_eq!(trace_bean.topic, CheetahString::from("topic"));
        assert_eq!(trace_bean.msg_id, CheetahString::from("msg_id"));
        assert_eq!(trace_bean.offset_msg_id, CheetahString::from("offset_msg_id"));
        assert_eq!(trace_bean.tags, CheetahString::from("tags"));
        assert_eq!(trace_bean.keys, CheetahString::from("keys"));
        assert_eq!(trace_bean.store_host, CheetahString::from("store_host"));
        assert_eq!(trace_bean.client_host, CheetahString::from("client_host"));
        assert_eq!(trace_bean.store_time, 123456789);
        assert_eq!(trace_bean.retry_times, 3);
        assert_eq!(trace_bean.body_length, 1024);
        assert_eq!(trace_bean.msg_type, Some(MessageType::NormalMsg));
        assert_eq!(trace_bean.transaction_state, Some(LocalTransactionState::CommitMessage));
        assert_eq!(trace_bean.transaction_id, Some(CheetahString::from("transaction_id")));
        assert!(trace_bean.from_transaction_check);
    }
}
