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
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::utils::util_all;

static LOCAL_ADDRESS: std::sync::LazyLock<CheetahString> = std::sync::LazyLock::new(util_all::get_ip_str);

#[derive(Debug, Clone)]
pub struct TraceView {
    pub msg_id: CheetahString,
    pub tags: CheetahString,
    pub keys: CheetahString,
    pub store_host: CheetahString,
    pub client_host: CheetahString,
    pub cost_time: i64,
    pub msg_type: Option<MessageType>,
    pub offset_msg_id: CheetahString,
    pub time_stamp: i64,
    pub born_time: i64,
    pub topic: CheetahString,
    pub group_name: CheetahString,
    pub status: CheetahString,
}

impl Default for TraceView {
    fn default() -> Self {
        TraceView {
            msg_id: CheetahString::default(),
            tags: CheetahString::default(),
            keys: CheetahString::default(),
            store_host: LOCAL_ADDRESS.clone(),
            client_host: LOCAL_ADDRESS.clone(),
            cost_time: 0,
            msg_type: None,
            offset_msg_id: CheetahString::default(),
            time_stamp: 0,
            born_time: 0,
            topic: CheetahString::default(),
            group_name: CheetahString::default(),
            status: CheetahString::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn trace_view_default_values() {
        let trace_view = TraceView::default();
        assert_eq!(trace_view.msg_id, CheetahString::default());
        assert_eq!(trace_view.tags, CheetahString::default());
        assert_eq!(trace_view.keys, CheetahString::default());
        assert_eq!(trace_view.store_host, LOCAL_ADDRESS.clone());
        assert_eq!(trace_view.client_host, LOCAL_ADDRESS.clone());
        assert_eq!(trace_view.cost_time, 0);
        assert_eq!(trace_view.msg_type, None);
        assert_eq!(trace_view.offset_msg_id, CheetahString::default());
        assert_eq!(trace_view.time_stamp, 0);
        assert_eq!(trace_view.born_time, 0);
        assert_eq!(trace_view.topic, CheetahString::default());
        assert_eq!(trace_view.group_name, CheetahString::default());
        assert_eq!(trace_view.status, CheetahString::default());
    }

    #[test]
    fn trace_view_with_values() {
        let trace_view = TraceView {
            msg_id: CheetahString::from("msg_id"),
            tags: CheetahString::from("tags"),
            keys: CheetahString::from("keys"),
            store_host: CheetahString::from("127.0.0.1"),
            client_host: CheetahString::from("127.0.0.1"),
            cost_time: 1734784743,
            msg_type: Some(MessageType::NormalMsg),
            offset_msg_id: CheetahString::from("offset_msg_id"),
            time_stamp: 1734784743,
            born_time: 1734784743,
            topic: CheetahString::from("topic"),
            group_name: CheetahString::from("group"),
            status: CheetahString::from("status"),
        };
        assert_eq!(trace_view.msg_id, CheetahString::from("msg_id"));
        assert_eq!(trace_view.tags, CheetahString::from("tags"));
        assert_eq!(trace_view.keys, CheetahString::from("keys"));
        assert_eq!(trace_view.store_host, CheetahString::from("127.0.0.1"));
        assert_eq!(trace_view.client_host, CheetahString::from("127.0.0.1"));
        assert_eq!(trace_view.cost_time, 1734784743);
        assert_eq!(trace_view.msg_type, Some(MessageType::NormalMsg));
        assert_eq!(trace_view.offset_msg_id, CheetahString::from("offset_msg_id"));
        assert_eq!(trace_view.time_stamp, 1734784743);
        assert_eq!(trace_view.born_time, 1734784743);
        assert_eq!(trace_view.topic, CheetahString::from("topic"));
        assert_eq!(trace_view.group_name, CheetahString::from("group"));
        assert_eq!(trace_view.status, CheetahString::from("status"));
    }
}
