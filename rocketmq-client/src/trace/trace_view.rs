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
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::utils::util_all;

use crate::trace::trace_data_encoder::TraceDataEncoder;

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

impl TraceView {
    pub fn decode_from_trace_trans_data(key: &str, message_ext: &MessageExt) -> Vec<TraceView> {
        let Some(body) = message_ext.body() else {
            return Vec::new();
        };
        if body.is_empty() {
            return Vec::new();
        }

        let message_body = String::from_utf8_lossy(&body);
        TraceDataEncoder::decoder_from_trace_data_string(&message_body)
            .into_iter()
            .filter_map(|context| {
                let trace_bean = context.trace_beans.as_ref()?.first()?;
                if trace_bean.msg_id != key {
                    return None;
                }

                Some(TraceView {
                    msg_id: trace_bean.msg_id.clone(),
                    tags: trace_bean.tags.clone(),
                    keys: trace_bean.keys.clone(),
                    store_host: trace_bean.store_host.clone(),
                    client_host: CheetahString::from_string(message_ext.born_host().to_string()),
                    cost_time: i64::from(context.cost_time),
                    msg_type: trace_bean.msg_type,
                    offset_msg_id: trace_bean.offset_msg_id.clone(),
                    time_stamp: i64::try_from(context.time_stamp).unwrap_or(i64::MAX),
                    born_time: 0,
                    topic: trace_bean.topic.clone(),
                    group_name: context.group_name.clone(),
                    status: if context.is_success {
                        CheetahString::from_static_str("success")
                    } else {
                        CheetahString::from_static_str("failed")
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_ext::MessageExt;
    use rocketmq_common::common::message::MessageTrait;

    use super::*;
    use crate::trace::trace_bean::TraceBean;
    use crate::trace::trace_context::TraceContext;
    use crate::trace::trace_data_encoder::TraceDataEncoder;
    use crate::trace::trace_type::TraceType;

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

    #[test]
    fn decode_from_trace_trans_data_matches_java_filter_and_projection() {
        let matching_bean = TraceBean {
            topic: CheetahString::from("topic_a"),
            msg_id: CheetahString::from("msg-a"),
            offset_msg_id: CheetahString::from("offset-a"),
            tags: CheetahString::from("tag-a"),
            keys: CheetahString::from("key-a"),
            store_host: CheetahString::from("store-host"),
            msg_type: Some(MessageType::NormalMsg),
            ..Default::default()
        };
        let other_bean = TraceBean {
            msg_id: CheetahString::from("msg-b"),
            ..matching_bean.clone()
        };
        let context = TraceContext {
            trace_type: Some(TraceType::Pub),
            time_stamp: 12345,
            group_name: CheetahString::from("group-a"),
            cost_time: 17,
            is_success: true,
            trace_beans: Some(vec![matching_bean, other_bean]),
            ..Default::default()
        };
        let transfer = TraceDataEncoder::encoder_from_context_bean(&context).expect("trace context should encode");
        let mut message_ext = MessageExt::default();
        message_ext.set_body(Bytes::from(transfer.trans_data.to_string()));

        let views = TraceView::decode_from_trace_trans_data("msg-a", &message_ext);

        assert_eq!(views.len(), 1);
        let view = &views[0];
        assert_eq!(view.msg_id, CheetahString::from("msg-a"));
        assert_eq!(view.topic, CheetahString::from("topic_a"));
        assert_eq!(view.group_name, CheetahString::from("group-a"));
        assert_eq!(view.status, CheetahString::from("success"));
        assert_eq!(view.cost_time, 17);
        assert_eq!(view.time_stamp, 12345);
        assert_eq!(view.offset_msg_id, CheetahString::from("offset-a"));
        assert_eq!(
            view.client_host,
            CheetahString::from_string(message_ext.born_host().to_string())
        );
        assert!(TraceView::decode_from_trace_trans_data("missing", &message_ext).is_empty());
    }
}
