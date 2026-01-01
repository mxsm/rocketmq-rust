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

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct PopMessageRequestHeader {
    #[required]
    pub consumer_group: CheetahString,
    #[required]
    pub topic: CheetahString,
    #[required]
    pub queue_id: i32,
    #[required]
    pub max_msg_nums: u32,
    #[required]
    pub invisible_time: u64,
    #[required]
    pub poll_time: u64,
    #[required]
    pub born_time: u64,
    #[required]
    pub init_mode: i32,
    pub exp_type: Option<CheetahString>,
    pub exp: Option<CheetahString>,
    pub order: Option<bool>,
    pub attempt_id: Option<CheetahString>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl PopMessageRequestHeader {
    pub fn is_timeout_too_much(&self) -> bool {
        get_current_millis() as i64 - self.born_time as i64 - self.poll_time as i64 > 500
    }
}

impl Default for PopMessageRequestHeader {
    fn default() -> Self {
        PopMessageRequestHeader {
            consumer_group: CheetahString::new(),
            topic: CheetahString::new(),
            queue_id: 0,
            max_msg_nums: 0,
            invisible_time: 0,
            poll_time: 0,
            born_time: 0,
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        }
    }
}

impl Display for PopMessageRequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopMessageRequestHeader [consumer_group={}, topic={}, queue_id={}, max_msg_nums={}, invisible_time={}, \
             poll_time={}, born_time={}, init_mode={}, exp_type={}, exp={}, order={}, attempt_id={}]",
            self.consumer_group,
            self.topic,
            self.queue_id,
            self.max_msg_nums,
            self.invisible_time,
            self.poll_time,
            self.born_time,
            self.init_mode,
            self.exp_type.as_ref().unwrap_or(&CheetahString::new()),
            self.exp.as_ref().unwrap_or(&CheetahString::new()),
            self.order.as_ref().unwrap_or(&false),
            self.attempt_id.as_ref().unwrap_or(&CheetahString::new())
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn default_pop_message_request_header() {
        let header = PopMessageRequestHeader::default();
        assert_eq!(header.consumer_group, CheetahString::new());
        assert_eq!(header.topic, CheetahString::new());
        assert_eq!(header.queue_id, 0);
        assert_eq!(header.max_msg_nums, 0);
        assert_eq!(header.invisible_time, 0);
        assert_eq!(header.poll_time, 0);
        assert_eq!(header.born_time, 0);
        assert_eq!(header.init_mode, 0);
        assert!(header.exp_type.is_none());
        assert!(header.exp.is_none());
        assert_eq!(header.order, Some(false));
        assert!(header.attempt_id.is_none());
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn display_pop_message_request_header() {
        let header = PopMessageRequestHeader {
            consumer_group: CheetahString::from("group1"),
            topic: CheetahString::from("topic1"),
            queue_id: 1,
            max_msg_nums: 10,
            invisible_time: 1000,
            poll_time: 2000,
            born_time: 3000,
            init_mode: 1,
            exp_type: Some(CheetahString::from("type1")),
            exp: Some(CheetahString::from("exp1")),
            order: Some(true),
            attempt_id: Some(CheetahString::from("attempt1")),
            topic_request_header: None,
        };
        assert_eq!(
            format!("{}", header),
            "PopMessageRequestHeader [consumer_group=group1, topic=topic1, queue_id=1, max_msg_nums=10, \
             invisible_time=1000, poll_time=2000, born_time=3000, init_mode=1, exp_type=type1, exp=exp1, order=true, \
             attempt_id=attempt1]"
        );
    }

    #[test]
    fn display_pop_message_request_header_with_defaults() {
        let header = PopMessageRequestHeader::default();
        assert_eq!(
            format!("{}", header),
            "PopMessageRequestHeader [consumer_group=, topic=, queue_id=0, max_msg_nums=0, invisible_time=0, \
             poll_time=0, born_time=0, init_mode=0, exp_type=, exp=, order=false, attempt_id=]"
        );
    }
}
