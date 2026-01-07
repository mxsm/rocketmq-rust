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
use rocketmq_remoting::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;

#[derive(Clone, Debug)]
pub struct PollingHeader {
    consumer_group: CheetahString,
    topic: CheetahString,
    queue_id: i32,
    born_time: i64,
    poll_time: i64,
}

impl Display for PollingHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PollingHeader [consumer_group={}, topic={}, queue_id={}, born_time={}, poll_time={}]",
            self.consumer_group, self.topic, self.queue_id, self.born_time, self.poll_time
        )
    }
}

impl PollingHeader {
    pub fn new_from_pop_message_request_header(request_header: &PopMessageRequestHeader) -> Self {
        Self {
            consumer_group: request_header.consumer_group.clone(),
            topic: request_header.topic.clone(),
            queue_id: request_header.queue_id,
            born_time: request_header.born_time as i64,
            poll_time: request_header.poll_time as i64,
        }
    }

    pub fn new_from_notification_request_header(request_header: &NotificationRequestHeader) -> Self {
        Self {
            consumer_group: request_header.consumer_group.clone(),
            topic: request_header.topic.clone(),
            queue_id: request_header.queue_id,
            born_time: request_header.born_time,
            poll_time: request_header.poll_time,
        }
    }

    pub fn get_consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn get_topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn get_queue_id(&self) -> i32 {
        self.queue_id
    }

    pub fn get_born_time(&self) -> i64 {
        self.born_time
    }

    pub fn get_poll_time(&self) -> i64 {
        self.poll_time
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_remoting::protocol::header::notification_request_header::NotificationRequestHeader;
    use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;

    use super::*;

    #[test]
    fn polling_header_display_format() {
        let header = PollingHeader {
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            born_time: 1234567890,
            poll_time: 1234567890,
        };

        let display = format!("{}", header);
        assert_eq!(
            display,
            "PollingHeader [consumer_group=test_group, topic=test_topic, queue_id=1, born_time=1234567890, \
             poll_time=1234567890]"
        );
    }

    #[test]
    fn polling_header_new_from_pop_message_request_header() {
        let request_header = PopMessageRequestHeader {
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            max_msg_nums: 0,
            born_time: 1234567890,
            init_mode: 0,
            exp_type: None,
            exp: None,
            order: None,
            attempt_id: None,
            poll_time: 1234567890,
            invisible_time: 0,
            topic_request_header: None,
        };

        let header = PollingHeader::new_from_pop_message_request_header(&request_header);
        assert_eq!(header.get_consumer_group(), &CheetahString::from("test_group"));
        assert_eq!(header.get_topic(), &CheetahString::from("test_topic"));
        assert_eq!(header.get_queue_id(), 1);
        assert_eq!(header.get_born_time(), 1234567890);
        assert_eq!(header.get_poll_time(), 1234567890);
    }

    #[test]
    fn polling_header_new_from_notification_request_header() {
        let request_header = NotificationRequestHeader {
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            born_time: 1234567890,
            order: false,
            attempt_id: None,
            poll_time: 1234567890,
            topic_request_header: None,
        };

        let header = PollingHeader::new_from_notification_request_header(&request_header);
        assert_eq!(header.get_consumer_group(), &CheetahString::from("test_group"));
        assert_eq!(header.get_topic(), &CheetahString::from("test_topic"));
        assert_eq!(header.get_queue_id(), 1);
        assert_eq!(header.get_born_time(), 1234567890);
        assert_eq!(header.get_poll_time(), 1234567890);
    }

    #[test]
    fn polling_header_getters() {
        let header = PollingHeader {
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            born_time: 1234567890,
            poll_time: 1234567890,
        };

        assert_eq!(header.get_consumer_group(), &CheetahString::from("test_group"));
        assert_eq!(header.get_topic(), &CheetahString::from("test_topic"));
        assert_eq!(header.get_queue_id(), 1);
        assert_eq!(header.get_born_time(), 1234567890);
        assert_eq!(header.get_poll_time(), 1234567890);
    }
}
