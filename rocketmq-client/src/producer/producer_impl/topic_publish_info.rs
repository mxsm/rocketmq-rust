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
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::common::thread_local_index::ThreadLocalIndex;
use crate::producer::producer_impl::queue_filter::QueueFilter;

#[derive(Default, Clone)]
pub struct TopicPublishInfo {
    pub order_topic: bool,
    pub have_topic_router_info: bool,
    pub message_queue_list: Vec<MessageQueue>,
    pub send_which_queue: ThreadLocalIndex,
    pub topic_route_data: Option<TopicRouteData>,
}

impl TopicPublishInfo {
    pub fn new() -> Self {
        TopicPublishInfo {
            order_topic: false,
            have_topic_router_info: false,
            message_queue_list: vec![],
            send_which_queue: ThreadLocalIndex,
            topic_route_data: None,
        }
    }

    pub fn ok(&self) -> bool {
        !self.message_queue_list.is_empty()
    }

    pub fn reset_index(&self) {
        self.send_which_queue.reset();
    }

    #[inline]
    pub fn select_one_message_queue_filters(&self, filters: &[&dyn QueueFilter]) -> Option<MessageQueue> {
        self.select_one_message_queue_with_filters_inner(&self.message_queue_list, &self.send_which_queue, filters)
    }

    pub fn select_one_message_queue_by_broker(&self, last_broker_name: Option<&CheetahString>) -> Option<MessageQueue> {
        if let Some(last_broker_name) = last_broker_name {
            for mq in &self.message_queue_list {
                if mq.get_broker_name() != last_broker_name {
                    return Some(mq.clone());
                }
            }
            self.select_one_message_queue_filters(&[])
        } else {
            self.select_one_message_queue_filters(&[])
        }
    }

    pub fn select_one_message_queue_with_filters_inner(
        &self,
        message_queue_list: &[MessageQueue],
        send_queue: &ThreadLocalIndex,
        filters: &[&dyn QueueFilter],
    ) -> Option<MessageQueue> {
        if message_queue_list.is_empty() {
            return None;
        }

        // If filters are provided, apply them
        if !filters.is_empty() {
            for _ in 0..message_queue_list.len() {
                let index = (send_queue.increment_and_get() as usize) % message_queue_list.len();
                let mq = &message_queue_list[index];

                // Check all filters
                let mut filter_result = true;
                for f in filters {
                    filter_result &= f.filter(mq);
                }

                // If filter passes, return the message queue
                if filter_result {
                    return Some(mq.clone());
                }
            }

            return None;
        }

        // If no filters are provided, select a message queue randomly
        let index = send_queue.increment_and_get() as usize % message_queue_list.len();
        Some(message_queue_list[index].clone())
    }

    pub fn select_one_message_queue(&self) -> Option<MessageQueue> {
        let index = self.send_which_queue.increment_and_get() as usize;
        let pos = index % self.message_queue_list.len();
        self.message_queue_list.get(pos).cloned()
    }
}
