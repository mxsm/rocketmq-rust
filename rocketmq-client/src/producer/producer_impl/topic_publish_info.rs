/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::common::thread_local_index::ThreadLocalIndex;

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
}
