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

use std::cmp::Ordering;
use std::collections::HashSet;

use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_transport::api::ClientMetadata;
use tracing::warn;

use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;

pub(super) fn cap_default_topic_route_queue_nums(topic_route_data: &mut TopicRouteData, default_topic_queue_nums: u32) {
    for data in &mut topic_route_data.queue_datas {
        let queue_nums = default_topic_queue_nums.min(data.read_queue_nums);
        data.read_queue_nums = queue_nums;
        data.write_queue_nums = queue_nums;
    }
}

pub fn topic_route_data2topic_publish_info(topic: &str, route: &mut TopicRouteData) -> TopicPublishInfo {
    let mut info = TopicPublishInfo {
        topic_route_data: Some(route.clone()),
        ..Default::default()
    };
    if let Some(order_topic_conf) = route.order_topic_conf.as_ref().filter(|conf| !conf.is_empty()) {
        for broker in order_topic_conf.split_char(';') {
            let item = broker.split(':').collect::<Vec<&str>>();
            if item.len() != 2 {
                continue;
            }
            let queue_num = match item[1].parse::<i32>() {
                Ok(queue_num) => queue_num,
                Err(error) => {
                    warn!(
                        "ignore invalid order topic conf entry for topic={}, broker={}, queue_nums={}, error={}",
                        topic, item[0], item[1], error
                    );
                    continue;
                }
            };
            for i in 0..queue_num {
                info.message_queue_list
                    .push(MessageQueue::from_parts(topic, item[0], i));
            }
        }
        info.order_topic = true;
    } else if route.order_topic_conf.is_none()
        && route
            .topic_queue_mapping_by_broker
            .as_ref()
            .is_some_and(|mapping| !mapping.is_empty())
    {
        info.order_topic = false;
        if let Some(endpoints) = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route) {
            info.message_queue_list.extend(endpoints.into_keys());
        }
        info.message_queue_list
            .sort_by(|left, right| match left.queue_id().cmp(&right.queue_id()) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => Ordering::Equal,
                Ordering::Greater => Ordering::Greater,
            });
    } else {
        route.queue_datas.sort();
        for queue_data in &route.queue_datas {
            if !PermName::is_writeable(queue_data.perm) {
                continue;
            }
            let Some(broker_data) = route
                .broker_datas
                .iter()
                .find(|data| data.broker_name() == queue_data.broker_name.as_str())
            else {
                continue;
            };
            if !broker_data.broker_addrs().contains_key(&mix_all::MASTER_ID) {
                continue;
            }
            for queue_id in 0..queue_data.write_queue_nums {
                info.message_queue_list.push(MessageQueue::from_parts(
                    topic,
                    queue_data.broker_name.as_str(),
                    queue_id as i32,
                ));
            }
        }
    }
    info
}

pub fn topic_route_data2_topic_publish_info(topic: &str, route: &mut TopicRouteData) -> TopicPublishInfo {
    topic_route_data2topic_publish_info(topic, route)
}

pub fn topic_route_data2topic_subscribe_info(topic: &str, route: &TopicRouteData) -> HashSet<MessageQueue> {
    if route
        .topic_queue_mapping_by_broker
        .as_ref()
        .is_some_and(|mapping| !mapping.is_empty())
    {
        return ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route)
            .unwrap_or_default()
            .into_keys()
            .collect();
    }
    let mut queues = HashSet::new();
    for queue_data in &route.queue_datas {
        if PermName::is_readable(queue_data.perm) {
            for queue_id in 0..queue_data.read_queue_nums {
                queues.insert(MessageQueue::from_parts(
                    topic,
                    queue_data.broker_name.as_str(),
                    queue_id as i32,
                ));
            }
        }
    }
    queues
}

pub fn topic_route_data2_topic_subscribe_info(topic: &str, route: &TopicRouteData) -> HashSet<MessageQueue> {
    topic_route_data2topic_subscribe_info(topic, route)
}
