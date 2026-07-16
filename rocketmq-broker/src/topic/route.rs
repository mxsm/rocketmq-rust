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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use tracing::warn;

/// Broker-owned publish route containing only queue projection and selection state.
///
/// Client producer cache state intentionally remains in `TopicPublishInfo`; the Broker keeps this
/// smaller owner-local projection for failover and lite sharding.
#[derive(Clone, Debug)]
pub(crate) struct BrokerPublishRoute {
    message_queues: Vec<MessageQueue>,
    next_queue: Arc<AtomicUsize>,
}

impl Default for BrokerPublishRoute {
    fn default() -> Self {
        Self::from_queues(Vec::new())
    }
}

impl BrokerPublishRoute {
    pub(crate) fn from_queues(message_queues: Vec<MessageQueue>) -> Self {
        Self {
            message_queues,
            next_queue: Arc::new(AtomicUsize::new(rand::random::<u32>() as usize)),
        }
    }

    pub(crate) fn from_topic_route_data(topic: &str, route: &mut TopicRouteData) -> Self {
        let mut message_queues = Vec::new();
        if let Some(order_topic_conf) = route.order_topic_conf.as_ref().filter(|conf| !conf.is_empty()) {
            for broker in order_topic_conf.split(';') {
                let Some((broker_name, queue_count)) = broker.split_once(':') else {
                    continue;
                };
                let queue_count = match queue_count.parse::<i32>() {
                    Ok(queue_count) => queue_count,
                    Err(error) => {
                        warn!(
                            topic,
                            broker_name,
                            queue_count,
                            %error,
                            "ignore invalid ordered-topic route entry"
                        );
                        continue;
                    }
                };
                for queue_id in 0..queue_count {
                    message_queues.push(MessageQueue::from_parts(topic, broker_name, queue_id));
                }
            }
        } else if route.order_topic_conf.is_none()
            && route
                .topic_queue_mapping_by_broker
                .as_ref()
                .is_some_and(|mapping| !mapping.is_empty())
        {
            if let Some(endpoints) = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route) {
                message_queues.extend(endpoints.into_keys());
                message_queues.sort_by_key(MessageQueue::queue_id);
            }
        } else {
            route.queue_datas.sort();
            for queue_data in &route.queue_datas {
                if !PermName::is_writeable(queue_data.perm) {
                    continue;
                }
                let has_master = route.broker_datas.iter().any(|broker_data| {
                    broker_data.broker_name() == queue_data.broker_name.as_str()
                        && broker_data.broker_addrs().contains_key(&mix_all::MASTER_ID)
                });
                if !has_master {
                    continue;
                }
                for queue_id in 0..queue_data.write_queue_nums {
                    message_queues.push(MessageQueue::from_parts(
                        topic,
                        queue_data.broker_name.as_str(),
                        queue_id as i32,
                    ));
                }
            }
        }
        Self::from_queues(message_queues)
    }

    pub(crate) fn is_usable(&self) -> bool {
        !self.message_queues.is_empty()
    }

    pub(crate) fn message_queues(&self) -> &[MessageQueue] {
        &self.message_queues
    }

    pub(crate) fn select_one_queue(&self) -> Option<MessageQueue> {
        let queue_count = self.message_queues.len();
        if queue_count == 0 {
            return None;
        }
        let index = self.next_queue.fetch_add(1, Ordering::Relaxed) % queue_count;
        self.message_queues.get(index).cloned()
    }

    pub(crate) fn select_one_queue_avoiding(&self, last_broker_name: Option<&CheetahString>) -> Option<MessageQueue> {
        let Some(last_broker_name) = last_broker_name else {
            return self.select_one_queue();
        };
        for _ in 0..self.message_queues.len() {
            let queue = self.select_one_queue()?;
            if queue.broker_name() != last_broker_name {
                return Some(queue);
            }
        }
        self.select_one_queue()
    }
}

pub(crate) fn topic_route_to_subscribe_queues(topic: &str, route: &TopicRouteData) -> HashSet<MessageQueue> {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;

    use super::*;

    #[test]
    fn publish_projection_keeps_only_writable_queues_with_a_master() {
        let mut route = TopicRouteData {
            queue_datas: vec![QueueData::new(
                "broker-a".into(),
                2,
                3,
                PermName::PERM_READ | PermName::PERM_WRITE,
                0,
            )],
            broker_datas: vec![BrokerData::new(
                "cluster-a".into(),
                "broker-a".into(),
                HashMap::from([(mix_all::MASTER_ID, "127.0.0.1:10911".into())]),
                None,
            )],
            ..Default::default()
        };

        let projected = BrokerPublishRoute::from_topic_route_data("topic-a", &mut route);

        assert_eq!(3, projected.message_queues().len());
        assert!(projected.is_usable());
    }

    #[test]
    fn broker_avoidance_prefers_a_different_broker() {
        let route = BrokerPublishRoute::from_queues(vec![
            MessageQueue::from_parts("topic-a", "broker-a", 0),
            MessageQueue::from_parts("topic-a", "broker-b", 1),
        ]);

        let selected = route
            .select_one_queue_avoiding(Some(&CheetahString::from_static_str("broker-a")))
            .expect("a queue should be selected");

        assert_eq!("broker-b", selected.broker_name().as_str());
    }
}
