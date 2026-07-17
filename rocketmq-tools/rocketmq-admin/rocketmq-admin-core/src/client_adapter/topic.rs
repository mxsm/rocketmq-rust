// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::BTreeMap;

use cheetah_string::CheetahString;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::topic::GetTopicRouteRequest;
use crate::core::topic::ListTopicsRequest;
use crate::core::topic::ListTopicsResult;
use crate::core::topic::TopicAdmin;
use crate::core::topic::TopicBroker;
use crate::core::topic::TopicQueue;
use crate::core::topic::TopicRoute;
use crate::core::topic::TopicSummary;
use crate::core::AdminError;
use crate::core::AdminFuture;

impl TopicAdmin for AdminSession {
    fn list_topics<'a>(&'a mut self, request: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| AdminError::backend("fetch_all_topic_list", error.to_string()))?;

            let Some(cluster_name) = request.cluster.as_deref() else {
                return Ok(ListTopicsResult {
                    topics: topic_list
                        .topic_list
                        .into_iter()
                        .map(|topic| TopicSummary {
                            topic: topic.to_string(),
                            cluster: None,
                            consumer_group: None,
                        })
                        .collect(),
                });
            };

            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| AdminError::backend("examine_broker_cluster_info", error.to_string()))?;
            let cluster_brokers = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(cluster_name))
                .cloned()
                .unwrap_or_default();

            let mut topics = Vec::new();
            for topic in topic_list.topic_list {
                if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                    continue;
                }
                let route = self
                    .inner
                    .examine_topic_route_info(topic.clone())
                    .await
                    .map_err(|error| AdminError::backend("examine_topic_route_info", error.to_string()))?;
                let Some(route) = route else {
                    continue;
                };
                if !route
                    .broker_datas
                    .iter()
                    .any(|broker| cluster_brokers.contains(broker.broker_name()))
                {
                    continue;
                }

                let group_list = self
                    .inner
                    .query_topic_consume_by_who(topic.clone())
                    .await
                    .map_err(|error| AdminError::backend("query_topic_consume_by_who", error.to_string()))?;
                if group_list.get_group_list().is_empty() {
                    topics.push(TopicSummary {
                        topic: topic.to_string(),
                        cluster: Some(cluster_name.to_string()),
                        consumer_group: None,
                    });
                } else {
                    topics.extend(group_list.get_group_list().iter().map(|group| TopicSummary {
                        topic: topic.to_string(),
                        cluster: Some(cluster_name.to_string()),
                        consumer_group: Some(group.to_string()),
                    }));
                }
            }
            topics.sort_by(|left, right| {
                left.topic
                    .cmp(&right.topic)
                    .then(left.consumer_group.cmp(&right.consumer_group))
            });
            Ok(ListTopicsResult { topics })
        })
    }

    fn get_topic_route<'a>(&'a mut self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>> {
        Box::pin(async move {
            self.ensure_open()?;
            let route = self
                .inner
                .examine_topic_route_info(CheetahString::from(request.topic.as_str()))
                .await
                .map_err(|error| AdminError::backend("examine_topic_route_info", error.to_string()))?;

            Ok(route.map(|route| {
                let mut brokers = route
                    .broker_datas
                    .iter()
                    .map(|broker| TopicBroker {
                        cluster: broker.cluster().to_string(),
                        broker_name: broker.broker_name().to_string(),
                        broker_addrs: broker
                            .broker_addrs()
                            .iter()
                            .map(|(broker_id, broker_addr)| (*broker_id, broker_addr.to_string()))
                            .collect::<BTreeMap<_, _>>(),
                        zone_name: broker.zone_name().map(ToString::to_string),
                        enable_acting_master: broker.enable_acting_master(),
                    })
                    .collect::<Vec<_>>();
                brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

                let mut queues = route
                    .queue_datas
                    .iter()
                    .map(|queue| TopicQueue {
                        broker_name: queue.broker_name().to_string(),
                        read_queue_nums: queue.read_queue_nums(),
                        write_queue_nums: queue.write_queue_nums(),
                        perm: queue.perm(),
                        topic_sys_flag: queue.topic_sys_flag(),
                    })
                    .collect::<Vec<_>>();
                queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
                TopicRoute { brokers, queues }
            }))
        })
    }
}
