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
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use futures::stream;
use futures::StreamExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_runtime::tokio_lock::RocketMQTokioMutex;
use rocketmq_transport::ClientMetadata;
use tracing::info;
use tracing::warn;

use super::client_tables::SharedBrokerAddrTable;
use super::client_tables::SharedConsumerTable;
use super::client_tables::SharedProducerTable;
use super::client_tables::SharedTopicEndPointsTable;
use super::client_tables::SharedTopicRouteTable;
use super::client_tables::TopicRouteRefreshState;
use super::mq_client_instance::topic_route_data2topic_publish_info;
use super::mq_client_instance::topic_route_data2topic_subscribe_info;
use crate::consumer::mq_consumer_inner::MQConsumerInner;
use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInnerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;

const NOTIFY_FANOUT: usize = 16;
const ROUTE_REFRESH_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TopicRouteApplyOutcome {
    Applied,
    Unchanged,
    Stale,
}

#[derive(Clone)]
pub(super) struct RouteUpdateCoordinator {
    producer_table: SharedProducerTable,
    consumer_table: SharedConsumerTable,
    topic_route_table: SharedTopicRouteTable,
    topic_end_points_table: SharedTopicEndPointsTable,
    broker_addr_table: SharedBrokerAddrTable,
    refresh_state: Arc<TopicRouteRefreshState>,
    commit_lock: Arc<RocketMQTokioMutex<()>>,
}

struct RouteUpdateSnapshot {
    old_route: Option<TopicRouteData>,
    producers: Vec<MQProducerInnerImpl>,
    consumers: Vec<MQConsumerInnerImpl>,
}

struct RouteUpdatePlan {
    route: TopicRouteData,
    endpoints: Option<std::collections::HashMap<MessageQueue, CheetahString>>,
    publish_info: TopicPublishInfo,
    subscribe_info: HashSet<MessageQueue>,
}

impl RouteUpdateCoordinator {
    pub(super) fn new(
        producer_table: SharedProducerTable,
        consumer_table: SharedConsumerTable,
        topic_route_table: SharedTopicRouteTable,
        topic_end_points_table: SharedTopicEndPointsTable,
        broker_addr_table: SharedBrokerAddrTable,
        refresh_state: Arc<TopicRouteRefreshState>,
        commit_lock: Arc<RocketMQTokioMutex<()>>,
    ) -> Self {
        Self {
            producer_table,
            consumer_table,
            topic_route_table,
            topic_end_points_table,
            broker_addr_table,
            refresh_state,
            commit_lock,
        }
    }

    pub(super) async fn apply_if_fresh(
        &self,
        topic: &CheetahString,
        topic_route_data: &mut TopicRouteData,
        request_version: u64,
    ) -> TopicRouteApplyOutcome {
        let deadline = tokio::time::Instant::now() + ROUTE_REFRESH_TIMEOUT;
        let snapshot = self.snapshot(topic);
        let changed = self.compute_changed(topic, topic_route_data, &snapshot, deadline).await;
        let plan = changed.then(|| self.build_plan(topic, topic_route_data));

        let commit_guard = self.commit_lock.lock().await;
        let current_version = self.topic_route_version(topic);
        if current_version != request_version {
            drop(commit_guard);
            self.refresh_state.metrics.record_version_conflict();
            warn!(
                "updateTopicRouteInfoFromNameServer skipped stale route snapshot, Topic: {}, requestVersion: {}, \
                 currentVersion: {}",
                topic, request_version, current_version
            );
            return TopicRouteApplyOutcome::Stale;
        }

        let Some(plan) = plan else {
            drop(commit_guard);
            return TopicRouteApplyOutcome::Unchanged;
        };
        self.commit(topic, snapshot.old_route.as_ref(), &plan, current_version);
        drop(commit_guard);

        self.notify(topic, &snapshot, &plan, deadline).await;
        TopicRouteApplyOutcome::Applied
    }

    fn snapshot(&self, topic: &CheetahString) -> RouteUpdateSnapshot {
        self.producer_table.retain(|_, producer| producer.is_alive());
        let old_route = self.topic_route_table.get(topic).map(|entry| entry.value().clone());
        let producers = self.producer_table.iter().map(|entry| entry.value().clone()).collect();
        let consumers = self.consumer_table.iter().map(|entry| entry.value().clone()).collect();
        RouteUpdateSnapshot {
            old_route,
            producers,
            consumers,
        }
    }

    async fn compute_changed(
        &self,
        topic: &CheetahString,
        route: &mut TopicRouteData,
        snapshot: &RouteUpdateSnapshot,
        deadline: tokio::time::Instant,
    ) -> bool {
        if route.topic_route_data_changed(snapshot.old_route.as_ref()) {
            info!(
                "the topic[{}] route info changed, old[{:?}] ,new[{:?}]",
                topic, snapshot.old_route, route
            );
            return true;
        }
        if snapshot
            .producers
            .iter()
            .any(|producer| producer.is_publish_topic_need_update(topic))
        {
            return true;
        }

        stream::iter(snapshot.consumers.clone())
            .map(|consumer| {
                let topic = topic.clone();
                async move {
                    tokio::time::timeout_at(deadline, consumer.is_subscribe_topic_need_update(&topic))
                        .await
                        .unwrap_or(true)
                }
            })
            .buffer_unordered(NOTIFY_FANOUT)
            .any(std::future::ready)
            .await
    }

    fn build_plan(&self, topic: &CheetahString, route: &mut TopicRouteData) -> RouteUpdatePlan {
        let endpoints = ClientMetadata::topic_route_data2endpoints_for_static_topic(topic, route)
            .filter(|endpoints| !endpoints.is_empty());
        let mut publish_info = topic_route_data2topic_publish_info(topic, route);
        publish_info.have_topic_router_info = true;
        let subscribe_info = topic_route_data2topic_subscribe_info(topic, route);
        RouteUpdatePlan {
            route: TopicRouteData::from_existing(route),
            endpoints,
            publish_info,
            subscribe_info,
        }
    }

    fn commit(
        &self,
        topic: &CheetahString,
        old_route: Option<&TopicRouteData>,
        plan: &RouteUpdatePlan,
        current_version: u64,
    ) {
        for broker_data in &plan.route.broker_datas {
            self.broker_addr_table
                .insert(broker_data.broker_name().clone(), broker_data.broker_addrs().clone());
        }
        self.update_broker_route_index(old_route, &plan.route);
        if let Some(endpoints) = &plan.endpoints {
            self.topic_end_points_table.insert(topic.clone(), endpoints.clone());
        }
        self.topic_route_table.insert(topic.clone(), plan.route.clone());
        self.refresh_state
            .versions
            .insert(topic.clone(), current_version.saturating_add(1));
    }

    async fn notify(
        &self,
        topic: &CheetahString,
        snapshot: &RouteUpdateSnapshot,
        plan: &RouteUpdatePlan,
        deadline: tokio::time::Instant,
    ) {
        for producer in &snapshot.producers {
            producer.update_topic_publish_info(topic.clone(), Some(plan.publish_info.clone()));
        }

        let subscribe_info = Arc::new(plan.subscribe_info.clone());
        let notify_results = stream::iter(snapshot.consumers.clone())
            .map(|consumer| {
                let topic = topic.clone();
                let subscribe_info = subscribe_info.clone();
                async move {
                    tokio::time::timeout_at(deadline, consumer.update_topic_subscribe_info(topic, &subscribe_info))
                        .await
                        .is_ok()
                }
            })
            .buffer_unordered(NOTIFY_FANOUT)
            .collect::<Vec<_>>()
            .await;
        let timed_out = notify_results.iter().filter(|completed| !**completed).count();
        if timed_out > 0 {
            self.refresh_state.metrics.record_partial_notification(timed_out as u64);
            warn!(
                topic = %topic,
                timed_out,
                consumers = snapshot.consumers.len(),
                "Topic route committed with partial consumer notification"
            );
        }
    }

    fn topic_route_version(&self, topic: &CheetahString) -> u64 {
        self.refresh_state
            .versions
            .get(topic)
            .map(|entry| *entry.value())
            .unwrap_or_default()
    }

    fn route_broker_addr_set(route: &TopicRouteData) -> HashSet<CheetahString> {
        route
            .broker_datas
            .iter()
            .flat_map(|broker_data| broker_data.broker_addrs().values().cloned())
            .filter(|addr| !addr.is_empty())
            .collect()
    }

    pub(super) fn update_broker_route_index(&self, old_route: Option<&TopicRouteData>, new_route: &TopicRouteData) {
        let old_addrs = old_route.map(Self::route_broker_addr_set).unwrap_or_default();
        let new_addrs = Self::route_broker_addr_set(new_route);

        for addr in old_addrs.difference(&new_addrs) {
            self.decrement_broker_route_index(addr);
        }
        for addr in new_addrs.difference(&old_addrs) {
            self.refresh_state
                .broker_addr_route_index
                .entry(addr.clone())
                .and_modify(|count| *count = count.saturating_add(1))
                .or_insert(1);
        }
    }

    fn decrement_broker_route_index(&self, addr: &CheetahString) {
        let should_remove = if let Some(mut count) = self.refresh_state.broker_addr_route_index.get_mut(addr) {
            if *count > 1 {
                *count -= 1;
                false
            } else {
                true
            }
        } else {
            false
        };
        if should_remove {
            self.refresh_state.broker_addr_route_index.remove(addr);
        }
    }
}
