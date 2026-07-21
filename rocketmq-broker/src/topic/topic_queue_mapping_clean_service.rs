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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all::LOGICAL_QUEUE_MOCK_BROKER_PREFIX;
use rocketmq_common::common::mix_all::METADATA_SCOPE_GLOBAL;
use rocketmq_common::UtilAll::is_it_time_to_do;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::admin::topic_offset::TopicOffset;
use rocketmq_remoting::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::broker_task_group_or_current;
use crate::out_api::broker_outer_api::BrokerOuterAPI;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

const INITIAL_DELAY: Duration = Duration::from_secs(5 * 60);
const CLEAN_INTERVAL: Duration = Duration::from_secs(5 * 60);
const TOPIC_SCAN_YIELD_DELAY: Duration = Duration::from_millis(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct CleanServiceLifecycle {
    task_group: Option<TaskGroup>,
    scheduled_tasks: Option<ScheduledTaskGroup>,
}

#[derive(Clone)]
pub(crate) struct TopicQueueMappingCleanConfig {
    current_broker: CheetahString,
    forward_timeout: u64,
    delete_when: String,
}

impl TopicQueueMappingCleanConfig {
    pub(crate) fn new(current_broker: CheetahString, forward_timeout: u64, delete_when: String) -> Self {
        Self {
            current_broker,
            forward_timeout,
            delete_when,
        }
    }
}

struct TopicQueueMappingCleanServiceInner {
    config: TopicQueueMappingCleanConfig,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    broker_outer_api: BrokerOuterAPI,
    parent_task_group: Option<TaskGroup>,
    running: AtomicBool,
    lifecycle: Mutex<CleanServiceLifecycle>,
}

pub struct TopicQueueMappingCleanService {
    inner: Arc<TopicQueueMappingCleanServiceInner>,
}

impl Clone for TopicQueueMappingCleanService {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl TopicQueueMappingCleanService {
    pub(crate) fn new(
        config: TopicQueueMappingCleanConfig,
        topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
        broker_outer_api: BrokerOuterAPI,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        Self {
            inner: Arc::new(TopicQueueMappingCleanServiceInner {
                config,
                topic_queue_mapping_manager,
                broker_outer_api,
                parent_task_group,
                running: AtomicBool::new(false),
                lifecycle: Mutex::new(CleanServiceLifecycle::default()),
            }),
        }
    }

    pub fn start(&self) {
        self.start_with_schedule(INITIAL_DELAY, CLEAN_INTERVAL);
    }

    pub(crate) fn start_with_schedule(&self, initial_delay: Duration, clean_interval: Duration) {
        if self
            .inner
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let Some(task_group) = broker_task_group_or_current(
            self.inner.parent_task_group.as_ref(),
            "rocketmq-broker.topic-queue-mapping-clean",
            "failed to start TopicQueueMappingCleanService outside Tokio runtime",
        ) else {
            self.inner.running.store(false, Ordering::Release);
            return;
        };
        let scheduled_tasks = ScheduledTaskGroup::new(task_group.child("scheduled"));
        let this = Self {
            inner: Arc::clone(&self.inner),
        };
        let mut config = ScheduledTaskConfig::fixed_delay("broker.topic-queue-mapping-clean.scan", clean_interval);
        config.initial_delay = initial_delay;

        if let Err(error) = scheduled_tasks.schedule_fixed_delay(config, move || {
            let this = this.clone();
            async move {
                if let Err(err) = this.run_once().await {
                    warn!("TopicQueueMappingCleanService scan failed: {}", err);
                }
            }
        }) {
            self.inner.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn TopicQueueMappingCleanService scan task");
            return;
        }

        let mut lifecycle = self.inner.lifecycle.lock();
        lifecycle.scheduled_tasks = Some(scheduled_tasks);
        lifecycle.task_group = Some(task_group);
        info!("TopicQueueMappingCleanService started");
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown_with_report().await;
    }

    pub(crate) async fn shutdown_with_report(&self) -> Option<ShutdownReport> {
        self.inner.running.store(false, Ordering::Release);
        let task_group = {
            let mut lifecycle = self.inner.lifecycle.lock();
            lifecycle.scheduled_tasks.take();
            lifecycle.task_group.take()
        };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(SHUTDOWN_TIMEOUT).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "TopicQueueMappingCleanService shutdown report is unhealthy"
                );
            }
            info!("TopicQueueMappingCleanService shutdown");
            return Some(report);
        }
        info!("TopicQueueMappingCleanService shutdown");
        None
    }

    pub(crate) fn task_count(&self) -> usize {
        let lifecycle = self.inner.lifecycle.lock();
        let root_count = lifecycle
            .task_group
            .as_ref()
            .map(TaskGroup::task_count)
            .unwrap_or_default();
        let scheduled_count = lifecycle
            .scheduled_tasks
            .as_ref()
            .map(|scheduled_tasks| scheduled_tasks.group().task_count())
            .unwrap_or_default();
        root_count + scheduled_count
    }

    pub(crate) fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.inner
            .lifecycle
            .lock()
            .scheduled_tasks
            .as_ref()
            .map(ScheduledTaskGroup::snapshot)
            .unwrap_or_default()
    }

    pub(crate) fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::Acquire)
    }

    pub(crate) async fn run_once(&self) -> RocketMQResult<bool> {
        let expired_changed = self.clean_item_expired_once().await?;
        let old_generation_changed = self.clean_item_list_more_than_second_gen_once().await?;
        Ok(expired_changed || old_generation_changed)
    }

    pub(crate) async fn clean_item_expired_once(&self) -> RocketMQResult<bool> {
        if !self.is_time_to_clean() {
            return Ok(false);
        }

        let start = Instant::now();
        let current_broker = &self.inner.config.current_broker;
        let timeout_millis = self.inner.config.forward_timeout;
        let snapshot = self
            .inner
            .topic_queue_mapping_manager
            .snapshot_topic_queue_mapping_table();
        let mut changed = false;

        for (topic, mapping_detail) in snapshot {
            if !Self::is_current_broker_mapping(&mapping_detail, current_broker) {
                warn!(
                    "TopicQueueMappingDetail for topic {} should not exist in broker {}: {:?}",
                    topic, current_broker, mapping_detail
                );
                continue;
            }

            let Some(hosted_queues) = mapping_detail.hosted_queues.as_ref() else {
                continue;
            };
            if hosted_queues.is_empty() {
                continue;
            }

            let mut brokers = HashSet::new();
            for items in hosted_queues.values() {
                if items.len() <= 1 || !TopicQueueMappingUtils::check_if_leader(items, &mapping_detail) {
                    continue;
                }
                if let Some(broker_name) = items.first().and_then(|item| item.bname.clone()) {
                    brokers.insert(broker_name);
                }
            }
            if brokers.is_empty() {
                continue;
            }

            if let Err(err) = self
                .inner
                .broker_outer_api
                .get_topic_route_info_from_name_server(&topic, timeout_millis, true)
                .await
            {
                warn!(
                    "Refresh route before cleaning expired mapping item failed for topic {}: {}",
                    topic, err
                );
            }

            let mut stats_table = HashMap::new();
            for broker_name in brokers {
                match self
                    .inner
                    .broker_outer_api
                    .get_topic_stats_info_from_broker(&broker_name, &topic, timeout_millis)
                    .await
                {
                    Ok(stats) => {
                        stats_table.insert(broker_name, stats);
                    }
                    Err(err) => {
                        warn!(
                            "Get remote topic {} stats failed from broker {}: {}",
                            topic, broker_name, err
                        );
                    }
                }
            }

            let mut replacements: HashMap<i32, (Vec<LogicQueueMappingItem>, Vec<LogicQueueMappingItem>)> =
                HashMap::new();
            for (qid, items) in hosted_queues {
                if items.len() <= 1 || !TopicQueueMappingUtils::check_if_leader(items, &mapping_detail) {
                    continue;
                }
                let Some(earliest_item) = items.first() else {
                    continue;
                };
                let Some(earliest_broker) = earliest_item.bname.as_ref() else {
                    continue;
                };
                let Some(topic_stats) = stats_table.get(earliest_broker) else {
                    continue;
                };

                let message_queue =
                    MessageQueue::from_parts(topic.clone(), earliest_broker.clone(), earliest_item.queue_id);
                let Some(topic_offset) = topic_stats.get_offset_table().get(&message_queue) else {
                    warn!(
                        "TopicQueueMappingCleanService got null TopicOffset for topic {} item {:?}",
                        topic, earliest_item
                    );
                    continue;
                };
                if Self::should_remove_earliest_item(topic_offset) {
                    let mut new_items = items.clone();
                    new_items.remove(0);
                    replacements.insert(*qid, (items.clone(), new_items));
                    info!(
                        "Remove expired logic queue item topic={} item={:?} because offset={}",
                        topic, earliest_item, topic_offset
                    );
                }
            }

            if !replacements.is_empty() {
                let expected_epoch = mapping_detail.topic_queue_mapping_info.epoch;
                let Some(expected_bname) = mapping_detail.topic_queue_mapping_info.bname.as_ref() else {
                    continue;
                };
                if self.inner.topic_queue_mapping_manager.replace_cleaned_queue_items(
                    &topic,
                    expected_epoch,
                    expected_bname,
                    &replacements,
                ) {
                    changed = true;
                }
            }

            tokio::time::sleep(TOPIC_SCAN_YIELD_DELAY).await;
        }

        if changed {
            self.inner.topic_queue_mapping_manager.persist_clean_result().await?;
            info!("cleanItemExpired changed");
        }
        info!("cleanItemExpired cost {} ms", start.elapsed().as_millis());
        Ok(changed)
    }

    pub(crate) async fn clean_item_list_more_than_second_gen_once(&self) -> RocketMQResult<bool> {
        if !self.is_time_to_clean() {
            return Ok(false);
        }

        let start = Instant::now();
        let current_broker = &self.inner.config.current_broker;
        let timeout_millis = self.inner.config.forward_timeout;
        let snapshot = self
            .inner
            .topic_queue_mapping_manager
            .snapshot_topic_queue_mapping_table();
        let client_metadata = ClientMetadata::new();
        let mut changed = false;

        for (topic, mapping_detail) in snapshot {
            if !Self::is_current_broker_mapping(&mapping_detail, current_broker) {
                warn!(
                    "TopicQueueMappingDetail for topic {} should not exist in broker {}: {:?}",
                    topic, current_broker, mapping_detail
                );
                continue;
            }

            let Some(hosted_queues) = mapping_detail.hosted_queues.as_ref() else {
                continue;
            };
            if hosted_queues.is_empty() {
                continue;
            }

            let Some(expected_bname) = mapping_detail.topic_queue_mapping_info.bname.as_ref() else {
                continue;
            };
            let mut qid_to_current_leader = HashMap::new();
            let mut local_expected_items = HashMap::new();
            for (qid, items) in hosted_queues {
                let Some(leader_item) = items.last() else {
                    continue;
                };
                let Some(leader_broker) = leader_item.bname.as_ref() else {
                    continue;
                };
                if leader_broker != expected_bname {
                    qid_to_current_leader.insert(*qid, leader_broker.clone());
                    local_expected_items.insert(*qid, items.clone());
                }
            }
            if qid_to_current_leader.is_empty() {
                continue;
            }

            let topic_route_data = match self
                .inner
                .broker_outer_api
                .get_topic_route_info_from_name_server(&topic, timeout_millis, true)
                .await
            {
                Ok(topic_route_data) => topic_route_data,
                Err(err) => {
                    warn!(
                        "Get topic route failed before cleaning old mapping list for topic {}: {}",
                        topic, err
                    );
                    continue;
                }
            };
            client_metadata.fresh_topic_route(&topic, Some(topic_route_data));

            let scope = mapping_detail
                .topic_queue_mapping_info
                .scope
                .as_ref()
                .map(CheetahString::as_str)
                .unwrap_or(METADATA_SCOPE_GLOBAL);
            let mock_broker_name = TopicQueueMappingUtils::get_mock_broker_name(scope);
            let mut qid_to_real_leader = HashMap::new();
            for qid in qid_to_current_leader.keys() {
                let message_queue = MessageQueue::from_parts(topic.clone(), mock_broker_name.clone(), *qid);
                if let Some(real_leader) = client_metadata.get_broker_name_from_message_queue(&message_queue) {
                    qid_to_real_leader.insert(*qid, real_leader);
                }
            }

            let mut brokers_to_fetch = HashSet::new();
            for broker_name in qid_to_real_leader.values() {
                if !broker_name.as_str().starts_with(LOGICAL_QUEUE_MOCK_BROKER_PREFIX) {
                    brokers_to_fetch.insert(broker_name.clone());
                }
            }

            let mut mapping_detail_by_broker = HashMap::new();
            for broker_name in brokers_to_fetch {
                match self
                    .inner
                    .broker_outer_api
                    .get_topic_config_from_broker(&broker_name, &topic, true, timeout_millis)
                    .await
                {
                    Ok(topic_config_mapping) => {
                        if let Some(remote_detail) = topic_config_mapping.topic_queue_mapping_detail {
                            if remote_detail.topic_queue_mapping_info.bname.as_ref() == Some(&broker_name) {
                                mapping_detail_by_broker.insert(broker_name, remote_detail.clone());
                            }
                        }
                    }
                    Err(err) => {
                        warn!(
                            "Get remote topic {} config failed from broker {}: {}",
                            topic, broker_name, err
                        );
                    }
                }
            }

            let mut expected_items_to_delete = HashMap::new();
            for (qid, current_leader) in &qid_to_current_leader {
                let Some(real_leader) = qid_to_real_leader.get(qid) else {
                    continue;
                };
                let Some(remote_mapping_detail) = mapping_detail_by_broker.get(real_leader) else {
                    continue;
                };
                if remote_mapping_detail.topic_queue_mapping_info.total_queues
                    != mapping_detail.topic_queue_mapping_info.total_queues
                    || remote_mapping_detail.topic_queue_mapping_info.epoch
                        != mapping_detail.topic_queue_mapping_info.epoch
                {
                    continue;
                }
                let Some(remote_items) = remote_mapping_detail
                    .hosted_queues
                    .as_ref()
                    .and_then(|queues| queues.get(qid))
                else {
                    continue;
                };
                let Some(remote_leader_item) = remote_items.last() else {
                    continue;
                };
                if remote_leader_item.bname.as_ref() != Some(real_leader) {
                    continue;
                }
                if real_leader != current_leader {
                    if let Some(expected_items) = local_expected_items.get(qid) {
                        expected_items_to_delete.insert(*qid, expected_items.clone());
                    }
                }
            }

            if !expected_items_to_delete.is_empty() {
                let expected_epoch = mapping_detail.topic_queue_mapping_info.epoch;
                if self.inner.topic_queue_mapping_manager.remove_cleaned_hosted_queues(
                    &topic,
                    expected_epoch,
                    expected_bname,
                    &expected_items_to_delete,
                ) {
                    changed = true;
                    for (qid, items) in expected_items_to_delete {
                        info!(
                            "Remove old generation mapping list topic={} qid={} items={:?}",
                            topic, qid, items
                        );
                    }
                }
            }

            tokio::time::sleep(TOPIC_SCAN_YIELD_DELAY).await;
        }

        if changed {
            self.inner.topic_queue_mapping_manager.persist_clean_result().await?;
        }
        info!("cleanItemListMoreThanSecondGen cost {} ms", start.elapsed().as_millis());
        Ok(changed)
    }

    fn is_time_to_clean(&self) -> bool {
        is_it_time_to_do(&self.inner.config.delete_when)
    }

    fn is_current_broker_mapping(mapping_detail: &TopicQueueMappingDetail, broker_name: &CheetahString) -> bool {
        mapping_detail.topic_queue_mapping_info.bname.as_ref() == Some(broker_name)
    }

    pub(crate) fn should_remove_earliest_item(topic_offset: &TopicOffset) -> bool {
        topic_offset.get_max_offset() == topic_offset.get_min_offset() || topic_offset.get_max_offset() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use crate::broker_runtime::BrokerRuntime;

    use super::*;

    #[test]
    fn should_remove_earliest_item_when_queue_is_empty() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(10);
        offset.set_max_offset(10);

        assert!(TopicQueueMappingCleanService::should_remove_earliest_item(&offset));
    }

    #[test]
    fn should_remove_earliest_item_when_max_offset_is_zero() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(-1);
        offset.set_max_offset(0);

        assert!(TopicQueueMappingCleanService::should_remove_earliest_item(&offset));
    }

    #[test]
    fn should_keep_earliest_item_when_queue_has_messages() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(10);
        offset.set_max_offset(20);

        assert!(!TopicQueueMappingCleanService::should_remove_earliest_item(&offset));
    }

    #[test]
    fn production_service_has_no_complete_runtime_owner() {
        let source = include_str!("topic_queue_mapping_clean_service.rs");
        let production_source = source.split("#[cfg(test)]").next().expect("production source");

        assert!(!production_source.contains("ArcMut"));
        assert!(!production_source.contains("BrokerRuntimeInner"));
        assert!(!production_source.contains("MessageStore"));
        assert!(!production_source.contains("TopicQueueMappingCleanService<"));
    }

    #[tokio::test]
    async fn run_once_respects_delete_when_window() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = MessageStoreConfig {
            delete_when: "99".to_string(),
            ..Default::default()
        };
        let mut broker_runtime = BrokerRuntime::new(broker_config, Arc::new(message_store_config));
        let service = broker_runtime
            .inner_for_test()
            .topic_queue_mapping_clean_service_unchecked()
            .clone();

        assert!(!service.run_once().await.expect("run_once should not fail"));
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn start_is_idempotent_and_shutdown_stops_background_task() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut broker_runtime = BrokerRuntime::new(broker_config, message_store_config);
        let service = broker_runtime
            .inner_for_test()
            .topic_queue_mapping_clean_service_unchecked()
            .clone();

        service.start();
        service.start();
        assert!(service.is_running());
        assert_eq!(service.task_count(), 1);

        let report = service
            .shutdown_with_report()
            .await
            .expect("shutdown should return a report");
        assert!(!service.is_running());
        assert_eq!(service.task_count(), 0);
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn service_context_parents_task_group() {
        let context = RuntimeContext::from_current("broker-topic-clean-context-test");
        let broker_service = context.service_context("broker-service");
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut broker_runtime =
            BrokerRuntime::new_with_service_context(broker_config, message_store_config, broker_service.clone());
        let service = broker_runtime
            .inner_for_test()
            .topic_queue_mapping_clean_service_unchecked()
            .clone();

        service.start();

        let task_group = service
            .inner
            .lifecycle
            .lock()
            .task_group
            .as_ref()
            .expect("topic queue mapping clean task group should be installed")
            .clone();
        assert_eq!(task_group.parent_id(), Some(broker_service.task_group().id()));

        let report = service
            .shutdown_with_report()
            .await
            .expect("shutdown should return a report");
        assert!(report.is_healthy(), "{}", report.to_json());
        let broker_report = broker_service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(broker_report.is_healthy(), "{}", broker_report.to_json());
    }
}
