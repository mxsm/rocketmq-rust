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
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

const INITIAL_DELAY: Duration = Duration::from_secs(5 * 60);
const CLEAN_INTERVAL: Duration = Duration::from_secs(5 * 60);
const TOPIC_SCAN_YIELD_DELAY: Duration = Duration::from_millis(10);

#[derive(Default)]
struct CleanServiceLifecycle {
    cancel: Option<CancellationToken>,
    handle: Option<JoinHandle<()>>,
}

struct TopicQueueMappingCleanServiceInner<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    running: AtomicBool,
    lifecycle: Mutex<CleanServiceLifecycle>,
}

pub struct TopicQueueMappingCleanService<MS: MessageStore> {
    inner: Arc<TopicQueueMappingCleanServiceInner<MS>>,
}

impl<MS: MessageStore> Clone for TopicQueueMappingCleanService<MS> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<MS: MessageStore> TopicQueueMappingCleanService<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            inner: Arc::new(TopicQueueMappingCleanServiceInner {
                broker_runtime_inner,
                running: AtomicBool::new(false),
                lifecycle: Mutex::new(CleanServiceLifecycle::default()),
            }),
        }
    }

    pub fn start(&self) {
        if self
            .inner
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let this = Self {
            inner: Arc::clone(&self.inner),
        };
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = cancel_for_task.cancelled() => {
                    this.inner.running.store(false, Ordering::Release);
                    return;
                }
                _ = tokio::time::sleep(INITIAL_DELAY) => {}
            }

            let mut interval = tokio::time::interval(CLEAN_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = cancel_for_task.cancelled() => {
                        this.inner.running.store(false, Ordering::Release);
                        return;
                    }
                    _ = interval.tick() => {
                        if let Err(err) = this.run_once().await {
                            warn!("TopicQueueMappingCleanService scan failed: {}", err);
                        }
                    }
                }
            }
        });

        let mut lifecycle = self.inner.lifecycle.lock();
        lifecycle.cancel = Some(cancel);
        lifecycle.handle = Some(handle);
        info!("TopicQueueMappingCleanService started");
    }

    pub fn shutdown(&self) {
        self.inner.running.store(false, Ordering::Release);
        let mut lifecycle = self.inner.lifecycle.lock();
        if let Some(cancel) = lifecycle.cancel.take() {
            cancel.cancel();
        }
        if let Some(handle) = lifecycle.handle.take() {
            handle.abort();
        }
        info!("TopicQueueMappingCleanService shutdown");
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
        let broker_runtime_inner = &self.inner.broker_runtime_inner;
        let current_broker = broker_runtime_inner.broker_config().broker_name().clone();
        let timeout_millis = broker_runtime_inner.broker_config().forward_timeout;
        let snapshot = broker_runtime_inner
            .topic_queue_mapping_manager()
            .snapshot_topic_queue_mapping_table();
        let mut changed = false;

        for (topic, mapping_detail) in snapshot {
            if !Self::is_current_broker_mapping(&mapping_detail, &current_broker) {
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

            if let Err(err) = broker_runtime_inner
                .broker_outer_api()
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
                match broker_runtime_inner
                    .broker_outer_api()
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
                if broker_runtime_inner
                    .topic_queue_mapping_manager()
                    .replace_cleaned_queue_items(&topic, expected_epoch, expected_bname, &replacements)
                {
                    changed = true;
                }
            }

            tokio::time::sleep(TOPIC_SCAN_YIELD_DELAY).await;
        }

        if changed {
            broker_runtime_inner
                .topic_queue_mapping_manager()
                .persist_clean_result()
                .await?;
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
        let broker_runtime_inner = &self.inner.broker_runtime_inner;
        let current_broker = broker_runtime_inner.broker_config().broker_name().clone();
        let timeout_millis = broker_runtime_inner.broker_config().forward_timeout;
        let snapshot = broker_runtime_inner
            .topic_queue_mapping_manager()
            .snapshot_topic_queue_mapping_table();
        let client_metadata = ClientMetadata::new();
        let mut changed = false;

        for (topic, mapping_detail) in snapshot {
            if !Self::is_current_broker_mapping(&mapping_detail, &current_broker) {
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

            let topic_route_data = match broker_runtime_inner
                .broker_outer_api()
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
                match broker_runtime_inner
                    .broker_outer_api()
                    .get_topic_config_from_broker(&broker_name, &topic, true, timeout_millis)
                    .await
                {
                    Ok(topic_config_mapping) => {
                        if let Some(remote_detail) = topic_config_mapping.topic_queue_mapping_detail {
                            if remote_detail.topic_queue_mapping_info.bname.as_ref() == Some(&broker_name) {
                                mapping_detail_by_broker.insert(broker_name, remote_detail.as_ref().clone());
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
                if broker_runtime_inner
                    .topic_queue_mapping_manager()
                    .remove_cleaned_hosted_queues(&topic, expected_epoch, expected_bname, &expected_items_to_delete)
                {
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
            broker_runtime_inner
                .topic_queue_mapping_manager()
                .persist_clean_result()
                .await?;
        }
        info!("cleanItemListMoreThanSecondGen cost {} ms", start.elapsed().as_millis());
        Ok(changed)
    }

    fn is_time_to_clean(&self) -> bool {
        is_it_time_to_do(&self.inner.broker_runtime_inner.message_store_config().delete_when)
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
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;

    use crate::broker_runtime::BrokerRuntime;

    use super::*;

    type LocalCleanService = TopicQueueMappingCleanService<LocalFileMessageStore>;

    #[test]
    fn should_remove_earliest_item_when_queue_is_empty() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(10);
        offset.set_max_offset(10);

        assert!(LocalCleanService::should_remove_earliest_item(&offset));
    }

    #[test]
    fn should_remove_earliest_item_when_max_offset_is_zero() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(-1);
        offset.set_max_offset(0);

        assert!(LocalCleanService::should_remove_earliest_item(&offset));
    }

    #[test]
    fn should_keep_earliest_item_when_queue_has_messages() {
        let mut offset = TopicOffset::new();
        offset.set_min_offset(10);
        offset.set_max_offset(20);

        assert!(!LocalCleanService::should_remove_earliest_item(&offset));
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

        service.shutdown();
        assert!(!service.is_running());
    }
}
