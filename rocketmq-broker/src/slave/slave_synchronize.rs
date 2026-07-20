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

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use cheetah_string::CheetahString;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::schedule::delay_offset_serialize_wrapper::DelayOffsetSerializeWrapper;

pub(crate) struct SlaveSynchronize<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    master_addr: Arc<SlaveMasterAddress>,
}

#[derive(Default)]
pub(crate) struct SlaveMasterAddress {
    current: ArcSwapOption<CheetahString>,
}

impl SlaveMasterAddress {
    pub(crate) fn load(&self) -> Option<Arc<CheetahString>> {
        self.current.load_full()
    }

    pub(crate) fn store(&self, addr: Option<&CheetahString>) {
        let current = self.load();
        if current.as_deref() == addr {
            return;
        }
        info!("Update master address from {:?} to {:?}", current, addr);
        self.current.store(addr.cloned().map(Arc::new));
    }
}

impl<MS> SlaveSynchronize<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
            master_addr: Arc::new(SlaveMasterAddress::default()),
        }
    }

    pub fn master_addr(&self) -> Option<Arc<CheetahString>> {
        self.master_addr.load()
    }

    pub(crate) fn master_addr_handle(&self) -> Arc<SlaveMasterAddress> {
        Arc::clone(&self.master_addr)
    }

    pub fn set_master_addr(&self, addr: Option<&CheetahString>) {
        self.master_addr.store(addr);
    }

    pub async fn sync_all(&self) {
        self.sync_topic_config().await;
        self.sync_consumer_offset().await;
        self.sync_delay_offset().await;
        self.sync_subscription_group_config().await;
        self.sync_message_request_mode().await;
        if self
            .broker_runtime_inner
            .message_store_unchecked()
            .get_message_store_config()
            .timer_wheel_enable
        {
            self.sync_timer_metrics().await;
        }
    }

    fn check_master_addr(&self) -> (bool, Option<CheetahString>) {
        let master_addr_bak = self.master_addr();
        match &master_addr_bak {
            None => {
                warn!("Master address is not set");
                (false, None)
            }
            Some(addr) if addr.as_str() == self.broker_runtime_inner.get_broker_addr() => {
                warn!(
                    "Master address is the same as broker address: {}",
                    self.broker_runtime_inner.get_broker_addr()
                );
                (false, None)
            }
            Some(addr) => (true, Some(addr.as_ref().clone())),
        }
    }

    pub async fn sync_timer_check_point(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if !flag {
            return;
        }

        if let Some(master_addr) = master_addr {
            let Some(timer_message_store) = self.broker_runtime_inner.timer_message_store() else {
                return;
            };
            if timer_message_store.is_should_running_dequeue() {
                return;
            }

            match self
                .broker_runtime_inner
                .broker_outer_api()
                .get_timer_check_point(&master_addr)
                .await
            {
                Ok(Some(checkpoint_snapshot)) => {
                    match timer_message_store.sync_checkpoint_from_master(&checkpoint_snapshot) {
                        Ok(true) => {
                            info!("Update slave timer checkpoint from master, {}", master_addr);
                        }
                        Ok(false) => {
                            warn!("Local timer checkpoint is not initialized, {}", master_addr);
                        }
                        Err(e) => {
                            error!("Persist synced timer checkpoint error, {}: {:?}", master_addr, e);
                        }
                    }
                }
                Ok(None) => {
                    warn!("GetTimerCheckPoint return null, {}", master_addr);
                }
                Err(e) => {
                    error!("SyncTimerCheckPoint Exception, {}: {:?}", master_addr, e);
                }
            }
        }
    }

    async fn sync_topic_config(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self.sync_topic_config_internal(&master_addr).await {
                    Ok(_) => {
                        info!("Update slave topic config from master, {}", master_addr);
                    }
                    Err(e) => {
                        error!("SyncTopicConfig Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_topic_config_internal(&self, master_addr: &CheetahString) -> RocketMQResult<()> {
        let topic_wrapper = self
            .broker_runtime_inner
            .broker_outer_api()
            .get_all_topic_config(master_addr)
            .await?;
        if topic_wrapper.is_none() {
            warn!("GetAllTopicConfig return null, {}", master_addr);
            return Ok(());
        }

        let topic_wrapper = topic_wrapper.unwrap();
        let topic_config_manager = self.broker_runtime_inner.topic_config_manager();
        if topic_config_manager.replace_topic_config_table_from_master(
            topic_wrapper.topic_config_serialize_wrapper.topic_config_table.clone(),
            topic_wrapper.topic_config_serialize_wrapper.data_version(),
        ) {
            self.broker_runtime_inner
                .topic_config_coordinator()
                .persist_and_wait()
                .await?;
        }

        // Sync topic queue mapping if present and data version differs
        let version = topic_wrapper.mapping_data_version;
        if version != self.broker_runtime_inner.topic_queue_mapping_manager().data_version() {
            self.broker_runtime_inner
                .topic_queue_mapping_manager()
                .data_version_clone()
                .lock()
                .assign_new_one(&version);

            self.broker_runtime_inner.topic_queue_mapping_manager().persist();
        }
        Ok(())
    }

    async fn sync_consumer_offset(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .get_all_consumer_offset(&master_addr)
                    .await
                {
                    Ok(offset_wrapper) => {
                        if let Some(offset_wrapper) = offset_wrapper {
                            let consumer_offset_manager = self.broker_runtime_inner.consumer_offset_manager();
                            let data_version = offset_wrapper.data_version().clone();
                            consumer_offset_manager
                                .merge_offsets_from_peer(offset_wrapper.offset_table(), data_version);
                            consumer_offset_manager.persist();
                            info!("Update slave consumer offset from master, {}", master_addr);
                        } else {
                            warn!("GetAllConsumerOffset return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncConsumerOffset Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_delay_offset(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .get_delay_offset(&master_addr)
                    .await
                {
                    Ok(offset) => {
                        if let Some(offset) = offset {
                            let snapshot = match SerdeJsonUtils::from_json_str::<DelayOffsetSerializeWrapper>(&offset) {
                                Ok(snapshot) => snapshot,
                                Err(error) => {
                                    error!("Decode delay offset failed, {}: {:?}", master_addr, error);
                                    return;
                                }
                            };
                            if let Err(e) = self
                                .broker_runtime_inner
                                .schedule_message_service()
                                .sync_delay_offset_from_peer(offset.as_str(), &snapshot)
                                .await
                            {
                                error!("Sync delay offset from peer error: {:?}", e);
                            }
                        } else {
                            warn!("GetDelayOffset return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncDelayOffset Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_subscription_group_config(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .get_all_subscription_group_config(&master_addr)
                    .await
                {
                    Ok(subscription_wrapper) => {
                        if let Some(subscription_wrapper) = subscription_wrapper {
                            let subscription_group_manager = self
                                .broker_runtime_inner
                                .mut_from_ref()
                                .subscription_group_manager_mut();

                            // Compare data versions using read locks
                            let current_version = subscription_group_manager.data_version().read().clone();
                            if current_version != *subscription_wrapper.data_version() {
                                // Update data version
                                *subscription_group_manager.data_version().write() =
                                    subscription_wrapper.data_version().clone();

                                let new_subscription_table = subscription_wrapper.subscription_group_table;
                                let subscription_table = subscription_group_manager.subscription_group_table();

                                // Clear and update subscription table using DashMap
                                subscription_table.clear();
                                for (key, value) in new_subscription_table {
                                    subscription_table.insert(key, value);
                                }
                                subscription_group_manager.persist();
                            }
                            info!("Update slave subscription group config from master, {}", master_addr);
                        } else {
                            warn!("GetAllSubscriptionGroupConfig return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncSubscriptionGroupConfig Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_message_request_mode(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if flag {
            if let Some(master_addr) = master_addr {
                match self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .get_message_request_mode(&master_addr)
                    .await
                {
                    Ok(mode) => {
                        if let Some(mode) = mode {
                            let query_assignment_processor =
                                self.broker_runtime_inner.query_assignment_processor_unchecked();
                            let message_request_mode_manager =
                                query_assignment_processor.message_request_mode_manager();
                            let message_request_mode_map = message_request_mode_manager.message_request_mode_map();
                            let mut message_request_mode_map = message_request_mode_map.lock();
                            message_request_mode_map.clear();
                            message_request_mode_map.extend(mode.into_inner());
                            drop(message_request_mode_map);
                            message_request_mode_manager.persist();
                        } else {
                            warn!("GetMessageRequestMode return null, {}", master_addr);
                        }
                    }
                    Err(e) => {
                        error!("SyncMessageRequestMode Exception, {}: {:?}", master_addr, e);
                    }
                }
            }
        }
    }

    async fn sync_timer_metrics(&self) {
        let (flag, master_addr) = self.check_master_addr();
        if !flag {
            return;
        }

        if let Some(master_addr) = master_addr {
            let Some(timer_message_store) = self.broker_runtime_inner.timer_message_store() else {
                return;
            };

            match self
                .broker_runtime_inner
                .broker_outer_api()
                .get_timer_metrics(&master_addr)
                .await
            {
                Ok(Some(metrics_wrapper)) => {
                    if timer_message_store.timer_metrics.data_version() != *metrics_wrapper.data_version() {
                        timer_message_store.timer_metrics.apply_wrapper(metrics_wrapper);
                        timer_message_store.timer_metrics.persist();
                    }
                    info!("Update slave timer metrics from master, {}", master_addr);
                }
                Ok(None) => {
                    warn!("GetTimerMetrics return null, {}", master_addr);
                }
                Err(e) => {
                    error!("SyncTimerMetrics Exception, {}: {:?}", master_addr, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::SlaveMasterAddress;

    #[test]
    fn slave_master_address_publishes_immutable_generations() {
        let address = SlaveMasterAddress::default();
        let first = CheetahString::from_static_str("127.0.0.1:10911");
        let second = CheetahString::from_static_str("127.0.0.2:10911");

        address.store(Some(&first));
        let first_generation = address.load().expect("first generation");
        address.store(Some(&second));

        assert_eq!(first_generation.as_str(), first.as_str());
        assert_eq!(address.load().as_deref(), Some(&second));
        address.store(None);
        assert!(address.load().is_none());
    }
}
