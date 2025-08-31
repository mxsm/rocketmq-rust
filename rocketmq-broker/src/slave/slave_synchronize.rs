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
use cheetah_string::CheetahString;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::store_path_config_helper;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

pub(crate) struct SlaveSynchronize<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    master_addr: Option<CheetahString>,
}

impl<MS> SlaveSynchronize<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            broker_runtime_inner,
            master_addr: None,
        }
    }

    pub fn master_addr(&self) -> Option<&CheetahString> {
        self.master_addr.as_ref()
    }

    pub fn set_master_addr(&mut self, addr: impl Into<CheetahString>) {
        let addr = addr.into();
        if let Some(current_addr) = &self.master_addr {
            if current_addr.as_str() == addr {
                return;
            }
        }
        info!(
            "Update master address from {} to {}",
            self.master_addr.as_deref().unwrap_or("None"),
            addr
        );
        self.master_addr = Some(addr);
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
        let master_addr_bak = self.master_addr.clone();
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
            Some(addr) => (true, Some(addr.clone())),
        }
    }

    pub async fn sync_timer_check_point(&self) {
        error!("SlaveSynchronize::sync_timer_check_point is not implemented yet");
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
        // Sync topic config if data version differs
        if self
            .broker_runtime_inner
            .topic_config_manager()
            .data_version_ref()
            != topic_wrapper.topic_config_serialize_wrapper.data_version()
        {
            let mut data_version = self
                .broker_runtime_inner
                .topic_config_manager()
                .data_version();
            data_version
                .assign_new_one(topic_wrapper.topic_config_serialize_wrapper.data_version());

            let new_topic_config_table = topic_wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .clone();
            let topic_config_table = self
                .broker_runtime_inner
                .topic_config_manager()
                .topic_config_table();

            let mut topic_config_table = topic_config_table.lock();

            // Delete entries not in new config
            topic_config_table.retain(|key, _| new_topic_config_table.contains_key(key));

            // Update with new entries
            topic_config_table.extend(new_topic_config_table);
            drop(topic_config_table);
            self.broker_runtime_inner.topic_config_manager().persist();
        }

        // Sync topic queue mapping if present and data version differs
        let new_topic_config_table = topic_wrapper
            .topic_config_serialize_wrapper
            .topic_config_table;
        let version = topic_wrapper.mapping_data_version;
        if version
            != self
                .broker_runtime_inner
                .topic_queue_mapping_manager()
                .data_version()
        {
            self.broker_runtime_inner
                .topic_queue_mapping_manager()
                .data_version_clone()
                .lock()
                .assign_new_one(&version);

            let topic_config_table = self
                .broker_runtime_inner
                .topic_config_manager()
                .topic_config_table();
            let mut topic_config_table = topic_config_table.lock();
            // Delete entries not in new config
            topic_config_table.retain(|key, _| new_topic_config_table.contains_key(key));

            // Update with new entries
            topic_config_table.extend(new_topic_config_table);
            drop(topic_config_table);
            self.broker_runtime_inner
                .topic_queue_mapping_manager()
                .persist();
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
                            let consumer_offset_manager =
                                self.broker_runtime_inner.consumer_offset_manager();
                            consumer_offset_manager
                                .data_version()
                                .assign_new_one(offset_wrapper.data_version());
                            let offset_table = consumer_offset_manager.offset_table();
                            let mut consumer_offset_table = offset_table.write();
                            consumer_offset_table.extend(offset_wrapper.offset_table());
                            drop(consumer_offset_table);
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
                            let file_name = store_path_config_helper::get_delay_offset_store_path(
                                self.broker_runtime_inner
                                    .message_store_config()
                                    .store_path_root_dir
                                    .as_str(),
                            );
                            match string_to_file(offset.as_str(), file_name.as_str()) {
                                Ok(_) => {
                                    if let Err(e) = self
                                        .broker_runtime_inner
                                        .schedule_message_service()
                                        .load_when_sync_delay_offset()
                                    {
                                        error!("LoadWhenSyncDelayOffset error: {:?}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Write delay offset to file error: {:?}", e);
                                }
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
        error!("SlaveSynchronize::sync_subscription_group_config is not implemented yet");
    }

    async fn sync_message_request_mode(&self) {
        error!("SlaveSynchronize::sync_message_request_mode is not implemented yet");
    }

    async fn sync_timer_metrics(&self) {
        error!("SlaveSynchronize::sync_timer_metrics is not implemented yet");
    }
}
