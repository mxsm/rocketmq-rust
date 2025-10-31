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

use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::producer_info::ProducerInfo;
use rocketmq_remoting::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::producer_change_listener::ArcProducerChangeListener;
use crate::client::producer_group_event::ProducerGroupEvent;

const CHANNEL_EXPIRED_TIMEOUT: u64 = 120_000; // 120 seconds
const GET_AVAILABLE_CHANNEL_RETRY_COUNT: u32 = 3; // 120 seconds

pub struct ProducerManager {
    group_channel_table: parking_lot::Mutex<
        HashMap<CheetahString /* group name */, HashMap<Channel, ClientChannelInfo>>,
    >,
    client_channel_table: parking_lot::Mutex<HashMap<CheetahString, Channel /* client ip:port */>>,
    positive_atomic_counter: Arc<AtomicI32>,
    producer_change_listener_vec: Vec<ArcProducerChangeListener>,
    broker_stats_manager: Option<Arc<BrokerStatsManager>>,
}

impl ProducerManager {
    pub fn new() -> Self {
        Self {
            group_channel_table: parking_lot::Mutex::new(HashMap::new()),
            client_channel_table: parking_lot::Mutex::new(HashMap::new()),
            positive_atomic_counter: Arc::new(Default::default()),
            producer_change_listener_vec: vec![],
            broker_stats_manager: None,
        }
    }

    pub fn set_broker_stats_manager(&mut self, broker_stats_manager: Arc<BrokerStatsManager>) {
        self.broker_stats_manager = Some(broker_stats_manager);
    }

    pub fn append_producer_change_listener_vec(
        &mut self,
        producer_change_listener: ArcProducerChangeListener,
    ) {
        self.producer_change_listener_vec
            .push(producer_change_listener);
    }
}

impl ProducerManager {
    /// Get a snapshot of all producer clients organized by group
    ///
    /// Collects information about all currently connected producers and
    /// organizes them by producer group.
    ///
    /// # Returns
    /// A table containing producer information for all connected producers
    pub fn get_producer_table(&self) -> ProducerTableInfo {
        let mut map: HashMap<String, Vec<ProducerInfo>> = HashMap::new();

        // Acquire read lock on group channel table
        let group_channel_table = self.group_channel_table.lock();

        // Iterate over all groups
        for (group, channel_map) in group_channel_table.iter() {
            // Iterate over all channels in this group
            for (_, client_channel_info) in channel_map.iter() {
                // Create producer info from client channel info
                let producer_info = ProducerInfo::new(
                    client_channel_info.client_id().to_string(),
                    client_channel_info.channel().remote_address().to_string(),
                    client_channel_info.language(),
                    client_channel_info.version(),
                    client_channel_info.last_update_timestamp() as i64,
                );

                // Add to map, creating a new vector if this is the first entry for this group
                match map.get_mut(group.as_str()) {
                    Some(producer_list) => {
                        producer_list.push(producer_info);
                    }
                    None => {
                        map.insert(group.to_string(), vec![producer_info]);
                    }
                }
            }
        }

        // Create and return producer table info
        ProducerTableInfo::from(map)
    }

    pub fn group_online(&self, group: String) -> bool {
        let binding = self.group_channel_table.lock();
        let channels = binding.get(group.as_str());
        if channels.is_none() {
            return false;
        }
        !channels.unwrap().is_empty()
    }

    pub fn unregister_producer(
        &self,
        group: &str,
        _client_channel_info: &ClientChannelInfo,
        ctx: &ConnectionHandlerContext,
    ) {
        let mut group_channel_table = self.group_channel_table.lock();
        let channel_table = group_channel_table.get_mut(group);
        if let Some(ct) = channel_table {
            if !ct.is_empty() {
                let old = ct.remove(ctx.channel());
                if old.is_some() {
                    info!("unregister a producer[{}] from groupChannelTable", group);
                    self.call_producer_change_listener(
                        ProducerGroupEvent::ClientUnregister,
                        group,
                        old.as_ref(),
                    );
                }
            }
            if ct.is_empty() {
                let _ = group_channel_table.remove(group);
                self.call_producer_change_listener(
                    ProducerGroupEvent::ClientUnregister,
                    group,
                    None,
                );
                info!(
                    "unregister a producer group[{}] from groupChannelTable",
                    group
                );
            }
        }
    }

    #[allow(clippy::mutable_key_type)]
    pub fn register_producer(
        &self,
        group: &CheetahString,
        client_channel_info: &ClientChannelInfo,
    ) {
        let mut group_channel_table = self.group_channel_table.lock();

        let key = group.clone();
        let channel_table = group_channel_table.entry(key).or_default();

        if let Some(client_channel_info_found) =
            channel_table.get_mut(client_channel_info.channel())
        {
            client_channel_info_found.set_last_update_timestamp(get_current_millis());
            return;
        }

        channel_table.insert(
            client_channel_info.channel().clone(),
            client_channel_info.clone(),
        );

        let mut client_channel_table = self.client_channel_table.lock();
        client_channel_table.insert(
            client_channel_info.client_id().clone(),
            client_channel_info.channel().clone(),
        );
    }

    pub fn find_channel(&self, client_id: &str) -> Option<Channel> {
        self.client_channel_table.lock().get(client_id).cloned()
    }

    pub fn get_available_channel(&self, group: Option<&CheetahString>) -> Option<Channel> {
        let group = group?;
        let group_channel_table = self.group_channel_table.lock();
        let channel_map = group_channel_table.get(group);
        if let Some(channel_map) = channel_map {
            if channel_map.is_empty() {
                return None;
            }
            let channels = channel_map.keys().collect::<Vec<&Channel>>();
            let size = channels.len();
            let index = self
                .positive_atomic_counter
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let mut index = index.unsigned_abs() as usize % size;
            let mut channel = channels[index];
            let mut ok = channel.connection_ref().is_healthy();
            for _ in 0..GET_AVAILABLE_CHANNEL_RETRY_COUNT {
                if ok {
                    return Some(channel.clone());
                }
                index = (index + 1) % size;
                channel = channels[index];
                ok = channel.connection_ref().is_healthy();
            }
            return None;
        }
        None
    }

    pub fn scan_not_active_channel(&self) {
        let mut groups_to_remove = Vec::new();

        let mut group_channel_table = self.group_channel_table.lock();
        for (group, chl_map) in group_channel_table.iter_mut() {
            let mut channels_to_remove = Vec::new();

            {
                for (channel, info) in chl_map.iter() {
                    let diff = get_current_millis() - info.last_update_timestamp();
                    if diff > CHANNEL_EXPIRED_TIMEOUT {
                        channels_to_remove.push((channel.clone(), info.clone()));
                    }
                }
            }

            for (channel, info) in channels_to_remove {
                chl_map.remove(&channel);
                let mut client_channel_table = self.client_channel_table.lock();
                if let Some(channel_in_client_table) = client_channel_table.get(info.client_id()) {
                    if *channel_in_client_table == channel {
                        client_channel_table.remove(info.client_id());
                    }
                }

                warn!(
                    "ProducerManager#scan_not_active_channel: remove expired channel[{}] from \
                     ProducerManager groupChannelTable, producer group name: {}",
                    channel.remote_address(),
                    group
                );
                self.call_producer_change_listener(
                    ProducerGroupEvent::ClientUnregister,
                    group,
                    Some(&info),
                );
            }

            if chl_map.is_empty() {
                groups_to_remove.push(group.clone());
            }
        }

        for group in groups_to_remove {
            group_channel_table.remove(&group);
            warn!(
                "SCAN: remove expired channel from ProducerManager groupChannelTable, all clear, \
                 group={}",
                group
            );
            self.call_producer_change_listener(ProducerGroupEvent::GroupUnregister, &group, None);
        }
    }

    pub fn do_channel_close_event(&self, remote_addr: &str, channel: &Channel) -> bool {
        let mut removed = false;
        let mut group_channel_table = self.group_channel_table.lock();
        let mut groups_to_process = Vec::new();
        for (group, client_channel_info_table) in group_channel_table.iter_mut() {
            if let Some(client_channel_info) = client_channel_info_table.remove(channel) {
                self.client_channel_table
                    .lock()
                    .remove(client_channel_info.client_id());
                removed = true;
                info!(
                    "Channel Close event: remove channel[{}][{}] from ProducerManager \
                     groupChannelTable, producer group: {}",
                    client_channel_info.channel(),
                    remote_addr,
                    group
                );

                self.call_producer_change_listener(
                    ProducerGroupEvent::ClientUnregister,
                    group,
                    Some(&client_channel_info),
                );

                if client_channel_info_table.is_empty() {
                    groups_to_process.push(group.clone());
                }
            }
        }
        for group in groups_to_process {
            if group_channel_table.remove(&group).is_some() {
                info!(
                    "unregister a producer group[{}] from groupChannelTable",
                    group
                );
                self.call_producer_change_listener(
                    ProducerGroupEvent::GroupUnregister,
                    &group,
                    None,
                );
            }
        }
        removed
    }

    fn call_producer_change_listener(
        &self,
        event: ProducerGroupEvent,
        group: &str,
        client_channel_info: Option<&ClientChannelInfo>,
    ) {
        for listener in &self.producer_change_listener_vec {
            listener.handle(event, group, client_channel_info);
        }
    }
}
