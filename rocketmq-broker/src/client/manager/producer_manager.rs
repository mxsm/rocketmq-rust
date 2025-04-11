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
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::error;
use tracing::info;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::producer_change_listener::ArcProducerChangeListener;

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
        client_channel_info: &ClientChannelInfo,
        ctx: &ConnectionHandlerContext,
    ) {
        let mut mutex_guard = self.group_channel_table.lock();
        let channel_table = mutex_guard.get_mut(group);
        if let Some(ct) = channel_table {
            if !ct.is_empty() {
                let old = ct.remove(ctx.channel());
                //let old = ct.remove(client_channel_info.channel());
                if old.is_some() {
                    info!(
                        "unregister a producer[{}] from groupChannelTable {:?}",
                        group, client_channel_info
                    );
                }
            }
            if ct.is_empty() {
                let _ = mutex_guard.remove(group);
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
            let index = self
                .positive_atomic_counter
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let index = index.unsigned_abs() as usize % channels.len();
            let channel = channels[index].clone();
            return Some(channel);
        }
        None
    }

    pub fn scan_not_active_channel(&self) {
        error!("scan_not_active_channel not implemented");
    }

    pub fn do_channel_close_event(&self, _remote_addr: &str, _channel: &Channel) {}
}
