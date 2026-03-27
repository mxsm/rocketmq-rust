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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;

use crate::ha::default_ha_client::DefaultHAClient;
use crate::ha::default_ha_client::HAClientError;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::message_store::local_file_message_store::LocalFileMessageStore;

pub struct AutoSwitchHAClient {
    delegate: ArcMut<DefaultHAClient>,
    reported_broker_id: Arc<AtomicI64>,
    master_address: Arc<Mutex<Option<String>>>,
    ha_master_address: Arc<Mutex<Option<String>>>,
}

impl AutoSwitchHAClient {
    pub fn new(message_store: ArcMut<LocalFileMessageStore>, broker_id: Option<i64>) -> Result<Self, HAClientError> {
        let client = DefaultHAClient::new(message_store)?;
        client.set_reported_broker_id(broker_id);
        Ok(Self {
            delegate: ArcMut::new(client),
            reported_broker_id: Arc::new(AtomicI64::new(broker_id.unwrap_or(-1))),
            master_address: Arc::new(Mutex::new(None)),
            ha_master_address: Arc::new(Mutex::new(None)),
        })
    }

    pub fn set_reported_broker_id(&self, broker_id: Option<i64>) {
        self.reported_broker_id.store(broker_id.unwrap_or(-1), Ordering::SeqCst);
        self.delegate.set_reported_broker_id(broker_id);
    }

    pub fn reported_broker_id(&self) -> Option<i64> {
        match self.reported_broker_id.load(Ordering::SeqCst) {
            id if id >= 0 => Some(id),
            _ => None,
        }
    }

    pub async fn sync_controller_master_target(&self, new_address: &str) {
        {
            let mut master_address = self.master_address.lock().await;
            *master_address = (!new_address.is_empty()).then_some(new_address.to_string());
        }
        {
            let mut ha_master_address = self.ha_master_address.lock().await;
            *ha_master_address = (!new_address.is_empty()).then_some(new_address.to_string());
        }
        self.delegate.update_master_address(new_address).await;
        self.delegate.update_ha_master_address(new_address).await;
    }

    pub async fn clear_controller_master_target(&self) {
        self.delegate.close_master().await;
        self.sync_controller_master_target("").await;
    }
}

impl HAClient for AutoSwitchHAClient {
    async fn start(&mut self) {
        self.delegate.start().await;
    }

    async fn shutdown(&self) {
        self.delegate.shutdown().await;
    }

    async fn wakeup(&self) {
        self.delegate.wakeup().await;
    }

    async fn update_master_address(&self, new_address: &str) {
        {
            let mut master_address = self.master_address.lock().await;
            *master_address = (!new_address.is_empty()).then_some(new_address.to_string());
        }
        self.delegate.update_master_address(new_address).await;
    }

    async fn update_ha_master_address(&self, new_address: &str) {
        {
            let mut ha_master_address = self.ha_master_address.lock().await;
            *ha_master_address = (!new_address.is_empty()).then_some(new_address.to_string());
        }
        self.delegate.update_ha_master_address(new_address).await;
    }

    fn get_master_address(&self) -> String {
        self.master_address
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_else(|| HAClient::get_master_address(&*self.delegate))
    }

    fn get_ha_master_address(&self) -> String {
        self.ha_master_address
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_else(|| HAClient::get_ha_master_address(&*self.delegate))
    }

    fn get_last_read_timestamp(&self) -> i64 {
        HAClient::get_last_read_timestamp(&*self.delegate)
    }

    fn get_last_write_timestamp(&self) -> i64 {
        HAClient::get_last_write_timestamp(&*self.delegate)
    }

    fn get_current_state(&self) -> HAConnectionState {
        HAClient::get_current_state(&*self.delegate)
    }

    fn change_current_state(&self, ha_connection_state: HAConnectionState) {
        self.delegate.change_current_state(ha_connection_state);
    }

    async fn close_master(&self) {
        self.delegate.close_master().await;
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        HAClient::get_transferred_byte_in_second(&*self.delegate)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::TimeUtils::current_millis;

    use super::*;
    use crate::config::message_store_config::MessageStoreConfig;

    fn new_test_message_store(root: &Path) -> ArcMut<LocalFileMessageStore> {
        std::fs::create_dir_all(root).expect("create temp root dir");

        let broker_config = BrokerConfig {
            enable_controller_mode: true,
            ..BrokerConfig::default()
        };

        let message_store_config = MessageStoreConfig {
            enable_controller_mode: true,
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        };

        let topic_table: Arc<DashMap<CheetahString, ArcMut<TopicConfig>>> = Arc::new(DashMap::new());
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(message_store_config),
            Arc::new(broker_config),
            topic_table,
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        store
    }

    #[tokio::test]
    async fn auto_switch_client_tracks_reported_broker_id_and_controller_target() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-client-target-{}", current_millis()));
        let store = new_test_message_store(&temp_root);
        let client = AutoSwitchHAClient::new(store, Some(9)).expect("create auto switch client");

        assert_eq!(client.reported_broker_id(), Some(9));

        client.sync_controller_master_target("127.0.0.1:10912").await;

        assert_eq!(client.get_master_address(), "127.0.0.1:10912");
        assert_eq!(client.get_ha_master_address(), "127.0.0.1:10912");

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn auto_switch_client_clear_controller_target_resets_addresses_but_keeps_broker_identity() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-client-clear-{}", current_millis()));
        let store = new_test_message_store(&temp_root);
        let client = AutoSwitchHAClient::new(store, Some(11)).expect("create auto switch client");

        client.sync_controller_master_target("127.0.0.1:10912").await;
        client.clear_controller_master_target().await;

        assert_eq!(client.reported_broker_id(), Some(11));
        assert_eq!(client.get_master_address(), "");
        assert_eq!(client.get_ha_master_address(), "");

        let _ = std::fs::remove_dir_all(temp_root);
    }
}
