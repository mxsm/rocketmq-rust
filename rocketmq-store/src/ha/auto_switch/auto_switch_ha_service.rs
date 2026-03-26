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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tokio::sync::Notify;

use crate::base::message_store::MessageStore;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::store_error::HAResult;

pub struct AutoSwitchHAService {
    delegate: ArcMut<DefaultHAService>,
    message_store: ArcMut<LocalFileMessageStore>,
    is_master: AtomicBool,
}

impl AutoSwitchHAService {
    pub fn new(message_store: ArcMut<LocalFileMessageStore>) -> Self {
        let is_master = message_store.message_store_config_ref().broker_role != BrokerRole::Slave;
        Self {
            delegate: ArcMut::new(DefaultHAService::new(message_store.clone())),
            message_store,
            is_master: AtomicBool::new(is_master),
        }
    }

    pub(crate) fn init(this: &mut ArcMut<Self>, general_ha_service: GeneralHAService) -> HAResult<()> {
        let mut delegate = this.delegate.clone();
        DefaultHAService::init(&mut delegate, general_ha_service)?;
        delegate.ensure_ha_client()?;
        Ok(())
    }

    async fn clear_master_target(&self) {
        if let Some(client) = self.delegate.get_ha_client() {
            client.close_master().await;
            client.update_master_address("").await;
            client.update_ha_master_address("").await;
        }
    }

    async fn update_master_target(&self, new_master_addr: &str) {
        if let Some(client) = self.delegate.get_ha_client() {
            client.update_master_address(new_master_addr).await;
            client.update_ha_master_address(new_master_addr).await;
            client.wakeup().await;
        }
    }
}

impl HAService for AutoSwitchHAService {
    async fn start(&mut self) -> HAResult<()> {
        self.delegate.as_mut().start().await
    }

    async fn shutdown(&self) {
        self.delegate.shutdown().await;
    }

    async fn change_to_master(&self, master_epoch: i32) -> HAResult<bool> {
        self.is_master.store(true, Ordering::SeqCst);
        let _ = master_epoch;
        self.clear_master_target().await;
        Ok(true)
    }

    async fn change_to_master_when_last_role_is_master(&self, master_epoch: i32) -> HAResult<bool> {
        self.change_to_master(master_epoch).await
    }

    async fn change_to_slave(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
        slave_id: Option<i64>,
    ) -> HAResult<bool> {
        self.is_master.store(false, Ordering::SeqCst);
        let _ = (new_master_epoch, slave_id);
        self.update_master_target(new_master_addr).await;
        Ok(true)
    }

    async fn change_to_slave_when_master_not_change(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
    ) -> HAResult<bool> {
        self.change_to_slave(new_master_addr, new_master_epoch, None).await
    }

    async fn update_master_address(&self, new_addr: &str) {
        self.delegate.update_master_address(new_addr).await;
    }

    async fn update_ha_master_address(&self, new_addr: &str) {
        self.delegate.update_ha_master_address(new_addr).await;
    }

    fn in_sync_replicas_nums(&self, _master_put_where: i64) -> i32 {
        self.message_store.get_alive_replica_num_in_group().max(1)
    }

    fn get_connection_count(&self) -> &AtomicU32 {
        self.delegate.get_connection_count()
    }

    async fn put_request(&self, request: GroupCommitRequest) {
        self.delegate.put_request(request).await;
    }

    async fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest) {
        self.delegate.put_group_connection_state_request(request).await;
    }

    async fn get_connection_list(&self) -> Vec<ArcMut<GeneralHAConnection>> {
        self.delegate.get_connection_list().await
    }

    fn get_ha_client(&self) -> Option<&GeneralHAClient> {
        self.delegate.get_ha_client()
    }

    fn get_ha_client_mut(&mut self) -> Option<&mut GeneralHAClient> {
        self.delegate.as_mut().get_ha_client_mut()
    }

    fn get_push_to_slave_max_offset(&self) -> i64 {
        self.delegate.get_push_to_slave_max_offset()
    }

    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo {
        let mut runtime_info = self.delegate.get_runtime_info(master_put_where);
        runtime_info.master = self.is_master.load(Ordering::SeqCst);
        runtime_info.in_sync_slave_nums = (self.in_sync_replicas_nums(master_put_where) - 1).max(0);
        runtime_info
    }

    fn get_wait_notify_object(&self) -> &Notify {
        self.delegate.get_wait_notify_object()
    }

    async fn is_slave_ok(&self, master_put_where: i64) -> bool {
        self.in_sync_replicas_nums(master_put_where) > 1 || self.delegate.is_slave_ok(master_put_where).await
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
    use crate::ha::ha_client::HAClient;

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
    async fn auto_switch_service_initializes_client_and_tracks_sync_state_set_size() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-ha-service-{}", current_millis()));
        let store = new_test_message_store(&temp_root);
        store.set_alive_replica_num_in_group(3);

        let mut service = ArcMut::new(AutoSwitchHAService::new(store.clone()));
        let general_service = GeneralHAService::new_with_auto_switch_ha_service(service.clone());
        AutoSwitchHAService::init(&mut service, general_service).expect("init auto switch ha service");

        assert!(service.get_ha_client().is_some());
        assert_eq!(service.in_sync_replicas_nums(0), 3);

        service
            .change_to_slave("127.0.0.1:10912", 1, Some(2))
            .await
            .expect("switch to slave");
        let client = service.get_ha_client().expect("ha client");
        assert_eq!(client.get_master_address(), "127.0.0.1:10912");
        assert_eq!(client.get_ha_master_address(), "127.0.0.1:10912");

        service.change_to_master(2).await.expect("switch to master");
        let client = service.get_ha_client().expect("ha client");
        assert_eq!(client.get_master_address(), "");
        assert_eq!(client.get_ha_master_address(), "");

        let _ = std::fs::remove_dir_all(temp_root);
    }
}
