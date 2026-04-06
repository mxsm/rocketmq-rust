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
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tokio::sync::Notify;

use crate::base::message_store::MessageStore;
use crate::ha::auto_switch::auto_switch_ha_client::AutoSwitchHAClient;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::general_ha_service::HAAckedReplicaSnapshot;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::store_error::HAResult;

#[derive(Default)]
struct SyncStateTracker {
    local_sync_state_set: HashSet<i64>,
    remote_sync_state_set: HashSet<i64>,
    connection_caught_up_time_table: HashMap<i64, u64>,
}

pub struct AutoSwitchHAService {
    delegate: ArcMut<DefaultHAService>,
    message_store: ArcMut<LocalFileMessageStore>,
    is_master: AtomicBool,
    sync_state_tracker: Mutex<SyncStateTracker>,
    is_synchronizing_sync_state_set: AtomicBool,
    local_broker_id: AtomicI64,
}

impl AutoSwitchHAService {
    pub fn new(message_store: ArcMut<LocalFileMessageStore>) -> Self {
        let is_master = message_store.message_store_config_ref().broker_role != BrokerRole::Slave;
        Self {
            delegate: ArcMut::new(DefaultHAService::new(message_store.clone())),
            message_store,
            is_master: AtomicBool::new(is_master),
            sync_state_tracker: Mutex::new(SyncStateTracker::default()),
            is_synchronizing_sync_state_set: AtomicBool::new(false),
            local_broker_id: AtomicI64::new(-1),
        }
    }

    pub(crate) fn try_snapshot_acked_replicas(&self) -> Option<Vec<HAAckedReplicaSnapshot>> {
        self.delegate.try_snapshot_acked_replicas()
    }

    pub(crate) fn init(this: &mut ArcMut<Self>, general_ha_service: GeneralHAService) -> HAResult<()> {
        let mut delegate = this.delegate.clone();
        DefaultHAService::init(&mut delegate, general_ha_service)?;
        delegate.set_auto_switch_service(ArcMut::downgrade(this));
        let client = AutoSwitchHAClient::new(this.message_store.clone(), None)
            .map_err(|error| crate::store_error::HAError::Service(error.to_string()))?;
        delegate.set_general_ha_client(GeneralHAClient::new_with_auto_switch_ha_client(client));
        Ok(())
    }

    async fn clear_master_target(&self) {
        self.delegate.set_ha_client_reported_broker_id(None);
        if let Some(client) = self.delegate.get_ha_client() {
            client.clear_controller_master_target().await;
        }
    }

    async fn update_master_target(&self, new_master_addr: &str) {
        if let Some(client) = self.delegate.get_ha_client() {
            client.sync_controller_master_target(new_master_addr).await;
            client.wakeup().await;
        }
    }

    pub fn sync_controller_sync_state_set(&self, local_broker_id: i64, sync_state_set: &HashSet<i64>) {
        self.set_local_broker_id(local_broker_id);
        self.set_sync_state_set(sync_state_set.clone());
    }

    pub fn set_local_broker_id(&self, local_broker_id: i64) {
        self.local_broker_id.store(local_broker_id, Ordering::SeqCst);
        self.delegate
            .set_ha_client_reported_broker_id((local_broker_id >= 0).then_some(local_broker_id));

        if self.is_master.load(Ordering::SeqCst) && local_broker_id >= 0 {
            let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
            if tracker.local_sync_state_set.is_empty() {
                tracker.local_sync_state_set.insert(local_broker_id);
            }
        }
    }

    pub fn get_local_sync_state_set(&self) -> HashSet<i64> {
        let tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
        tracker.local_sync_state_set.clone()
    }

    pub fn get_sync_state_set(&self) -> HashSet<i64> {
        let tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
        if self.is_synchronizing_sync_state_set.load(Ordering::SeqCst) {
            let mut sync_state_set =
                HashSet::with_capacity(tracker.local_sync_state_set.len() + tracker.remote_sync_state_set.len());
            sync_state_set.extend(tracker.local_sync_state_set.iter().copied());
            sync_state_set.extend(tracker.remote_sync_state_set.iter().copied());
            sync_state_set
        } else {
            tracker.local_sync_state_set.clone()
        }
    }

    pub fn set_sync_state_set(&self, sync_state_set: HashSet<i64>) {
        {
            let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
            self.is_synchronizing_sync_state_set.store(false, Ordering::SeqCst);
            tracker.local_sync_state_set = sync_state_set;
            tracker.remote_sync_state_set.clear();
            tracker.connection_caught_up_time_table.clear();
        }

        let max_phy_offset = self.message_store.get_max_phy_offset();
        let current_confirm_offset = self.message_store.get_commit_log().get_confirm_offset_directly();
        let confirm_offset = self.compute_confirm_offset(current_confirm_offset, max_phy_offset);
        self.message_store
            .clone()
            .mut_from_ref()
            .set_confirm_offset(confirm_offset);
    }

    pub fn is_synchronizing_sync_state_set(&self) -> bool {
        self.is_synchronizing_sync_state_set.load(Ordering::SeqCst)
    }

    pub fn update_connection_last_caught_up_time(&self, slave_broker_id: i64, last_caught_up_time_ms: u64) {
        let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
        let previous = tracker
            .connection_caught_up_time_table
            .get(&slave_broker_id)
            .copied()
            .unwrap_or_default();
        tracker
            .connection_caught_up_time_table
            .insert(slave_broker_id, previous.max(last_caught_up_time_ms));
    }

    pub fn maybe_shrink_sync_state_set(&self) -> HashSet<i64> {
        let local_broker_id = self.local_broker_id.load(Ordering::SeqCst);
        let now = current_millis();
        let timeout_millis = self
            .message_store
            .message_store_config_ref()
            .ha_max_time_slave_not_catchup as u64;

        let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
        let mut next_sync_state_set = tracker.local_sync_state_set.clone();
        let mut changed = false;

        next_sync_state_set.retain(|broker_id| {
            if *broker_id == local_broker_id {
                return true;
            }

            match tracker.connection_caught_up_time_table.get(broker_id).copied() {
                Some(last_caught_up_time) if now.saturating_sub(last_caught_up_time) <= timeout_millis => true,
                _ => {
                    changed = true;
                    false
                }
            }
        });

        if changed {
            self.is_synchronizing_sync_state_set.store(true, Ordering::SeqCst);
            tracker.remote_sync_state_set = next_sync_state_set.clone();
        }

        next_sync_state_set
    }

    pub fn maybe_expand_in_sync_state_set(&self, slave_broker_id: i64, slave_max_offset: i64) -> Option<HashSet<i64>> {
        let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
        if tracker.local_sync_state_set.contains(&slave_broker_id) {
            return None;
        }

        let confirm_offset = self.message_store.get_commit_log().get_confirm_offset_directly();
        if slave_max_offset < confirm_offset {
            return None;
        }

        let mut next_sync_state_set = tracker.local_sync_state_set.clone();
        next_sync_state_set.insert(slave_broker_id);
        self.is_synchronizing_sync_state_set.store(true, Ordering::SeqCst);
        tracker.remote_sync_state_set = next_sync_state_set.clone();
        Some(next_sync_state_set)
    }

    pub fn update_confirm_offset_when_slave_ack(&self, slave_broker_id: i64) {
        if !self.get_local_sync_state_set().contains(&slave_broker_id) {
            return;
        }

        let max_phy_offset = self.message_store.get_max_phy_offset();
        let current_confirm_offset = self.message_store.get_commit_log().get_confirm_offset_directly();
        let confirm_offset = self.compute_confirm_offset(current_confirm_offset, max_phy_offset);
        self.message_store
            .clone()
            .mut_from_ref()
            .set_confirm_offset(confirm_offset);
    }

    pub(crate) fn handle_connection_added(&self, connection: &GeneralHAConnection) {
        let Some(slave_broker_id) = Self::slave_broker_id(connection) else {
            return;
        };

        self.update_connection_last_caught_up_time(slave_broker_id, current_millis());
        let slave_ack_offset = connection.get_slave_ack_offset();
        if slave_ack_offset >= 0 {
            self.handle_connection_ack(connection, slave_ack_offset);
        }
    }

    pub(crate) fn handle_connection_ack(&self, connection: &GeneralHAConnection, slave_ack_offset: i64) {
        let Some(slave_broker_id) = Self::slave_broker_id(connection) else {
            return;
        };

        self.update_connection_last_caught_up_time(slave_broker_id, current_millis());
        let _ = self.maybe_expand_in_sync_state_set(slave_broker_id, slave_ack_offset);
        self.update_confirm_offset_when_slave_ack(slave_broker_id);
    }

    pub(crate) fn handle_connection_caught_up(&self, connection: &GeneralHAConnection) {
        let Some(slave_broker_id) = Self::slave_broker_id(connection) else {
            return;
        };

        self.update_connection_last_caught_up_time(slave_broker_id, current_millis());
    }

    pub(crate) fn handle_connection_removed(&self, connection: &GeneralHAConnection) {
        if self.message_store.is_shutdown() {
            return;
        }

        let Some(slave_broker_id) = Self::slave_broker_id(connection) else {
            return;
        };

        let removed_from_sync_state_set = {
            let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
            let removed_from_sync_state_set = tracker.local_sync_state_set.remove(&slave_broker_id);
            tracker.remote_sync_state_set.remove(&slave_broker_id);
            tracker.connection_caught_up_time_table.remove(&slave_broker_id);
            removed_from_sync_state_set
        };

        if removed_from_sync_state_set {
            let max_phy_offset = self.message_store.get_max_phy_offset();
            let current_confirm_offset = self.message_store.get_commit_log().get_confirm_offset_directly();
            let confirm_offset = self.compute_confirm_offset(current_confirm_offset, max_phy_offset);
            self.message_store
                .clone()
                .mut_from_ref()
                .set_confirm_offset(confirm_offset);
        }
    }

    fn slave_broker_id(connection: &GeneralHAConnection) -> Option<i64> {
        connection
            .is_auto_switch()
            .then(|| connection.slave_broker_id())
            .flatten()
    }

    fn tracked_sync_state_set_size(&self) -> Option<usize> {
        let tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
        if self.is_synchronizing_sync_state_set.load(Ordering::SeqCst) {
            Some(
                tracker
                    .local_sync_state_set
                    .len()
                    .max(tracker.remote_sync_state_set.len()),
            )
        } else if !tracker.local_sync_state_set.is_empty() {
            Some(tracker.local_sync_state_set.len())
        } else {
            None
        }
    }

    pub fn local_sync_state_set_size(&self, master_put_where: i64) -> usize {
        self.tracked_sync_state_set_size()
            .unwrap_or_else(|| self.in_sync_replicas_nums(master_put_where).max(1) as usize)
    }

    pub fn compute_confirm_offset(&self, current_confirm_offset: i64, max_phy_offset: i64) -> i64 {
        let runtime_info = self.get_runtime_info(max_phy_offset);
        let expected_sync_state_set_size = self
            .tracked_sync_state_set_size()
            .unwrap_or_else(|| self.message_store.get_alive_replica_num_in_group().max(1) as usize);
        Self::compute_confirm_offset_from_runtime(
            current_confirm_offset,
            max_phy_offset,
            expected_sync_state_set_size,
            &runtime_info,
        )
    }

    fn compute_confirm_offset_from_runtime(
        current_confirm_offset: i64,
        max_phy_offset: i64,
        expected_sync_state_set_size: usize,
        runtime_info: &HARuntimeInfo,
    ) -> i64 {
        let in_sync_connection_offsets = runtime_info
            .ha_connection_info
            .iter()
            .filter(|connection| connection.in_sync && connection.slave_ack_offset > 0)
            .filter_map(|connection| i64::try_from(connection.slave_ack_offset).ok())
            .collect::<Vec<_>>();

        let candidate_offsets = if in_sync_connection_offsets.is_empty() {
            runtime_info
                .ha_connection_info
                .iter()
                .filter(|connection| connection.slave_ack_offset > 0)
                .filter_map(|connection| i64::try_from(connection.slave_ack_offset).ok())
                .collect::<Vec<_>>()
        } else {
            in_sync_connection_offsets
        };

        let expected_slave_count = expected_sync_state_set_size.saturating_sub(1);
        if expected_slave_count > candidate_offsets.len() {
            return current_confirm_offset;
        }

        candidate_offsets.into_iter().min().unwrap_or(max_phy_offset)
    }

    fn clear_pending_sync_state_tracking(&self) {
        let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
        self.is_synchronizing_sync_state_set.store(false, Ordering::SeqCst);
        tracker.remote_sync_state_set.clear();
        tracker.connection_caught_up_time_table.clear();
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
        self.clear_pending_sync_state_tracking();
        let local_broker_id = self.local_broker_id.load(Ordering::SeqCst);
        if local_broker_id >= 0 {
            let mut tracker = self.sync_state_tracker.lock().expect("lock sync state tracker");
            if tracker.local_sync_state_set.is_empty() {
                tracker.local_sync_state_set.insert(local_broker_id);
            }
        }
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
        let _ = new_master_epoch;
        if let Some(slave_id) = slave_id {
            self.set_local_broker_id(slave_id);
        }
        self.clear_pending_sync_state_tracking();
        self.delegate.destroy_connections().await;
        self.delegate.set_ha_client_reported_broker_id(slave_id);
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
        self.tracked_sync_state_set_size()
            .map(|size| size.max(1) as i32)
            .unwrap_or_else(|| self.message_store.get_alive_replica_num_in_group().max(1))
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
        runtime_info.in_sync_slave_nums = (self.local_sync_state_set_size(master_put_where) as i32 - 1).max(0);
        if runtime_info.master {
            let current_sync_state_set = self.get_local_sync_state_set();
            runtime_info.ha_connection_info = self
                .delegate
                .try_snapshot_connections(master_put_where)
                .into_iter()
                .map(|connection| HAConnectionRuntimeInfo {
                    addr: connection.addr,
                    slave_ack_offset: connection.slave_ack_offset.max(0) as u64,
                    diff: connection.diff,
                    in_sync: connection
                        .slave_broker_id
                        .is_some_and(|slave_broker_id| current_sync_state_set.contains(&slave_broker_id)),
                    transferred_byte_in_second: connection.transferred_byte_in_second.max(0) as u64,
                    transfer_from_where: connection.transfer_from_where.max(0) as u64,
                })
                .collect();
        }
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
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::TimeUtils::current_millis;
    use rocketmq_remoting::protocol::body::ha_client_runtime_info::HAClientRuntimeInfo;
    use rocketmq_remoting::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;

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
            ha_max_time_slave_not_catchup: 1000,
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

    fn new_runtime_info(in_sync_slave_nums: i32, connections: &[(u64, bool)]) -> HARuntimeInfo {
        HARuntimeInfo {
            master: true,
            master_commit_log_max_offset: 0,
            in_sync_slave_nums,
            ha_connection_info: connections
                .iter()
                .enumerate()
                .map(|(index, (slave_ack_offset, in_sync))| HAConnectionRuntimeInfo {
                    addr: format!("127.0.0.1:{}", 10911 + index),
                    slave_ack_offset: *slave_ack_offset,
                    diff: 0,
                    in_sync: *in_sync,
                    transferred_byte_in_second: 0,
                    transfer_from_where: 0,
                })
                .collect(),
            ha_client_runtime_info: HAClientRuntimeInfo::default(),
        }
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

    #[tokio::test]
    async fn sync_controller_sync_state_set_updates_local_membership() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-sync-state-set-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root);
        store.init().await.expect("init message store");

        let mut service = ArcMut::new(AutoSwitchHAService::new(store.clone()));
        let general_service = GeneralHAService::new_with_auto_switch_ha_service(service.clone());
        AutoSwitchHAService::init(&mut service, general_service).expect("init auto switch ha service");

        service.sync_controller_sync_state_set(7, &HashSet::from([7_i64, 9_i64]));

        assert_eq!(service.get_local_sync_state_set(), HashSet::from([7_i64, 9_i64]));
        assert_eq!(service.local_sync_state_set_size(0), 2);
        assert!(!service.is_synchronizing_sync_state_set());

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn compute_confirm_offset_uses_min_in_sync_ack() {
        let runtime_info = new_runtime_info(2, &[(128, true), (96, true), (48, false)]);

        let confirm_offset = AutoSwitchHAService::compute_confirm_offset_from_runtime(-1, 256, 3, &runtime_info);

        assert_eq!(confirm_offset, 96);
    }

    #[test]
    fn compute_confirm_offset_keeps_current_value_when_sync_state_set_exceeds_connections() {
        let runtime_info = new_runtime_info(2, &[(128, true)]);

        let confirm_offset = AutoSwitchHAService::compute_confirm_offset_from_runtime(64, 256, 3, &runtime_info);

        assert_eq!(confirm_offset, 64);
    }

    #[tokio::test]
    async fn maybe_expand_in_sync_state_set_marks_remote_membership() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-expand-sync-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let mut service = ArcMut::new(AutoSwitchHAService::new(store.clone()));
        let general_service = GeneralHAService::new_with_auto_switch_ha_service(service.clone());
        AutoSwitchHAService::init(&mut service, general_service).expect("init auto switch ha service");
        service.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));

        let expanded = service
            .maybe_expand_in_sync_state_set(9, 4)
            .expect("slave should be added to sync state set");

        assert_eq!(expanded, HashSet::from([7_i64, 9_i64]));
        assert_eq!(service.get_sync_state_set(), HashSet::from([7_i64, 9_i64]));
        assert!(service.is_synchronizing_sync_state_set());

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn maybe_shrink_sync_state_set_removes_stale_members() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-shrink-sync-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root);
        store.init().await.expect("init message store");

        let mut service = ArcMut::new(AutoSwitchHAService::new(store.clone()));
        let general_service = GeneralHAService::new_with_auto_switch_ha_service(service.clone());
        AutoSwitchHAService::init(&mut service, general_service).expect("init auto switch ha service");
        service.sync_controller_sync_state_set(7, &HashSet::from([7_i64, 9_i64, 10_i64]));
        service.update_connection_last_caught_up_time(9, current_millis());

        let shrunk = service.maybe_shrink_sync_state_set();

        assert_eq!(shrunk, HashSet::from([7_i64, 9_i64]));
        assert_eq!(service.get_sync_state_set(), HashSet::from([7_i64, 9_i64, 10_i64]));
        assert!(service.is_synchronizing_sync_state_set());

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn sync_controller_sync_state_set_clears_pending_remote_membership() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-sync-clear-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let mut service = ArcMut::new(AutoSwitchHAService::new(store.clone()));
        let general_service = GeneralHAService::new_with_auto_switch_ha_service(service.clone());
        AutoSwitchHAService::init(&mut service, general_service).expect("init auto switch ha service");
        service.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));
        service
            .maybe_expand_in_sync_state_set(9, 4)
            .expect("slave should be added to sync state set");
        assert!(service.is_synchronizing_sync_state_set());

        service.sync_controller_sync_state_set(7, &HashSet::from([7_i64, 9_i64]));

        assert_eq!(service.get_local_sync_state_set(), HashSet::from([7_i64, 9_i64]));
        assert_eq!(service.get_sync_state_set(), HashSet::from([7_i64, 9_i64]));
        assert!(!service.is_synchronizing_sync_state_set());

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn change_to_slave_clears_pending_remote_membership_and_updates_reported_broker_id() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-change-slave-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let mut service = ArcMut::new(AutoSwitchHAService::new(store.clone()));
        let general_service = GeneralHAService::new_with_auto_switch_ha_service(service.clone());
        AutoSwitchHAService::init(&mut service, general_service).expect("init auto switch ha service");
        service.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));
        service
            .maybe_expand_in_sync_state_set(9, 4)
            .expect("slave should be added to sync state set");
        assert!(service.is_synchronizing_sync_state_set());

        service
            .change_to_slave("127.0.0.1:10912", 3, Some(11))
            .await
            .expect("change to slave should succeed");

        assert_eq!(service.get_local_sync_state_set(), HashSet::from([7_i64]));
        assert_eq!(service.get_sync_state_set(), HashSet::from([7_i64]));
        assert!(!service.is_synchronizing_sync_state_set());
        assert_eq!(
            service.get_ha_client().expect("ha client").get_master_address(),
            "127.0.0.1:10912"
        );

        let _ = std::fs::remove_dir_all(temp_root);
    }
}
