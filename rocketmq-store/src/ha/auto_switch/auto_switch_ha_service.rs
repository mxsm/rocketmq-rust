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
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use tokio::sync::Notify;

use crate::ha::auto_switch::auto_switch_ha_client::AutoSwitchHAClient;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::general_ha_service::GeneralHAServiceReference;
use crate::ha::group_transfer_service::GroupTransferRuntimeInfo;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAAckedReplicaSnapshot;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::store_error::HAResult;
use rocketmq_store_local::ha::replication::EpochTransition;
use rocketmq_store_local::ha::replication::HAReplicaRuntimeSnapshot;
use rocketmq_store_local::ha::replication::ReplicationStateRoot;

pub struct AutoSwitchHAService {
    delegate: Box<DefaultHAService>,
    replication: Arc<ReplicationStateRoot>,
}

impl AutoSwitchHAService {
    pub fn new(delegate: DefaultHAService) -> Self {
        let is_master = delegate.replica_store().message_store_config_ref().broker_role
            != rocketmq_common::common::broker::broker_role::BrokerRole::Slave;
        Self {
            delegate: Box::new(delegate),
            replication: Arc::new(ReplicationStateRoot::new(is_master)),
        }
    }

    pub(crate) fn group_transfer_runtime_info(&self) -> GroupTransferRuntimeInfo {
        self.delegate.group_transfer_runtime_info()
    }

    pub(crate) fn default_delegate(&self) -> &DefaultHAService {
        &self.delegate
    }

    pub(crate) fn init(&mut self, service_reference: GeneralHAServiceReference) -> HAResult<()> {
        let replication = self.replication_state();
        self.delegate.init(service_reference, Some(replication))?;
        let client = self
            .delegate
            .create_default_ha_client()
            .map_err(|error| crate::store_error::HAError::Service(error.to_string()))?;
        let client = AutoSwitchHAClient::from_delegate(client, None);
        self.delegate
            .set_general_ha_client(GeneralHAClient::new_with_auto_switch_ha_client(client));
        Ok(())
    }

    pub(crate) fn replication_state(&self) -> Arc<ReplicationStateRoot> {
        Arc::clone(&self.replication)
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
        self.replication.set_local_broker_id(local_broker_id);
        self.delegate
            .set_ha_client_reported_broker_id((local_broker_id >= 0).then_some(local_broker_id));
    }

    pub fn get_local_sync_state_set(&self) -> HashSet<i64> {
        self.replication.local_sync_state_set()
    }

    pub fn get_sync_state_set(&self) -> HashSet<i64> {
        self.replication.sync_state_set()
    }

    pub fn set_sync_state_set(&self, sync_state_set: HashSet<i64>) {
        self.replication.replace_sync_state_set(sync_state_set);

        let replica_store = self.delegate.replica_store();
        let max_phy_offset = replica_store.get_max_phy_offset();
        let current_confirm_offset = replica_store.get_confirm_offset_directly();
        let confirm_offset = self.compute_confirm_offset(current_confirm_offset, max_phy_offset);
        replica_store.publish_confirm_offset(confirm_offset);
    }

    pub fn is_synchronizing_sync_state_set(&self) -> bool {
        self.replication.is_synchronizing_sync_state_set()
    }

    pub fn update_connection_last_caught_up_time(&self, slave_broker_id: i64, last_caught_up_time_ms: u64) {
        self.replication
            .record_caught_up(slave_broker_id, last_caught_up_time_ms);
    }

    pub fn maybe_shrink_sync_state_set(&self) -> HashSet<i64> {
        let now = current_millis();
        let timeout_millis = self
            .delegate
            .replica_store()
            .message_store_config_ref()
            .ha_max_time_slave_not_catchup as u64;
        self.replication.maybe_shrink_sync_state_set(now, timeout_millis)
    }

    pub fn maybe_expand_in_sync_state_set(&self, slave_broker_id: i64, slave_max_offset: i64) -> Option<HashSet<i64>> {
        let confirm_offset = self.delegate.replica_store().get_confirm_offset_directly();
        self.replication
            .maybe_expand_sync_state_set(slave_broker_id, slave_max_offset, confirm_offset)
    }

    pub fn update_confirm_offset_when_slave_ack(&self, slave_broker_id: i64) {
        if !self.get_local_sync_state_set().contains(&slave_broker_id) {
            return;
        }

        let replica_store = self.delegate.replica_store();
        let max_phy_offset = replica_store.get_max_phy_offset();
        let current_confirm_offset = replica_store.get_confirm_offset_directly();
        let confirm_offset = self.compute_confirm_offset(current_confirm_offset, max_phy_offset);
        replica_store.publish_confirm_offset(confirm_offset);
    }

    fn tracked_sync_state_set_size(&self) -> Option<usize> {
        self.replication.tracked_sync_state_set_size()
    }

    pub fn local_sync_state_set_size(&self, master_put_where: i64) -> usize {
        self.tracked_sync_state_set_size()
            .unwrap_or_else(|| self.in_sync_replicas_nums(master_put_where).max(1) as usize)
    }

    pub fn compute_confirm_offset(&self, current_confirm_offset: i64, max_phy_offset: i64) -> i64 {
        let runtime_info = self.get_runtime_info(max_phy_offset);
        let expected_sync_state_set_size = self
            .tracked_sync_state_set_size()
            .unwrap_or_else(|| self.delegate.replica_store().get_alive_replica_num_in_group().max(1) as usize);
        Self::compute_confirm_offset_from_runtime(
            current_confirm_offset,
            max_phy_offset,
            expected_sync_state_set_size,
            &runtime_info,
        )
    }

    pub(crate) fn compute_confirm_offset_from_runtime(
        current_confirm_offset: i64,
        max_phy_offset: i64,
        expected_sync_state_set_size: usize,
        runtime_info: &HARuntimeInfo,
    ) -> i64 {
        let replicas = runtime_info
            .ha_connection_info
            .iter()
            .filter_map(|connection| {
                i64::try_from(connection.slave_ack_offset)
                    .ok()
                    .map(|slave_ack_offset| HAReplicaRuntimeSnapshot {
                        slave_ack_offset,
                        in_sync: connection.in_sync,
                    })
            })
            .collect::<Vec<_>>();
        rocketmq_store_local::ha::replication::compute_confirm_offset(
            current_confirm_offset,
            max_phy_offset,
            expected_sync_state_set_size,
            &replicas,
        )
    }

    fn clear_pending_sync_state_tracking(&self) {
        self.replication.clear_pending_sync_state_tracking();
    }

    fn apply_epoch_transition(&self, next_epoch: i32) -> bool {
        match self.replication.apply_epoch_transition(next_epoch) {
            EpochTransition::Rejected => return false,
            EpochTransition::Unchanged => {}
            EpochTransition::Advanced { epoch } => {
                let replica_store = self.delegate.replica_store();
                let epoch_start_offset = replica_store
                    .get_max_phy_offset()
                    .max(replica_store.get_min_phy_offset());
                replica_store.publish_state_machine_version(epoch as i64);
                replica_store.publish_controller_epoch_start_offset(epoch_start_offset);
            }
        }
        true
    }

    fn refresh_confirm_offset_after_role_change(&self) {
        let replica_store = self.delegate.replica_store();
        let min_phy_offset = replica_store.get_min_phy_offset();
        let max_phy_offset = replica_store.get_max_phy_offset().max(min_phy_offset);
        let next_confirm_offset = if self.replication.is_master() {
            max_phy_offset
        } else {
            replica_store.get_confirm_offset_directly()
        };
        replica_store.publish_confirm_offset(next_confirm_offset.clamp(min_phy_offset, max_phy_offset));
    }

    pub fn current_master_epoch(&self) -> i32 {
        self.replication.current_master_epoch()
    }
}

impl HAService for AutoSwitchHAService {
    async fn start(&self) -> HAResult<()> {
        self.delegate.start().await
    }

    async fn shutdown(&self) {
        self.delegate.shutdown().await;
    }

    async fn change_to_master(&self, master_epoch: i32) -> HAResult<bool> {
        if !self.apply_epoch_transition(master_epoch) {
            return Ok(false);
        }
        self.replication.set_master(true);
        self.clear_pending_sync_state_tracking();
        self.replication.ensure_local_member();
        self.clear_master_target().await;
        self.refresh_confirm_offset_after_role_change();
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
        if !self.apply_epoch_transition(new_master_epoch) {
            return Ok(false);
        }
        self.replication.set_master(false);
        if let Some(slave_id) = slave_id {
            self.set_local_broker_id(slave_id);
        }
        self.clear_pending_sync_state_tracking();
        self.delegate.destroy_connections().await;
        self.delegate.set_ha_client_reported_broker_id(slave_id);
        self.update_master_target(new_master_addr).await;
        self.refresh_confirm_offset_after_role_change();
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
            .unwrap_or_else(|| self.delegate.replica_store().get_alive_replica_num_in_group().max(1))
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

    async fn snapshot_acked_replicas(&self) -> Vec<HAAckedReplicaSnapshot> {
        self.delegate.snapshot_acked_replicas().await
    }

    async fn connection_state(&self, remote_addr: &str) -> Option<HAConnectionState> {
        self.delegate.connection_state(remote_addr).await
    }

    fn get_ha_client(&self) -> Option<&GeneralHAClient> {
        self.delegate.get_ha_client()
    }

    fn get_push_to_slave_max_offset(&self) -> i64 {
        self.delegate.get_push_to_slave_max_offset()
    }

    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo {
        let mut runtime_info = self.delegate.get_runtime_info(master_put_where);
        runtime_info.master = self.replication.is_master();
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
    use std::time::Duration;

    use rocketmq_common::TimeUtils::current_millis;
    use rocketmq_remoting::protocol::body::ha_client_runtime_info::HAClientRuntimeInfo;
    use rocketmq_remoting::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;

    use super::*;
    use crate::base::message_store::MessageStore;
    use crate::ha::general_ha_service::GeneralHAService;
    use crate::ha::ha_client::HAClient;
    use crate::ha::test_support::new_test_message_store;

    fn new_default_ha_service(
        store: &crate::message_store::local_file_message_store::LocalFileMessageStore,
    ) -> DefaultHAService {
        DefaultHAService::new(store.ha_replica_store_handle())
    }

    fn new_initialized_auto_switch_service(
        store: &crate::message_store::local_file_message_store::LocalFileMessageStore,
    ) -> GeneralHAService {
        let mut service =
            GeneralHAService::new_with_auto_switch_ha_service(AutoSwitchHAService::new(new_default_ha_service(store)));
        service.init().expect("init auto switch ha service");
        service
    }

    fn new_runtime_info(in_sync_slave_nums: i32, connections: &[(u64, bool)]) -> HARuntimeInfo {
        HARuntimeInfo {
            master: true,
            master_commit_log_max_offset: 0,
            in_sync_slave_nums,
            pending_group_transfer_request_count: 0,
            pending_group_transfer_oldest_wait_millis: 0,
            group_transfer_ack_notify_count: 0,
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

    #[test]
    fn constructor_does_not_retain_message_store_root() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-owner-{}", current_millis()));
        let store = new_test_message_store(&temp_root, true);
        let expected_store_path = store.message_store_config().store_path_root_dir.clone();

        let service = AutoSwitchHAService::new(new_default_ha_service(&store));
        drop(store);

        assert_eq!(
            service
                .default_delegate()
                .replica_store()
                .message_store_config()
                .store_path_root_dir,
            expected_store_path
        );

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn production_service_directly_owns_default_delegate() {
        let production = include_str!("auto_switch_ha_service.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source");

        assert!(production.contains("delegate: Box<DefaultHAService>"));
        assert!(!production.contains("delegate: ArcMut<DefaultHAService>"));
        assert!(!production.contains("ArcMut::new(delegate)"));
        assert!(production.contains("fn init(&mut self"));
    }

    #[tokio::test]
    async fn auto_switch_service_initializes_client_and_tracks_sync_state_set_size() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-ha-service-{}", current_millis()));
        let store = new_test_message_store(&temp_root, true);
        store.set_alive_replica_num_in_group(3);

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");

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
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");

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
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");
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
    async fn maybe_expand_in_sync_state_set_does_not_reenter_sync_state_lock() {
        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-rust-auto-switch-expand-no-deadlock-{}",
            current_millis()
        ));
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");
        service.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));

        let service_for_thread = general_service.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let expanded = service_for_thread
                .auto_switch_service()
                .expect("auto switch service")
                .maybe_expand_in_sync_state_set(9, 4);
            let _ = tx.send(expanded);
        });

        let expanded = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("maybe_expand_in_sync_state_set should not deadlock");
        handle.join().expect("maybe_expand thread should finish");

        assert_eq!(expanded, Some(HashSet::from([7_i64, 9_i64])));

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn maybe_shrink_sync_state_set_removes_stale_members() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-shrink-sync-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");
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
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");
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
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");
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
        assert_eq!(service.current_master_epoch(), 3);
        assert_eq!(store.get_state_machine_version(), 3);
        assert_eq!(store.get_controller_epoch_start_offset(), 4);
        assert_eq!(
            service.get_ha_client().expect("ha client").get_master_address(),
            "127.0.0.1:10912"
        );

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn change_to_master_records_epoch_boundary_and_refreshes_confirm_offset() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-change-master-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");
        store.set_confirm_offset(0);

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");

        let switched = service.change_to_master(5).await.expect("change to master");

        assert!(switched);
        assert_eq!(service.current_master_epoch(), 5);
        assert_eq!(store.get_state_machine_version(), 5);
        assert_eq!(store.get_controller_epoch_start_offset(), 4);
        assert_eq!(store.get_confirm_offset(), 4);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn follower_rejects_stale_epoch() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-auto-switch-stale-epoch-{}", current_millis()));
        let mut store = new_test_message_store(&temp_root, true);
        store.init().await.expect("init message store");

        let general_service = new_initialized_auto_switch_service(&store);
        let service = general_service.auto_switch_service().expect("auto switch service");

        assert!(service.change_to_master(6).await.expect("change to master"));
        let stale = service
            .change_to_slave("127.0.0.1:10912", 5, Some(11))
            .await
            .expect("stale change should be handled");

        assert!(!stale);
        assert!(service.get_runtime_info(0).master);
        assert_eq!(service.current_master_epoch(), 6);
        assert_eq!(store.get_state_machine_version(), 6);
        assert_eq!(service.get_ha_client().expect("ha client").get_master_address(), "");

        let _ = std::fs::remove_dir_all(temp_root);
    }
}
