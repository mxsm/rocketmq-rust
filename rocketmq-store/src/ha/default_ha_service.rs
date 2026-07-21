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
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_remoting::protocol::body::ha_client_runtime_info::HAClientRuntimeInfo;
use rocketmq_remoting::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::auto_switch::auto_switch_ha_connection::AutoSwitchHAConnection;
use crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService;
use crate::ha::default_ha_client::DefaultHAClient;
use crate::ha::default_ha_client::HAClientError;
use crate::ha::default_ha_connection::DefaultHAConnection;
use crate::ha::default_ha_connection::HAConnectionRuntimeHandle;
use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::group_transfer_service::GroupTransferRuntimeInfo;
use crate::ha::group_transfer_service::GroupTransferService;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_connection_state_notification_service::HAConnectionStateNotificationService;
use crate::ha::ha_service::HAAckedReplicaSnapshot;
use crate::ha::ha_service::HAService;
use crate::ha::transfer_metrics::HaTransferMetrics;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::store_error::HAError;
use crate::store_error::HAResult;
use rocketmq_store_local::ha::replication::ReplicationProgress;

type AutoSwitchReplicationState = rocketmq_store_local::ha::replication::ReplicationStateRoot;

#[derive(Clone, Debug)]
pub(crate) struct HAConnectionRuntimeSnapshot {
    pub addr: String,
    pub slave_ack_offset: i64,
    pub diff: i64,
    pub transferred_byte_in_second: i64,
    pub transfer_from_where: i64,
    pub slave_broker_id: Option<i64>,
}

pub struct DefaultHAService {
    connection_count: Arc<AtomicU32>,
    connections: Arc<Mutex<HashMap<HAConnectionId, ArcMut<GeneralHAConnection>>>>,
    accept_socket_service: Option<AcceptSocketService>,
    default_message_store: ArcMut<LocalFileMessageStore>,
    wait_notify_object: Arc<Notify>,
    replication_progress: ReplicationProgress,
    group_transfer_service: Option<GroupTransferService>,
    ha_client: Option<GeneralHAClient>,
    ha_connection_state_notification_service: Option<HAConnectionStateNotificationService>,
    auto_switch_replication: Option<Arc<AutoSwitchReplicationState>>,
    ha_transfer_metrics: Arc<HaTransferMetrics>,
}

impl DefaultHAService {
    pub fn new(message_store: ArcMut<LocalFileMessageStore>) -> Self {
        DefaultHAService {
            connection_count: Arc::new(AtomicU32::new(0)),
            connections: Arc::new(Mutex::new(HashMap::new())),
            accept_socket_service: None,
            default_message_store: message_store,
            wait_notify_object: Arc::new(Notify::new()),
            replication_progress: ReplicationProgress::default(),
            group_transfer_service: None,
            ha_client: None,
            ha_connection_state_notification_service: None,
            auto_switch_replication: None,
            ha_transfer_metrics: Arc::new(HaTransferMetrics::default()),
        }
    }

    pub fn get_default_message_store(&self) -> &LocalFileMessageStore {
        self.default_message_store.as_ref()
    }

    pub fn ha_transfer_metrics(&self) -> Arc<HaTransferMetrics> {
        self.ha_transfer_metrics.clone()
    }

    pub(crate) fn group_transfer_runtime_info(&self) -> GroupTransferRuntimeInfo {
        self.group_transfer_service
            .as_ref()
            .map_or_else(GroupTransferRuntimeInfo::default, GroupTransferService::runtime_info)
    }

    pub(crate) fn ensure_ha_client(&mut self) -> HAResult<bool> {
        if self.ha_client.is_some() {
            return Ok(false);
        }

        let client = self
            .create_default_ha_client()
            .map_err(|e| HAError::Service(format!("Failed to create DefaultHAClient: {e}")))?;
        self.ha_client = Some(GeneralHAClient::new_with_default_ha_client(client));
        Ok(true)
    }

    pub(crate) fn create_default_ha_client(&self) -> Result<DefaultHAClient, HAClientError> {
        DefaultHAClient::new(self.default_message_store.clone())
    }

    pub(crate) fn set_ha_client_reported_broker_id(&self, broker_id: Option<i64>) {
        if let Some(client) = &self.ha_client {
            client.set_reported_broker_id(broker_id);
        }
    }

    pub(crate) fn set_general_ha_client(&mut self, ha_client: GeneralHAClient) {
        self.ha_client = Some(ha_client);
    }

    pub async fn notify_transfer_some(&self, offset: i64) {
        self.replication_progress.record_ack(offset);

        if let Some(service) = &self.group_transfer_service {
            service.notify_transfer_some();
        }
    }

    pub(crate) fn init(this: &mut ArcMut<Self>, general_ha_service: GeneralHAService) -> HAResult<()> {
        // Initialize the DefaultHAService with the provided message store.
        let config = this.default_message_store.get_message_store_config();
        let is_auto_switch = general_ha_service.is_auto_switch_enabled();
        this.auto_switch_replication = match &general_ha_service {
            GeneralHAService::AutoSwitchHAService(auto_switch_service) => Some(auto_switch_service.replication_state()),
            GeneralHAService::DefaultHAService(_) => None,
        };

        let group_transfer_service = GroupTransferService::new(general_ha_service.clone());
        this.group_transfer_service = Some(group_transfer_service);

        if config.broker_role == BrokerRole::Slave && !is_auto_switch {
            this.ensure_ha_client()?;
        }

        let state_notification_service =
            HAConnectionStateNotificationService::new(general_ha_service, Arc::clone(&config));
        this.ha_connection_state_notification_service = Some(state_notification_service);

        let arc_mut = this.clone();
        this.accept_socket_service = Some(AcceptSocketService::new(
            this.default_message_store.get_message_store_config(),
            arc_mut,
            is_auto_switch,
        ));
        Ok(())
    }

    pub async fn add_connection(&self, connection: ArcMut<GeneralHAConnection>) {
        // Add a new connection to the service
        let mut connections = self.connections.lock().await;
        connections.insert(connection.get_ha_connection_id().clone(), connection.clone());
        drop(connections);

        self.handle_connection_added(connection.as_ref());
    }

    pub async fn remove_connection(&self, connection: ArcMut<GeneralHAConnection>) {
        self.handle_connection_removed(connection.as_ref());

        if let Some(ha_connection_state_notification_service) = &self.ha_connection_state_notification_service {
            let remote_addr = connection.remote_address();
            let connection_state = connection.get_current_state().await;
            let _ = ha_connection_state_notification_service
                .check_connection_state_and_notify(&remote_addr, connection_state)
                .await;
        }

        let mut connections = self.connections.lock().await;
        connections.remove(connection.get_ha_connection_id());
    }

    pub(crate) async fn remove_runtime_connection(&self, connection: &HAConnectionRuntimeHandle) {
        self.handle_runtime_connection_removed(connection);

        if let Some(ha_connection_state_notification_service) = &self.ha_connection_state_notification_service {
            let connection_state = connection.current_state().await;
            let _ = ha_connection_state_notification_service
                .check_connection_state_and_notify(connection.remote_address(), connection_state)
                .await;
        }

        let mut connections = self.connections.lock().await;
        connections.remove(connection.connection_id());
    }

    pub async fn destroy_connections(&self) {
        let connections = {
            let mut connections = self.connections.lock().await;
            connections
                .drain()
                .map(|(_, connection)| connection)
                .collect::<Vec<_>>()
        };
        for mut connection in connections {
            let connection_id = connection.get_ha_connection_id().to_owned();
            if timeout(Duration::from_secs(3), connection.shutdown()).await.is_err() {
                warn!("Timed out shutting down HA connection {}", connection_id);
            }
        }
    }

    pub(crate) fn try_snapshot_connections(&self, master_put_where: i64) -> Vec<HAConnectionRuntimeSnapshot> {
        self.connections
            .try_lock()
            .map(|connections| {
                connections
                    .values()
                    .map(|connection| {
                        let slave_ack_offset = connection.get_slave_ack_offset();
                        HAConnectionRuntimeSnapshot {
                            addr: connection.remote_address(),
                            slave_ack_offset,
                            diff: master_put_where.saturating_sub(slave_ack_offset),
                            transferred_byte_in_second: connection.get_transferred_byte_in_second(),
                            transfer_from_where: connection.get_transfer_from_where(),
                            slave_broker_id: connection.slave_broker_id(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn handle_connection_added(&self, connection: &GeneralHAConnection) {
        if let Some(replication) = self.auto_switch_replication.as_deref() {
            self.handle_auto_switch_connection_added(
                replication,
                Self::auto_switch_slave_broker_id(connection),
                connection.get_slave_ack_offset(),
            );
        }
    }

    pub(crate) fn handle_connection_ack(&self, connection: &GeneralHAConnection, slave_ack_offset: i64) {
        if let Some(replication) = self.auto_switch_replication.as_deref() {
            self.handle_auto_switch_connection_ack(
                replication,
                Self::auto_switch_slave_broker_id(connection),
                slave_ack_offset,
            );
        }
    }

    pub(crate) fn handle_connection_caught_up(&self, connection: &GeneralHAConnection) {
        if let Some(replication) = self.auto_switch_replication.as_deref() {
            Self::handle_auto_switch_connection_caught_up(replication, Self::auto_switch_slave_broker_id(connection));
        }
    }

    pub(crate) fn handle_connection_removed(&self, connection: &GeneralHAConnection) {
        if let Some(replication) = self.auto_switch_replication.as_deref() {
            self.handle_auto_switch_connection_removed(replication, Self::auto_switch_slave_broker_id(connection));
        }
    }

    pub(crate) fn handle_runtime_connection_ack(&self, connection: &HAConnectionRuntimeHandle, slave_ack_offset: i64) {
        if let Some(replication) = self.auto_switch_replication.as_deref() {
            self.handle_auto_switch_connection_ack(replication, connection.slave_broker_id(), slave_ack_offset);
        }
    }

    pub(crate) fn handle_runtime_connection_caught_up(&self, connection: &HAConnectionRuntimeHandle) {
        if let Some(replication) = self.auto_switch_replication.as_deref() {
            Self::handle_auto_switch_connection_caught_up(replication, connection.slave_broker_id());
        }
    }

    fn handle_runtime_connection_removed(&self, connection: &HAConnectionRuntimeHandle) {
        if let Some(replication) = self.auto_switch_replication.as_deref() {
            self.handle_auto_switch_connection_removed(replication, connection.slave_broker_id());
        }
    }

    fn handle_auto_switch_connection_added(
        &self,
        replication: &AutoSwitchReplicationState,
        slave_broker_id: Option<i64>,
        slave_ack_offset: i64,
    ) {
        let Some(slave_broker_id) = slave_broker_id else {
            return;
        };

        replication.record_caught_up(slave_broker_id, rocketmq_common::TimeUtils::current_millis());
        if slave_ack_offset >= 0 {
            self.handle_auto_switch_connection_ack(replication, Some(slave_broker_id), slave_ack_offset);
        }
    }

    fn handle_auto_switch_connection_ack(
        &self,
        replication: &AutoSwitchReplicationState,
        slave_broker_id: Option<i64>,
        slave_ack_offset: i64,
    ) {
        let Some(slave_broker_id) = slave_broker_id else {
            return;
        };

        replication.record_caught_up(slave_broker_id, rocketmq_common::TimeUtils::current_millis());
        let current_confirm_offset = self
            .default_message_store
            .get_commit_log()
            .get_confirm_offset_directly();
        let _ = replication.maybe_expand_sync_state_set(slave_broker_id, slave_ack_offset, current_confirm_offset);
        if replication.local_sync_state_set().contains(&slave_broker_id) {
            self.publish_auto_switch_confirm_offset(replication);
        }
    }

    fn handle_auto_switch_connection_caught_up(replication: &AutoSwitchReplicationState, slave_broker_id: Option<i64>) {
        if let Some(slave_broker_id) = slave_broker_id {
            replication.record_caught_up(slave_broker_id, rocketmq_common::TimeUtils::current_millis());
        }
    }

    fn handle_auto_switch_connection_removed(
        &self,
        replication: &AutoSwitchReplicationState,
        slave_broker_id: Option<i64>,
    ) {
        if self.default_message_store.is_shutdown() {
            return;
        }

        let Some(slave_broker_id) = slave_broker_id else {
            return;
        };

        if replication.remove_replica(slave_broker_id) {
            self.publish_auto_switch_confirm_offset(replication);
        }
    }

    fn publish_auto_switch_confirm_offset(&self, replication: &AutoSwitchReplicationState) {
        let max_phy_offset = self.default_message_store.get_max_phy_offset();
        let current_confirm_offset = self
            .default_message_store
            .get_commit_log()
            .get_confirm_offset_directly();
        let runtime_info = self.get_runtime_info(max_phy_offset);
        let expected_sync_state_set_size = replication
            .tracked_sync_state_set_size()
            .unwrap_or_else(|| self.default_message_store.get_alive_replica_num_in_group().max(1) as usize);
        let confirm_offset = AutoSwitchHAService::compute_confirm_offset_from_runtime(
            current_confirm_offset,
            max_phy_offset,
            expected_sync_state_set_size,
            &runtime_info,
        );
        self.default_message_store.publish_confirm_offset(confirm_offset);
    }

    fn auto_switch_slave_broker_id(connection: &GeneralHAConnection) -> Option<i64> {
        connection
            .is_auto_switch()
            .then(|| connection.slave_broker_id())
            .flatten()
    }
}

impl HAService for DefaultHAService {
    async fn start(&mut self) -> HAResult<()> {
        self.accept_socket_service
            .as_mut()
            .ok_or_else(|| HAError::Service("AcceptSocketService not initialized".to_string()))?
            .start()
            .await?;
        self.group_transfer_service
            .as_mut()
            .ok_or_else(|| HAError::Service("GroupTransferService not initialized".to_string()))?
            .start()
            .await?;
        self.ha_connection_state_notification_service
            .as_mut()
            .ok_or_else(|| HAError::Service("HAConnectionStateNotificationService not initialized".to_string()))?
            .start()
            .await?;
        if let Some(ref mut ha_client) = self.ha_client {
            ha_client.start().await;
        }
        Ok(())
    }

    async fn shutdown(&self) {
        info!("Shutting down DefaultHAService");

        if let Some(ref ha_client) = self.ha_client {
            if timeout(Duration::from_secs(3), ha_client.shutdown()).await.is_err() {
                warn!("Timed out shutting down HA client");
            }
        }

        if let Some(ref accept_socket_service) = self.accept_socket_service {
            if timeout(Duration::from_secs(3), accept_socket_service.shutdown())
                .await
                .is_err()
            {
                warn!("Timed out shutting down HA accept socket service");
            }
        }
        if timeout(Duration::from_secs(3), self.destroy_connections())
            .await
            .is_err()
        {
            warn!("Timed out destroying HA connections");
        }

        if let Some(ref group_transfer_service) = self.group_transfer_service {
            if timeout(Duration::from_secs(3), group_transfer_service.shutdown())
                .await
                .is_err()
            {
                warn!("Timed out shutting down HA group transfer service");
            }
        }

        if let Some(ref ha_connection_state_notification_service) = self.ha_connection_state_notification_service {
            if timeout(
                Duration::from_secs(3),
                ha_connection_state_notification_service.shutdown(),
            )
            .await
            .is_err()
            {
                warn!("Timed out shutting down HA connection state notification service");
            }
        }
    }

    async fn change_to_master(&self, master_epoch: i32) -> HAResult<bool> {
        Ok(false)
    }

    async fn change_to_master_when_last_role_is_master(&self, master_epoch: i32) -> HAResult<bool> {
        Ok(false)
    }

    async fn change_to_slave(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
        slave_id: Option<i64>,
    ) -> HAResult<bool> {
        Ok(false)
    }

    async fn change_to_slave_when_master_not_change(
        &self,
        new_master_addr: &str,
        new_master_epoch: i32,
    ) -> HAResult<bool> {
        Ok(false)
    }

    async fn update_master_address(&self, new_addr: &str) {
        if let Some(ref ha_client) = self.ha_client {
            ha_client.update_master_address(new_addr).await;
        } else {
            error!("No HAClient initialized to update master address");
        }
    }

    async fn update_ha_master_address(&self, new_addr: &str) {
        if let Some(ref ha_client) = self.ha_client {
            ha_client.update_ha_master_address(new_addr).await;
        } else {
            error!("No HAClient initialized to update HA master address");
        }
    }

    fn in_sync_replicas_nums(&self, master_put_where: i64) -> i32 {
        1 + self
            .try_snapshot_connections(master_put_where)
            .into_iter()
            .filter(|connection| connection.slave_ack_offset >= master_put_where)
            .count() as i32
    }

    fn get_connection_count(&self) -> &AtomicU32 {
        self.connection_count.as_ref()
    }

    async fn put_request(&self, request: GroupCommitRequest) {
        if let Some(ref group_transfer_service) = self.group_transfer_service {
            group_transfer_service.put_request(request).await;
        } else {
            error!("No GroupTransferService initialized to put request");
        }
    }

    async fn put_group_connection_state_request(&self, request: HAConnectionStateNotificationRequest) {
        if let Some(ref ha_connection_state_notification_service) = self.ha_connection_state_notification_service {
            ha_connection_state_notification_service.set_request(request).await;
        } else {
            error!("No HAConnectionStateNotificationService initialized to put state request");
        }
    }

    async fn snapshot_acked_replicas(&self) -> Vec<HAAckedReplicaSnapshot> {
        let connections = self.connections.lock().await;
        connections
            .values()
            .map(|connection| HAAckedReplicaSnapshot {
                slave_broker_id: connection.slave_broker_id(),
                slave_ack_offset: connection.get_slave_ack_offset(),
            })
            .collect()
    }

    async fn connection_state(&self, remote_addr: &str) -> Option<HAConnectionState> {
        let connection = {
            let connections = self.connections.lock().await;
            connections
                .values()
                .find(|connection| connection.remote_address() == remote_addr)
                .cloned()
        };
        match connection {
            Some(connection) => Some(connection.get_current_state().await),
            None => None,
        }
    }

    fn get_ha_client(&self) -> Option<&GeneralHAClient> {
        self.ha_client.as_ref()
    }

    fn get_ha_client_mut(&mut self) -> Option<&mut GeneralHAClient> {
        self.ha_client.as_mut()
    }

    fn get_push_to_slave_max_offset(&self) -> i64 {
        self.replication_progress.max_ack_offset()
    }

    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo {
        let mut runtime_info = HARuntimeInfo {
            master: self.default_message_store.message_store_config_ref().broker_role != BrokerRole::Slave,
            master_commit_log_max_offset: master_put_where.max(0) as u64,
            in_sync_slave_nums: (self.in_sync_replicas_nums(master_put_where) - 1).max(0),
            pending_group_transfer_request_count: 0,
            pending_group_transfer_oldest_wait_millis: 0,
            group_transfer_ack_notify_count: 0,
            ha_connection_info: Vec::new(),
            ha_client_runtime_info: HAClientRuntimeInfo::default(),
        };

        runtime_info.ha_connection_info = self
            .try_snapshot_connections(master_put_where)
            .into_iter()
            .map(|connection| HAConnectionRuntimeInfo {
                addr: connection.addr,
                slave_ack_offset: connection.slave_ack_offset.max(0) as u64,
                diff: connection.diff,
                in_sync: connection.slave_ack_offset >= master_put_where,
                transferred_byte_in_second: connection.transferred_byte_in_second.max(0) as u64,
                transfer_from_where: connection.transfer_from_where.max(0) as u64,
            })
            .collect();

        if let Some(ha_client) = &self.ha_client {
            runtime_info.ha_client_runtime_info = HAClientRuntimeInfo {
                master_addr: ha_client.get_ha_master_address(),
                transferred_byte_in_second: ha_client.get_transferred_byte_in_second().max(0) as u64,
                max_offset: self.default_message_store.get_max_phy_offset().max(0) as u64,
                last_read_timestamp: ha_client.get_last_read_timestamp().max(0) as u64,
                last_write_timestamp: ha_client.get_last_write_timestamp().max(0) as u64,
                master_flush_offset: self.default_message_store.get_master_flushed_offset().max(0) as u64,
                is_activated: ha_client.get_current_state().is_active(),
            };
        }

        if let Some(group_transfer_service) = &self.group_transfer_service {
            let group_transfer_runtime_info = group_transfer_service.runtime_info();
            runtime_info.pending_group_transfer_request_count = group_transfer_runtime_info.pending_request_count;
            runtime_info.pending_group_transfer_oldest_wait_millis =
                group_transfer_runtime_info.pending_request_oldest_wait_millis;
            runtime_info.group_transfer_ack_notify_count = group_transfer_runtime_info.ack_notify_count;
        }

        runtime_info
    }

    fn get_wait_notify_object(&self) -> &Notify {
        self.wait_notify_object.as_ref()
    }

    async fn is_slave_ok(&self, master_put_where: i64) -> bool {
        !self.connections.lock().await.is_empty()
            && (master_put_where - self.replication_progress.max_ack_offset())
                < (self
                    .default_message_store
                    .message_store_config_ref()
                    .ha_max_gap_not_in_sync as i64)
    }
}

struct AcceptSocketService {
    socket_address_listen: SocketAddr,
    message_store_config: Arc<MessageStoreConfig>,
    is_auto_switch: bool,
    shutdown_token: CancellationToken,
    default_ha_service: ArcMut<DefaultHAService>,
    worker_group: Arc<Mutex<Option<rocketmq_runtime::TaskGroup>>>,
}

impl AcceptSocketService {
    pub fn new(
        message_store_config: Arc<MessageStoreConfig>,
        default_ha_service: ArcMut<DefaultHAService>,
        is_auto_switch: bool,
    ) -> Self {
        let ha_listen_port = message_store_config.ha_listen_port;
        let socket_address_listen = SocketAddr::from(([0u8, 0u8, 0u8, 0u8], ha_listen_port as u16));
        AcceptSocketService {
            socket_address_listen,
            message_store_config,
            is_auto_switch,
            shutdown_token: CancellationToken::new(),
            default_ha_service,
            worker_group: Arc::new(Mutex::new(None)),
        }
    }

    async fn build_connection(
        default_ha_service: ArcMut<DefaultHAService>,
        message_store_config: Arc<MessageStoreConfig>,
        stream: TcpStream,
        addr: SocketAddr,
        is_auto_switch: bool,
    ) -> Result<ArcMut<GeneralHAConnection>, crate::ha::HAConnectionError> {
        let default_connection =
            DefaultHAConnection::new(default_ha_service, stream, message_store_config, addr).await?;
        let general_connection = if is_auto_switch {
            GeneralHAConnection::new_with_auto_switch_ha_connection(AutoSwitchHAConnection::new(default_connection))
        } else {
            GeneralHAConnection::new_with_default_ha_connection(default_connection)
        };
        Ok(ArcMut::new(general_connection))
    }

    pub async fn start(&mut self) -> HAResult<()> {
        let mut worker_group_guard = self.worker_group.lock().await;
        if worker_group_guard.is_some() {
            warn!("AcceptSocketService is already started");
            return Ok(());
        }

        let listener = TcpListener::bind(self.socket_address_listen)
            .await
            .map_err(HAError::Io)?;
        self.shutdown_token = CancellationToken::new();
        let shutdown_token = self.shutdown_token.clone();
        let is_auto_switch = self.is_auto_switch;
        let message_store_config = self.message_store_config.clone();
        let default_ha_service = self.default_ha_service.clone();
        let worker_group = crate::runtime::task_group("rocketmq-store.ha.accept")
            .map_err(|error| HAError::Service(error.to_string()))?;
        worker_group
            .spawn_service("ha-accept-socket-service", async move {
                let message_store_config = message_store_config;
                let default_ha_service = default_ha_service;
                loop {
                    select! {
                        _ = shutdown_token.cancelled() => {
                            info!("AcceptSocketService is shutting down");
                            break;
                        }
                    // Accept new connections
                        accept_result = listener.accept() => {
                            match accept_result {
                                Ok((stream, addr)) => {
                                    info!("HAService receive new connection, {}", addr);
                                    match AcceptSocketService::build_connection(
                                        default_ha_service.clone(),
                                        message_store_config.clone(),
                                        stream,
                                        addr,
                                        is_auto_switch,
                                    )
                                    .await
                                    {
                                        Ok(mut general_conn) => {
                                            if let Err(e) = general_conn.start().await {
                                                error!("Error starting HAService: {}", e);
                                            } else {
                                                info!("HAService accept new connection, {}", addr);
                                                default_ha_service.add_connection(general_conn).await;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Error creating HAConnection: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to accept connection: {}", e);
                                    // Add a small delay to prevent busy-waiting on persistent errors
                                    sleep(Duration::from_millis(100)).await;
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|error| HAError::Service(error.to_string()))?;
        *worker_group_guard = Some(worker_group);
        Ok(())
    }

    pub async fn shutdown(&self) {
        info!("Shutting down AcceptSocketService");
        self.shutdown_token.cancel();
        let worker_group = self.worker_group.lock().await.take();
        if let Some(worker_group) = worker_group {
            let report = worker_group.shutdown(Duration::from_secs(3)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("AcceptSocketService", report) {
                warn!("AcceptSocketService failed during shutdown: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::atomic::Ordering;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::TimeUtils::current_millis;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::ha::auto_switch::auto_switch_ha_service::AutoSwitchHAService;

    fn new_test_message_store(root: &Path, enable_controller_mode: bool) -> ArcMut<LocalFileMessageStore> {
        std::fs::create_dir_all(root).expect("create temp root dir");

        let broker_config = BrokerConfig {
            enable_controller_mode,
            ..BrokerConfig::default()
        };

        let message_store_config = MessageStoreConfig {
            enable_controller_mode,
            ha_max_time_slave_not_catchup: 1000,
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        };

        let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
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

    fn new_auto_switch_services(root: &Path) -> (ArcMut<DefaultHAService>, ArcMut<AutoSwitchHAService>) {
        let store = new_test_message_store(root, true);
        let mut default_service = ArcMut::new(DefaultHAService::new(store.clone()));
        let auto_switch_service = ArcMut::new(AutoSwitchHAService::new(DefaultHAService::new(store)));

        DefaultHAService::init(
            &mut default_service,
            GeneralHAService::new_with_auto_switch_ha_service(auto_switch_service.clone()),
        )
        .expect("init default ha service");

        (default_service, auto_switch_service)
    }

    async fn new_server_stream() -> (TcpStream, SocketAddr, TcpStream) {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind loopback listener");
        let listen_addr = listener.local_addr().expect("listener local addr");
        let client = TcpStream::connect(listen_addr).await.expect("connect loopback client");
        let (server_stream, remote_addr) = listener.accept().await.expect("accept loopback client");
        (server_stream, remote_addr, client)
    }

    #[tokio::test]
    async fn start_without_init_returns_error() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-default-ha-no-init-{}", current_millis()));
        let store = new_test_message_store(&temp_root, false);
        let mut service = DefaultHAService::new(store);

        let error = service.start().await.expect_err("start should fail before init");

        assert!(matches!(error, HAError::Service(message) if message.contains("AcceptSocketService")));
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn default_ha_service_exposes_shared_transfer_metrics() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-default-ha-metrics-{}", current_millis()));
        let store = new_test_message_store(&temp_root, false);
        let service = DefaultHAService::new(store);
        let metrics = service.ha_transfer_metrics();

        metrics.record_fallback(
            crate::ha::transfer_engine::TransferEngineKind::IoUring,
            crate::ha::transfer_engine::TransferEngineKind::Vectored,
            "io_uring unavailable",
        );

        assert_eq!(service.ha_transfer_metrics().snapshot().fallback_total, 1);
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn notify_transfer_some_counts_repeated_same_offset_acks() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-default-ha-repeated-ack-{}", current_millis()));
        let store = new_test_message_store(&temp_root, false);
        let mut service = ArcMut::new(DefaultHAService::new(store));
        let general_service = GeneralHAService::new_with_default_ha_service(service.clone());
        DefaultHAService::init(&mut service, general_service).expect("init default ha service");

        service.notify_transfer_some(128).await;
        service.notify_transfer_some(128).await;

        let runtime_info = service.get_runtime_info(128);
        assert_eq!(service.get_push_to_slave_max_offset(), 128);
        assert_eq!(runtime_info.group_transfer_ack_notify_count, 2);
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn accept_socket_service_shutdown_joins_worker() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-default-ha-shutdown-{}", current_millis()));
        let store = new_test_message_store(&temp_root, true);
        let mut service = ArcMut::new(DefaultHAService::new(store.clone()));
        let auto_switch_service = ArcMut::new(AutoSwitchHAService::new(DefaultHAService::new(store)));

        DefaultHAService::init(
            &mut service,
            GeneralHAService::new_with_auto_switch_ha_service(auto_switch_service),
        )
        .expect("init default ha service");

        let accept_service = service
            .accept_socket_service
            .as_mut()
            .expect("accept socket service should be initialized");
        accept_service.socket_address_listen = SocketAddr::from(([127u8, 0u8, 0u8, 1u8], 0));

        accept_service.start().await.expect("start accept socket service");
        assert!(accept_service.worker_group.lock().await.is_some());

        accept_service.shutdown().await;

        assert!(accept_service.worker_group.lock().await.is_none());
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn init_uses_auto_switch_accept_socket_service_for_auto_switch_mode() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-default-ha-init-{}", current_millis()));
        let store = new_test_message_store(&temp_root, true);
        let mut service = ArcMut::new(DefaultHAService::new(store.clone()));
        let auto_switch_service = ArcMut::new(AutoSwitchHAService::new(DefaultHAService::new(store)));

        DefaultHAService::init(
            &mut service,
            GeneralHAService::new_with_auto_switch_ha_service(auto_switch_service),
        )
        .expect("init default ha service");

        assert!(
            service
                .accept_socket_service
                .as_ref()
                .expect("accept socket service")
                .is_auto_switch
        );

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn build_connection_wraps_auto_switch_connections_when_enabled() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-default-ha-build-{}", current_millis()));
        let store = new_test_message_store(&temp_root, true);
        let service = ArcMut::new(DefaultHAService::new(store));
        let (server_stream, remote_addr, _client) = new_server_stream().await;

        let mut connection = AcceptSocketService::build_connection(
            service.clone(),
            service.get_default_message_store().message_store_config(),
            server_stream,
            remote_addr,
            true,
        )
        .await
        .expect("build auto-switch connection");

        assert!(connection.is_auto_switch());
        assert_eq!(connection.slave_broker_id(), None);
        connection.set_slave_broker_id(Some(9));
        assert_eq!(connection.slave_broker_id(), Some(9));

        connection.shutdown().await;

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn destroy_connections_does_not_hold_connection_table_lock_during_connection_shutdown() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-default-ha-destroy-{}", current_millis()));
        let (service, auto_switch_service) = new_auto_switch_services(&temp_root);
        auto_switch_service.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));
        let (server_stream, remote_addr, _client) = new_server_stream().await;
        let mut connection = AcceptSocketService::build_connection(
            service.clone(),
            service.get_default_message_store().message_store_config(),
            server_stream,
            remote_addr,
            true,
        )
        .await
        .expect("build auto-switch connection");
        connection.start().await.expect("start connection");
        service.add_connection(connection).await;

        tokio::time::timeout(Duration::from_secs(3), service.destroy_connections())
            .await
            .expect("destroy_connections should not deadlock on connection table lock");

        assert_eq!(service.get_connection_count().load(Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn connection_ack_callback_expands_auto_switch_sync_state_set() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-default-ha-ack-callback-{}", current_millis()));
        let (service, auto_switch_service) = new_auto_switch_services(&temp_root);
        assert_eq!(auto_switch_service.weak_count(), 0);
        auto_switch_service.sync_controller_sync_state_set(7, &HashSet::from([7_i64]));

        let (server_stream, remote_addr, _client) = new_server_stream().await;
        let mut connection = AcceptSocketService::build_connection(
            service.clone(),
            service.get_default_message_store().message_store_config(),
            server_stream,
            remote_addr,
            true,
        )
        .await
        .expect("build auto-switch connection");
        connection.set_slave_broker_id(Some(9));

        service.handle_connection_ack(connection.as_ref(), 4);

        assert_eq!(auto_switch_service.get_sync_state_set(), HashSet::from([7_i64, 9_i64]));
        assert!(auto_switch_service.is_synchronizing_sync_state_set());

        connection.shutdown().await;
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn connection_caught_up_callback_keeps_sync_state_member_alive() {
        let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-default-ha-caught-up-{}", current_millis()));
        let (service, auto_switch_service) = new_auto_switch_services(&temp_root);
        auto_switch_service.sync_controller_sync_state_set(7, &HashSet::from([7_i64, 9_i64]));

        let (server_stream, remote_addr, _client) = new_server_stream().await;
        let mut connection = AcceptSocketService::build_connection(
            service.clone(),
            service.get_default_message_store().message_store_config(),
            server_stream,
            remote_addr,
            true,
        )
        .await
        .expect("build auto-switch connection");
        connection.set_slave_broker_id(Some(9));

        service.handle_connection_caught_up(connection.as_ref());
        sleep(Duration::from_millis(2)).await;

        let shrunk = auto_switch_service.maybe_shrink_sync_state_set();
        assert_eq!(shrunk, HashSet::from([7_i64, 9_i64]));

        connection.shutdown().await;
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn remove_connection_callback_prunes_auto_switch_sync_state_set() {
        let temp_root =
            std::env::temp_dir().join(format!("rocketmq-rust-default-ha-remove-callback-{}", current_millis()));
        let (service, auto_switch_service) = new_auto_switch_services(&temp_root);
        auto_switch_service.sync_controller_sync_state_set(7, &HashSet::from([7_i64, 9_i64]));

        let (server_stream, remote_addr, _client) = new_server_stream().await;
        let mut connection = AcceptSocketService::build_connection(
            service.clone(),
            service.get_default_message_store().message_store_config(),
            server_stream,
            remote_addr,
            true,
        )
        .await
        .expect("build auto-switch connection");
        connection.set_slave_broker_id(Some(9));

        service.add_connection(connection.clone()).await;
        service.remove_connection(connection.clone()).await;

        assert_eq!(auto_switch_service.get_local_sync_state_set(), HashSet::from([7_i64]));

        connection.shutdown().await;
        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn in_sync_replicas_count_requires_slave_ack_to_reach_master_offset() {
        let temp_root = std::env::temp_dir().join(format!(
            "rocketmq-rust-default-ha-in-sync-replicas-{}",
            current_millis()
        ));
        let store = new_test_message_store(&temp_root, false);
        let service = ArcMut::new(DefaultHAService::new(store));
        let (server_stream, remote_addr, mut client) = new_server_stream().await;
        let mut connection = AcceptSocketService::build_connection(
            service.clone(),
            service.get_default_message_store().message_store_config(),
            server_stream,
            remote_addr,
            false,
        )
        .await
        .expect("build default connection");
        connection.start().await.expect("start connection");
        service.add_connection(connection.clone()).await;

        client
            .write_all(&64_i64.to_be_bytes())
            .await
            .expect("write slave ack offset");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if connection.get_slave_ack_offset() == 64 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("connection should observe slave ack offset");

        let acked_replicas = service.snapshot_acked_replicas().await;
        assert_eq!(acked_replicas.len(), 1);
        assert_eq!(acked_replicas[0].slave_broker_id, None);
        assert_eq!(acked_replicas[0].slave_ack_offset, 64);
        assert_eq!(
            service.connection_state(&remote_addr.to_string()).await,
            Some(HAConnectionState::Transfer)
        );
        assert_eq!(service.connection_state("127.0.0.1:1").await, None);
        assert_eq!(service.in_sync_replicas_nums(64), 2);
        assert_eq!(service.in_sync_replicas_nums(65), 1);

        connection.shutdown().await;
        let _ = std::fs::remove_dir_all(temp_root);
    }
}
