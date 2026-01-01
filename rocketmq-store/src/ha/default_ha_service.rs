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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_rust::ArcMut;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing::error;
use tracing::info;

use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::default_ha_client::DefaultHAClient;
use crate::ha::default_ha_connection::DefaultHAConnection;
use crate::ha::general_ha_client::GeneralHAClient;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::group_transfer_service::GroupTransferService;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_connection_state_notification_service::HAConnectionStateNotificationService;
use crate::ha::ha_service::HAService;
use crate::log_file::group_commit_request::GroupCommitRequest;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::store_error::HAError;
use crate::store_error::HAResult;

pub struct DefaultHAService {
    connection_count: Arc<AtomicU32>,
    connections: Arc<Mutex<HashMap<HAConnectionId, ArcMut<GeneralHAConnection>>>>,
    accept_socket_service: Option<AcceptSocketService>,
    default_message_store: ArcMut<LocalFileMessageStore>,
    wait_notify_object: Arc<Notify>,
    push2_slave_max_offset: Arc<AtomicU64>,
    group_transfer_service: Option<GroupTransferService>,
    ha_client: Option<GeneralHAClient>,
    ha_connection_state_notification_service: Option<HAConnectionStateNotificationService>,
}

impl DefaultHAService {
    pub fn new(message_store: ArcMut<LocalFileMessageStore>) -> Self {
        DefaultHAService {
            connection_count: Arc::new(AtomicU32::new(0)),
            connections: Arc::new(Mutex::new(HashMap::new())),
            accept_socket_service: None,
            default_message_store: message_store,
            wait_notify_object: Arc::new(Notify::new()),
            push2_slave_max_offset: Arc::new(AtomicU64::new(0)),
            group_transfer_service: None,
            ha_client: None,
            ha_connection_state_notification_service: None,
        }
    }

    pub fn get_default_message_store(&self) -> &LocalFileMessageStore {
        self.default_message_store.as_ref()
    }

    pub async fn notify_transfer_some(&self, offset: i64) {
        let mut value = self.push2_slave_max_offset.load(Ordering::Relaxed);

        while (offset as u64) > value {
            match self.push2_slave_max_offset.compare_exchange_weak(
                value,
                offset as u64,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully updated the value
                    if let Some(service) = &self.group_transfer_service {
                        service.notify_transfer_some();
                    }
                    break;
                }
                Err(current_value) => {
                    // Update failed, retry with the current value
                    value = current_value;
                }
            }
        }
    }

    pub(crate) fn init(this: &mut ArcMut<Self>, general_ha_service: GeneralHAService) -> HAResult<()> {
        // Initialize the DefaultHAService with the provided message store.
        let config = this.default_message_store.get_message_store_config();

        let group_transfer_service = GroupTransferService::new(general_ha_service.clone());
        this.group_transfer_service = Some(group_transfer_service);

        if config.broker_role == BrokerRole::Slave {
            let default_message_store = this.default_message_store.clone();
            let client = DefaultHAClient::new(default_message_store)
                .map_err(|e| HAError::Service(format!("Failed to create DefaultHAClient: {e}")))?;

            let ha_client = GeneralHAClient::new_with_default_ha_client(client);

            this.ha_client = Some(ha_client);
        }

        let state_notification_service =
            HAConnectionStateNotificationService::new(general_ha_service, this.default_message_store.clone());
        this.ha_connection_state_notification_service = Some(state_notification_service);

        let arc_mut = this.clone();
        this.accept_socket_service = Some(AcceptSocketService::new(
            this.default_message_store.get_message_store_config(),
            arc_mut,
            false,
        ));
        Ok(())
    }

    pub async fn add_connection(&self, connection: ArcMut<GeneralHAConnection>) {
        // Add a new connection to the service
        let mut connections = self.connections.lock().await;
        connections.insert(connection.get_ha_connection_id().clone(), connection);
    }

    pub async fn remove_connection(&self, connection: ArcMut<GeneralHAConnection>) {
        if let Some(ha_connection_state_notification_service) = &self.ha_connection_state_notification_service {
            let _ = ha_connection_state_notification_service
                .check_connection_state_and_notify(connection.as_ref())
                .await;
        }

        let mut connections = self.connections.lock().await;
        connections.remove(connection.get_ha_connection_id());
    }

    pub async fn destroy_connections(&self) {
        let mut connections = self.connections.lock().await;
        for (_, mut connection) in connections.drain() {
            connection.shutdown().await;
        }
    }
}

impl HAService for DefaultHAService {
    async fn start(&mut self) -> HAResult<()> {
        self.accept_socket_service
            .as_mut()
            .expect("AcceptSocketService not initialized")
            .start()
            .await?;
        self.group_transfer_service
            .as_mut()
            .expect("GroupTransferService not initialized")
            .start()
            .await?;
        self.ha_connection_state_notification_service
            .as_mut()
            .expect("HAConnectionStateNotificationService not initialized")
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
            ha_client.shutdown().await;
        }

        if let Some(ref accept_socket_service) = self.accept_socket_service {
            accept_socket_service.shutdown();
        }
        self.destroy_connections().await;

        if let Some(ref group_transfer_service) = self.group_transfer_service {
            group_transfer_service.shutdown().await;
        }

        if let Some(ref ha_connection_state_notification_service) = self.ha_connection_state_notification_service {
            ha_connection_state_notification_service.shutdown().await;
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
        todo!()
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

    async fn get_connection_list(&self) -> Vec<ArcMut<GeneralHAConnection>> {
        let connections = self.connections.lock().await;
        connections.values().cloned().collect()
    }

    fn get_ha_client(&self) -> Option<&GeneralHAClient> {
        self.ha_client.as_ref()
    }

    fn get_ha_client_mut(&mut self) -> Option<&mut GeneralHAClient> {
        self.ha_client.as_mut()
    }

    fn get_push_to_slave_max_offset(&self) -> i64 {
        self.push2_slave_max_offset.load(std::sync::atomic::Ordering::Relaxed) as i64
    }

    fn get_runtime_info(&self, master_put_where: i64) -> HARuntimeInfo {
        todo!()
    }

    fn get_wait_notify_object(&self) -> &Notify {
        self.wait_notify_object.as_ref()
    }

    async fn is_slave_ok(&self, master_put_where: i64) -> bool {
        !self.connections.lock().await.is_empty()
            && (master_put_where - self.push2_slave_max_offset.load(std::sync::atomic::Ordering::Relaxed) as i64)
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
    shutdown_notify: Arc<Notify>,
    default_ha_service: ArcMut<DefaultHAService>,
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
            shutdown_notify: Arc::new(Notify::new()),
            default_ha_service,
        }
    }

    pub async fn start(&mut self) -> HAResult<()> {
        let listener = TcpListener::bind(self.socket_address_listen)
            .await
            .map_err(HAError::Io)?;
        let shutdown_notify = self.shutdown_notify.clone();
        let is_auto_switch = self.is_auto_switch;
        let message_store_config = self.message_store_config.clone();
        let default_ha_service = self.default_ha_service.clone();
        tokio::spawn(async move {
            let message_store_config = message_store_config;
            let default_ha_service = default_ha_service;
            loop {
                select! {
                    _ = shutdown_notify.notified() => {
                        info!("AcceptSocketService is shutting down");
                        break;
                    }
                // Accept new connections
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, addr)) => {
                                info!("HAService receive new connection, {}", addr);
                               if is_auto_switch {
                                    unimplemented!("Auto-switching is not implemented yet");
                                }else{
                                    let default_conn = DefaultHAConnection::new(default_ha_service.clone(), stream,message_store_config.clone(),addr).await.expect("Error creating HAConnection");
                                    let mut general_conn = ArcMut::new(GeneralHAConnection::new_with_default_ha_connection(default_conn));
                                    let  conn_weak= ArcMut::downgrade(&general_conn);
                                    if  let Err(e) =  general_conn.start(conn_weak).await {
                                        error!("Error starting HAService: {}", e);
                                    }else {
                                        info!("HAService accept new connection, {}", addr);
                                        default_ha_service.add_connection(general_conn).await;
                                    }

                                };
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
        });
        Ok(())
    }

    pub fn shutdown(&self) {
        info!("Shutting down AcceptSocketService");
        self.shutdown_notify.notify_waiters();
    }
}
