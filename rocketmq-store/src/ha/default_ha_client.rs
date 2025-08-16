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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use bytes::BytesMut;
use futures_util::SinkExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::ha::flow_monitor::FlowMonitor;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::message_store::local_file_message_store::LocalFileMessageStore;

/// Report header buffer size. Schema: slaveMaxOffset. Format:
/// ┌───────────────────────────────────────────────┐
/// │                  slaveMaxOffset               │
/// │                    (8bytes)                   │
/// ├───────────────────────────────────────────────┤
/// │                                               │
/// │                  Report Header                │
/// └───────────────────────────────────────────────┘
pub const REPORT_HEADER_SIZE: usize = 8;

/// Maximum read buffer size (4MB)
const READ_MAX_BUFFER_SIZE: usize = 1024 * 1024 * 4;

/// Transfer header size from DefaultHAConnection
const TRANSFER_HEADER_SIZE: usize = 12; // 8 bytes offset + 4 bytes body size

/// Default HA Client implementation using bytes crate
pub struct DefaultHAClient {
    inner: ArcMut<Inner>,
    /// Service handle
    service_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

struct Inner {
    /// Master HA address (atomic reference)
    master_ha_address: Arc<tokio::sync::Mutex<Option<String>>>,

    /// Master address (atomic reference)
    master_address: Arc<tokio::sync::Mutex<Option<String>>>,

    /// TCP connection to master
    //socket_stream: Arc<RwLock<Option<TcpStream>>>,
    write_stream: Option<FramedWrite<OwnedWriteHalf, BytesCodec>>,

    read_stream: Option<FramedRead<OwnedReadHalf, BytesCodec>>,

    /// Last time slave read data from master
    last_read_timestamp: AtomicI64,

    /// Last time slave reported offset to master
    last_write_timestamp: AtomicI64,

    /// Current reported offset
    current_reported_offset: AtomicI64,

    /// Dispatch position in read buffer
    dispatch_position: AtomicUsize,

    /// Read buffer using BytesMut for efficient manipulation
    byte_buffer_read: BytesMut,

    /// Backup buffer for reallocation
    byte_buffer_backup: Arc<RwLock<BytesMut>>,

    /// Report offset buffer
    report_offset: BytesMut,

    /// Message store reference
    default_message_store: ArcMut<LocalFileMessageStore>,

    /// Current connection state
    current_state: Arc<RwLock<HAConnectionState>>,

    /// Flow monitor
    flow_monitor: Arc<FlowMonitor>,

    /// Shutdown notification
    shutdown_notify: Arc<Notify>,
}

impl Inner {
    async fn close_master(&mut self) {
        //maybe not need to take, just set to None is ok
        let write = self.write_stream.take();
        let read = self.read_stream.take();
        drop(write);
        drop(read);

        let addr = self.master_ha_address.lock().await;
        info!("HAClient close connection with master {:?}", addr.as_ref());

        // Clear streams
        self.change_current_state(HAConnectionState::Ready).await;

        // Reset state
        self.last_read_timestamp.store(0, Ordering::SeqCst);
        self.dispatch_position.store(0, Ordering::SeqCst);

        // Reset buffers using bytes operations

        let mut backup_buffer = self.byte_buffer_backup.write().await;

        self.byte_buffer_read.clear();
        backup_buffer.clear();
    }
    async fn close_master_and_wait(&mut self) {}

    async fn connect_master(&mut self) -> Result<bool, HAClientError> {
        if self.write_stream.is_none() || self.read_stream.is_none() {
            let addr = self.master_ha_address.lock().await.clone();
            if let Some(addr_str) = addr {
                match TcpStream::connect(&addr_str).await {
                    Ok(stream) => {
                        // Configure socket
                        if let Err(e) = stream.set_nodelay(true) {
                            warn!("Failed to set TCP_NODELAY: {}", e);
                        }

                        let (read, write) = stream.into_split();
                        self.write_stream = Some(FramedWrite::new(write, BytesCodec::new()));
                        self.read_stream = Some(FramedRead::new(read, BytesCodec::new()));
                        info!("HAClient connect to master {}", addr_str);

                        self.change_current_state(HAConnectionState::Transfer).await;
                        // Initialize current reported offset
                        let max_offset = self.default_message_store.get_max_phy_offset();
                        self.current_reported_offset
                            .store(max_offset, Ordering::SeqCst);
                        let now = get_current_millis() as i64;
                        self.last_read_timestamp.store(now, Ordering::SeqCst);
                        return Ok(true);
                    }
                    Err(e) => {
                        error!("Failed to connect to master {}: {}", addr_str, e);
                        return Ok(false);
                    }
                }
            }
        }
        Ok(self.write_stream.is_some() && self.read_stream.is_some())
    }

    async fn transfer_from_master(&mut self) -> Result<bool, HAClientError> {
        // Check if it's time to report offset
        if self.is_time_to_report_offset() {
            let current_offset = self.current_reported_offset.load(Ordering::SeqCst);
            info!("Slave report current offset {}", current_offset);
            if !self.report_slave_max_offset(current_offset).await {
                return Ok(false);
            }
        }

        // Process read events with timeout
        match timeout(Duration::from_secs(1), self.process_read_event()).await {
            Ok(result) => {
                if !result {
                    return Ok(false);
                }
            }
            Err(_) => {
                // Timeout is normal, continue
            }
        }

        Ok(self.report_slave_max_offset_plus().await)
    }

    /// Report slave max offset plus
    async fn report_slave_max_offset_plus(&mut self) -> bool {
        let current_phy_offset = self.default_message_store.get_max_phy_offset();
        let current_reported = self.current_reported_offset.load(Ordering::SeqCst);

        if current_phy_offset > current_reported {
            self.current_reported_offset
                .store(current_phy_offset, Ordering::SeqCst);
            let result = self.report_slave_max_offset(current_phy_offset).await;
            if !result {
                self.close_master().await;
                error!(
                    "HAClient, reportSlaveMaxOffset error, {}",
                    current_phy_offset
                );
                return false;
            }
        }
        true
    }

    pub fn notify_shutdown(&mut self) {
        self.shutdown_notify.notify_waiters();
    }

    /// Process read events from master using bytes buffer
    async fn process_read_event(&mut self) -> bool {
        /*if let Some(read_stream) = self.read_stream.as_mut() {
            // Read data from master
            let mut has_next = true;
            loop {
                match read_stream.try_next().await {
                    Ok(Some(data)) => {
                        has_next = true;
                        self.flow_monitor
                            .add_byte_count_transferred(data.len() as i64);

                        self.last_read_timestamp
                            .store(get_current_millis() as i64, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        has_next = false;
                    }
                    Err(e) => {
                        return false;
                    }
                }
                if !has_next {
                    return true;
                }
            }
        }*/
        true
    }

    /// Dispatch read request and process received data using bytes buffer
    async fn dispatch_read_request(&mut self) -> bool {
        true
    }

    /// Report slave max offset to master
    async fn report_slave_max_offset(&mut self, max_offset: i64) -> bool {
        if let Some(write_stream) = self.write_stream.as_mut() {
            // Prepare report buffer using bytes
            self.report_offset.clear();
            self.report_offset.put_i64(max_offset);

            let bytes = self.report_offset.split().freeze();
            let result = match write_stream.send(bytes).await {
                Ok(_) => true,
                Err(e) => {
                    error!("HAClient, reportSlaveMaxOffset write error: {}", e);
                    false
                }
            };
            self.last_write_timestamp
                .store(get_current_millis() as i64, Ordering::Relaxed);
            return result;
        }
        false
    }

    /// Check if it's time to report offset
    fn is_time_to_report_offset(&self) -> bool {
        let now = self.default_message_store.now();
        let last_write = self.last_write_timestamp.load(Ordering::SeqCst);
        let interval = now as i64 - last_write;
        let heartbeat_interval = self
            .default_message_store
            .get_message_store_config()
            .ha_send_heartbeat_interval;

        interval > heartbeat_interval as i64
    }
    /// Change current connection state
    pub async fn change_current_state(&self, new_state: HAConnectionState) {
        info!("change state to {:?}", new_state);
        let mut state = self.current_state.write().await;
        *state = new_state;
    }

    async fn state_step(&mut self) -> bool {
        let state = *self.current_state.read().await;
        match state {
            HAConnectionState::Shutdown => {
                self.flow_monitor.shutdown_with_interrupt(true).await;
                return false; // Stop the service
            }
            HAConnectionState::Ready => {
                match self.connect_master().await {
                    Ok(true) => return true,
                    Ok(false) => {
                        warn!(
                            "HAClient connect to master {:?} failed",
                            self.ha_master_address().await
                        );
                        sleep(Duration::from_secs(5)).await;
                        return true; // Retry connection
                    }
                    Err(_) => {
                        warn!(
                            "HAClient connect to master {:?} failed",
                            self.ha_master_address().await
                        );
                        self.close_master_and_wait().await;
                        return true; // Retry connection
                    }
                }
            }
            HAConnectionState::Transfer => {
                match self.transfer_from_master().await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(
                            "HAClient transfer from master {:?} failed",
                            self.ha_master_address().await
                        );
                        self.close_master_and_wait().await;
                        return true; // Retry connection
                    }
                    Err(_) => {
                        warn!(
                            "HAClient transfer from master {:?} failed",
                            self.ha_master_address().await
                        );
                        self.close_master_and_wait().await;
                        return true; // Retry connection
                    }
                }
            }
            _ => {
                // Other states like Handshake, Suspend, etc. do nothing
                sleep(Duration::from_secs(2)).await;
            }
        }

        // Housekeeping
        let interval =
            get_current_millis() - (self.last_read_timestamp.load(Ordering::Relaxed) as u64);
        if interval
            > self
                .default_message_store
                .get_message_store_config()
                .ha_housekeeping_interval
        {
            warn!(
                "AutoRecoverHAClient, housekeeping, connection [{:?}] expired, {}",
                self.ha_master_address().await,
                interval
            );
            self.close_master().await;
            warn!("AutoRecoverHAClient, master not response some time, so close connection");
        }
        true
    }

    pub async fn ha_master_address(&self) -> Option<String> {
        self.master_ha_address.lock().await.clone()
    }
}

impl DefaultHAClient {
    /// Create a new DefaultHAClient
    pub fn new(
        default_message_store: ArcMut<LocalFileMessageStore>,
    ) -> Result<Self, HAClientError> {
        let flow_monitor = Arc::new(FlowMonitor::new(
            default_message_store.message_store_config(),
        ));

        let now = get_current_millis() as i64;

        Ok(Self {
            inner: ArcMut::new(Inner {
                master_ha_address: Arc::new(tokio::sync::Mutex::new(None)),
                master_address: Arc::new(tokio::sync::Mutex::new(None)),
                //socket_stream: Arc::new(RwLock::new(None)),
                write_stream: None,
                read_stream: None,
                last_read_timestamp: AtomicI64::new(now),
                last_write_timestamp: AtomicI64::new(now),
                current_reported_offset: AtomicI64::new(0),
                dispatch_position: AtomicUsize::new(0),
                byte_buffer_read: BytesMut::with_capacity(READ_MAX_BUFFER_SIZE),
                byte_buffer_backup: Arc::new(RwLock::new(BytesMut::with_capacity(
                    READ_MAX_BUFFER_SIZE,
                ))),
                report_offset: BytesMut::with_capacity(REPORT_HEADER_SIZE),
                default_message_store,
                current_state: Arc::new(RwLock::new(HAConnectionState::Ready)),
                flow_monitor,
                shutdown_notify: Arc::new(Notify::new()),
            }),
            service_handle: Arc::new(RwLock::new(None)),
        })
    }

    /// Get HA master address
    pub async fn get_ha_master_address(&self) -> Option<String> {
        self.inner.master_ha_address.lock().await.clone()
    }

    /// Get master address
    pub async fn get_master_address(&self) -> Option<String> {
        self.inner.master_address.lock().await.clone()
    }

    /// Reallocate byte buffer when it's full using bytes operations
    async fn reallocate_byte_buffer(&self) {
        /*let dispatch_pos = self.inner.dispatch_position.load(Ordering::SeqCst);
        let mut read_buffer = self.inner.byte_buffer_read.write().await;
        let mut backup_buffer = self.inner.byte_buffer_backup.write().await;

        let remain = read_buffer.len() - dispatch_pos;

        if remain > 0 {
            // Copy remaining data to backup buffer
            backup_buffer.clear();
            backup_buffer.extend_from_slice(&read_buffer[dispatch_pos..]);
        } else {
            backup_buffer.clear();
        }

        // Swap buffers - efficient with bytes
        std::mem::swap(&mut *read_buffer, &mut *backup_buffer);

        // Reserve capacity if needed
        if read_buffer.capacity() < READ_MAX_BUFFER_SIZE {
            let capacity = read_buffer.capacity();
            read_buffer.reserve(READ_MAX_BUFFER_SIZE - capacity);
        }

        self.inner.dispatch_position.store(0, Ordering::SeqCst);*/
    }

    /// Close master and wait
    pub async fn close_master_and_wait(&self) {
        self.close_master().await;
        sleep(Duration::from_secs(5)).await;
    }

    // Start the HA client service
    /*    pub async fn start(self: ArcMut<Self>) -> Result<(), HAClientError> {
        let self_clone = ArcMut::clone(&self);
        let handle = tokio::spawn(async move {
            self_clone.run_service().await;
        });

        let mut service_handle = self.service_handle.write().await;
        *service_handle = Some(handle);

        Ok(())
    }*/

    /// Shutdown the HA client
    pub async fn shutdown(self: Arc<Self>) {
        self.change_current_state(HAConnectionState::Shutdown);
        self.inner.flow_monitor.shutdown().await;
        self.inner.shutdown_notify.notify_waiters();

        // Wait for service to stop
        let mut service_handle = self.service_handle.write().await;
        if let Some(handle) = service_handle.take() {
            let _ = handle.await;
        }

        self.close_master().await;
    }

    /// Get service name
    pub fn get_service_name(&self) -> &'static str {
        "DefaultHAClient"
    }

    /// Get last write timestamp
    pub fn get_last_write_timestamp(&self) -> i64 {
        self.inner.last_write_timestamp.load(Ordering::SeqCst)
    }

    /// Get last read timestamp
    pub fn get_last_read_timestamp(&self) -> i64 {
        self.inner.last_read_timestamp.load(Ordering::SeqCst)
    }

    /// Get current state
    pub async fn get_current_state(&self) -> HAConnectionState {
        *self.inner.current_state.read().await
    }

    /// Get transferred bytes per second
    pub fn get_transferred_byte_in_second(&self) -> u64 {
        self.inner.flow_monitor.get_transferred_byte_in_second() as u64
    }
}

impl HAClient for DefaultHAClient {
    async fn start(&mut self) {
        // Idempotent start: if a service handle already exists, do nothing
        if self.service_handle.read().await.is_some() {
            warn!("HAClient service is already running");
            return;
        }

        self.inner.flow_monitor.start().await;
        let mut client = ArcMut::clone(&self.inner);
        let join_handle = tokio::spawn(async move {
            let client_inner = ArcMut::clone(&client);
            loop {
                select! {
                    biased;
                    _ = client_inner.shutdown_notify.notified() => {
                        info!("HAClient received shutdown notification");
                        break;
                    }
                    result = client.state_step() => {
                        if !result {
                            info!("HAClient service stopped");
                            break;
                        }
                    }
                }
            }
            client.flow_monitor.shutdown_with_interrupt(true).await;
            info!("HAClient service finished");
        });
        let mut service_handle = self.service_handle.write().await;
        *service_handle = Some(join_handle);
    }

    async fn shutdown(&self) {
        self.inner
            .change_current_state(HAConnectionState::Shutdown)
            .await;
        self.inner.shutdown_notify.notify_waiters();

        // Wait for service to stop
        let mut service_handle = self.service_handle.write().await;
        if let Some(handle) = service_handle.take() {
            let _ = handle.await;
        }
        self.close_master().await;
    }

    async fn wakeup(&self) {
        todo!()
    }

    /// Update master address
    async fn update_master_address(&self, new_address: &str) {
        let mut master_address = self.inner.master_address.lock().await;
        if master_address.is_none() || master_address.as_ref().unwrap() != new_address {
            *master_address = Some(new_address.to_string());
            info!("Updated master address to: {}", new_address);
        }
    }

    async fn update_ha_master_address(&self, new_address: &str) {
        let mut master_ha_address = self.inner.master_ha_address.lock().await;
        if master_ha_address.is_none() || master_ha_address.as_ref().unwrap() != new_address {
            *master_ha_address = Some(new_address.to_string());
            info!("Updated HA master address to: {}", new_address);
        }
    }

    fn get_master_address(&self) -> String {
        todo!()
    }

    fn get_ha_master_address(&self) -> String {
        todo!()
    }

    fn get_last_read_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_last_write_timestamp(&self) -> i64 {
        todo!()
    }

    fn get_current_state(&self) -> HAConnectionState {
        todo!()
    }

    fn change_current_state(&self, ha_connection_state: HAConnectionState) {
        todo!()
    }

    async fn close_master(&self) {
        todo!()
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        todo!()
    }
}
/// Error types
#[derive(Debug, thiserror::Error)]
pub enum HAClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Service error: {0}")]
    Service(String),
}
