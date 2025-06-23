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
use std::io::Cursor;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::timeout;
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
    /// Master HA address (atomic reference)
    master_ha_address: Arc<RwLock<Option<String>>>,

    /// Master address (atomic reference)
    master_address: Arc<AtomicPtr<String>>,

    /// TCP connection to master
    socket_stream: Arc<RwLock<Option<TcpStream>>>,

    /// Last time slave read data from master
    last_read_timestamp: AtomicI64,

    /// Last time slave reported offset to master
    last_write_timestamp: AtomicI64,

    /// Current reported offset
    current_reported_offset: AtomicI64,

    /// Dispatch position in read buffer
    dispatch_position: AtomicUsize,

    /// Read buffer using BytesMut for efficient manipulation
    byte_buffer_read: Arc<RwLock<BytesMut>>,

    /// Backup buffer for reallocation
    byte_buffer_backup: Arc<RwLock<BytesMut>>,

    /// Report offset buffer
    report_offset: Arc<RwLock<BytesMut>>,

    /// Message store reference
    default_message_store: ArcMut<LocalFileMessageStore>,

    /// Current connection state
    current_state: Arc<RwLock<HAConnectionState>>,

    /// Flow monitor
    flow_monitor: Arc<FlowMonitor>,

    /// Shutdown notification
    shutdown_notify: Arc<Notify>,

    /// Service handle
    service_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl DefaultHAClient {
    /// Create a new DefaultHAClient
    pub fn new(
        default_message_store: ArcMut<LocalFileMessageStore>,
    ) -> Result<ArcMut<Self>, HAClientError> {
        let flow_monitor = Arc::new(FlowMonitor::new(
            default_message_store.message_store_config(),
        ));

        let now = get_current_millis() as i64;

        Ok(ArcMut::new(Self {
            master_ha_address: Arc::new(RwLock::new(None)),
            master_address: Arc::new(AtomicPtr::default()),
            socket_stream: Arc::new(RwLock::new(None)),
            last_read_timestamp: AtomicI64::new(now),
            last_write_timestamp: AtomicI64::new(now),
            current_reported_offset: AtomicI64::new(0),
            dispatch_position: AtomicUsize::new(0),
            byte_buffer_read: Arc::new(RwLock::new(BytesMut::with_capacity(READ_MAX_BUFFER_SIZE))),
            byte_buffer_backup: Arc::new(RwLock::new(BytesMut::with_capacity(
                READ_MAX_BUFFER_SIZE,
            ))),
            report_offset: Arc::new(RwLock::new(BytesMut::with_capacity(REPORT_HEADER_SIZE))),
            default_message_store,
            current_state: Arc::new(RwLock::new(HAConnectionState::Ready)),
            flow_monitor,
            shutdown_notify: Arc::new(Notify::new()),
            service_handle: Arc::new(RwLock::new(None)),
        }))
    }

    /// Update HA master address
    pub async fn update_ha_master_address(&self, new_addr: Option<String>) {
        let mut current_addr = self.master_ha_address.write().await;
        let old_addr = current_addr.clone();
        *current_addr = new_addr.clone();

        info!(
            "update master ha address, OLD: {:?} NEW: {:?}",
            old_addr, new_addr
        );
    }

    /// Get HA master address
    pub async fn get_ha_master_address(&self) -> Option<String> {
        self.master_ha_address.read().await.clone()
    }

    /// Get master address
    pub async fn get_master_address(&self) -> Option<String> {
        let addr_ptr = self.master_address.load(Ordering::SeqCst);
        if !addr_ptr.is_null() {
            let address = unsafe { (*addr_ptr).clone() };
            Some(address)
        } else {
            None
        }
    }

    /// Check if it's time to report offset
    async fn is_time_to_report_offset(&self) -> bool {
        let now = self.default_message_store.now();
        let last_write = self.last_write_timestamp.load(Ordering::SeqCst);
        let interval = now as i64 - last_write;
        let heartbeat_interval = self
            .default_message_store
            .get_message_store_config()
            .ha_send_heartbeat_interval;

        interval > heartbeat_interval as i64
    }

    /// Report slave max offset to master
    async fn report_slave_max_offset(&self, max_offset: i64) -> bool {
        let mut socket_guard = self.socket_stream.write().await;
        if let Some(ref mut socket) = socket_guard.as_mut() {
            // Prepare report buffer using bytes
            let mut report_buffer = self.report_offset.write().await;
            report_buffer.clear();
            report_buffer.put_i64(max_offset);

            // Try up to 3 times to send the complete offset
            if (0..3).next().is_some() {
                if report_buffer.is_empty() {
                    return false;
                }

                match socket.write_all(&report_buffer[..]).await {
                    Ok(_) => {
                        let now = self.default_message_store.now() as i64;
                        self.last_write_timestamp.store(now, Ordering::SeqCst);
                        report_buffer.clear();
                        return true;
                    }
                    Err(e) => {
                        error!(
                            "{} reportSlaveMaxOffset write exception: {}",
                            self.get_service_name(),
                            e
                        );
                        return false;
                    }
                }
            }

            return report_buffer.is_empty();
        }

        false
    }

    /// Reallocate byte buffer when it's full using bytes operations
    async fn reallocate_byte_buffer(&self) {
        let dispatch_pos = self.dispatch_position.load(Ordering::SeqCst);
        let mut read_buffer = self.byte_buffer_read.write().await;
        let mut backup_buffer = self.byte_buffer_backup.write().await;

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

        self.dispatch_position.store(0, Ordering::SeqCst);
    }

    /// Process read events from master using bytes buffer
    async fn process_read_event(&self) -> bool {
        /*  let mut read_size_zero_times = 0;
        let mut socket_guard = self.socket_stream.write().await;

        if let Some(ref mut socket) = socket_guard.as_mut() {
            let mut read_buffer = self.byte_buffer_read.write().await;

            loop {
                // Ensure we have space to read
                if read_buffer.len() >= READ_MAX_BUFFER_SIZE {
                    // Buffer is full, need to reallocate
                    drop(read_buffer);
                    drop(socket_guard);
                    self.reallocate_byte_buffer().await;
                    socket_guard = self.socket_stream.write().await;
                    read_buffer = self.byte_buffer_read.write().await;
                    continue;
                }

                // Reserve space for reading
                read_buffer.reserve(1024);
                let current_len = read_buffer.len();

                // Create a temporary buffer for reading
                let mut temp_buf = vec![0u8; 1024];

                match socket.read(&mut temp_buf).await {
                    Ok(read_size) => {
                        if read_size > 0 {
                            // Add to flow monitor
                            self.flow_monitor
                                .add_byte_count_transferred(read_size as i64);
                            read_size_zero_times = 0;

                            // Append read data to buffer
                            read_buffer.extend_from_slice(&temp_buf[..read_size]);

                            // Dispatch read request
                            drop(read_buffer);
                            drop(socket_guard);

                            if !self.dispatch_read_request().await {
                                error!("HAClient, dispatchReadRequest error");
                                return false;
                            }

                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as i64;
                            self.last_read_timestamp.store(now, Ordering::SeqCst);

                            // Re-acquire locks
                            socket_guard = self.socket_stream.write().await;
                            read_buffer = self.byte_buffer_read.write().await;
                        } else {
                            read_size_zero_times += 1;
                            if read_size_zero_times >= 3 {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        info!("HAClient, processReadEvent read socket exception: {}", e);
                        return false;
                    }
                }
            }
        }

        true*/
        unimplemented!(
            "process_read_event is not implemented yet, please implement it using bytes operations"
        );
    }
    /// Dispatch read request and process received data using bytes buffer
    async fn dispatch_read_request(&self) -> bool {
        let mut _read_buffer = self.byte_buffer_read.write().await;
        let mut _dispatch_pos = self.dispatch_position.load(Ordering::SeqCst);

        loop {
            let available_data = _read_buffer.len() - _dispatch_pos;

            if available_data >= TRANSFER_HEADER_SIZE {
                // Create a cursor for reading from the buffer
                let mut cursor = Cursor::new(&_read_buffer[_dispatch_pos..]);

                // Parse header: [physicOffset (8bytes)][bodySize (4bytes)]
                let master_phy_offset = cursor.get_i64();
                let body_size = cursor.get_i32() as usize;

                let slave_phy_offset = self.default_message_store.get_max_phy_offset();

                // Validate offset consistency
                if slave_phy_offset != 0 && slave_phy_offset != master_phy_offset {
                    error!(
                        "master pushed offset not equal the max phy offset in slave, SLAVE: {} \
                         MASTER: {}",
                        slave_phy_offset, master_phy_offset
                    );
                    return false;
                }

                // Check if we have the complete message
                if available_data >= TRANSFER_HEADER_SIZE + body_size {
                    let data_start = _dispatch_pos + TRANSFER_HEADER_SIZE;
                    let data_end = data_start + body_size;

                    // Extract body data as Bytes for zero-copy
                    let bytes = _read_buffer.clone().freeze().slice(data_start..data_end);
                    // Append to commit log
                    drop(_read_buffer);
                    if let Err(e) = self.default_message_store.append_to_commit_log(
                        master_phy_offset,
                        bytes.as_ref(),
                        data_start as i32,
                        body_size as i32,
                    ) {
                        error!("Failed to append to commit log: {}", e);
                        return false;
                    }

                    // Update dispatch position
                    _dispatch_pos += TRANSFER_HEADER_SIZE + body_size;
                    self.dispatch_position
                        .store(_dispatch_pos, Ordering::SeqCst);

                    // Report slave max offset
                    if !self.report_slave_max_offset_plus().await {
                        return false;
                    }

                    // Re-acquire buffer lock and continue
                    _read_buffer = self.byte_buffer_read.write().await;
                    continue;
                }
            }

            // Check if buffer is full and needs reallocation
            if _read_buffer.len() >= READ_MAX_BUFFER_SIZE {
                drop(_read_buffer);
                self.reallocate_byte_buffer().await;
                _read_buffer = self.byte_buffer_read.write().await;
                _dispatch_pos = self.dispatch_position.load(Ordering::SeqCst);
            }

            break;
        }

        true
    }

    /// Report slave max offset plus
    async fn report_slave_max_offset_plus(&self) -> bool {
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

    /// Change current connection state
    pub async fn change_current_state(&self, new_state: HAConnectionState) {
        info!("change state to {:?}", new_state);
        let mut state = self.current_state.write().await;
        *state = new_state;
    }

    /// Connect to master
    pub async fn connect_master(&self) -> Result<bool, HAClientError> {
        let mut socket_guard = self.socket_stream.write().await;

        if socket_guard.is_none() {
            let addr = self.get_ha_master_address().await;
            if let Some(addr_str) = addr {
                match TcpStream::connect(&addr_str).await {
                    Ok(stream) => {
                        // Configure socket
                        if let Err(e) = stream.set_nodelay(true) {
                            warn!("Failed to set TCP_NODELAY: {}", e);
                        }

                        *socket_guard = Some(stream);
                        info!("HAClient connect to master {}", addr_str);

                        drop(socket_guard);
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

        Ok(socket_guard.is_some())
    }

    /// Close connection to master
    pub async fn close_master(&self) {
        let mut socket_guard = self.socket_stream.write().await;
        if let Some(mut socket) = socket_guard.take() {
            if let Err(e) = socket.shutdown().await {
                warn!("closeMaster exception: {}", e);
            }

            let addr = self.get_ha_master_address().await;
            info!("HAClient close connection with master {:?}", addr);

            drop(socket_guard);
            self.change_current_state(HAConnectionState::Ready).await;

            // Reset state
            self.last_read_timestamp.store(0, Ordering::SeqCst);
            self.dispatch_position.store(0, Ordering::SeqCst);

            // Reset buffers using bytes operations
            let mut read_buffer = self.byte_buffer_read.write().await;
            let mut backup_buffer = self.byte_buffer_backup.write().await;

            read_buffer.clear();
            backup_buffer.clear();
        }
    }

    /// Main service loop
    async fn run_service(self: Arc<Self>) {
        info!("{} service started", self.get_service_name());

        self.flow_monitor.start().await;

        loop {
            // Check for shutdown
            select! {
                _ = self.shutdown_notify.notified() => {
                    self.flow_monitor.shutdown_with_interrupt(true).await;
                    break;
                }
                _ = sleep(Duration::from_millis(100)) => {
                    // Continue with normal processing
                }
            }

            let current_state = *self.current_state.read().await;

            match current_state {
                HAConnectionState::Shutdown => {
                    self.flow_monitor.shutdown_with_interrupt(true).await;
                    break;
                }
                HAConnectionState::Ready => {
                    match self.connect_master().await {
                        Ok(connected) => {
                            if !connected {
                                let addr = self.get_ha_master_address().await;
                                warn!("HAClient connect to master {:?} failed", addr);
                                sleep(Duration::from_secs(5)).await;
                            }
                        }
                        Err(e) => {
                            error!("Connect master error: {}", e);
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                    continue;
                }
                HAConnectionState::Transfer => {
                    if !self.transfer_from_master().await {
                        self.close_master_and_wait().await;
                        continue;
                    }
                }
                _ => {}
            }

            // Check for housekeeping
            let now = self.default_message_store.now();
            let last_read = self.last_read_timestamp.load(Ordering::SeqCst);
            let interval = now as i64 - last_read;
            let housekeeping_interval = self
                .default_message_store
                .get_message_store_config()
                .ha_housekeeping_interval;

            if interval > housekeeping_interval as i64 {
                let addr = self.get_ha_master_address().await;
                warn!(
                    "AutoRecoverHAClient, housekeeping, found this connection[{:?}] expired, {}",
                    addr, interval
                );
                self.close_master().await;
                warn!("AutoRecoverHAClient, master not response some time, so close connection");
            }
        }

        self.flow_monitor.shutdown_with_interrupt(true).await;
        info!("{} service end", self.get_service_name());
    }

    /// Transfer data from master
    async fn transfer_from_master(&self) -> bool {
        // Check if it's time to report offset
        if self.is_time_to_report_offset().await {
            let current_offset = self.current_reported_offset.load(Ordering::SeqCst);
            info!("Slave report current offset {}", current_offset);

            if !self.report_slave_max_offset(current_offset).await {
                return false;
            }
        }

        // Process read events with timeout
        match timeout(Duration::from_secs(1), self.process_read_event()).await {
            Ok(result) => {
                if !result {
                    return false;
                }
            }
            Err(_) => {
                // Timeout is normal, continue
            }
        }

        self.report_slave_max_offset_plus().await
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
        self.change_current_state(HAConnectionState::Shutdown).await;
        self.flow_monitor.shutdown().await;
        self.shutdown_notify.notify_waiters();

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
        self.last_write_timestamp.load(Ordering::SeqCst)
    }

    /// Get last read timestamp
    pub fn get_last_read_timestamp(&self) -> i64 {
        self.last_read_timestamp.load(Ordering::SeqCst)
    }

    /// Get current state
    pub async fn get_current_state(&self) -> HAConnectionState {
        *self.current_state.read().await
    }

    /// Get transferred bytes per second
    pub fn get_transferred_byte_in_second(&self) -> u64 {
        self.flow_monitor.get_transferred_byte_in_second() as u64
    }
}

impl HAClient for DefaultHAClient {
    async fn start(&self) {
        error!("GeneralHAService does not implement start directly, use specific service");
    }

    async fn shutdown(&self) {
        todo!()
    }

    async fn wakeup(&self) {
        todo!()
    }

    /// Update master address
    fn update_master_address(&self, new_address: &str) {
        // Safely free the old pointer before storing the new one
        let old_address = self.master_address.load(Ordering::SeqCst);
        if !old_address.is_null() {
            unsafe {
                // Convert the old pointer back into a Box to free the memory
                let _ = Box::from_raw(old_address);
            }
        }
        // Store the new address
        let new_address_ptr = Box::into_raw(Box::new(new_address.to_string()));
        self.master_address.store(new_address_ptr, Ordering::SeqCst);
    }

    fn update_ha_master_address(&self, new_address: &str) {
        todo!()
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
