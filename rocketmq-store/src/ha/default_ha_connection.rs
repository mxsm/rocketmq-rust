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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::timeout;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::flow_monitor::FlowMonitor;
use crate::ha::ha_connection_state::HAConnectionState;

/// Transfer Header buffer size. Schema: physic offset and body size.
/// Format: [physicOffset (8bytes)][bodySize (4bytes)]
pub const TRANSFER_HEADER_SIZE: usize = 8 + 4;

pub struct DefaultHAConnection {
    ha_service: Arc<DefaultHAService>,
    socket_stream: Arc<RwLock<Option<TcpStream>>>,
    client_address: String,
    write_socket_service: Option<WriteSocketService>,
    read_socket_service: Option<ReadSocketService>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>,
    slave_ack_offset: Arc<AtomicI64>,
    flow_monitor: Arc<FlowMonitor>,
    shutdown_sender: Option<mpsc::Sender<()>>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl DefaultHAConnection {
    /// Create a new DefaultHAConnection
    pub async fn new(
        ha_service: Arc<DefaultHAService>,
        socket_stream: TcpStream,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Result<Self, HAConnectionError> {
        // Get client address
        let client_address = socket_stream
            .peer_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        // Configure socket options
        if let Err(e) = socket_stream.set_nodelay(true) {
            warn!("Failed to set TCP_NODELAY: {}", e);
        }

        let socket_stream = Arc::new(RwLock::new(Some(socket_stream)));
        let flow_monitor = Arc::new(FlowMonitor::new(message_store_config.clone()));

        // Increment connection count
        // ha_service.increment_connection_count();

        let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);

        Ok(Self {
            ha_service,
            socket_stream,
            client_address,
            write_socket_service: None,
            read_socket_service: None,
            current_state: Arc::new(RwLock::new(HAConnectionState::Transfer)),
            slave_request_offset: Arc::new(AtomicI64::new(-1)),
            slave_ack_offset: Arc::new(AtomicI64::new(-1)),
            flow_monitor,
            shutdown_sender: Some(shutdown_sender),
            message_store_config,
        })
    }

    /// Start the HA connection services
    pub async fn start(&mut self) -> Result<(), HAConnectionError> {
        self.change_current_state(HAConnectionState::Transfer).await;

        // Start flow monitor
        self.flow_monitor.start().await;

        // Create and start read service
        let read_service = ReadSocketService::new(
            Arc::clone(&self.socket_stream),
            self.client_address.clone(),
            Arc::clone(&self.ha_service),
            Arc::clone(&self.current_state),
            self.slave_request_offset.clone(),
            self.slave_ack_offset.clone(),
            self.message_store_config.clone(),
        )
        .await?;

        // Create and start write service
        let write_service = WriteSocketService::new(
            Arc::clone(&self.socket_stream),
            self.client_address.clone(),
            Arc::clone(&self.ha_service),
            Arc::clone(&self.current_state),
            self.slave_request_offset.clone(),
            Arc::clone(&self.flow_monitor),
            self.message_store_config.clone(),
        )
        .await?;

        self.read_socket_service = Some(read_service);
        self.write_socket_service = Some(write_service);

        // Start services
        if let Some(ref mut read_service) = self.read_socket_service {
            read_service.start().await?;
        }
        if let Some(ref mut write_service) = self.write_socket_service {
            write_service.start().await?;
        }

        Ok(())
    }

    /// Shutdown the HA connection
    pub async fn shutdown(&mut self) {
        self.change_current_state(HAConnectionState::Shutdown).await;

        // Shutdown services
        if let Some(ref mut write_service) = self.write_socket_service {
            write_service.shutdown().await;
        }
        if let Some(ref mut read_service) = self.read_socket_service {
            read_service.shutdown().await;
        }

        // Shutdown flow monitor
        self.flow_monitor.shutdown().await;

        // Close socket
        self.close().await;

        // Decrement connection count
        //self.ha_service.decrement_connection_count();
    }

    /// Close the socket connection
    pub async fn close(&self) {
        let mut socket_guard = self.socket_stream.write().await;
        if let Some(mut socket) = socket_guard.take() {
            if let Err(e) = socket.shutdown().await {
                error!("Error closing socket: {}", e);
            }
        }
    }

    /// Change the current state
    pub async fn change_current_state(&self, new_state: HAConnectionState) {
        info!("change state to {:?}", new_state);
        let mut state_guard = self.current_state.write().await;
        *state_guard = new_state;
    }

    /// Get current state
    pub async fn get_current_state(&self) -> HAConnectionState {
        *self.current_state.read().await
    }

    /// Get client address
    pub fn get_client_address(&self) -> &str {
        &self.client_address
    }

    /// Get slave ack offset
    pub fn get_slave_ack_offset(&self) -> i64 {
        self.slave_ack_offset.load(Ordering::SeqCst)
    }

    /// Get transferred bytes per second
    pub fn get_transferred_byte_in_second(&self) -> u64 {
        self.flow_monitor.get_transferred_byte_in_second() as u64
    }

    /// Get transfer from where
    pub fn get_transfer_from_where(&self) -> i64 {
        if let Some(ref write_service) = self.write_socket_service {
            write_service.get_next_transfer_from_where()
        } else {
            -1
        }
    }
}

const READ_MAX_BUFFER_SIZE: usize = 1024 * 1024;
const REPORT_HEADER_SIZE: usize = 8;

/// Read Socket Service
pub struct ReadSocketService {
    socket_stream: Arc<RwLock<Option<TcpStream>>>,
    client_address: String,
    ha_service: Arc<DefaultHAService>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>,
    slave_ack_offset: Arc<AtomicI64>,
    shutdown_sender: Option<mpsc::Sender<()>>,
    service_handle: Option<tokio::task::JoinHandle<()>>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl ReadSocketService {
    pub async fn new(
        socket_stream: Arc<RwLock<Option<TcpStream>>>,
        client_address: String,
        ha_service: Arc<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        slave_ack_offset: Arc<AtomicI64>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Result<Self, HAConnectionError> {
        let (shutdown_sender, _) = mpsc::channel(1);

        Ok(Self {
            socket_stream,
            client_address,
            ha_service,
            current_state,
            slave_request_offset,
            slave_ack_offset,
            shutdown_sender: Some(shutdown_sender),
            service_handle: None,
            message_store_config,
        })
    }

    pub async fn start(&mut self) -> Result<(), HAConnectionError> {
        let socket_stream = Arc::clone(&self.socket_stream);
        let client_address = self.client_address.clone();
        let ha_service = Arc::clone(&self.ha_service);
        let current_state = Arc::clone(&self.current_state);
        let slave_request_offset = self.slave_request_offset.clone();
        let slave_ack_offset = self.slave_ack_offset.clone();
        let message_store_config = self.message_store_config.clone();

        let handle = tokio::spawn(async move {
            Self::run_service(
                socket_stream,
                client_address,
                ha_service,
                current_state,
                slave_request_offset,
                slave_ack_offset,
                message_store_config,
            )
            .await;
        });

        self.service_handle = Some(handle);
        Ok(())
    }

    async fn run_service(
        socket_stream: Arc<RwLock<Option<TcpStream>>>,
        client_address: String,
        ha_service: Arc<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        slave_ack_offset: Arc<AtomicI64>,
        message_store_config: Arc<MessageStoreConfig>,
    ) {
        info!("ReadSocketService started for client: {}", client_address);

        let mut buffer = vec![0u8; READ_MAX_BUFFER_SIZE];
        let mut process_position = 0usize;
        let mut last_read_timestamp = Instant::now();

        loop {
            // Check if we should stop
            {
                let state = current_state.read().await;
                if *state == HAConnectionState::Shutdown {
                    break;
                }
            }

            let mut socket_guard = socket_stream.write().await;
            if let Some(ref mut socket) = socket_guard.as_mut() {
                match timeout(
                    Duration::from_secs(1),
                    socket.read(&mut buffer[process_position..]),
                )
                .await
                {
                    Ok(Ok(bytes_read)) => {
                        if bytes_read > 0 {
                            last_read_timestamp = Instant::now();
                            process_position += bytes_read;

                            // Process the read data
                            if process_position >= REPORT_HEADER_SIZE {
                                let read_offset = i64::from_be_bytes([
                                    buffer[process_position - 8],
                                    buffer[process_position - 7],
                                    buffer[process_position - 6],
                                    buffer[process_position - 5],
                                    buffer[process_position - 4],
                                    buffer[process_position - 3],
                                    buffer[process_position - 2],
                                    buffer[process_position - 1],
                                ]);

                                slave_ack_offset.store(read_offset, Ordering::SeqCst);
                                if slave_request_offset.load(Ordering::SeqCst) < 0 {
                                    slave_request_offset.store(read_offset, Ordering::SeqCst);
                                    info!(
                                        "slave[{}] request offset {}",
                                        client_address, read_offset
                                    );
                                }

                                // ha_service.notify_transfer_some(read_offset).await;
                                process_position = 0; // Reset buffer
                            }
                        } else {
                            // Connection closed
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Read error for client {}: {}", client_address, e);
                        break;
                    }
                    Err(_) => {
                        // Timeout - check for housekeeping
                        let interval = last_read_timestamp.elapsed();
                        let housekeeping_interval = message_store_config.ha_housekeeping_interval;
                        if interval > Duration::from_millis(housekeeping_interval) {
                            warn!(
                                "ha housekeeping, found connection[{}] expired, {:?}",
                                client_address, interval
                            );
                            break;
                        }
                    }
                }
            } else {
                break;
            }
            drop(socket_guard);
        }

        // Cleanup
        {
            let mut state_guard = current_state.write().await;
            *state_guard = HAConnectionState::Shutdown;
        }

        // ha_service.remove_connection(&client_address).await;
        info!("ReadSocketService ended for client: {}", client_address);
    }

    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.service_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// Write Socket Service
pub struct WriteSocketService {
    socket_stream: Arc<RwLock<Option<TcpStream>>>,
    client_address: String,
    ha_service: Arc<DefaultHAService>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>,
    flow_monitor: Arc<FlowMonitor>,
    next_transfer_from_where: Arc<AtomicI64>,
    service_handle: Option<tokio::task::JoinHandle<()>>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl WriteSocketService {
    pub async fn new(
        socket_stream: Arc<RwLock<Option<TcpStream>>>,
        client_address: String,
        ha_service: Arc<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        flow_monitor: Arc<FlowMonitor>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Result<Self, HAConnectionError> {
        Ok(Self {
            socket_stream,
            client_address,
            ha_service,
            current_state,
            slave_request_offset,
            flow_monitor,
            next_transfer_from_where: Arc::new(AtomicI64::new(-1)),
            service_handle: None,
            message_store_config,
        })
    }

    pub async fn start(&mut self) -> Result<(), HAConnectionError> {
        let socket_stream = Arc::clone(&self.socket_stream);
        let client_address = self.client_address.clone();
        let ha_service = Arc::clone(&self.ha_service);
        let current_state = Arc::clone(&self.current_state);
        let slave_request_offset = self.slave_request_offset.clone();
        let flow_monitor = Arc::clone(&self.flow_monitor);
        let next_transfer_from_where = self.next_transfer_from_where.clone();
        let message_store_config = Arc::clone(&self.message_store_config);

        let handle = tokio::spawn(async move {
            Self::run_service(
                socket_stream,
                client_address,
                ha_service,
                current_state,
                slave_request_offset,
                flow_monitor,
                next_transfer_from_where,
                message_store_config,
            )
            .await;
        });

        self.service_handle = Some(handle);
        Ok(())
    }

    async fn run_service(
        socket_stream: Arc<RwLock<Option<TcpStream>>>,
        client_address: String,
        ha_service: Arc<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        flow_monitor: Arc<FlowMonitor>,
        next_transfer_from_where: Arc<AtomicI64>,
        message_store_config: Arc<MessageStoreConfig>,
    ) {
        info!("WriteSocketService started for client: {}", client_address);

        let mut last_write_over = true;
        let mut last_write_timestamp = Instant::now();

        loop {
            // Check if we should stop
            {
                let state = current_state.read().await;
                if *state == HAConnectionState::Shutdown {
                    break;
                }
            }

            // Wait for slave request
            if slave_request_offset.load(Ordering::SeqCst) == -1 {
                sleep(Duration::from_millis(10)).await;
                continue;
            }

            // Initialize transfer offset if needed
            if next_transfer_from_where.load(Ordering::SeqCst) == -1 {
                let slave_offset = slave_request_offset.load(Ordering::SeqCst);
                let transfer_offset = if slave_offset == 0 {
                    /*let master_offset = ha_service
                    .get_message_store()::<MS>()
                    .get_commit_log()
                    .get_max_offset();*/
                    let master_offset = 0;
                    let mapped_file_size = message_store_config.mapped_file_size_commit_log;
                    let aligned_offset = master_offset - (master_offset % mapped_file_size as i64);
                    if aligned_offset < 0 {
                        0
                    } else {
                        aligned_offset
                    }
                } else {
                    slave_offset
                };

                next_transfer_from_where.store(transfer_offset, Ordering::SeqCst);
                info!(
                    "master transfer data from {} to slave[{}], and slave request {}",
                    transfer_offset, client_address, slave_offset
                );
            }

            // Handle heartbeat or data transfer
            if last_write_over {
                let interval = last_write_timestamp.elapsed();
                let heartbeat_interval = message_store_config.ha_send_heartbeat_interval;

                if interval > Duration::from_millis(heartbeat_interval) {
                    // Send heartbeat
                    let header =
                        Self::build_header(next_transfer_from_where.load(Ordering::SeqCst), 0);
                    last_write_over =
                        Self::transfer_header(&socket_stream, &header, &flow_monitor).await;
                    if !last_write_over {
                        continue;
                    }
                    last_write_timestamp = Instant::now();
                }
            }

            // Transfer data
            if let Some(data) = MessageStore::get_commit_log_data(
                ha_service.get_default_message_store(),
                next_transfer_from_where.load(Ordering::SeqCst),
            ) {
                //let mut size = data.len();
                let mut size = 0_i32;
                let max_batch_size = message_store_config.ha_transfer_batch_size as i32;
                if size > max_batch_size {
                    size = max_batch_size;
                }

                let can_transfer_max = flow_monitor.can_transfer_max_byte_num();
                if size > can_transfer_max {
                    size = can_transfer_max;
                }

                let this_offset = next_transfer_from_where.load(Ordering::SeqCst);
                next_transfer_from_where.store(this_offset + size as i64, Ordering::SeqCst);

                let header = Self::build_header(this_offset, size);

                // Transfer header and data
                if Self::transfer_header(&socket_stream, &header, &flow_monitor).await {
                    /*last_write_over =
                    Self::transfer_data(&socket_stream, &data[..size as usize], &flow_monitor).await;*/
                    last_write_timestamp = Instant::now();
                }
            } else {
                // No data available, wait
                sleep(Duration::from_millis(100)).await;
            }
        }

        info!("WriteSocketService ended for client: {}", client_address);
    }

    fn build_header(offset: i64, size: i32) -> Vec<u8> {
        let mut header = Vec::with_capacity(TRANSFER_HEADER_SIZE);
        header.extend_from_slice(&offset.to_be_bytes());
        header.extend_from_slice(&size.to_be_bytes());
        header
    }

    async fn transfer_header(
        socket_stream: &Arc<RwLock<Option<TcpStream>>>,
        header: &[u8],
        flow_monitor: &Arc<FlowMonitor>,
    ) -> bool {
        let mut socket_guard = socket_stream.write().await;
        if let Some(ref mut socket) = socket_guard.as_mut() {
            match socket.write_all(header).await {
                Ok(_) => {
                    flow_monitor.add_byte_count_transferred(header.len() as i64);
                    true
                }
                Err(e) => {
                    error!("Failed to write header: {}", e);
                    false
                }
            }
        } else {
            false
        }
    }

    async fn transfer_data(
        socket_stream: &Arc<RwLock<Option<TcpStream>>>,
        data: &[u8],
        flow_monitor: &Arc<FlowMonitor>,
    ) -> bool {
        let mut socket_guard = socket_stream.write().await;
        if let Some(ref mut socket) = socket_guard.as_mut() {
            match socket.write_all(data).await {
                Ok(_) => {
                    flow_monitor.add_byte_count_transferred(data.len() as i64);
                    true
                }
                Err(e) => {
                    error!("Failed to write data: {}", e);
                    false
                }
            }
        } else {
            false
        }
    }

    pub fn get_next_transfer_from_where(&self) -> i64 {
        self.next_transfer_from_where.load(Ordering::SeqCst)
    }

    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.service_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// Error types
#[derive(Debug, thiserror::Error)]
pub enum HAConnectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Service error: {0}")]
    Service(String),
}
