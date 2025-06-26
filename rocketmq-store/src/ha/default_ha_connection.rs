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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::SinkExt;
use futures_util::StreamExt;
use rocketmq_rust::ArcMut;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::Framed;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::flow_monitor::FlowMonitor;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::HAConnectionError;

/// Transfer Header buffer size. Schema: physic offset and body size.
/// Format: [physicOffset (8bytes)][bodySize (4bytes)]
pub const TRANSFER_HEADER_SIZE: usize = 8 + 4;

pub struct DefaultHAConnection {
    ha_service: ArcMut<DefaultHAService>,
    socket_stream: Option<TcpStream>,
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
        ha_service: ArcMut<DefaultHAService>,
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

        let socket_stream = Some(socket_stream);
        let flow_monitor = Arc::new(FlowMonitor::new(message_store_config.clone()));

        // Increment connection count
        ha_service
            .get_connection_count()
            .fetch_add(1, Ordering::SeqCst);

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

    /// Change the current state
    pub async fn change_current_state(&self, new_state: HAConnectionState) {
        info!("change state to {:?}", new_state);
        let mut state_guard = self.current_state.write().await;
        *state_guard = new_state;
    }
}

impl HAConnection for DefaultHAConnection {
    async fn start(&mut self) -> Result<(), HAConnectionError> {
        self.change_current_state(HAConnectionState::Transfer).await;

        // Start flow monitor
        self.flow_monitor.start().await;

        let tcp_stream = self.socket_stream.take().unwrap();
        let framed = Framed::with_capacity(tcp_stream, BytesCodec::new(), 1024 * 4);
        let (writer, reader) = framed.split();

        // Create and start read service
        let read_service = ReadSocketService::new(
            reader,
            self.client_address.clone(),
            ArcMut::clone(&self.ha_service),
            Arc::clone(&self.current_state),
            self.slave_request_offset.clone(),
            self.slave_ack_offset.clone(),
            self.message_store_config.clone(),
        )
        .await?;

        // Create and start write service
        let write_service = WriteSocketService::new(
            writer,
            self.client_address.clone(),
            ArcMut::clone(&self.ha_service),
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

    async fn shutdown(&mut self) {
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
        self.close();

        // Decrement connection count
        self.ha_service
            .get_connection_count()
            .fetch_sub(1, Ordering::SeqCst);
    }

    fn close(&self) {
        //nothing to do here, the socket will be closed by the services
    }

    fn get_socket(&self) -> &TcpStream {
        todo!()
    }

    async fn get_current_state(&self) -> HAConnectionState {
        *self.current_state.read().await
    }

    fn get_client_address(&self) -> &str {
        &self.client_address
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        self.flow_monitor.get_transferred_byte_in_second()
    }

    fn get_transfer_from_where(&self) -> i64 {
        if let Some(ref write_service) = self.write_socket_service {
            write_service.get_next_transfer_from_where()
        } else {
            -1
        }
    }

    fn get_slave_ack_offset(&self) -> i64 {
        self.slave_ack_offset.load(Ordering::SeqCst)
    }
}

const READ_MAX_BUFFER_SIZE: usize = 1024 * 1024;
const REPORT_HEADER_SIZE: usize = 8;

/// Read Socket Service
/// The main node processes requests from the slave nodes, reads the maximum request offset of the
/// slave nodes.
pub struct ReadSocketService {
    reader: Option<SplitStream<Framed<TcpStream, BytesCodec>>>,
    client_address: String,
    ha_service: ArcMut<DefaultHAService>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>,
    slave_ack_offset: Arc<AtomicI64>,
    shutdown_sender: Option<mpsc::Sender<()>>,
    service_handle: Option<tokio::task::JoinHandle<()>>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl ReadSocketService {
    pub async fn new(
        reader: SplitStream<Framed<TcpStream, BytesCodec>>,
        client_address: String,
        ha_service: ArcMut<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        slave_ack_offset: Arc<AtomicI64>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Result<Self, HAConnectionError> {
        let (shutdown_sender, _) = mpsc::channel(1);

        Ok(Self {
            reader: Some(reader),
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
        let socket_stream = self.reader.take();
        let client_address = self.client_address.clone();
        let ha_service = ArcMut::clone(&self.ha_service);
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
        socket_stream: Option<SplitStream<Framed<TcpStream, BytesCodec>>>,
        client_address: String,
        ha_service: ArcMut<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        slave_ack_offset: Arc<AtomicI64>,
        message_store_config: Arc<MessageStoreConfig>,
    ) {
        /* info!("ReadSocketService started for client: {}", client_address);

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

            if let Some(ref mut socket) = socket_stream {
                loop {
                    if buffer.has_remaining_mut() {
                        buffer.clear();
                        process_position = 0;
                    }
                    match timeout(
                        Duration::from_secs(1),
                        socket.next(),
                    )
                    .await
                    {
                        Ok(Ok(bytes_read)) => {
                            if bytes_read > 0 {
                                last_read_timestamp = Instant::now();
                                // Process the read data
                                if buffer.len() - process_position >= REPORT_HEADER_SIZE {
                                    //In general, pos is equal to buffer.len(), mainly to handle
                                    // the alignment issue of the buffer.
                                    let pos = buffer.len() - (buffer.len() % REPORT_HEADER_SIZE);
                                    let read_offset = i64::from_be_bytes([
                                        buffer[pos - 8],
                                        buffer[pos - 7],
                                        buffer[pos - 6],
                                        buffer[pos - 5],
                                        buffer[pos - 4],
                                        buffer[pos - 3],
                                        buffer[pos - 2],
                                        buffer[pos - 1],
                                    ]);
                                    process_position = pos;
                                    slave_ack_offset.store(read_offset, Ordering::SeqCst);
                                    if slave_request_offset.load(Ordering::SeqCst) < 0 {
                                        slave_request_offset.store(read_offset, Ordering::SeqCst);
                                        info!(
                                            "slave[{}] request offset {}",
                                            client_address, read_offset
                                        );
                                    }
                                    ha_service.notify_transfer_some(read_offset).await;
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
                            let housekeeping_interval =
                                message_store_config.ha_housekeeping_interval;
                            if interval > Duration::from_millis(housekeeping_interval) {
                                warn!(
                                    "ha housekeeping, found connection[{}] expired, {:?}",
                                    client_address, interval
                                );
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                break;
            }
        }

        // Cleanup
        {
            let mut state_guard = current_state.write().await;
            *state_guard = HAConnectionState::Shutdown;
        }

        // ha_service.remove_connection(&client_address).await;
        info!("ReadSocketService ended for client: {}", client_address);*/
        unimplemented!("ReadSocketService is not implemented yet");
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
    writer: Option<SplitSink<Framed<TcpStream, BytesCodec>, Bytes>>,
    client_address: String,
    ha_service: ArcMut<DefaultHAService>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>,
    flow_monitor: Arc<FlowMonitor>,
    next_transfer_from_where: Arc<AtomicI64>,
    service_handle: Option<tokio::task::JoinHandle<()>>,
    message_store_config: Arc<MessageStoreConfig>,
}

impl WriteSocketService {
    pub async fn new(
        writer: SplitSink<Framed<TcpStream, BytesCodec>, Bytes>,
        client_address: String,
        ha_service: ArcMut<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        flow_monitor: Arc<FlowMonitor>,
        message_store_config: Arc<MessageStoreConfig>,
    ) -> Result<Self, HAConnectionError> {
        Ok(Self {
            writer: Some(writer),
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
        let socket_stream = self.writer.take();
        let client_address = self.client_address.clone();
        let ha_service = ArcMut::clone(&self.ha_service);
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
        mut socket_stream: Option<SplitSink<Framed<TcpStream, BytesCodec>, Bytes>>,
        client_address: String,
        ha_service: ArcMut<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        flow_monitor: Arc<FlowMonitor>,
        next_transfer_from_where: Arc<AtomicI64>,
        message_store_config: Arc<MessageStoreConfig>,
    ) {
        info!("WriteSocketService started for client: {}", client_address);

        let mut last_write_over = true;
        let mut last_write_timestamp = Instant::now();
        let mut byte_buffer_header = BytesMut::with_capacity(TRANSFER_HEADER_SIZE);

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
                    let master_offset = ha_service
                        .get_default_message_store()
                        .get_commit_log()
                        .get_max_offset();
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
                    byte_buffer_header.clear();
                    // Send heartbeat
                    byte_buffer_header.put_i64(next_transfer_from_where.load(Ordering::SeqCst));
                    byte_buffer_header.put_i32(0);
                    let header = byte_buffer_header.split().freeze();
                    last_write_over =
                        Self::transfer_data(&mut socket_stream, header, None, &flow_monitor).await;
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
                let mut size = data.size;
                let max_batch_size = message_store_config.ha_transfer_batch_size as i32;
                if size > max_batch_size {
                    size = max_batch_size;
                }

                let can_transfer_max = flow_monitor.can_transfer_max_byte_num();
                if size > can_transfer_max {
                    size = can_transfer_max;
                }

                let this_offset = next_transfer_from_where.fetch_add(size as i64, Ordering::SeqCst);

                byte_buffer_header.clear();
                // Send heartbeat
                byte_buffer_header.put_i64(next_transfer_from_where.load(Ordering::SeqCst));
                byte_buffer_header.put_i32(size);
                let header = byte_buffer_header.split().freeze();
                let data_buffer = data.bytes.as_ref().map(|bytes| {
                    // If the data is larger than the size, slice it
                    bytes.slice(..size as usize)
                });
                Self::transfer_data(&mut socket_stream, header, data_buffer, &flow_monitor).await;
            } else {
                // No data available, wait
                sleep(Duration::from_millis(100)).await;
            }
        }

        info!("WriteSocketService ended for client: {}", client_address);
    }

    async fn transfer_data(
        socket_stream: &mut Option<SplitSink<Framed<TcpStream, BytesCodec>, Bytes>>,
        buffer_header: Bytes,
        select_mapped_buffer: Option<Bytes>,
        flow_monitor: &Arc<FlowMonitor>,
    ) -> bool {
        if let Some(ref mut socket) = socket_stream {
            let len = buffer_header.len();
            let result = match socket.send(buffer_header).await {
                Ok(_) => {
                    flow_monitor.add_byte_count_transferred(len as i64);
                    true
                }
                Err(e) => {
                    error!("Failed to write data: {}", e);
                    false
                }
            };

            if let Some(data) = select_mapped_buffer {
                let len = data.len();
                match socket.send(data).await {
                    Ok(_) => {
                        flow_monitor.add_byte_count_transferred(len as i64);
                        true
                    }
                    Err(e) => {
                        error!("Failed to write data: {}", e);
                        false
                    }
                }
            } else {
                result
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
