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

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_util::SinkExt;
use futures_util::StreamExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::Decoder;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::flow_monitor::FlowMonitor;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_service::HAService;
use crate::ha::HAConnectionError;

/// Transfer Header buffer size. Schema: physic offset and body size.
/// Format: [physicOffset (8bytes)][bodySize (4bytes)]
pub const TRANSFER_HEADER_SIZE: usize = 8 + 4;

pub struct DefaultHAConnection {
    ha_service: ArcMut<DefaultHAService>,
    socket_stream: Option<TcpStream>,
    client_address: String,
    read_service_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    write_service_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>,
    slave_ack_offset: Arc<AtomicI64>,
    flow_monitor: Arc<FlowMonitor>,
    shutdown_tx: Arc<Mutex<Option<tokio::sync::broadcast::Sender<()>>>>,
    message_store_config: Arc<MessageStoreConfig>,
    next_transfer_from_where: Arc<AtomicI64>,
    id: HAConnectionId,
    remote_addr: SocketAddr,
}

impl DefaultHAConnection {
    /// Create a new DefaultHAConnection
    pub async fn new(
        ha_service: ArcMut<DefaultHAService>,
        socket_stream: TcpStream,
        message_store_config: Arc<MessageStoreConfig>,
        remote_addr: SocketAddr,
    ) -> Result<Self, HAConnectionError> {
        // Configure socket options early
        socket_stream.set_nodelay(true).map_err(HAConnectionError::Io)?;

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
        ha_service.get_connection_count().fetch_add(1, Ordering::SeqCst);

        let (shutdown_sender, shutdown_receiver) = mpsc::channel::<()>(1);

        Ok(Self {
            ha_service,
            socket_stream,
            client_address,
            read_service_handle: Arc::new(Mutex::new(None)),
            write_service_handle: Arc::new(Mutex::new(None)),
            current_state: Arc::new(RwLock::new(HAConnectionState::Transfer)),
            slave_request_offset: Arc::new(AtomicI64::new(-1)),
            slave_ack_offset: Arc::new(AtomicI64::new(-1)),
            flow_monitor,
            shutdown_tx: Arc::new(Mutex::new(None)),
            message_store_config,
            next_transfer_from_where: Arc::new(AtomicI64::new(-1)),
            id: HAConnectionId::default(),
            remote_addr,
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
    async fn start(&mut self, conn: WeakArcMut<GeneralHAConnection>) -> Result<(), HAConnectionError> {
        const CAPACITY: usize = 1024 * 8;
        self.change_current_state(HAConnectionState::Transfer).await;

        // Start flow monitor
        self.flow_monitor.start().await;

        let tcp_stream = self
            .socket_stream
            .take()
            .ok_or_else(|| HAConnectionError::InvalidState("Socket already taken".into()))?;
        // let framed = Framed::with_capacity(tcp_stream, BytesCodec::new(), CAPACITY);
        let (reader, write) = tcp_stream.into_split();

        // Create shutdown channel
        // Create shutdown channel with bounded capacity
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(16);
        let read_shutdown_rx = shutdown_tx.subscribe();
        let write_shutdown_rx = shutdown_tx.subscribe();

        // Store shutdown sender before spawning tasks
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        // Create and start read service
        let read_service = ReadSocketService::new(
            FramedRead::new(reader, OffsetDecoder),
            self.client_address.clone(),
            ArcMut::clone(&self.ha_service),
            Arc::clone(&self.current_state),
            self.slave_request_offset.clone(),
            self.slave_ack_offset.clone(),
            self.message_store_config.clone(),
            conn.clone(),
        )
        .await?;

        // Create and start write service
        let write_service = WriteSocketService::new(
            FramedWrite::new(write, BytesCodec::new()),
            self.client_address.clone(),
            ArcMut::clone(&self.ha_service),
            Arc::clone(&self.current_state),
            self.slave_request_offset.clone(),
            Arc::clone(&self.flow_monitor),
            self.message_store_config.clone(),
            conn,
            self.next_transfer_from_where.clone(),
        )
        .await?;

        let read_handle = tokio::spawn(async move {
            read_service.run(read_shutdown_rx).await;
        });

        let write_handle = tokio::spawn(async move {
            write_service.run(write_shutdown_rx).await;
        });

        // Start services
        *self.read_service_handle.lock().await = Some(read_handle);
        *self.write_service_handle.lock().await = Some(write_handle);

        info!("HAConnection started for {}", self.client_address);
        Ok(())
    }

    async fn shutdown(&mut self) {
        self.change_current_state(HAConnectionState::Shutdown).await;

        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }

        // Wait for services to stop
        if let Some(handle) = self.read_service_handle.lock().await.take() {
            let _ = handle.await;
        }

        if let Some(handle) = self.write_service_handle.lock().await.take() {
            let _ = handle.await;
        }

        // Shutdown flow monitor
        self.flow_monitor.shutdown().await;

        // Close socket
        self.close();

        // Decrement connection count
        let connection_count = self.ha_service.get_connection_count();
        if connection_count.load(Ordering::SeqCst) > 0 {
            connection_count.fetch_sub(1, Ordering::SeqCst);
        }
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
        self.next_transfer_from_where.load(Ordering::Relaxed)
    }

    fn get_slave_ack_offset(&self) -> i64 {
        self.slave_ack_offset.load(Ordering::SeqCst)
    }

    fn get_ha_connection_id(&self) -> &HAConnectionId {
        &self.id
    }

    fn remote_address(&self) -> String {
        self.remote_addr.to_string()
    }
}

const READ_MAX_BUFFER_SIZE: usize = 1024 * 1024; // 1 MB
const REPORT_HEADER_SIZE: usize = 8;
const SELECT_TIMEOUT: Duration = Duration::from_millis(1000);

#[derive(Debug)]
pub(in crate::ha) struct OffsetFrame(pub i64);

pub(in crate::ha) struct OffsetDecoder;

impl Decoder for OffsetDecoder {
    type Item = OffsetFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < REPORT_HEADER_SIZE {
            return Ok(None); // not enough data yet
        }

        // ensure we work on 8-byte alignment
        let aligned_size = src.len() - (src.len() % REPORT_HEADER_SIZE);
        if aligned_size < 8 {
            return Ok(None);
        }

        // We have at least 8 bytes, safe to read
        // Parse the first 8 bytes as big-endian i64
        // SAFETY: We just checked src.len() >= 8
        let offset_bytes: [u8; 8] = src[..REPORT_HEADER_SIZE]
            .try_into()
            .expect("Slice with incorrect length");
        let offset = i64::from_be_bytes(offset_bytes);

        // Advance the buffer by the size of the consumed frame (8 bytes)
        src.advance(REPORT_HEADER_SIZE);

        Ok(Some(OffsetFrame(offset)))
    }
}

/// Read Socket Service
/// The main node processes requests from the slave nodes, reads the maximum request offset of the
/// slave nodes.
pub struct ReadSocketService {
    reader: FramedRead<OwnedReadHalf, OffsetDecoder>,
    client_address: String,
    ha_service: ArcMut<DefaultHAService>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>,
    slave_ack_offset: Arc<AtomicI64>,
    buffer: BytesMut,
    process_position: usize,
    message_store_config: Arc<MessageStoreConfig>,
    last_read_timestamp: AtomicU64,
    connection: WeakArcMut<GeneralHAConnection>,
}

impl ReadSocketService {
    pub async fn new(
        reader: FramedRead<OwnedReadHalf, OffsetDecoder>,
        client_address: String,
        ha_service: ArcMut<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        slave_ack_offset: Arc<AtomicI64>,
        message_store_config: Arc<MessageStoreConfig>,
        connection: WeakArcMut<GeneralHAConnection>,
    ) -> Result<Self, HAConnectionError> {
        let (shutdown_sender, _) = mpsc::channel::<()>(1);

        Ok(Self {
            reader,
            client_address,
            ha_service,
            current_state,
            slave_request_offset,
            slave_ack_offset,
            buffer: BytesMut::with_capacity(READ_MAX_BUFFER_SIZE),
            process_position: 0,
            message_store_config,
            last_read_timestamp: AtomicU64::new(get_current_millis()),
            connection,
        })
    }

    pub async fn run(mut self, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) {
        info!("{} service started", self.get_service_name());

        loop {
            //let select_result = timeout(SELECT_TIMEOUT, self.reader.next()).await;
            let select_result = select! {
                _ = shutdown_rx.recv() => {
                    info!("Received shutdown signal");
                    break;
                }
                select_result = self.reader.next() => {
                    select_result
                }
            };
            match select_result {
                None => {
                    info!("Stream closed by peer");
                    break;
                }
                Some(Ok(OffsetFrame(offset))) => {
                    self.last_read_timestamp.store(get_current_millis(), Ordering::Relaxed);
                    self.slave_ack_offset.store(offset, Ordering::Relaxed);

                    if self.slave_request_offset.load(Ordering::Acquire) < 0 {
                        self.slave_request_offset.store(offset, Ordering::Release);
                        info!("slave[{}] request offset {}", self.client_address, offset);
                    }
                    self.ha_service.notify_transfer_some(offset).await;
                }
                Some(Err(e)) => {
                    error!("Stream error: {}", e);
                    break;
                }
            }

            let current_time = get_current_millis();
            let last_read = self.last_read_timestamp.load(Ordering::Relaxed);
            let interval = current_time - last_read;

            if interval > self.message_store_config.ha_housekeeping_interval {
                warn!(
                    "ha housekeeping, found this connection[{}] expired, {}",
                    self.client_address, interval
                );
                break;
            }

            if self.is_stopped().await {
                break;
            }
        }

        self.cleanup().await;
        info!("{} service end", self.get_service_name());
    }

    async fn process_incoming_data(&mut self, data: BytesMut) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.buffer.len() + data.len() > READ_MAX_BUFFER_SIZE {
            self.compact_buffer();
        }

        self.buffer.extend_from_slice(&data);

        while self.can_process_message() {
            self.process_message().await?;
        }

        Ok(())
    }

    fn can_process_message(&self) -> bool {
        let available_data = self.buffer.len() - self.process_position;
        available_data >= REPORT_HEADER_SIZE
    }

    async fn process_message(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let buffer_position = self.buffer.len();
        let available_data = buffer_position - self.process_position;

        if available_data >= REPORT_HEADER_SIZE {
            let aligned_size = available_data - (available_data % REPORT_HEADER_SIZE);
            let pos = self.process_position + aligned_size;

            if pos >= 8 {
                let offset_start = pos - 8;
                let offset_bytes = &self.buffer[offset_start..pos];

                let read_offset = i64::from_be_bytes([
                    offset_bytes[0],
                    offset_bytes[1],
                    offset_bytes[2],
                    offset_bytes[3],
                    offset_bytes[4],
                    offset_bytes[5],
                    offset_bytes[6],
                    offset_bytes[7],
                ]);
                self.process_position = pos;
                self.slave_ack_offset.store(read_offset, Ordering::Relaxed);
                if self.slave_request_offset.load(Ordering::Acquire) < 0 {
                    self.slave_request_offset.store(read_offset, Ordering::Release);
                    info!("slave[{}] request offset {}", self.client_address, read_offset);
                }

                self.ha_service.notify_transfer_some(read_offset).await;
            }
        }

        Ok(())
    }

    fn compact_buffer(&mut self) {
        if self.process_position > 0 {
            let remaining_data = self.buffer.len() - self.process_position;
            if remaining_data > 0 {
                let remaining = self.buffer.split_off(self.process_position);
                self.buffer = remaining;
            } else {
                self.buffer.clear();
            }
            self.process_position = 0;
        }
    }

    async fn cleanup(&self) {}

    async fn is_stopped(&self) -> bool {
        matches!(*self.current_state.read().await, HAConnectionState::Shutdown)
    }

    fn get_service_name(&self) -> String {
        format!("ReadSocketService[{}]", self.client_address)
    }
}

/// Write Socket Service
pub struct WriteSocketService {
    //writer: SplitSink<Framed<TcpStream, BytesCodec>, Bytes>,
    writer: FramedWrite<OwnedWriteHalf, BytesCodec>,
    client_address: String,
    ha_service: ArcMut<DefaultHAService>,
    current_state: Arc<RwLock<HAConnectionState>>,
    slave_request_offset: Arc<AtomicI64>, // this is the offset requested by the slave
    flow_monitor: Arc<FlowMonitor>,
    next_transfer_from_where: Arc<AtomicI64>,
    message_store_config: Arc<MessageStoreConfig>,
    byte_buffer_header: BytesMut,
    connection: WeakArcMut<GeneralHAConnection>,
    last_write_timestamp: AtomicU64,
    last_print_timestamp: AtomicU64,
    last_write_over: AtomicBool,
}

impl WriteSocketService {
    pub async fn new(
        // writer: SplitSink<Framed<TcpStream, BytesCodec>, Bytes>,
        writer: FramedWrite<OwnedWriteHalf, BytesCodec>,
        client_address: String,
        ha_service: ArcMut<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        flow_monitor: Arc<FlowMonitor>,
        message_store_config: Arc<MessageStoreConfig>,
        connection: WeakArcMut<GeneralHAConnection>,
        next_transfer_from_where: Arc<AtomicI64>,
    ) -> Result<Self, HAConnectionError> {
        Ok(Self {
            writer,
            client_address,
            ha_service,
            current_state,
            slave_request_offset,
            flow_monitor,
            next_transfer_from_where,
            message_store_config,
            connection,
            last_write_timestamp: AtomicU64::new(get_current_millis()),
            last_print_timestamp: AtomicU64::new(get_current_millis()),
            byte_buffer_header: BytesMut::with_capacity(TRANSFER_HEADER_SIZE),
            last_write_over: AtomicBool::new(true),
        })
    }

    pub async fn run(mut self, mut shutdown_rx: tokio::sync::broadcast::Receiver<()>) {
        info!("{} service started", self.get_service_name());

        loop {
            let select_result = timeout(SELECT_TIMEOUT, async {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Received shutdown signal");
                         false
                    }
                    _ = tokio::task::yield_now() => {
                         true
                    }
                }
            })
            .await;

            match select_result {
                Ok(false) => {
                    break;
                }
                Ok(true) => {
                    if let Err(e) = self.process_transfer().await {
                        error!("Transfer error: {}", e);
                        break;
                    }
                }
                Err(_) => {
                    if let Err(e) = self.process_transfer().await {
                        error!("Transfer error: {}", e);
                        break;
                    }
                }
            }
            if self.is_stopped().await {
                break;
            }
        }

        self.cleanup().await;
        info!("{} service end", self.get_service_name());
    }

    async fn process_transfer(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let slave_request_offset = self.slave_request_offset.load(Ordering::Relaxed);
        if slave_request_offset == -1 {
            sleep(Duration::from_millis(10)).await;
            return Ok(());
        }

        if self.next_transfer_from_where.load(Ordering::Relaxed) == -1 {
            // If next_transfer_from_where is -1, we need to set it to the master offset
            let next_offset = if slave_request_offset == 0 {
                let mut master_offset = self
                    .ha_service
                    .get_default_message_store()
                    .get_commit_log()
                    .get_max_offset();
                let mapped_file_size = self.message_store_config.mapped_file_size_commit_log;
                master_offset = master_offset - (master_offset % mapped_file_size as i64);
                if master_offset < 0 {
                    master_offset = 0;
                }
                master_offset
            } else {
                slave_request_offset
            };
            //set next_transfer_from_where to the next_offset
            self.next_transfer_from_where.store(next_offset, Ordering::Relaxed);
            info!(
                "master transfer data from {} to slave[{}], and slave request {}",
                next_offset, self.client_address, slave_request_offset
            );
        }

        if self.last_write_over.load(Ordering::Relaxed) {
            let current_time = get_current_millis();
            let last_write = self.last_write_timestamp.load(Ordering::Relaxed);
            let interval = current_time - last_write;

            let heartbeat_interval = self.message_store_config.ha_send_heartbeat_interval;

            if interval > heartbeat_interval {
                match self.send_heartbeat().await {
                    Ok(_) => {
                        self.last_write_over.store(true, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.last_write_over.store(false, Ordering::Relaxed);
                        return Ok(());
                    }
                }
            }
        } else {
            match self.send_heartbeat().await {
                Ok(_) => {
                    self.last_write_over.store(true, Ordering::Relaxed);
                }
                Err(_) => {
                    self.last_write_over.store(false, Ordering::Relaxed);
                    return Ok(());
                }
            }
        }

        let next_offset = self.next_transfer_from_where.load(Ordering::Relaxed);
        if let Some(select_result) = self
            .ha_service
            .get_default_message_store()
            .get_commit_log_data(next_offset)
        {
            let mut size = select_result.size as usize;
            let max_batch_size = self.message_store_config.ha_transfer_batch_size;

            if size > max_batch_size {
                size = max_batch_size;
            }

            let can_transfer_max_bytes = self.flow_monitor.can_transfer_max_byte_num();
            if size > can_transfer_max_bytes as usize {
                let current_time = get_current_millis();
                let last_print = self.last_print_timestamp.load(Ordering::Relaxed);

                if current_time - last_print > 1000 {
                    warn!(
                        "Trigger HA flow control, max transfer speed {:.2}KB/s, current speed: {:.2}KB/s",
                        self.flow_monitor.max_transfer_byte_in_second() as f64 / 1024.0,
                        self.flow_monitor.get_transferred_byte_in_second() as f64 / 1024.0
                    );
                    self.last_print_timestamp.store(current_time, Ordering::Relaxed);
                }
                size = can_transfer_max_bytes as usize;
            }

            let this_offset = next_offset;
            self.next_transfer_from_where
                .store(next_offset + size as i64, Ordering::Relaxed);

            self.send_data(this_offset, select_result.get_bytes(), size).await?;
        } else {
            //self.ha_service.wait_for_running(100).await;
        }

        Ok(())
    }

    async fn send_heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.byte_buffer_header.clear();
        let next_offset = self.next_transfer_from_where.load(Ordering::Relaxed);
        self.byte_buffer_header.put_i64(next_offset);
        self.byte_buffer_header.put_i32(0); // 0 size indicates heartbeat

        let bytes = self.byte_buffer_header.split().freeze();
        self.writer.send(bytes).await?;

        self.last_write_timestamp.store(get_current_millis(), Ordering::Relaxed);
        self.flow_monitor
            .add_byte_count_transferred(TRANSFER_HEADER_SIZE as i64);
        self.last_write_over.store(true, Ordering::Relaxed);

        Ok(())
    }

    async fn send_data(
        &mut self,
        offset: i64,
        select_result: Option<Bytes>,
        size: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.byte_buffer_header.clear();
        self.byte_buffer_header.put_i64(offset);
        self.byte_buffer_header.put_i32(size as i32);
        let header_bytes = self.byte_buffer_header.split().freeze();

        self.writer.send(header_bytes).await?;
        if let Some(mut data) = select_result {
            self.writer.send(data.split_to(size)).await?;
        } else {
            warn!("No data to send for offset {}", offset);
        }

        self.last_write_timestamp.store(get_current_millis(), Ordering::Relaxed);
        self.flow_monitor
            .add_byte_count_transferred((TRANSFER_HEADER_SIZE + size) as i64);
        self.last_write_over.store(true, Ordering::Relaxed);

        Ok(())
    }

    async fn cleanup(&mut self) {
        *self.current_state.write().await = HAConnectionState::Shutdown;

        /*        if let Err(e) = self.writer.close().await {
            error!("Error closing sink: {}", e);
        }*/

        if let Some(connection) = self.connection.upgrade() {
            self.ha_service.remove_connection(connection).await;
        }

        self.flow_monitor.shutdown().await;
    }

    async fn is_stopped(&self) -> bool {
        matches!(*self.current_state.read().await, HAConnectionState::Shutdown)
    }

    fn get_service_name(&self) -> String {
        format!("WriteSocketService[{}]", self.client_address)
    }
}
