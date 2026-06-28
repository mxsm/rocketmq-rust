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
use futures_util::StreamExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::io::AsyncWrite;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_util::codec::Decoder;
use tokio_util::codec::FramedRead;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::config::message_store_config::LinuxTransferEngine;
use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::default_ha_client::CONTROLLER_REPORT_HEADER_SIZE;
use crate::ha::default_ha_service::DefaultHAService;
use crate::ha::flow_monitor::FlowMonitor;
use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection::HAConnectionId;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_service::HAService;
use crate::ha::transfer_engine::select_transfer_engine_with_availability;
use crate::ha::transfer_engine::HaTransferEngine;
use crate::ha::transfer_engine::TransferEngineAvailability;
use crate::ha::transfer_engine::TransferEngineKind;
use crate::ha::transfer_engine::TransferEnginePreference;
use crate::ha::HAConnectionError;
use crate::transfer::batch::TransferBatch;
use crate::transfer::batch::TransferKind;
use crate::transfer::batch::TransferPlan;
use crate::transfer::planner::TransferPlanInput;
use crate::transfer::planner::TransferPlanner;

/// Transfer Header buffer size. Schema: physic offset and body size.
/// Format: [physicOffset (8bytes)][bodySize (4bytes)]
pub const TRANSFER_HEADER_SIZE: usize = 8 + 4;
/// Controller transfer header extends the default header with confirm offset.
/// Format: [physicOffset (8bytes)][bodySize (4bytes)][confirmOffset (8bytes)]
pub(crate) const CONTROLLER_TRANSFER_HEADER_SIZE: usize = TRANSFER_HEADER_SIZE + 8;
pub(crate) const DEFAULT_HA_TRANSFER_BATCH_SIZE: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransferHeader {
    pub master_phy_offset: i64,
    pub body_size: usize,
    pub confirm_offset: Option<i64>,
}

pub(crate) const fn transfer_header_size(enable_controller_mode: bool) -> usize {
    if enable_controller_mode {
        CONTROLLER_TRANSFER_HEADER_SIZE
    } else {
        TRANSFER_HEADER_SIZE
    }
}

pub(crate) fn encode_transfer_header(
    byte_buffer_header: &mut BytesMut,
    master_phy_offset: i64,
    body_size: usize,
    enable_controller_mode: bool,
    confirm_offset: i64,
) -> Bytes {
    byte_buffer_header.clear();
    byte_buffer_header.put_i64(master_phy_offset);
    byte_buffer_header.put_i32(i32::try_from(body_size).expect("transfer body size exceeds i32"));
    if enable_controller_mode {
        byte_buffer_header.put_i64(confirm_offset);
    }
    byte_buffer_header.split().freeze()
}

pub(crate) const fn effective_ha_transfer_batch_size(configured_batch_size: usize) -> usize {
    if configured_batch_size == 0 {
        DEFAULT_HA_TRANSFER_BATCH_SIZE
    } else {
        configured_batch_size
    }
}

pub(crate) fn decode_transfer_header(
    src: &[u8],
    enable_controller_mode: bool,
) -> Result<TransferHeader, HAConnectionError> {
    let header_size = transfer_header_size(enable_controller_mode);
    if src.len() < header_size {
        return Err(HAConnectionError::Service(format!(
            "transfer header underflow: expected at least {header_size} bytes, got {}",
            src.len()
        )));
    }

    let master_phy_offset = i64::from_be_bytes(src[0..8].try_into().expect("slice len 8"));
    let body_size = i32::from_be_bytes(src[8..12].try_into().expect("slice len 4"));
    if body_size < 0 {
        return Err(HAConnectionError::Service(format!(
            "transfer header contains negative body size: {body_size}"
        )));
    }
    let confirm_offset = enable_controller_mode.then(|| {
        i64::from_be_bytes(
            src[TRANSFER_HEADER_SIZE..CONTROLLER_TRANSFER_HEADER_SIZE]
                .try_into()
                .expect("slice len 8"),
        )
    });

    Ok(TransferHeader {
        master_phy_offset,
        body_size: body_size as usize,
        confirm_offset,
    })
}

pub struct DefaultHAConnection {
    ha_service: ArcMut<DefaultHAService>,
    socket_stream: Option<TcpStream>,
    client_address: String,
    task_group: Arc<Mutex<Option<rocketmq_runtime::TaskGroup>>>,
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
            task_group: Arc::new(Mutex::new(None)),
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
        self.change_current_state(HAConnectionState::Transfer).await;

        // Start flow monitor
        self.flow_monitor
            .start()
            .await
            .map_err(|error| HAConnectionError::Service(format!("failed to start flow monitor: {error}")))?;

        let tcp_stream = self
            .socket_stream
            .take()
            .ok_or_else(|| HAConnectionError::InvalidState("Socket already taken".into()))?;
        let std_stream = tcp_stream.into_std().map_err(HAConnectionError::Io)?;
        let retained_std_stream = std_stream.try_clone().map_err(HAConnectionError::Io)?;
        let retained_stream = TcpStream::from_std(retained_std_stream).map_err(HAConnectionError::Io)?;
        let split_stream = TcpStream::from_std(std_stream).map_err(HAConnectionError::Io)?;
        self.socket_stream = Some(retained_stream);
        let (reader, write) = split_stream.into_split();

        // Create shutdown channel
        // Create shutdown channel with bounded capacity
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(16);
        let read_shutdown_rx = shutdown_tx.subscribe();
        let write_shutdown_rx = shutdown_tx.subscribe();

        // Store shutdown sender before spawning tasks
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        // Create and start read service
        let read_service = ReadSocketService::new(
            FramedRead::new(
                reader,
                OffsetDecoder::new(if self.message_store_config.enable_controller_mode {
                    CONTROLLER_REPORT_HEADER_SIZE
                } else {
                    REPORT_HEADER_SIZE
                }),
            ),
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
            write,
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

        let task_group = crate::runtime::task_group("rocketmq-store.ha.connection")
            .map_err(|error| HAConnectionError::Service(error.to_string()))?;
        task_group
            .spawn_service("ha-read-socket-service", async move {
                read_service.run(read_shutdown_rx).await;
            })
            .map_err(|error| HAConnectionError::Service(error.to_string()))?;

        if let Err(error) = task_group.spawn_service("ha-write-socket-service", async move {
            write_service.run(write_shutdown_rx).await;
        }) {
            if let Some(tx) = self.shutdown_tx.lock().await.take() {
                let _ = tx.send(());
            }
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            if let Err(shutdown_error) =
                crate::runtime::shutdown_report_result("DefaultHAConnection partial start", report)
            {
                warn!("DefaultHAConnection partial start cleanup reported an error: {shutdown_error}");
            }
            return Err(HAConnectionError::Service(error.to_string()));
        }

        *self.task_group.lock().await = Some(task_group);

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
        if let Some(task_group) = self.task_group.lock().await.take() {
            let report = task_group.shutdown(Duration::from_secs(3)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("DefaultHAConnection", report) {
                warn!("DefaultHAConnection task shutdown reported an error: {error}");
            }
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
        self.socket_stream
            .as_ref()
            .expect("socket stream should remain available after connection start")
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
pub(in crate::ha) struct OffsetFrame {
    pub offset: i64,
    pub broker_id: Option<i64>,
}

pub(in crate::ha) struct OffsetDecoder {
    frame_size: usize,
}

impl OffsetDecoder {
    pub const fn new(frame_size: usize) -> Self {
        Self { frame_size }
    }
}

impl Decoder for OffsetDecoder {
    type Item = OffsetFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < self.frame_size {
            return Ok(None); // not enough data yet
        }

        // ensure we work on 8-byte alignment
        let aligned_size = src.len() - (src.len() % self.frame_size);
        if aligned_size < self.frame_size {
            return Ok(None);
        }

        let offset_bytes: [u8; 8] = src[..REPORT_HEADER_SIZE]
            .try_into()
            .expect("Slice with incorrect length");
        let offset = i64::from_be_bytes(offset_bytes);
        let broker_id = if self.frame_size >= CONTROLLER_REPORT_HEADER_SIZE {
            Some(i64::from_be_bytes(
                src[REPORT_HEADER_SIZE..CONTROLLER_REPORT_HEADER_SIZE]
                    .try_into()
                    .expect("Slice with incorrect length"),
            ))
        } else {
            None
        };

        src.advance(self.frame_size);

        Ok(Some(OffsetFrame { offset, broker_id }))
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
            last_read_timestamp: AtomicU64::new(current_millis()),
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
                Some(Ok(OffsetFrame { offset, broker_id })) => {
                    self.last_read_timestamp.store(current_millis(), Ordering::Relaxed);
                    self.slave_ack_offset.store(offset, Ordering::Relaxed);

                    if self.slave_request_offset.load(Ordering::Acquire) < 0 {
                        self.slave_request_offset.store(offset, Ordering::Release);
                        info!("slave[{}] request offset {}", self.client_address, offset);
                    }
                    if let Some(connection) = self.connection.upgrade() {
                        if let Some(broker_id) = broker_id.filter(|broker_id| *broker_id >= 0) {
                            connection.set_slave_broker_id(Some(broker_id));
                        }
                        self.ha_service.handle_connection_ack(connection.as_ref(), offset);
                    }
                    self.ha_service.notify_transfer_some(offset).await;
                }
                Some(Err(e)) => {
                    error!("Stream error: {}", e);
                    break;
                }
            }

            let current_time = current_millis();
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
    writer: HaTransferEngine<OwnedWriteHalf>,
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
        writer: OwnedWriteHalf,
        client_address: String,
        ha_service: ArcMut<DefaultHAService>,
        current_state: Arc<RwLock<HAConnectionState>>,
        slave_request_offset: Arc<AtomicI64>,
        flow_monitor: Arc<FlowMonitor>,
        message_store_config: Arc<MessageStoreConfig>,
        connection: WeakArcMut<GeneralHAConnection>,
        next_transfer_from_where: Arc<AtomicI64>,
    ) -> Result<Self, HAConnectionError> {
        let enable_controller_mode = message_store_config.enable_controller_mode;
        let preference = transfer_engine_preference(&message_store_config);
        let selection = select_transfer_engine_with_availability(
            preference,
            TransferEngineAvailability {
                vectored_write_available: writer.is_write_vectored(),
                sendfile_available: message_store_config.effective_linux_ha_sendfile_enable()
                    && cfg!(target_os = "linux"),
                io_uring_available: message_store_config.linux_io_uring_enable
                    && crate::log_file::mapped_file::io_uring_impl::probe_io_uring_runtime_capability()
                        .basic_path_available(),
            },
        );
        if let Some(reason) = selection.fallback_reason {
            info!(
                "HA transfer engine fallback to {:?} for slave[{}]: {}",
                selection.engine, client_address, reason
            );
            ha_service.ha_transfer_metrics().record_fallback(
                transfer_engine_kind(preference),
                selection.engine,
                reason,
            );
        } else {
            info!(
                "HA transfer engine selected {:?} for slave[{}]",
                selection.engine, client_address
            );
        }
        let writer = HaTransferEngine::from_selection(writer, selection.engine);
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
            last_write_timestamp: AtomicU64::new(current_millis()),
            last_print_timestamp: AtomicU64::new(current_millis()),
            byte_buffer_header: BytesMut::with_capacity(transfer_header_size(enable_controller_mode)),
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
            let current_time = current_millis();
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
        let max_commit_log_offset = self
            .ha_service
            .get_default_message_store()
            .get_commit_log()
            .get_max_offset();
        if next_offset >= max_commit_log_offset {
            if let Some(connection) = self.connection.upgrade() {
                self.ha_service.handle_connection_caught_up(connection.as_ref());
            }
            return Ok(());
        }

        let configured_max_batch_size =
            effective_ha_transfer_batch_size(self.message_store_config.ha_transfer_batch_size);
        let can_transfer_max_bytes = self.flow_monitor.can_transfer_max_byte_num() as usize;
        self.maybe_log_flow_control(
            next_offset,
            max_commit_log_offset,
            configured_max_batch_size,
            can_transfer_max_bytes,
        );

        let plan = TransferPlanner::plan(
            TransferPlanInput {
                requested_offset: slave_request_offset,
                next_transfer_offset: next_offset,
                max_commit_log_offset,
                configured_max_batch_bytes: self.message_store_config.ha_transfer_batch_size,
                flow_control_available_bytes: can_transfer_max_bytes,
                mapped_file_size: self.message_store_config.mapped_file_size_commit_log,
                allow_cross_file_batch: false,
                heartbeat_due: false,
            },
            |offset, max_bytes, allow_cross_file| {
                self.ha_service
                    .get_default_message_store()
                    .get_commit_log()
                    .select_segments(offset, max_bytes, allow_cross_file)
            },
        )?;

        if let TransferPlan::Data(batch) = plan {
            self.next_transfer_from_where
                .store(batch.next_offset, Ordering::Relaxed);
            self.send_data(batch).await?;
        }

        Ok(())
    }

    fn maybe_log_flow_control(
        &self,
        next_offset: i64,
        max_commit_log_offset: i64,
        configured_max_batch_size: usize,
        can_transfer_max_bytes: usize,
    ) {
        let available = (max_commit_log_offset - next_offset).max(0) as usize;
        let planned_without_flow = configured_max_batch_size.min(available);
        if planned_without_flow <= can_transfer_max_bytes {
            return;
        }

        let current_time = current_millis();
        let last_print = self.last_print_timestamp.load(Ordering::Relaxed);
        if current_time - last_print > 1000 {
            warn!(
                "Trigger HA flow control, max transfer speed {:.2}KB/s, current speed: {:.2}KB/s",
                self.flow_monitor.max_transfer_byte_in_second() as f64 / 1024.0,
                self.flow_monitor.get_transferred_byte_in_second() as f64 / 1024.0
            );
            self.last_print_timestamp.store(current_time, Ordering::Relaxed);
        }
    }

    async fn send_heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let next_offset = self.next_transfer_from_where.load(Ordering::Relaxed);
        let confirm_offset = self.ha_service.get_default_message_store().get_confirm_offset();
        let frame_header = encode_transfer_header(
            &mut self.byte_buffer_header,
            next_offset,
            0,
            self.message_store_config.enable_controller_mode,
            confirm_offset,
        );
        let batch = TransferBatch {
            frame_header,
            segments: Vec::new(),
            total_body_len: 0,
            start_offset: next_offset,
            next_offset,
            kind: TransferKind::Heartbeat,
        };
        let stats = self.writer.send_batch(&batch).await?;

        self.last_write_timestamp.store(current_millis(), Ordering::Relaxed);
        self.ha_service.ha_transfer_metrics().record_transfer(&stats);
        self.flow_monitor.add_byte_count_transferred(stats.bytes_written as i64);
        self.last_write_over.store(true, Ordering::Relaxed);

        Ok(())
    }

    async fn send_data(&mut self, mut batch: TransferBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let confirm_offset = self.ha_service.get_default_message_store().get_confirm_offset();
        let header_bytes = encode_transfer_header(
            &mut self.byte_buffer_header,
            batch.start_offset,
            batch.total_body_len,
            self.message_store_config.enable_controller_mode,
            confirm_offset,
        );
        batch.frame_header = header_bytes;

        let stats = self.writer.send_batch(&batch).await?;

        self.last_write_timestamp.store(current_millis(), Ordering::Relaxed);
        self.ha_service.ha_transfer_metrics().record_transfer(&stats);
        self.flow_monitor.add_byte_count_transferred(stats.bytes_written as i64);
        self.last_write_over.store(true, Ordering::Relaxed);

        Ok(())
    }

    async fn cleanup(&mut self) {
        *self.current_state.write().await = HAConnectionState::Shutdown;

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

fn transfer_engine_preference(message_store_config: &MessageStoreConfig) -> TransferEnginePreference {
    match message_store_config.effective_linux_transfer_engine() {
        LinuxTransferEngine::Auto => TransferEnginePreference::Auto,
        LinuxTransferEngine::Bytes => TransferEnginePreference::Bytes,
        LinuxTransferEngine::Vectored => TransferEnginePreference::Vectored,
        LinuxTransferEngine::Sendfile => TransferEnginePreference::Sendfile,
        LinuxTransferEngine::IoUring => TransferEnginePreference::IoUring,
    }
}

fn transfer_engine_kind(preference: TransferEnginePreference) -> TransferEngineKind {
    match preference {
        TransferEnginePreference::Auto => TransferEngineKind::Vectored,
        TransferEnginePreference::Bytes => TransferEngineKind::Bytes,
        TransferEnginePreference::Vectored => TransferEngineKind::Vectored,
        TransferEnginePreference::Sendfile => TransferEngineKind::Sendfile,
        TransferEnginePreference::IoUring => TransferEngineKind::IoUring,
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    #[test]
    fn offset_decoder_reads_default_offset_frame() {
        let mut src = BytesMut::with_capacity(REPORT_HEADER_SIZE);
        src.put_i64(128);
        let mut decoder = OffsetDecoder::new(REPORT_HEADER_SIZE);

        let frame = decoder.decode(&mut src).expect("decode offset frame").expect("frame");

        assert_eq!(frame.offset, 128);
        assert_eq!(frame.broker_id, None);
        assert!(src.is_empty());
    }

    #[test]
    fn offset_decoder_reads_controller_offset_frame_with_broker_id() {
        let mut src = BytesMut::with_capacity(CONTROLLER_REPORT_HEADER_SIZE);
        src.put_i64(256);
        src.put_i64(9);
        let mut decoder = OffsetDecoder::new(CONTROLLER_REPORT_HEADER_SIZE);

        let frame = decoder
            .decode(&mut src)
            .expect("decode controller offset frame")
            .expect("frame");

        assert_eq!(frame.offset, 256);
        assert_eq!(frame.broker_id, Some(9));
        assert!(src.is_empty());
    }

    #[test]
    fn controller_transfer_header_round_trips_confirm_offset() {
        let encoded = encode_transfer_header(
            &mut BytesMut::with_capacity(CONTROLLER_TRANSFER_HEADER_SIZE),
            128,
            64,
            true,
            96,
        );

        assert_eq!(encoded.len(), CONTROLLER_TRANSFER_HEADER_SIZE);

        let header = decode_transfer_header(&encoded, true).expect("decode controller transfer header");

        assert_eq!(header.master_phy_offset, 128);
        assert_eq!(header.body_size, 64);
        assert_eq!(header.confirm_offset, Some(96));
    }

    #[test]
    fn zero_configured_ha_transfer_batch_size_uses_safe_default() {
        let effective = effective_ha_transfer_batch_size(0);

        assert_eq!(effective, DEFAULT_HA_TRANSFER_BATCH_SIZE);
        assert!(effective > 0);
    }

    #[test]
    fn non_zero_configured_ha_transfer_batch_size_is_preserved() {
        assert_eq!(effective_ha_transfer_batch_size(64 * 1024), 64 * 1024);
    }
}
