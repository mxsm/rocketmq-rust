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

use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use bytes::BufMut;
use bytes::BytesMut;
use futures_util::SinkExt;
use futures_util::StreamExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio::time::sleep;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::message_store::MessageStore;
use crate::ha::default_ha_connection::decode_transfer_header;
use crate::ha::default_ha_connection::transfer_header_size;
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
pub const CONTROLLER_REPORT_HEADER_SIZE: usize = 16;

/// Maximum read buffer size (4MB)
const READ_MAX_BUFFER_SIZE: usize = 1024 * 1024 * 4;

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
    last_read_timestamp: Arc<AtomicU64>,

    /// Last time slave reported offset to master
    last_write_timestamp: Arc<AtomicU64>,

    /// Current reported offset
    current_reported_offset: Arc<AtomicI64>,
    reported_broker_id: Arc<AtomicI64>,

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
    async fn close_master_and_wait(&mut self) {
        self.close_master().await;
        sleep(Duration::from_secs(5)).await; // Wait for 5 seconds before retrying
    }

    async fn connect_master(&mut self) -> Result<Option<TcpStream>, HAClientError> {
        let ha_address_guard = self.master_ha_address.lock().await;
        let addr = ha_address_guard.as_ref();
        if let Some(addr_str) = addr {
            match TcpStream::connect(addr_str).await {
                Ok(stream) => {
                    // Configure socket
                    if let Err(e) = stream.set_nodelay(true) {
                        error!("Failed to set TCP_NODELAY: {}", e);
                        return Err(HAClientError::Io(e));
                    }
                    info!("HAClient connect to master {}", addr_str);
                    self.change_current_state(HAConnectionState::Transfer).await;
                    // Initialize current reported offset
                    let max_offset = self.default_message_store.get_max_phy_offset();
                    self.current_reported_offset.store(max_offset, Ordering::Release);
                    let now = current_millis();
                    self.last_read_timestamp.store(now, Ordering::SeqCst);
                    Ok(Some(stream))
                }
                Err(e) => {
                    error!("Failed to connect to master {}: {}", addr_str, e);
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn notify_shutdown(&mut self) {
        self.shutdown_notify.notify_waiters();
    }

    /// Check if it's time to report offset
    fn is_time_to_report_offset(&self) -> bool {
        let now = self.default_message_store.now();
        let last_write = self.last_write_timestamp.load(Ordering::SeqCst);
        let interval = now.saturating_sub(last_write);
        let heartbeat_interval = self
            .default_message_store
            .get_message_store_config()
            .ha_send_heartbeat_interval;

        interval > heartbeat_interval
    }
    /// Change current connection state
    pub async fn change_current_state(&self, new_state: HAConnectionState) {
        info!("change state to {:?}", new_state);
        let mut state = self.current_state.write().await;
        *state = new_state;
    }

    pub async fn ha_master_address(&self) -> Option<String> {
        self.master_ha_address.lock().await.clone()
    }
}

impl DefaultHAClient {
    /// Create a new DefaultHAClient
    pub fn new(default_message_store: ArcMut<LocalFileMessageStore>) -> Result<Self, HAClientError> {
        let flow_monitor = Arc::new(FlowMonitor::new(default_message_store.message_store_config()));

        let now = current_millis();

        Ok(Self {
            inner: ArcMut::new(Inner {
                master_ha_address: Arc::new(tokio::sync::Mutex::new(None)),
                master_address: Arc::new(tokio::sync::Mutex::new(None)),
                //socket_stream: Arc::new(RwLock::new(None)),
                write_stream: None,
                read_stream: None,
                last_read_timestamp: Arc::new(AtomicU64::new(now)),
                last_write_timestamp: Arc::new(AtomicU64::new(now)),
                current_reported_offset: Arc::new(AtomicI64::new(0)),
                reported_broker_id: Arc::new(AtomicI64::new(-1)),
                dispatch_position: AtomicUsize::new(0),
                byte_buffer_read: BytesMut::with_capacity(READ_MAX_BUFFER_SIZE),
                byte_buffer_backup: Arc::new(RwLock::new(BytesMut::with_capacity(READ_MAX_BUFFER_SIZE))),
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

    /// Close master and wait
    pub async fn close_master_and_wait(&self) {
        self.close_master().await;
        sleep(Duration::from_secs(5)).await;
    }

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
    pub fn get_last_write_timestamp(&self) -> u64 {
        self.inner.last_write_timestamp.load(Ordering::SeqCst)
    }

    /// Get last read timestamp
    pub fn get_last_read_timestamp(&self) -> u64 {
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

    pub fn set_reported_broker_id(&self, broker_id: Option<i64>) {
        self.inner
            .reported_broker_id
            .store(broker_id.unwrap_or(-1), Ordering::SeqCst);
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
            // main loop: connect -> start read/write tasks -> supervise/reconnect
            loop {
                let read_guard = client.current_state.read().await;
                if *read_guard == HAConnectionState::Shutdown {
                    break;
                }
                // READY: try to connect to master
                if *read_guard == HAConnectionState::Ready {
                    drop(read_guard);
                    match client.connect_master().await {
                        Ok(Some(stream)) => {
                            client.change_current_state(HAConnectionState::Transfer).await;

                            //split stream into read/write halves
                            let (reader, writer) = stream.into_split();
                            let framed_rd = FramedRead::new(reader, BytesCodec::new());
                            let framed_wr = FramedWrite::new(writer, BytesCodec::new());

                            // channel: reader -> writer report offset; main loop -> writer
                            // heartbeat
                            let (offset_tx, offset_rx) = tokio::sync::mpsc::unbounded_channel::<i64>();
                            let (kick_tx, kick_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

                            // use reader/writer to send errors back to main loop
                            let (err_tx, mut err_rx) = tokio::sync::mpsc::unbounded_channel::<anyhow::Error>();

                            // reader task: read data from master and dispatch to message store
                            let reader_shutdown = client.shutdown_notify.clone();
                            let store = client.default_message_store.clone();
                            let flow = client.flow_monitor.clone();
                            let mut reader_client = ReaderTask {
                                reader: framed_rd,
                                buf: BytesMut::with_capacity(READ_MAX_BUFFER_SIZE),
                                dispatch_pos: 0,
                                offset_tx,
                                err_tx,
                                store,
                                flow_monitor: flow,
                                last_read_timestamp: client.last_read_timestamp.clone(),
                                enable_controller_mode: client
                                    .default_message_store
                                    .message_store_config_ref()
                                    .enable_controller_mode,
                            };
                            let reader_handle = tokio::spawn(async move {
                                tokio::select! {
                                    res = reader_client.run() => res,
                                    _ = reader_shutdown.notified() => Ok(()),
                                }
                            });

                            // writer task: write data to master and report offsets
                            let writer_shutdown = client.shutdown_notify.clone();
                            let cfg = WriterCfg {
                                heartbeat_interval_ms: client
                                    .default_message_store
                                    .message_store_config_ref()
                                    .ha_send_heartbeat_interval,
                                enable_controller_mode: client
                                    .default_message_store
                                    .message_store_config_ref()
                                    .enable_controller_mode,
                            };

                            let mut writer_client = WriterTask {
                                wr: framed_wr,
                                last_write_timestamp: client.last_write_timestamp.clone(),
                                current_reported_offset_ref: client.current_reported_offset.clone(),
                                reported_broker_id_ref: client.reported_broker_id.clone(),
                                cfg,
                                offset_rx,
                                kick_rx,
                                report_offset: BytesMut::with_capacity(CONTROLLER_REPORT_HEADER_SIZE),
                            };
                            let writer_handle = tokio::spawn(async move {
                                tokio::select! {
                                    res = writer_client.run() => res,
                                    _ = writer_shutdown.notified() => Ok(()),
                                }
                            });
                            // main loop for housekeeping and monitoring
                            let mut house = interval(Duration::from_millis(
                                client
                                    .default_message_store
                                    .message_store_config_ref()
                                    .ha_housekeeping_interval,
                            ));

                            let exit = loop {
                                tokio::select! {
                                    // subtask error
                                    Some(e) = err_rx.recv() => {
                                        warn!("HAClient subtask error: {e:#}");
                                        break false;
                                    }
                                    // housekeeping
                                    _ = house.tick() => {
                                        let interval = current_millis().saturating_sub(client.last_read_timestamp.load(Ordering::SeqCst));
                                        // If the interval exceeds the configured value, it indicates that the connection may have been disconnected.
                                        if interval > client.default_message_store.message_store_config_ref().ha_housekeeping_interval {
                                            warn!(
                                                "AutoRecoverHAClient, housekeeping, connection [{:?}] expired, {}",
                                                client.ha_master_address().await, interval
                                            );
                                            break false;
                                        }
                                        // Is it time for the heartbeat? (Even if the offset remains unchanged)
                                        if client.is_time_to_report_offset() {
                                            let _ = kick_tx.send(());
                                        }
                                    }
                                    // outer shutdown
                                    _ = client.shutdown_notify.notified() => {
                                        break true;
                                    }
                                }
                            };

                            // stop reader/writer
                            client.shutdown_notify.notify_waiters();
                            let _ = reader_handle.await;
                            let _ = writer_handle.await;

                            if !exit {
                                // need to reconnect
                                client.change_current_state(HAConnectionState::Ready).await;
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            } else {
                                // normal shutdown
                                break;
                            }
                        }
                        Ok(None) => {
                            warn!(
                                "HAClient connect to master {:?} failed",
                                client.ha_master_address().await
                            );
                            sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                        Err(e) => {
                            warn!("connect_master error: {e:#}");
                            sleep(Duration::from_secs(5)).await;
                            continue;
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
        self.inner.change_current_state(HAConnectionState::Shutdown).await;
        self.inner.shutdown_notify.notify_waiters();

        // Wait for service to stop
        let mut service_handle = self.service_handle.write().await;
        if let Some(handle) = service_handle.take() {
            let _ = handle.await;
        }
        self.close_master().await;
    }

    async fn wakeup(&self) {
        self.inner.change_current_state(HAConnectionState::Ready).await;
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
        self.inner
            .master_address
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn get_ha_master_address(&self) -> String {
        self.inner
            .master_ha_address
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn get_last_read_timestamp(&self) -> i64 {
        self.inner.last_read_timestamp.load(Ordering::SeqCst) as i64
    }

    fn get_last_write_timestamp(&self) -> i64 {
        self.inner.last_write_timestamp.load(Ordering::SeqCst) as i64
    }

    fn get_current_state(&self) -> HAConnectionState {
        self.inner
            .current_state
            .try_read()
            .map(|state| *state)
            .unwrap_or(HAConnectionState::Ready)
    }

    fn change_current_state(&self, ha_connection_state: HAConnectionState) {
        if let Ok(mut state) = self.inner.current_state.try_write() {
            *state = ha_connection_state;
        }
    }

    async fn close_master(&self) {
        let mut inner = self.inner.clone();
        inner.close_master().await;
    }

    fn get_transferred_byte_in_second(&self) -> i64 {
        self.inner.flow_monitor.get_transferred_byte_in_second()
    }
}

// ====== Reader（read task）======

struct ReaderTask {
    reader: FramedRead<OwnedReadHalf, BytesCodec>,
    buf: BytesMut,
    dispatch_pos: usize,
    offset_tx: tokio::sync::mpsc::UnboundedSender<i64>,
    err_tx: tokio::sync::mpsc::UnboundedSender<anyhow::Error>,
    store: ArcMut<LocalFileMessageStore>,
    flow_monitor: Arc<FlowMonitor>,
    /// Last time slave read data from master
    last_read_timestamp: Arc<AtomicU64>,
    enable_controller_mode: bool,
}

impl ReaderTask {
    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            match self.reader.next().await {
                Some(Ok(bytes)) => {
                    // framed - once for one piece of data; we are still doing custom protocol
                    // unpacking in the local buffe
                    self.flow_monitor.add_byte_count_transferred(bytes.len() as i64);
                    self.buf.extend_from_slice(&bytes);

                    if !self.dispatch_read().await? {
                        bail!("dispatchReadRequest error");
                    }
                    self.last_read_timestamp.store(current_millis(), Ordering::SeqCst);
                }
                Some(Err(e)) => {
                    bail!(e);
                }
                None => {
                    bail!("read EOF");
                }
            }
        }
    }

    async fn dispatch_read(&mut self) -> anyhow::Result<bool> {
        loop {
            let header_size = transfer_header_size(self.enable_controller_mode);
            let diff = self.buf.len().saturating_sub(self.dispatch_pos);
            if diff < header_size {
                self.compact();
                return Ok(true);
            }

            let header = decode_transfer_header(
                &self.buf[self.dispatch_pos..self.dispatch_pos + header_size],
                self.enable_controller_mode,
            )?;
            let master_phy_offset = header.master_phy_offset;
            let body_size = header.body_size;

            let slave_phy_offset = self.store.get_max_phy_offset();
            if slave_phy_offset != 0 && slave_phy_offset != master_phy_offset {
                bail!(
                    "master pushed offset != slave max, slave: {}, master: {}",
                    slave_phy_offset,
                    master_phy_offset
                );
            }

            if diff < header_size + body_size {
                self.compact();
                return Ok(true);
            }

            let data_start = self.dispatch_pos + header_size;
            let data_end = data_start + body_size;
            let body = &self.buf[data_start..data_end];

            if body_size > 0 {
                self.store
                    .append_to_commit_log(master_phy_offset, body, 0, body_size as i32)
                    .await?;
            }

            Self::apply_master_confirm_offset(&self.store, header.confirm_offset);

            self.dispatch_pos = data_end;

            if body_size > 0 {
                let cur = self.store.get_max_phy_offset();
                let _ = self.offset_tx.send(cur);
            }
        }
    }

    fn apply_master_confirm_offset(store: &ArcMut<LocalFileMessageStore>, confirm_offset: Option<i64>) {
        let Some(confirm_offset) = confirm_offset else {
            return;
        };

        let min_phy_offset = store.get_min_phy_offset();
        let max_phy_offset = store.get_max_phy_offset().max(min_phy_offset);
        store
            .clone()
            .mut_from_ref()
            .set_confirm_offset(confirm_offset.clamp(min_phy_offset, max_phy_offset));
    }

    // Move the unconsumed data to the start of the buffer to save space.
    fn compact(&mut self) {
        if self.dispatch_pos > 0 {
            let len = self.buf.len();
            self.buf.copy_within(self.dispatch_pos..len, 0);
            self.buf.truncate(len - self.dispatch_pos); // drop [0..dispatch_pos]
            self.dispatch_pos = 0;
        }
        // Limit the maximum capacity (to prevent explosion)
        if self.buf.capacity() > READ_MAX_BUFFER_SIZE * 2 {
            self.buf
                .reserve(READ_MAX_BUFFER_SIZE.saturating_sub(self.buf.capacity()));
        }
    }
}

// ====== Writer（write task）======

#[derive(Clone, Copy)]
struct WriterCfg {
    heartbeat_interval_ms: u64,
    enable_controller_mode: bool,
}

struct WriterTask {
    wr: FramedWrite<OwnedWriteHalf, BytesCodec>,
    last_write_timestamp: Arc<AtomicU64>,
    current_reported_offset_ref: Arc<AtomicI64>,
    reported_broker_id_ref: Arc<AtomicI64>,
    cfg: WriterCfg,
    offset_rx: tokio::sync::mpsc::UnboundedReceiver<i64>,
    kick_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    report_offset: BytesMut,
}

impl WriterTask {
    async fn run(&mut self) -> anyhow::Result<()> {
        let mut ticker = interval(Duration::from_millis(self.cfg.heartbeat_interval_ms.max(1000)));

        loop {
            tokio::select! {
                Some(off) = self.offset_rx.recv() => {
                    if off > self.current_reported_offset_ref.load(Ordering::Relaxed) {
                        self.current_reported_offset_ref.store(off, Ordering::Relaxed);
                        self.send_offset(off).await?;
                    }
                }
                Some(_) = self.kick_rx.recv() => {
                    let off = self.current_reported_offset_ref.load(Ordering::Relaxed);
                    self.send_offset(off).await?;
                }
                _ = ticker.tick() => {
                    let off = self.current_reported_offset_ref.load(Ordering::Relaxed);
                    self.send_offset(off).await?;
                }
            }
        }
    }

    async fn send_offset(&mut self, max_off: i64) -> anyhow::Result<()> {
        let broker_id = self.reported_broker_id_ref.load(Ordering::Relaxed);
        let bytes = Self::encode_offset_report(
            &mut self.report_offset,
            max_off,
            self.cfg.enable_controller_mode,
            broker_id,
        );
        self.wr.send(bytes).await?;
        self.last_write_timestamp.store(current_millis(), Ordering::Release);
        Ok(())
    }

    fn encode_offset_report(
        report_offset: &mut BytesMut,
        max_off: i64,
        enable_controller_mode: bool,
        reported_broker_id: i64,
    ) -> bytes::Bytes {
        report_offset.clear();
        report_offset.put_i64(max_off);
        if enable_controller_mode {
            report_offset.put_i64(reported_broker_id);
        }
        report_offset.split().freeze()
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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use tempfile::tempdir;

    use super::*;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::ha::default_ha_connection::decode_transfer_header;
    use crate::ha::default_ha_connection::encode_transfer_header;
    use crate::ha::default_ha_connection::CONTROLLER_TRANSFER_HEADER_SIZE;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;

    fn new_test_message_store(root: &Path) -> ArcMut<LocalFileMessageStore> {
        std::fs::create_dir_all(root).expect("create temp root dir");

        let broker_config = BrokerConfig {
            enable_controller_mode: true,
            ..BrokerConfig::default()
        };

        let message_store_config = MessageStoreConfig {
            enable_controller_mode: true,
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

    #[test]
    fn writer_task_encodes_controller_report_with_broker_id() {
        let encoded = WriterTask::encode_offset_report(
            &mut BytesMut::with_capacity(CONTROLLER_REPORT_HEADER_SIZE),
            128,
            true,
            9,
        );

        assert_eq!(encoded.len(), CONTROLLER_REPORT_HEADER_SIZE);
        assert_eq!(i64::from_be_bytes(encoded[0..8].try_into().expect("offset bytes")), 128);
        assert_eq!(
            i64::from_be_bytes(encoded[8..16].try_into().expect("broker id bytes")),
            9
        );
    }

    #[tokio::test]
    async fn apply_master_confirm_offset_clamps_to_local_max_phy_offset() {
        let temp_dir = tempdir().expect("temp dir");
        let mut store = new_test_message_store(temp_dir.path());
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let encoded = encode_transfer_header(
            &mut BytesMut::with_capacity(CONTROLLER_TRANSFER_HEADER_SIZE),
            4,
            0,
            true,
            128,
        );
        let header = decode_transfer_header(&encoded, true).expect("decode transfer header");

        ReaderTask::apply_master_confirm_offset(&store, header.confirm_offset);

        assert_eq!(store.get_confirm_offset(), 4);
    }
}
