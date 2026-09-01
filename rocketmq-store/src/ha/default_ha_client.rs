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

use std::future::Future;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::SinkExt;
use futures_util::StreamExt;
use parking_lot::Mutex as ParkingMutex;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::TaskKind;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio::time::sleep;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::FramedRead;
use tokio_util::codec::FramedWrite;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use crate::ha::flow_monitor::FlowMonitor;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::message_store::local_file_message_store::HAReplicaStoreHandle;
use crate::store_error::StoreError;

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
const HA_ERROR_CHANNEL_CAPACITY: usize = 4;
const DEFAULT_HA_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

type HAClientTaskResult<T> = Result<T, HAClientError>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MasterEndpointSnapshot {
    address: Option<String>,
    generation: u64,
}

struct ConnectedMaster {
    stream: TcpStream,
    endpoint: MasterEndpointSnapshot,
    endpoint_updates: watch::Receiver<MasterEndpointSnapshot>,
}

enum MasterConnectOutcome {
    Connected(ConnectedMaster),
    Retry { observed_generation: u64 },
    EndpointChanged,
    Shutdown,
}

/// Default HA Client implementation using bytes crate
pub struct DefaultHAClient {
    inner: Arc<Inner>,
    runtime_scope: crate::runtime::StoreRuntimeScope,
    /// Service task group
    service_group: Arc<RwLock<Option<rocketmq_runtime::TaskGroup>>>,
}

struct Inner {
    /// Latest immutable master endpoint and its monotonic generation.
    master_endpoint: watch::Sender<MasterEndpointSnapshot>,

    /// Master address (atomic reference)
    master_address: Arc<tokio::sync::Mutex<Option<String>>>,

    /// Last time slave read data from master
    last_read_timestamp: Arc<AtomicU64>,

    /// Last time slave reported offset to master
    last_write_timestamp: Arc<AtomicU64>,

    /// Current reported offset
    current_reported_offset: Arc<AtomicI64>,
    reported_broker_id: Arc<AtomicI64>,

    /// Narrow replica-store capability
    replica_store: HAReplicaStoreHandle,

    /// Current connection state
    current_state: Arc<RwLock<HAConnectionState>>,

    /// Flow monitor
    flow_monitor: Arc<FlowMonitor>,

    /// Shutdown notification
    shutdown_notify: Arc<Notify>,

    /// Persistent owner cancellation for connect and reconnect waits.
    owner_cancel: CancellationToken,

    /// Serializes endpoint replacement with generation-scoped state publication.
    endpoint_publication_gate: ParkingMutex<()>,

    /// Maximum duration of one outbound connection attempt.
    connect_timeout: Duration,
}

impl Inner {
    async fn close_master(&self) {
        let endpoint = self.master_endpoint_snapshot();
        info!(
            "HAClient close connection with master {:?} at generation {}",
            endpoint.address.as_ref(),
            endpoint.generation
        );

        // A reconnect closes the current stream and returns to Ready, while a
        // service shutdown must remain terminal.
        self.mark_ready_unless_shutdown().await;

        // Reset state
        self.last_read_timestamp.store(0, Ordering::SeqCst);
    }

    async fn connect_master(&self) -> Result<MasterConnectOutcome, HAClientError> {
        self.connect_master_with(TcpStream::connect).await
    }

    async fn connect_master_with<C, F>(&self, connector: C) -> Result<MasterConnectOutcome, HAClientError>
    where
        C: FnOnce(String) -> F,
        F: Future<Output = std::io::Result<TcpStream>>,
    {
        let mut endpoint_updates = self.master_endpoint.subscribe();
        let endpoint = endpoint_updates.borrow_and_update().clone();
        let Some(address) = endpoint.address.clone() else {
            return Ok(MasterConnectOutcome::Retry {
                observed_generation: endpoint.generation,
            });
        };
        if self.owner_cancel.is_cancelled() {
            return Ok(MasterConnectOutcome::Shutdown);
        }

        let connect_timeout = self.connect_timeout;
        let timeout = sleep(connect_timeout);
        tokio::pin!(timeout);
        let connect_result = tokio::select! {
            biased;
            _ = self.owner_cancel.cancelled() => return Ok(MasterConnectOutcome::Shutdown),
            changed = endpoint_updates.changed() => {
                return Ok(if changed.is_ok() {
                    MasterConnectOutcome::EndpointChanged
                } else {
                    MasterConnectOutcome::Shutdown
                });
            }
            _ = &mut timeout => {
                warn!(
                    generation = endpoint.generation,
                    timeout_millis = connect_timeout.as_millis() as u64,
                    "HAClient connection attempt timed out"
                );
                return Ok(MasterConnectOutcome::Retry {
                    observed_generation: endpoint.generation,
                });
            }
            result = connector(address.clone()) => result,
        };

        let stream = match connect_result {
            Ok(stream) => stream,
            Err(error) => {
                warn!(
                    generation = endpoint.generation,
                    %error,
                    "HAClient failed to connect to the current master endpoint"
                );
                return Ok(MasterConnectOutcome::Retry {
                    observed_generation: endpoint.generation,
                });
            }
        };
        stream.set_nodelay(true)?;

        if !self.activate_connected_endpoint(&endpoint).await {
            return Ok(if self.owner_cancel.is_cancelled() {
                MasterConnectOutcome::Shutdown
            } else {
                MasterConnectOutcome::EndpointChanged
            });
        }

        info!(
            generation = endpoint.generation,
            "HAClient connected to master endpoint"
        );
        Ok(MasterConnectOutcome::Connected(ConnectedMaster {
            stream,
            endpoint,
            endpoint_updates,
        }))
    }

    async fn activate_connected_endpoint(&self, endpoint: &MasterEndpointSnapshot) -> bool {
        let mut state = self.current_state.write().await;
        if *state == HAConnectionState::Shutdown {
            return false;
        }

        self.commit_if_current(endpoint, || {
            let max_offset = self.replica_store.get_max_phy_offset();
            self.current_reported_offset.store(max_offset, Ordering::Release);
            self.last_read_timestamp.store(current_millis(), Ordering::Release);
            *state = HAConnectionState::Transfer;
        })
        .is_some()
    }

    fn master_endpoint_snapshot(&self) -> MasterEndpointSnapshot {
        self.master_endpoint.borrow().clone()
    }

    async fn update_master_endpoint(&self, new_address: &str) -> bool {
        let address = (!new_address.is_empty()).then(|| new_address.to_string());
        let mut state = self.current_state.write().await;
        let _publication_guard = self.endpoint_publication_gate.lock();
        let previous = self.master_endpoint_snapshot();
        if previous.address == address {
            return false;
        }

        let next = MasterEndpointSnapshot {
            address,
            generation: previous.generation.saturating_add(1),
        };
        self.master_endpoint.send_replace(next.clone());
        if *state != HAConnectionState::Shutdown {
            *state = HAConnectionState::Ready;
        }
        drop(state);
        info!(generation = next.generation, "Updated HA master endpoint");
        true
    }

    fn commit_if_current<R>(&self, endpoint: &MasterEndpointSnapshot, commit: impl FnOnce() -> R) -> Option<R> {
        let _publication_guard = self.endpoint_publication_gate.lock();
        if self.owner_cancel.is_cancelled() || self.master_endpoint_snapshot() != *endpoint {
            return None;
        }
        Some(commit())
    }

    async fn mark_ready_unless_shutdown(&self) {
        let mut state = self.current_state.write().await;
        if !self.owner_cancel.is_cancelled() && *state != HAConnectionState::Shutdown {
            *state = HAConnectionState::Ready;
        }
    }

    pub fn notify_shutdown(&self) {
        self.shutdown_notify.notify_waiters();
    }

    async fn wait_reconnect_delay(&self) -> bool {
        self.wait_reconnect_delay_after(self.master_endpoint_snapshot().generation)
            .await
    }

    async fn wait_reconnect_delay_after(&self, observed_generation: u64) -> bool {
        if self.owner_cancel.is_cancelled() || *self.current_state.read().await == HAConnectionState::Shutdown {
            return true;
        }
        let mut endpoint_updates = self.master_endpoint.subscribe();
        if endpoint_updates.borrow_and_update().generation != observed_generation {
            return false;
        }
        tokio::select! {
            _ = self.owner_cancel.cancelled() => true,
            changed = endpoint_updates.changed() => changed.is_err(),
            _ = sleep(Duration::from_secs(5)) => {
                self.owner_cancel.is_cancelled()
                    || *self.current_state.read().await == HAConnectionState::Shutdown
            },
        }
    }

    /// Check if it's time to report offset
    fn is_time_to_report_offset(&self) -> bool {
        let now = current_millis();
        let last_write = self.last_write_timestamp.load(Ordering::SeqCst);
        let interval = now.saturating_sub(last_write);
        let heartbeat_interval = self.replica_store.message_store_config_ref().ha_send_heartbeat_interval;

        interval > heartbeat_interval
    }
    /// Change current connection state
    pub async fn change_current_state(&self, new_state: HAConnectionState) {
        info!("change state to {:?}", new_state);
        let mut state = self.current_state.write().await;
        *state = new_state;
    }

    pub async fn ha_master_address(&self) -> Option<String> {
        self.master_endpoint_snapshot().address
    }
}

impl DefaultHAClient {
    /// Create a new DefaultHAClient
    pub(crate) fn new(
        replica_store: HAReplicaStoreHandle,
        runtime_scope: crate::runtime::StoreRuntimeScope,
    ) -> Result<Self, HAClientError> {
        Self::new_with_connect_timeout(replica_store, runtime_scope, DEFAULT_HA_CONNECT_TIMEOUT)
    }

    fn new_with_connect_timeout(
        replica_store: HAReplicaStoreHandle,
        runtime_scope: crate::runtime::StoreRuntimeScope,
        connect_timeout: Duration,
    ) -> Result<Self, HAClientError> {
        let flow_monitor = Arc::new(FlowMonitor::new(
            replica_store.message_store_config(),
            runtime_scope.task_group("ha-client-flow-monitor"),
        ));

        let now = current_millis();
        let (master_endpoint, _) = watch::channel(MasterEndpointSnapshot::default());
        let owner_cancel = runtime_scope.child_cancellation_token();

        Ok(Self {
            inner: Arc::new(Inner {
                master_endpoint,
                master_address: Arc::new(tokio::sync::Mutex::new(None)),
                last_read_timestamp: Arc::new(AtomicU64::new(now)),
                last_write_timestamp: Arc::new(AtomicU64::new(now)),
                current_reported_offset: Arc::new(AtomicI64::new(0)),
                reported_broker_id: Arc::new(AtomicI64::new(-1)),
                replica_store,
                current_state: Arc::new(RwLock::new(HAConnectionState::Ready)),
                flow_monitor,
                shutdown_notify: Arc::new(Notify::new()),
                owner_cancel,
                endpoint_publication_gate: ParkingMutex::new(()),
                connect_timeout: connect_timeout.max(Duration::from_millis(1)),
            }),
            runtime_scope,
            service_group: Arc::new(RwLock::new(None)),
        })
    }

    /// Get HA master address
    pub async fn get_ha_master_address(&self) -> Option<String> {
        self.inner.master_endpoint_snapshot().address
    }

    /// Get master address
    pub async fn get_master_address(&self) -> Option<String> {
        self.inner.master_address.lock().await.clone()
    }

    /// Close master and wait
    pub async fn close_master_and_wait(&self) {
        self.close_master().await;
        let _ = self.inner.wait_reconnect_delay().await;
    }

    /// Shutdown the HA client
    pub async fn shutdown(self: Arc<Self>) {
        self.inner.change_current_state(HAConnectionState::Shutdown).await;
        self.inner.owner_cancel.cancel();
        self.inner.shutdown_notify.notify_waiters();
        self.inner.flow_monitor.shutdown().await;

        // Wait for service to stop
        let service_group = self.service_group.write().await.take();
        if let Some(service_group) = service_group {
            let report = service_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("DefaultHAClient", report) {
                warn!("DefaultHAClient task shutdown reported an error: {error}");
            }
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
    async fn start(&self) {
        // Idempotent start: if a service group already exists, do nothing
        if self.service_group.read().await.is_some() {
            warn!("HAClient service is already running");
            return;
        }

        if let Err(error) = self.inner.flow_monitor.start().await {
            warn!("HAClient flow monitor not started: {error}");
            return;
        }
        let client = Arc::clone(&self.inner);
        let service_group = crate::runtime::task_group(&self.runtime_scope, "rocketmq-store.ha.client");
        let service_loop_group = service_group.clone();
        if let Err(error) = service_group.spawn_service("ha-client-service", async move {
            // main loop: connect -> start read/write tasks -> supervise/reconnect
            loop {
                if client.owner_cancel.is_cancelled() {
                    break;
                }
                let read_guard = client.current_state.read().await;
                if *read_guard == HAConnectionState::Shutdown {
                    break;
                }
                // READY: try to connect to master
                if *read_guard == HAConnectionState::Ready {
                    drop(read_guard);
                    match client.connect_master().await {
                        Ok(MasterConnectOutcome::Connected(connection)) => {
                            let ConnectedMaster {
                                stream,
                                endpoint,
                                mut endpoint_updates,
                            } = connection;
                            //split stream into read/write halves
                            let (reader, writer) = stream.into_split();
                            let framed_rd = FramedRead::new(reader, BytesCodec::new());
                            let framed_wr = FramedWrite::new(writer, BytesCodec::new());

                            // channel: reader -> writer report offset; main loop -> writer
                            // heartbeat
                            let initial_offset = client.current_reported_offset.load(Ordering::Acquire);
                            let (offset_tx, offset_rx) = watch::channel(initial_offset);
                            let kick = Arc::new(Notify::new());

                            // use reader/writer to send errors back to main loop
                            let (err_tx, mut err_rx) = mpsc::channel::<HAClientError>(HA_ERROR_CHANNEL_CAPACITY);
                            let connection_operation = OperationContext::without_deadline(TaskKind::Worker);

                            // reader task: read data from master and dispatch to message store
                            let reader_shutdown = client.shutdown_notify.clone();
                            let replica_store = client.replica_store.clone();
                            let flow = client.flow_monitor.clone();
                            let mut reader_client = ReaderTask {
                                reader: framed_rd,
                                buf: BytesMut::with_capacity(READ_MAX_BUFFER_SIZE),
                                dispatch_pos: 0,
                                offset_tx,
                                err_tx: err_tx.clone(),
                                replica_store,
                                flow_monitor: flow,
                                last_read_timestamp: client.last_read_timestamp.clone(),
                                endpoint: endpoint.clone(),
                                endpoint_updates: endpoint_updates.clone(),
                                endpoint_owner: Arc::clone(&client),
                                enable_controller_mode: client
                                    .replica_store
                                    .message_store_config_ref()
                                    .enable_controller_mode,
                            };
                            let reader_err_tx = reader_client.err_tx.clone();
                            if let Err(error) = service_loop_group.spawn_operation(
                                &connection_operation,
                                "ha-client-reader",
                                async move {
                                let result = tokio::select! {
                                    res = reader_client.run() => res,
                                    _ = reader_shutdown.notified() => Ok(()),
                                };
                                if let Err(error) = result {
                                    let _ = reader_err_tx.send(error).await;
                                }
                            }) {
                                warn!("HAClient failed to spawn reader task: {error}");
                                client.mark_ready_unless_shutdown().await;
                                if client
                                    .wait_reconnect_delay_after(endpoint.generation)
                                    .await
                                {
                                    break;
                                }
                                continue;
                            }

                            // writer task: write data to master and report offsets
                            let writer_shutdown = client.shutdown_notify.clone();
                            let cfg = WriterCfg {
                                heartbeat_interval_ms: client
                                    .replica_store
                                    .message_store_config_ref()
                                    .ha_send_heartbeat_interval,
                                enable_controller_mode: client
                                    .replica_store
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
                                kick: Arc::clone(&kick),
                                report_offset: BytesMut::with_capacity(CONTROLLER_REPORT_HEADER_SIZE),
                                endpoint: endpoint.clone(),
                                endpoint_updates: endpoint_updates.clone(),
                                endpoint_owner: Arc::clone(&client),
                            };
                            let writer_err_tx = err_tx.clone();
                            if let Err(error) = service_loop_group.spawn_operation(
                                &connection_operation,
                                "ha-client-writer",
                                async move {
                                let result = tokio::select! {
                                    res = writer_client.run() => res,
                                    _ = writer_shutdown.notified() => Ok(()),
                                };
                                if let Err(error) = result {
                                    let _ = writer_err_tx.send(error).await;
                                }
                            }) {
                                warn!("HAClient failed to spawn writer task: {error}");
                                client.shutdown_notify.notify_waiters();
                                if !connection_operation
                                    .cancel_and_wait(&service_loop_group, Duration::from_secs(3))
                                    .await
                                    .unwrap_or(false)
                                {
                                    warn!("HAClient partial connection shutdown exceeded its deadline");
                                }
                                client.mark_ready_unless_shutdown().await;
                                if client
                                    .wait_reconnect_delay_after(endpoint.generation)
                                    .await
                                {
                                    break;
                                }
                                continue;
                            }
                            // main loop for housekeeping and monitoring
                            let mut house = interval(Duration::from_millis(
                                client
                                    .replica_store
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
                                        if interval > client.replica_store.message_store_config_ref().ha_housekeeping_interval {
                                            warn!(
                                                "AutoRecoverHAClient, housekeeping, connection [{:?}] expired, {}",
                                                client.ha_master_address().await, interval
                                            );
                                            break false;
                                        }
                                        // Is it time for the heartbeat? (Even if the offset remains unchanged)
                                        if client.is_time_to_report_offset() {
                                            kick.notify_one();
                                        }
                                    }
                                    changed = endpoint_updates.changed() => {
                                        if changed.is_ok() {
                                            info!(
                                                previous_generation = endpoint.generation,
                                                current_generation = endpoint_updates.borrow().generation,
                                                "HAClient master endpoint changed; retiring the active connection"
                                            );
                                            break false;
                                        }
                                        break true;
                                    }
                                    // outer shutdown
                                    _ = client.owner_cancel.cancelled() => {
                                        break true;
                                    }
                                }
                            };

                            // stop reader/writer
                            client.shutdown_notify.notify_waiters();
                            if !connection_operation
                                .cancel_and_wait(&service_loop_group, Duration::from_secs(3))
                                .await
                                .unwrap_or(false)
                            {
                                warn!("HAClient connection task shutdown exceeded its deadline");
                            }

                            if !exit {
                                // need to reconnect
                                client.mark_ready_unless_shutdown().await;
                                if client
                                    .wait_reconnect_delay_after(endpoint.generation)
                                    .await
                                {
                                    break;
                                }
                                continue;
                            } else {
                                // normal shutdown
                                break;
                            }
                        }
                        Ok(MasterConnectOutcome::Retry { observed_generation }) => {
                            if client.wait_reconnect_delay_after(observed_generation).await {
                                break;
                            }
                            continue;
                        }
                        Ok(MasterConnectOutcome::EndpointChanged) => continue,
                        Ok(MasterConnectOutcome::Shutdown) => break,
                        Err(e) => {
                            warn!("connect_master error: {e:#}");
                            if client.wait_reconnect_delay().await {
                                break;
                            }
                            continue;
                        }
                    }
                }
            }
            client.flow_monitor.shutdown_with_interrupt(true).await;
            info!("HAClient service finished");
        }) {
            warn!("HAClient service not started: {error}");
            return;
        }
        let mut service_group_guard = self.service_group.write().await;
        *service_group_guard = Some(service_group);
    }

    async fn shutdown(&self) {
        self.inner.change_current_state(HAConnectionState::Shutdown).await;
        self.inner.owner_cancel.cancel();
        self.inner.shutdown_notify.notify_waiters();

        // Wait for service to stop
        let service_group = self.service_group.write().await.take();
        if let Some(service_group) = service_group {
            let report = service_group.shutdown(Duration::from_secs(5)).await;
            if let Err(error) = crate::runtime::shutdown_report_result("DefaultHAClient", report) {
                warn!("DefaultHAClient task shutdown reported an error: {error}");
            }
        }
        self.close_master().await;
    }

    async fn wakeup(&self) {
        self.inner.mark_ready_unless_shutdown().await;
    }

    /// Update master address
    async fn update_master_address(&self, new_address: &str) {
        let mut master_address = self.inner.master_address.lock().await;
        let next_address = (!new_address.is_empty()).then(|| new_address.to_string());
        if *master_address != next_address {
            *master_address = next_address;
            info!("Updated master address to: {}", new_address);
        }
    }

    async fn update_ha_master_address(&self, new_address: &str) {
        self.inner.update_master_endpoint(new_address).await;
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
        self.inner.master_endpoint_snapshot().address.unwrap_or_default()
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
        self.inner.close_master().await;
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
    offset_tx: watch::Sender<i64>,
    err_tx: mpsc::Sender<HAClientError>,
    replica_store: HAReplicaStoreHandle,
    flow_monitor: Arc<FlowMonitor>,
    /// Last time slave read data from master
    last_read_timestamp: Arc<AtomicU64>,
    endpoint: MasterEndpointSnapshot,
    endpoint_updates: watch::Receiver<MasterEndpointSnapshot>,
    endpoint_owner: Arc<Inner>,
    enable_controller_mode: bool,
}

impl ReaderTask {
    async fn run(&mut self) -> HAClientTaskResult<()> {
        loop {
            let next = tokio::select! {
                biased;
                changed = self.endpoint_updates.changed() => {
                    return if changed.is_ok() {
                        Ok(())
                    } else {
                        Err(HAClientError::Service("HA endpoint publisher closed".to_string()))
                    };
                }
                next = self.reader.next() => next,
            };
            match next {
                Some(Ok(bytes)) => {
                    // framed - once for one piece of data; we are still doing custom protocol
                    // unpacking in the local buffe
                    self.flow_monitor.add_byte_count_transferred(bytes.len() as i64);
                    self.buf.extend_from_slice(&bytes);

                    if !self.dispatch_read().await? {
                        return Err(HAClientError::Service("dispatchReadRequest error".to_string()));
                    }
                    self.publish_last_read_timestamp_if_current();
                }
                Some(Err(e)) => {
                    return Err(HAClientError::Io(e));
                }
                None => {
                    return Err(HAClientError::Connection("read EOF".to_string()));
                }
            }
        }
    }

    async fn dispatch_read(&mut self) -> HAClientTaskResult<bool> {
        loop {
            if !self.endpoint_is_current() {
                return Ok(true);
            }
            let slave_phy_offset = self.replica_store.get_max_phy_offset();
            let frame = rocketmq_store_local::ha::wire::plan_replica_frame(
                &self.buf,
                self.dispatch_pos,
                self.enable_controller_mode,
                slave_phy_offset,
            )?;
            let Some(frame) = frame else {
                self.compact();
                return Ok(true);
            };
            let body = &self.buf[frame.body_range.clone()];

            if !body.is_empty() {
                self.replica_store
                    .append_replica_data(
                        frame.master_phy_offset,
                        body,
                        0,
                        i32::try_from(body.len())
                            .map_err(|_| HAClientError::Service("HA frame body exceeds i32".to_string()))?,
                    )
                    .await?;
            }

            let current_offset = (!body.is_empty()).then(|| self.replica_store.get_max_phy_offset());
            if !self.publish_replication_progress_if_current(frame.confirm_offset, current_offset) {
                return Ok(true);
            }

            self.dispatch_pos = frame.next_dispatch_position;
        }
    }

    fn endpoint_is_current(&self) -> bool {
        self.endpoint_updates.borrow().generation == self.endpoint.generation
    }

    fn publish_replication_progress_if_current(
        &self,
        confirm_offset: Option<i64>,
        current_offset: Option<i64>,
    ) -> bool {
        self.endpoint_owner
            .commit_if_current(&self.endpoint, || {
                Self::apply_master_confirm_offset(&self.replica_store, confirm_offset);
                if let Some(current_offset) = current_offset {
                    self.offset_tx.send_replace(current_offset);
                }
            })
            .is_some()
    }

    fn publish_last_read_timestamp_if_current(&self) {
        let _ = self.endpoint_owner.commit_if_current(&self.endpoint, || {
            self.last_read_timestamp.store(current_millis(), Ordering::Release);
        });
    }

    fn apply_master_confirm_offset(store: &HAReplicaStoreHandle, confirm_offset: Option<i64>) {
        let Some(confirm_offset) = confirm_offset else {
            return;
        };

        let min_phy_offset = store.get_min_phy_offset();
        let max_phy_offset = store.get_max_phy_offset().max(min_phy_offset);
        let confirm_offset =
            rocketmq_store_local::ha::wire::clamp_confirm_offset(confirm_offset, min_phy_offset, max_phy_offset);
        store.publish_confirm_offset(confirm_offset);
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
    offset_rx: watch::Receiver<i64>,
    kick: Arc<Notify>,
    report_offset: BytesMut,
    endpoint: MasterEndpointSnapshot,
    endpoint_updates: watch::Receiver<MasterEndpointSnapshot>,
    endpoint_owner: Arc<Inner>,
}

impl WriterTask {
    async fn run(&mut self) -> HAClientTaskResult<()> {
        let mut ticker = interval(Duration::from_millis(self.cfg.heartbeat_interval_ms.max(1000)));

        loop {
            tokio::select! {
                biased;
                changed = self.endpoint_updates.changed() => {
                    return if changed.is_ok() {
                        Ok(())
                    } else {
                        Err(HAClientError::Service("HA endpoint publisher closed".to_string()))
                    };
                }
                Ok(()) = self.offset_rx.changed() => {
                    let off = *self.offset_rx.borrow_and_update();
                    if self.publish_reported_offset_if_current(off)
                        && !self.send_offset(off).await?
                    {
                        return Ok(());
                    }
                }
                _ = self.kick.notified() => {
                    let off = self.current_reported_offset_ref.load(Ordering::Relaxed);
                    if !self.send_offset(off).await? {
                        return Ok(());
                    }
                }
                _ = ticker.tick() => {
                    let off = self.current_reported_offset_ref.load(Ordering::Relaxed);
                    if !self.send_offset(off).await? {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn send_offset(&mut self, max_off: i64) -> HAClientTaskResult<bool> {
        if !self.endpoint_is_current() {
            return Ok(false);
        }
        let broker_id = self.reported_broker_id_ref.load(Ordering::Relaxed);
        let bytes = Self::encode_offset_report(
            &mut self.report_offset,
            max_off,
            self.cfg.enable_controller_mode,
            broker_id,
        );
        let send_result = tokio::select! {
            biased;
            changed = self.endpoint_updates.changed() => {
                return if changed.is_ok() {
                    Ok(false)
                } else {
                    Err(HAClientError::Service("HA endpoint publisher closed".to_string()))
                };
            }
            result = self.wr.send(bytes) => result,
        };
        send_result?;
        Ok(self.publish_last_write_timestamp_if_current())
    }

    fn endpoint_is_current(&self) -> bool {
        self.endpoint_updates.borrow().generation == self.endpoint.generation
    }

    fn publish_reported_offset_if_current(&self, offset: i64) -> bool {
        self.endpoint_owner
            .commit_if_current(&self.endpoint, || {
                if offset > self.current_reported_offset_ref.load(Ordering::Relaxed) {
                    self.current_reported_offset_ref.store(offset, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            })
            .unwrap_or(false)
    }

    fn publish_last_write_timestamp_if_current(&self) -> bool {
        self.endpoint_owner
            .commit_if_current(&self.endpoint, || {
                self.last_write_timestamp.store(current_millis(), Ordering::Release);
            })
            .is_some()
    }

    fn encode_offset_report(
        report_offset: &mut BytesMut,
        max_off: i64,
        enable_controller_mode: bool,
        reported_broker_id: i64,
    ) -> bytes::Bytes {
        rocketmq_store_local::ha::wire::encode_offset_report(
            report_offset,
            max_off,
            enable_controller_mode,
            reported_broker_id,
        )
    }
}

/// Error types
#[derive(Debug, thiserror::Error)]
pub enum HAClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Wire(#[from] rocketmq_store_local::ha::wire::HaWireViolation),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Service error: {0}")]
    Service(String),
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use crate::config::store_runtime_config::StoreRuntimeConfig;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_model::common::config::TopicConfig;
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    use super::*;
    use crate::base::backend_ops::BackendOps;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::ha::default_ha_connection::decode_transfer_header;
    use crate::ha::default_ha_connection::encode_transfer_header;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    use rocketmq_store_local::ha::wire::CONTROLLER_TRANSFER_HEADER_SIZE;

    fn new_test_message_store(root: &Path) -> LocalFileMessageStore {
        std::fs::create_dir_all(root).expect("create temp root dir");

        let broker_config = StoreRuntimeConfig {
            duplication_enable: true,
            enable_controller_mode: true,
            ..StoreRuntimeConfig::default()
        };

        let message_store_config = MessageStoreConfig {
            duplication_enable: true,
            enable_controller_mode: true,
            store_path_root_dir: root.to_string_lossy().into_owned().into(),
            timer_wheel_enable: false,
            ..MessageStoreConfig::default()
        };

        let topic_table: Arc<DashMap<CheetahString, Arc<TopicConfig>>> = Arc::new(DashMap::new());
        let mut store = LocalFileMessageStore::new(
            Arc::new(message_store_config),
            Arc::new(broker_config),
            topic_table,
            None,
            false,
            crate::runtime::test_service_context("default-ha-client-store-test"),
        );
        store
            .wire_owned_root_dependencies()
            .expect("wire owned test store dependencies");
        store
    }

    #[test]
    fn client_runtime_uses_standard_arc_and_task_local_buffers() {
        let source = include_str!("default_ha_client.rs").replace("\r\n", "\n");
        let production = source
            .split_once("#[cfg(test)]\nmod tests")
            .map(|(source, _)| source)
            .expect("DefaultHAClient production section");

        assert!(production.contains("inner: Arc<Inner>,"));
        assert!(!production.contains("inner: ArcMut<Inner>,"));
        assert!(production.contains("replica_store: HAReplicaStoreHandle,"));
        assert!(!production.contains("ArcMut<LocalFileMessageStore>"));
        assert!(production.contains("inner: Arc::new(Inner {"));
        assert!(production.contains("let client = Arc::clone(&self.inner);"));
        assert!(!production.contains("ArcMut::new(Inner {"));
        assert!(!production.contains("ArcMut::clone(&self.inner)"));
        assert!(production.contains("async fn close_master(&self)"));
        for unused_runtime_field in [
            "write_stream:",
            "read_stream:",
            "dispatch_position:",
            "byte_buffer_read:",
            "byte_buffer_backup:",
        ] {
            assert!(!production.contains(unused_runtime_field));
        }
        assert!(production.contains("struct ReaderTask"));
        assert!(production.contains("buf: BytesMut,"));
        assert!(production.contains("struct WriterTask"));
        assert!(production.contains("report_offset: BytesMut,"));
    }

    #[tokio::test]
    async fn reconnect_delay_observes_shutdown_even_when_notification_precedes_wait() {
        let temp_dir = tempdir().expect("temp dir");
        let store = new_test_message_store(temp_dir.path());
        let client = DefaultHAClient::new(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("default-ha-reconnect-notification-test"),
        )
        .expect("create default HA client");

        client.inner.change_current_state(HAConnectionState::Shutdown).await;
        client.inner.shutdown_notify.notify_waiters();

        let shutdown_observed = tokio::time::timeout(Duration::from_millis(100), client.inner.wait_reconnect_delay())
            .await
            .expect("shutdown state should bypass reconnect delay");
        assert!(shutdown_observed);
    }

    #[tokio::test]
    async fn pending_connect_is_interrupted_by_endpoint_change() {
        let temp_dir = tempdir().expect("temp dir");
        let store = new_test_message_store(temp_dir.path());
        let client = DefaultHAClient::new(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("default-ha-endpoint-switch-test"),
        )
        .expect("create default HA client");
        assert!(client.inner.update_master_endpoint("192.0.2.1:10912").await);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let connect = client.inner.connect_master_with(move |_| async move {
            let _ = started_tx.send(());
            std::future::pending::<std::io::Result<TcpStream>>().await
        });
        let update = async {
            started_rx.await.expect("connect attempt should start");
            client.inner.update_master_endpoint("192.0.2.2:10912").await
        };
        let (outcome, changed) = tokio::join!(connect, update);

        assert!(changed);
        assert!(matches!(
            outcome.expect("connect outcome"),
            MasterConnectOutcome::EndpointChanged
        ));
        assert_eq!(
            client.inner.master_endpoint_snapshot(),
            MasterEndpointSnapshot {
                address: Some("192.0.2.2:10912".to_string()),
                generation: 2,
            }
        );
    }

    #[tokio::test]
    async fn pending_connect_observes_persistent_owner_cancellation() {
        let temp_dir = tempdir().expect("temp dir");
        let store = new_test_message_store(temp_dir.path());
        let client = DefaultHAClient::new(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("default-ha-connect-cancellation-test"),
        )
        .expect("create default HA client");
        client.inner.update_master_endpoint("192.0.2.1:10912").await;

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let connect = client.inner.connect_master_with(move |_| async move {
            let _ = started_tx.send(());
            std::future::pending::<std::io::Result<TcpStream>>().await
        });
        let cancel = async {
            started_rx.await.expect("connect attempt should start");
            client.inner.owner_cancel.cancel();
        };
        let (outcome, ()) = tokio::join!(connect, cancel);

        assert!(matches!(
            outcome.expect("connect outcome"),
            MasterConnectOutcome::Shutdown
        ));
    }

    #[tokio::test]
    async fn pending_connect_is_bounded_by_configured_deadline() {
        let temp_dir = tempdir().expect("temp dir");
        let store = new_test_message_store(temp_dir.path());
        let client = DefaultHAClient::new_with_connect_timeout(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("default-ha-connect-timeout-test"),
            Duration::from_millis(10),
        )
        .expect("create default HA client");
        client.inner.update_master_endpoint("192.0.2.1:10912").await;

        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            client
                .inner
                .connect_master_with(|_| async { std::future::pending::<std::io::Result<TcpStream>>().await }),
        )
        .await
        .expect("configured connect deadline should complete the attempt")
        .expect("connect outcome");

        assert!(matches!(
            outcome,
            MasterConnectOutcome::Retry { observed_generation: 1 }
        ));
    }

    #[tokio::test]
    async fn stale_endpoint_cannot_publish_transfer_state_or_reported_offset() {
        let temp_dir = tempdir().expect("temp dir");
        let store = new_test_message_store(temp_dir.path());
        let client = DefaultHAClient::new(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("default-ha-stale-generation-test"),
        )
        .expect("create default HA client");
        client.inner.update_master_endpoint("192.0.2.1:10912").await;
        let stale_endpoint = client.inner.master_endpoint_snapshot();
        client.inner.current_reported_offset.store(73, Ordering::Release);

        client.inner.update_master_endpoint("192.0.2.2:10912").await;
        assert!(!client.inner.activate_connected_endpoint(&stale_endpoint).await);

        assert_eq!(client.inner.current_reported_offset.load(Ordering::Acquire), 73);
        assert_eq!(*client.inner.current_state.read().await, HAConnectionState::Ready);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endpoint_generation_commit_is_linearized_with_switch() {
        let temp_dir = tempdir().expect("temp dir");
        let store = new_test_message_store(temp_dir.path());
        let client = DefaultHAClient::new(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("default-ha-linearized-generation-test"),
        )
        .expect("create default HA client");
        client.inner.update_master_endpoint("192.0.2.1:10912").await;
        let old_endpoint = client.inner.master_endpoint_snapshot();
        let publication = Arc::new(AtomicI64::new(0));
        let (commit_entered_tx, commit_entered_rx) = std::sync::mpsc::channel();
        let (release_commit_tx, release_commit_rx) = std::sync::mpsc::channel();

        let commit_owner = Arc::clone(&client.inner);
        let commit_endpoint = old_endpoint.clone();
        let committed_value = Arc::clone(&publication);
        let commit_thread = std::thread::spawn(move || {
            let committed = commit_owner.commit_if_current(&commit_endpoint, || {
                commit_entered_tx.send(()).expect("signal entered commit");
                release_commit_rx.recv().expect("release generation commit");
                committed_value.store(1, Ordering::Release);
            });
            assert!(committed.is_some(), "the commit linearized before the switch");
        });
        commit_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("generation commit should acquire the publication gate");

        let update_owner = Arc::clone(&client.inner);
        let update = tokio::spawn(async move { update_owner.update_master_endpoint("192.0.2.2:10912").await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if client.inner.current_state.try_read().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("endpoint update should wait behind the in-flight commit");

        release_commit_tx.send(()).expect("release generation commit");
        commit_thread.join().expect("generation commit thread");
        assert!(update.await.expect("endpoint update task"));
        assert_eq!(publication.load(Ordering::Acquire), 1);

        assert!(
            client
                .inner
                .commit_if_current(&old_endpoint, || publication.store(2, Ordering::Release))
                .is_none(),
            "the stale generation must not publish after the switch"
        );
        assert_eq!(publication.load(Ordering::Acquire), 1);

        let current_endpoint = client.inner.master_endpoint_snapshot();
        assert!(client
            .inner
            .commit_if_current(&current_endpoint, || publication.store(3, Ordering::Release))
            .is_some());
        assert_eq!(publication.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn shutdown_cancels_and_joins_reconnect_loop() {
        let temp_dir = tempdir().expect("temp dir");
        let store = new_test_message_store(temp_dir.path());
        let client = Arc::new(
            DefaultHAClient::new(
                store.ha_replica_store_handle(),
                crate::runtime::test_scope("default-ha-reconnect-shutdown-test"),
            )
            .expect("create default HA client"),
        );

        HAClient::start(client.as_ref()).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.service_group.read().await.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("HA reconnect loop should start");

        tokio::time::timeout(Duration::from_secs(1), Arc::clone(&client).shutdown())
            .await
            .expect("HA shutdown should interrupt reconnect delay and join its task group");

        assert!(client.service_group.read().await.is_none());
        assert_eq!(client.get_current_state().await, HAConnectionState::Shutdown);
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

        ReaderTask::apply_master_confirm_offset(&store.ha_replica_store_handle(), header.confirm_offset);

        assert_eq!(store.get_confirm_offset(), 4);
    }

    #[tokio::test]
    async fn reader_task_reports_master_slave_offsets_when_append_offset_mismatches() {
        let temp_dir = tempdir().expect("temp dir");
        let mut store = new_test_message_store(temp_dir.path());
        store.init().await.expect("init message store");
        store
            .get_commit_log_mut()
            .append_data(0, &[1, 2, 3, 4], 0, 4)
            .await
            .expect("append data");

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind loopback listener");
        let client = TcpStream::connect(listener.local_addr().expect("listener addr"))
            .await
            .expect("connect loopback client");
        let (server, _) = listener.accept().await.expect("accept loopback client");
        let (reader_half, _) = server.into_split();
        drop(client);

        let (offset_tx, _offset_rx) = tokio::sync::watch::channel(0);
        let (err_tx, _err_rx) = tokio::sync::mpsc::channel(4);
        let endpoint_owner = DefaultHAClient::new(
            store.ha_replica_store_handle(),
            crate::runtime::test_scope("default-ha-reader-generation-test"),
        )
        .expect("create reader endpoint owner")
        .inner;
        let endpoint = endpoint_owner.master_endpoint_snapshot();
        let endpoint_updates = endpoint_owner.master_endpoint.subscribe();
        let mut reader = ReaderTask {
            reader: FramedRead::new(reader_half, BytesCodec::new()),
            buf: BytesMut::from(
                &encode_transfer_header(
                    &mut BytesMut::with_capacity(CONTROLLER_TRANSFER_HEADER_SIZE),
                    8,
                    0,
                    true,
                    4,
                )[..],
            ),
            dispatch_pos: 0,
            offset_tx,
            err_tx,
            replica_store: store.ha_replica_store_handle(),
            flow_monitor: Arc::new(FlowMonitor::new(
                store.message_store_config(),
                crate::runtime::test_scope("default-ha-reader-flow-test").task_group("flow-monitor"),
            )),
            last_read_timestamp: Arc::new(AtomicU64::new(0)),
            endpoint,
            endpoint_updates,
            endpoint_owner,
            enable_controller_mode: true,
        };

        let error = reader
            .dispatch_read()
            .await
            .expect_err("offset mismatch should stop dispatch");
        let message = error.to_string();
        assert!(message.contains("master pushed offset != slave max"));
        assert!(message.contains("slave: 4"));
        assert!(message.contains("master: 8"));
    }
}
