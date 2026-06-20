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
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_rust::ArcMut;
use serde::Serialize;
use std::sync::LazyLock;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::store::controllable_offset::ControllableOffset;
use crate::consumer::store::offset_serialize::OffsetSerialize;
use crate::consumer::store::offset_serialize_wrapper::OffsetSerializeWrapper;
use crate::consumer::store::offset_store::OffsetStoreTrait;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::runtime::schedule_client_fixed_delay_task;
use crate::runtime::spawn_client_tracked_task;
use crate::runtime::ClientScheduledTaskHandle;
use crate::runtime::ClientTrackedTaskHandle;

const PERSIST_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const PERIODIC_PERSIST_INTERVAL: Duration = Duration::from_secs(5);

static LOCAL_OFFSET_STORE_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    #[cfg(target_os = "windows")]
    let home = std::env::var("USERPROFILE").map_or(PathBuf::from("C:\\tmp\\.rocketmq_offsets"), |home| {
        PathBuf::from(home).join(".rocketmq_offsets")
    });

    #[cfg(not(target_os = "windows"))]
    let home = std::env::var("HOME").map_or(PathBuf::from("/tmp/.rocketmq_offsets"), |home| {
        PathBuf::from(home).join(".rocketmq_offsets")
    });

    std::env::var("rocketmq.client.localOffsetStoreDir").map_or(home, PathBuf::from)
});

enum PersistCommand {
    PersistMQs(
        HashSet<MessageQueue>,
        Option<tokio::sync::oneshot::Sender<Result<(), std::io::Error>>>,
    ),
    PersistAll(Option<tokio::sync::oneshot::Sender<Result<(), std::io::Error>>>),
    Shutdown,
}

pub struct LocalFileOffsetStore {
    client_instance: ArcMut<MQClientInstance>,
    group_name: CheetahString,
    store_path: CheetahString,
    offset_table: Arc<DashMap<MessageQueue, ControllableOffset>>,
    dirty_flag: Arc<AtomicBool>,
    persist_tx: mpsc::UnboundedSender<PersistCommand>,
    persist_handle: PersistTaskHandle,
}

enum PersistTaskHandle {
    Tokio {
        command_handle: ClientTrackedTaskHandle,
        scheduled_handle: Option<ClientScheduledTaskHandle>,
    },
    NotStarted,
}

impl PersistTaskHandle {
    fn is_finished(&self) -> bool {
        match self {
            Self::Tokio {
                command_handle,
                scheduled_handle,
            } => command_handle.is_finished() && scheduled_handle.as_ref().is_none_or(|handle| !handle.is_running()),
            Self::NotStarted => true,
        }
    }

    async fn shutdown(self, shutdown_tx: &mpsc::UnboundedSender<PersistCommand>, timeout: Duration) -> bool {
        match self {
            Self::Tokio {
                command_handle,
                scheduled_handle,
            } => {
                let started_at = std::time::Instant::now();
                let mut healthy = true;
                if let Some(scheduled_handle) = scheduled_handle {
                    let report = scheduled_handle.shutdown(timeout).await;
                    if !report.is_healthy() {
                        warn!(
                            report = %report.to_json(),
                            "local offset store periodic persist shutdown report is unhealthy"
                        );
                        healthy = false;
                    }
                }

                let _ = shutdown_tx.send(PersistCommand::Shutdown);
                let remaining = timeout.checked_sub(started_at.elapsed()).unwrap_or(Duration::ZERO);
                Self::shutdown_command_task(command_handle, remaining).await && healthy
            }
            Self::NotStarted => {
                let _ = shutdown_tx.send(PersistCommand::Shutdown);
                true
            }
        }
    }

    fn shutdown_now(self) {
        if let Self::Tokio {
            scheduled_handle: Some(scheduled_handle),
            ..
        } = self
        {
            let report = scheduled_handle.shutdown_now();
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "local offset store periodic persist shutdown_now report is unhealthy"
                );
            }
        }
    }

    async fn shutdown_command_task(handle: ClientTrackedTaskHandle, timeout: Duration) -> bool {
        let report = handle.shutdown(timeout).await;
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "local offset store persist task shutdown report is unhealthy"
            );
        }
        report.is_healthy()
    }

    fn task_count(&self) -> usize {
        match self {
            Self::Tokio {
                command_handle,
                scheduled_handle,
            } => {
                command_handle.task_count()
                    + scheduled_handle
                        .as_ref()
                        .map(ClientScheduledTaskHandle::task_count)
                        .unwrap_or_default()
            }
            Self::NotStarted => 0,
        }
    }

    fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        match self {
            Self::Tokio {
                scheduled_handle: Some(scheduled_handle),
                ..
            } => scheduled_handle.schedule_snapshot(),
            _ => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalFileOffsetStoreLifecycleProbe {
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub scheduled_runs: u64,
    pub scheduled_skips: u64,
    pub scheduled_overlaps: u64,
    pub scheduled_failures: u64,
    pub persisted_offset_file: bool,
    pub shutdown_elapsed_us: u128,
    pub healthy: bool,
}

impl LocalFileOffsetStore {
    pub fn new(client_instance: ArcMut<MQClientInstance>, group_name: CheetahString) -> Self {
        let store_path = LOCAL_OFFSET_STORE_DIR
            .clone()
            .join(client_instance.client_id.as_str())
            .join(group_name.as_str())
            .join("offsets.json")
            .to_string_lossy()
            .to_string();

        let offset_table = Arc::new(DashMap::new());
        let dirty_flag = Arc::new(AtomicBool::new(false));
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();

        // Clone for background task
        let offset_table_clone = offset_table.clone();
        let dirty_flag_clone = dirty_flag.clone();
        let store_path_clone = store_path.clone();

        let persist_handle = Self::spawn_background_persist_task_with_interval(
            offset_table_clone,
            dirty_flag_clone,
            store_path_clone,
            persist_rx,
            PERIODIC_PERSIST_INTERVAL,
        );

        Self {
            client_instance,
            group_name,
            store_path: CheetahString::from(store_path),
            offset_table,
            dirty_flag,
            persist_tx,
            persist_handle,
        }
    }

    fn spawn_background_persist_task(
        offset_table: Arc<DashMap<MessageQueue, ControllableOffset>>,
        dirty_flag: Arc<AtomicBool>,
        store_path: String,
        persist_rx: mpsc::UnboundedReceiver<PersistCommand>,
    ) -> PersistTaskHandle {
        Self::spawn_background_persist_task_with_interval(
            offset_table,
            dirty_flag,
            store_path,
            persist_rx,
            PERIODIC_PERSIST_INTERVAL,
        )
    }

    fn spawn_background_persist_task_with_interval(
        offset_table: Arc<DashMap<MessageQueue, ControllableOffset>>,
        dirty_flag: Arc<AtomicBool>,
        store_path: String,
        persist_rx: mpsc::UnboundedReceiver<PersistCommand>,
        persist_interval: Duration,
    ) -> PersistTaskHandle {
        let persist_lock = Arc::new(TokioMutex::new(()));
        let command_offset_table = offset_table.clone();
        let command_dirty_flag = dirty_flag.clone();
        let command_store_path = store_path.clone();
        let command_persist_lock = persist_lock.clone();
        let task = async move {
            Self::background_persist_task(
                command_offset_table,
                command_dirty_flag,
                command_store_path,
                persist_rx,
                command_persist_lock,
            )
            .await;
        };

        let command_handle = match spawn_client_tracked_task("rocketmq-client-local-offset-store", task) {
            Ok(handle) => handle,
            Err(error) => {
                error!("Failed to spawn LocalFileOffsetStore background task: {}", error);
                return PersistTaskHandle::NotStarted;
            }
        };

        let scheduled_offset_table = offset_table;
        let scheduled_dirty_flag = dirty_flag;
        let scheduled_store_path = store_path;
        let scheduled_handle = match schedule_client_fixed_delay_task(
            "rocketmq-client-local-offset-store-periodic",
            persist_interval,
            persist_interval,
            PERSIST_TASK_SHUTDOWN_TIMEOUT,
            move || {
                let offset_table = scheduled_offset_table.clone();
                let dirty_flag = scheduled_dirty_flag.clone();
                let store_path = scheduled_store_path.clone();
                let persist_lock = persist_lock.clone();
                async move {
                    if dirty_flag.swap(false, Ordering::AcqRel) {
                        let _guard = persist_lock.lock().await;
                        if let Err(error) = Self::do_persist(&offset_table, &store_path, None).await {
                            error!("Background persist failed: {}", error);
                            dirty_flag.store(true, Ordering::Release);
                        }
                    }
                }
            },
        ) {
            Ok(handle) => Some(handle),
            Err(error) => {
                error!("Failed to spawn LocalFileOffsetStore periodic task: {}", error);
                None
            }
        };

        PersistTaskHandle::Tokio {
            command_handle,
            scheduled_handle,
        }
    }

    pub async fn shutdown(&mut self) -> bool {
        self.shutdown_with_timeout(PERSIST_TASK_SHUTDOWN_TIMEOUT).await
    }

    pub async fn shutdown_with_timeout(&mut self, timeout: Duration) -> bool {
        let handle = std::mem::replace(&mut self.persist_handle, PersistTaskHandle::NotStarted);
        handle.shutdown(&self.persist_tx, timeout).await
    }

    fn persist_task_count(&self) -> usize {
        self.persist_handle.task_count()
    }

    fn periodic_persist_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.persist_handle.schedule_snapshot()
    }

    async fn read_local_offset(&self) -> rocketmq_error::RocketMQResult<Option<OffsetSerializeWrapper>> {
        let content = fs::read_to_string(self.store_path.as_str()).await.unwrap_or_default();

        if content.is_empty() {
            warn!(
                "Offset store file not found or empty, trying backup: {}",
                self.store_path
            );
            self.read_local_offset_bak().await
        } else {
            match OffsetSerialize::decode(content.as_bytes()) {
                Ok(value) => Ok(Some(value.into())),
                Err(e) => {
                    error!("Failed to deserialize local offset: {}, trying backup", e);
                    self.read_local_offset_bak().await
                }
            }
        }
    }

    async fn read_local_offset_bak(&self) -> rocketmq_error::RocketMQResult<Option<OffsetSerializeWrapper>> {
        let bak_path = format!("{}.bak", self.store_path);
        let content = fs::read_to_string(&bak_path).await.unwrap_or_default();

        if content.is_empty() {
            warn!("No backup file found, starting with empty offset table");
            Ok(None)
        } else {
            match OffsetSerialize::decode(content.as_bytes()) {
                Ok(value) => {
                    info!("Successfully loaded offset from backup file");
                    Ok(Some(value.into()))
                }
                Err(e) => Err(mq_client_err!(format!("Failed to parse backup offset file: {}", e))),
            }
        }
    }

    async fn atomic_write_to_file(store_path: &str, content: &str) -> Result<(), std::io::Error> {
        let tmp_path = format!("{}.tmp", store_path);
        let bak_path = format!("{}.bak", store_path);

        // 1. Write to temporary file and fsync
        let mut tmp_file = fs::File::create(&tmp_path).await?;
        tmp_file.write_all(content.as_bytes()).await?;
        tmp_file.sync_all().await?;
        drop(tmp_file);

        // 2. Backup existing file if it exists
        if fs::try_exists(store_path).await.unwrap_or(false) {
            fs::rename(store_path, &bak_path).await.ok();
        }

        // 3. Atomic rename (POSIX guarantees atomicity)
        fs::rename(&tmp_path, store_path).await?;

        // 4. Fsync the directory to ensure rename is durable
        if let Some(parent) = Path::new(store_path).parent() {
            if let Ok(dir) = fs::File::open(parent).await {
                dir.sync_all().await.ok();
            }
        }

        Ok(())
    }

    async fn background_persist_task(
        offset_table: Arc<DashMap<MessageQueue, ControllableOffset>>,
        dirty_flag: Arc<AtomicBool>,
        store_path: String,
        mut rx: mpsc::UnboundedReceiver<PersistCommand>,
        persist_lock: Arc<TokioMutex<()>>,
    ) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                PersistCommand::PersistMQs(mqs, reply) => {
                    let _guard = persist_lock.lock().await;
                    let result = Self::do_persist(&offset_table, &store_path, Some(mqs)).await;
                    if let Err(ref e) = result {
                        error!("Persist MQs failed: {}", e);
                    }
                    if let Some(tx) = reply {
                        tx.send(result).ok();
                    }
                }
                PersistCommand::PersistAll(reply) => {
                    let _guard = persist_lock.lock().await;
                    let result = Self::do_persist(&offset_table, &store_path, None).await;
                    if let Err(ref e) = result {
                        error!("Persist all failed: {}", e);
                    }
                    if let Some(tx) = reply {
                        tx.send(result).ok();
                    }
                }
                PersistCommand::Shutdown => {
                    // Final persist before shutdown
                    if dirty_flag.load(Ordering::Acquire) {
                        let _guard = persist_lock.lock().await;
                        if let Err(e) = Self::do_persist(&offset_table, &store_path, None).await {
                            error!("Final persist on shutdown failed: {}", e);
                        }
                    }
                    break;
                }
            }
        }
    }

    async fn do_persist(
        offset_table: &DashMap<MessageQueue, ControllableOffset>,
        store_path: &str,
        mqs: Option<HashSet<MessageQueue>>,
    ) -> Result<(), std::io::Error> {
        // Read existing offsets
        let existing = fs::read_to_string(store_path).await.unwrap_or_default();

        let mut wrapper = if !existing.is_empty() {
            OffsetSerialize::decode(existing.as_bytes())
                .map(|v: OffsetSerialize| OffsetSerializeWrapper::from(v))
                .unwrap_or_default()
        } else {
            OffsetSerializeWrapper::default()
        };

        // Collect offsets to persist
        match mqs {
            Some(ref set) => {
                for mq in set {
                    if let Some(entry) = offset_table.get(mq) {
                        wrapper
                            .offset_table
                            .insert(mq.clone(), AtomicI64::new(entry.value().get_offset()));
                    }
                }
            }
            None => {
                for entry in offset_table.iter() {
                    wrapper
                        .offset_table
                        .insert(entry.key().clone(), AtomicI64::new(entry.value().get_offset()));
                }
            }
        }

        // Serialize
        let content = OffsetSerialize::from(wrapper)
            .serialize_json_pretty()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        if content.is_empty() {
            return Ok(());
        }

        // Use atomic write function
        Self::atomic_write_to_file(store_path, &content).await
    }
}

impl OffsetStoreTrait for LocalFileOffsetStore {
    async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        let offset_serialize_wrapper = self.read_local_offset().await?;
        if let Some(offset_serialize_wrapper) = offset_serialize_wrapper {
            let offset_table = offset_serialize_wrapper.offset_table;
            self.offset_table.clear();
            for (mq, offset) in offset_table {
                let offset = offset.load(Ordering::Relaxed);
                info!("load consumer's offset, {} {} {}", self.group_name, mq, offset);
                self.offset_table.insert(mq, ControllableOffset::new(offset));
            }
        }
        Ok(())
    }

    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        self.offset_table
            .entry(mq.clone())
            .and_modify(|v| {
                if increase_only {
                    v.update(offset, true);
                } else {
                    v.update_unconditionally(offset);
                }
            })
            .or_insert_with(|| ControllableOffset::new(offset));

        self.dirty_flag.store(true, Ordering::Release);
    }

    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        self.offset_table
            .entry(mq.clone())
            .and_modify(|v| v.update_and_freeze(offset))
            .or_insert_with(|| ControllableOffset::new_frozen(offset));

        self.dirty_flag.store(true, Ordering::Release);
    }

    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        match type_ {
            ReadOffsetType::ReadFromMemory => self
                .offset_table
                .get(mq)
                .map(|entry| entry.value().get_offset())
                .unwrap_or(-1),
            ReadOffsetType::MemoryFirstThenStore => {
                if let Some(offset) = self.offset_table.get(mq).map(|entry| entry.value().get_offset()) {
                    return offset;
                }
                match self.read_local_offset().await {
                    Ok(offset_serialize_wrapper) => {
                        if let Some(offset_serialize_wrapper) = offset_serialize_wrapper {
                            if let Some(offset) = offset_serialize_wrapper.offset_table.get(mq) {
                                let offset = offset.load(Ordering::Relaxed);
                                self.update_offset(mq, offset, false).await;
                                offset
                            } else {
                                -1
                            }
                        } else {
                            -1
                        }
                    }
                    Err(_) => -1,
                }
            }
            ReadOffsetType::ReadFromStore => match self.read_local_offset().await {
                Ok(offset_serialize_wrapper) => {
                    if let Some(offset_serialize_wrapper) = offset_serialize_wrapper {
                        if let Some(offset) = offset_serialize_wrapper.offset_table.get(mq) {
                            let offset = offset.load(Ordering::Relaxed);
                            self.update_offset(mq, offset, false).await;
                            offset
                        } else {
                            -1
                        }
                    } else {
                        -1
                    }
                }
                Err(_) => -1,
            },
        }
    }

    async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>) {
        if mqs.is_empty() {
            return;
        }

        // Send persist command and wait for completion to maintain original semantics
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .persist_tx
            .send(PersistCommand::PersistMQs(mqs.clone(), Some(tx)))
            .is_ok()
        {
            // Wait for persist to complete
            if let Ok(Err(e)) = rx.await {
                error!("persistAll consumer offset failed: {}", e);
            }
        }
    }

    async fn persist(&mut self, mq: &MessageQueue) {
        // Send persist command and wait for completion to maintain original semantics
        let mut set = HashSet::new();
        set.insert(mq.clone());

        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.persist_tx.send(PersistCommand::PersistMQs(set, Some(tx))).is_ok() {
            // Wait for persist to complete
            if let Ok(Err(e)) = rx.await {
                error!("persist consumer offset failed: {}", e);
            }
        }
    }

    async fn remove_offset(&self, mq: &MessageQueue) {
        self.offset_table.remove(mq);
        self.dirty_flag.store(true, Ordering::Release);
        info!(
            "remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}",
            mq,
            self.group_name,
            self.offset_table.len()
        );
    }

    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        self.offset_table
            .iter()
            .filter(|entry| topic.is_empty() || entry.key().topic_str() == topic)
            .map(|entry| (entry.key().clone(), entry.value().get_offset()))
            .collect()
    }

    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }
}

impl Drop for LocalFileOffsetStore {
    fn drop(&mut self) {
        // Send shutdown command to background task
        self.persist_tx.send(PersistCommand::Shutdown).ok();
        let handle = std::mem::replace(&mut self.persist_handle, PersistTaskHandle::NotStarted);
        handle.shutdown_now();
        // Note: We can't await the command worker in Drop (it's not async).
        // The worker receives Shutdown and performs the final persist best-effort.
    }
}

#[doc(hidden)]
pub async fn run_local_file_offset_store_lifecycle_probe() -> LocalFileOffsetStoreLifecycleProbe {
    use crate::base::client_config::ClientConfig;

    let root = std::env::temp_dir().join(format!(
        "rocketmq-rust-local-offset-store-probe-{}",
        rocketmq_common::TimeUtils::current_millis()
    ));
    let store_path = root.join("offsets.json").to_string_lossy().to_string();
    if let Some(parent) = Path::new(&store_path).parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .expect("local offset probe directory should be created");
    }

    let client_instance = MQClientInstance::new_arc(
        ClientConfig::default(),
        0,
        format!("local-offset-store-probe-{}", std::process::id()),
        None,
    );
    let offset_table = Arc::new(DashMap::<MessageQueue, ControllableOffset>::new());
    let dirty_flag = Arc::new(AtomicBool::new(false));
    let (persist_tx, persist_rx) = mpsc::unbounded_channel();
    let persist_handle = LocalFileOffsetStore::spawn_background_persist_task_with_interval(
        offset_table.clone(),
        dirty_flag.clone(),
        store_path.clone(),
        persist_rx,
        Duration::from_millis(1),
    );
    let mut store = LocalFileOffsetStore {
        client_instance,
        group_name: CheetahString::from_static_str("local_offset_store_probe_group"),
        store_path: CheetahString::from(store_path.clone()),
        offset_table,
        dirty_flag,
        persist_tx,
        persist_handle,
    };
    let mq = MessageQueue::from_parts("local-offset-store-probe-topic", "broker-a", 0);
    store.update_offset(&mq, 66, false).await;

    let mut snapshots = store.periodic_persist_snapshot();
    for _ in 0..100 {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            && tokio::fs::try_exists(&store_path).await.unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        snapshots = store.periodic_persist_snapshot();
    }

    let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
    let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
    let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
    let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
    let task_count_before_shutdown = store.persist_task_count();
    let persisted_offset_file = tokio::fs::try_exists(&store_path).await.unwrap_or(false);
    let shutdown_started_at = std::time::Instant::now();
    let shutdown_healthy = store.shutdown().await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let task_count_after_shutdown = store.persist_task_count();
    let healthy = shutdown_healthy
        && scheduled_runs > 0
        && scheduled_overlaps == 0
        && scheduled_failures == 0
        && persisted_offset_file
        && task_count_before_shutdown > 0
        && task_count_after_shutdown == 0;

    let _ = tokio::fs::remove_file(&store_path).await;
    let _ = tokio::fs::remove_file(format!("{store_path}.bak")).await;
    let _ = tokio::fs::remove_file(format!("{store_path}.tmp")).await;
    let _ = tokio::fs::remove_dir_all(root).await;

    LocalFileOffsetStoreLifecycleProbe {
        task_count_before_shutdown,
        task_count_after_shutdown,
        scheduled_runs,
        scheduled_skips,
        scheduled_overlaps,
        scheduled_failures,
        persisted_offset_file,
        shutdown_elapsed_us,
        healthy,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_remoting::protocol::RemotingDeserializable;
    use rocketmq_remoting::protocol::RemotingSerializable;
    use tokio::sync::mpsc;

    use super::PersistTaskHandle;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::store::controllable_offset::ControllableOffset;
    use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
    use crate::consumer::store::offset_serialize::OffsetSerialize;
    use crate::consumer::store::offset_store::OffsetStoreTrait;
    use crate::consumer::store::read_offset_type::ReadOffsetType;
    use crate::factory::mq_client_instance::MQClientInstance;
    use crate::runtime::spawn_client_tracked_task;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn new_without_tokio_runtime_does_not_spawn_panic() {
        let client_instance =
            MQClientInstance::new_arc(ClientConfig::default(), 0, "local-offset-store-no-runtime-test", None);
        let store = LocalFileOffsetStore::new(
            client_instance,
            CheetahString::from_static_str("local_offset_store_no_runtime_group"),
        );

        drop(store);
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    #[tokio::test]
    async fn memory_first_then_store_reads_local_file_on_memory_miss_like_java() {
        let client_id = format!("local-offset-store-read-fallback-test-{}", std::process::id());
        let group = CheetahString::from(format!("local_offset_store_read_fallback_group_{}", std::process::id()));
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, client_id, None);
        let store_dir = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("tmp")
            .join("local_offset_store_read_fallback");
        let store_path = store_dir.join("offsets.json").to_string_lossy().to_string();
        let offset_table = Arc::new(DashMap::<MessageQueue, ControllableOffset>::new());
        let dirty_flag = Arc::new(AtomicBool::new(false));
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let persist_handle = LocalFileOffsetStore::spawn_background_persist_task(
            offset_table.clone(),
            dirty_flag.clone(),
            store_path.clone(),
            persist_rx,
        );
        let mut store = LocalFileOffsetStore {
            client_instance,
            group_name: group,
            store_path: CheetahString::from(store_path),
            offset_table,
            dirty_flag,
            persist_tx,
            persist_handle,
        };
        let mq = MessageQueue::from_parts("local-offset-store-topic", "broker-a", 0);

        if let Some(parent) = Path::new(store.store_path.as_str()).parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        store.update_offset(&mq, 42, false).await;
        store.persist(&mq).await;
        store.offset_table.clear();

        let offset = store.read_offset(&mq, ReadOffsetType::MemoryFirstThenStore).await;
        assert_eq!(42, offset);
        assert_eq!(42, store.read_offset(&mq, ReadOffsetType::ReadFromMemory).await);

        let store_path = store.store_path.to_string();
        store.dirty_flag.store(false, Ordering::Release);
        drop(store);
        let _ = tokio::fs::remove_file(&store_path).await;
        let _ = tokio::fs::remove_file(format!("{store_path}.bak")).await;
        let _ = tokio::fs::remove_file(format!("{store_path}.tmp")).await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_final_persist() {
        let client_id = format!("local-offset-store-shutdown-test-{}", std::process::id());
        let group = CheetahString::from(format!("local_offset_store_shutdown_group_{}", std::process::id()));
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, client_id, None);
        let store_dir = std::env::current_dir()
            .unwrap()
            .join("target")
            .join("tmp")
            .join("local_offset_store_shutdown");
        let store_path = store_dir.join("offsets.json").to_string_lossy().to_string();
        let offset_table = Arc::new(DashMap::<MessageQueue, ControllableOffset>::new());
        let dirty_flag = Arc::new(AtomicBool::new(false));
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let persist_handle = LocalFileOffsetStore::spawn_background_persist_task(
            offset_table.clone(),
            dirty_flag.clone(),
            store_path.clone(),
            persist_rx,
        );
        let mut store = LocalFileOffsetStore {
            client_instance,
            group_name: group,
            store_path: CheetahString::from(store_path.clone()),
            offset_table,
            dirty_flag,
            persist_tx,
            persist_handle,
        };
        let mq = MessageQueue::from_parts("local-offset-store-shutdown-topic", "broker-a", 0);

        if let Some(parent) = Path::new(&store_path).parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        store.update_offset(&mq, 88, false).await;

        assert!(store.shutdown().await);
        assert!(store.persist_handle.is_finished());

        let content = tokio::fs::read(&store_path).await.unwrap();
        let persisted = OffsetSerialize::decode(&content).expect("shutdown should persist offsets");
        let mq_key = mq.serialize_json().expect("message queue should serialize");
        assert_eq!(persisted.offset_table.get(&mq_key).copied(), Some(88));

        let _ = tokio::fs::remove_file(&store_path).await;
        let _ = tokio::fs::remove_file(format!("{store_path}.bak")).await;
        let _ = tokio::fs::remove_file(format!("{store_path}.tmp")).await;
    }

    #[tokio::test]
    async fn persist_task_shutdown_aborts_after_timeout() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let handle = spawn_client_tracked_task("rocketmq-client-local-offset-store-timeout-test", async move {
            let _drop_flag = DropFlag(dropped_in_task);
            pending::<()>().await;
        })
        .expect("timeout test command task should spawn");
        let handle = PersistTaskHandle::Tokio {
            command_handle: handle,
            scheduled_handle: None,
        };
        let (persist_tx, _persist_rx) = mpsc::unbounded_channel();

        assert!(!handle.shutdown(&persist_tx, std::time::Duration::from_millis(20)).await);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn local_file_offset_store_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::run_local_file_offset_store_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.persisted_offset_file, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
