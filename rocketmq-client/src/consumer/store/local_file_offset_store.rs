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
use rocketmq_rust::ArcMut;
use std::sync::LazyLock;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::consumer::store::controllable_offset::ControllableOffset;
use crate::consumer::store::offset_serialize::OffsetSerialize;
use crate::consumer::store::offset_serialize_wrapper::OffsetSerializeWrapper;
use crate::consumer::store::offset_store::OffsetStoreTrait;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

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
    #[allow(dead_code)]
    persist_handle: Arc<JoinHandle<()>>,
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

        // Start background persistence task
        let persist_handle = Arc::new(tokio::spawn(async move {
            Self::background_persist_task(offset_table_clone, dirty_flag_clone, store_path_clone, persist_rx).await;
        }));

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
    ) {
        let mut ticker = interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    // Periodic full persist if dirty
                    if dirty_flag.swap(false, Ordering::AcqRel) {
                        if let Err(e) = Self::do_persist(&offset_table, &store_path, None).await {
                            error!("Background persist failed: {}", e);
                            dirty_flag.store(true, Ordering::Release);
                        }
                    }
                }
                Some(cmd) = rx.recv() => {
                    match cmd {
                        PersistCommand::PersistMQs(mqs, reply) => {
                            let result = Self::do_persist(&offset_table, &store_path, Some(mqs)).await;
                            if let Err(ref e) = result {
                                error!("Persist MQs failed: {}", e);
                            }
                            if let Some(tx) = reply {
                                tx.send(result).ok();
                            }
                        }
                        PersistCommand::PersistAll(reply) => {
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
                                if let Err(e) = Self::do_persist(&offset_table, &store_path, None).await {
                                    error!("Final persist on shutdown failed: {}", e);
                                }
                            }
                            break;
                        }
                    }
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
            ReadOffsetType::ReadFromMemory | ReadOffsetType::MemoryFirstThenStore => self
                .offset_table
                .get(mq)
                .map(|entry| entry.value().get_offset())
                .unwrap_or(-1),
            ReadOffsetType::ReadFromStore => match self.read_local_offset().await {
                Ok(offset_serialize_wrapper) => {
                    if let Some(offset_serialize_wrapper) = offset_serialize_wrapper {
                        if let Some(offset) = offset_serialize_wrapper.offset_table.get(mq) {
                            offset.load(Ordering::Relaxed)
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
        // Note: We can't await the handle in Drop (it's not async)
        // The background task will complete its final persist and exit
    }
}
