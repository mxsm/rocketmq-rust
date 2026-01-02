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
use std::path::PathBuf;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use once_cell::sync::Lazy;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::utils::file_utils;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;

use crate::consumer::store::controllable_offset::ControllableOffset;
use crate::consumer::store::offset_serialize::OffsetSerialize;
use crate::consumer::store::offset_serialize_wrapper::OffsetSerializeWrapper;
use crate::consumer::store::offset_store::OffsetStoreTrait;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::factory::mq_client_instance::MQClientInstance;

static LOCAL_OFFSET_STORE_DIR: Lazy<PathBuf> = Lazy::new(|| {
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

pub struct LocalFileOffsetStore {
    client_instance: ArcMut<MQClientInstance>,
    group_name: CheetahString,
    store_path: CheetahString,
    offset_table: Arc<Mutex<HashMap<MessageQueue, ControllableOffset>>>,
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
        Self {
            client_instance,
            group_name,
            store_path: CheetahString::from(store_path),
            offset_table: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn read_local_offset(&self) -> rocketmq_error::RocketMQResult<Option<OffsetSerializeWrapper>> {
        let content = file_utils::file_to_string(self.store_path.as_str()).map_or("".to_string(), |content| content);
        if content.is_empty() {
            self.read_local_offset_bak()
        } else {
            match OffsetSerialize::decode(content.as_bytes()) {
                Ok(value) => Ok(Some(value.into())),
                Err(e) => Err(mq_client_err!(format!("Failed to deserialize local offset: {}", e))),
            }
        }
    }
    fn read_local_offset_bak(&self) -> rocketmq_error::RocketMQResult<Option<OffsetSerializeWrapper>> {
        let content = file_utils::file_to_string(format!("{}{}", self.store_path, ".bak"))
            .map_or("".to_string(), |content| content);
        if content.is_empty() {
            Ok(None)
        } else {
            match OffsetSerialize::decode(content.as_bytes()) {
                Ok(value) => Ok(Some(value.into())),
                Err(_) => Err(mq_client_err!(format!(
                    "read local offset bak failed, content: {}",
                    content
                ))),
            }
        }
    }
}

impl OffsetStoreTrait for LocalFileOffsetStore {
    async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        let offset_serialize_wrapper = self.read_local_offset()?;
        if let Some(offset_serialize_wrapper) = offset_serialize_wrapper {
            let offset_table = offset_serialize_wrapper.offset_table;
            let mut offset_table_inner = self.offset_table.lock().await;
            for (mq, offset) in offset_table {
                let offset = offset.load(Ordering::Relaxed);
                info!("load consumer's offset, {} {} {}", self.group_name, mq, offset);
                offset_table_inner.insert(mq, ControllableOffset::new(offset));
            }
        }
        Ok(())
    }

    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        let mut offset_table = self.offset_table.lock().await;
        let offset_old = offset_table
            .entry(mq.clone())
            .or_insert_with(|| ControllableOffset::new(offset));
        if increase_only {
            offset_old.update(offset, true);
        } else {
            offset_old.update_unconditionally(offset);
        }
    }

    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        let mut offset_table = self.offset_table.lock().await;
        offset_table
            .entry(mq.clone())
            .or_insert_with(|| ControllableOffset::new(offset))
            .update_and_freeze(offset);
    }

    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        match type_ {
            ReadOffsetType::ReadFromMemory | ReadOffsetType::MemoryFirstThenStore => {
                let offset_table = self.offset_table.lock().await;
                if let Some(offset) = offset_table.get(mq) {
                    offset.get_offset()
                } else {
                    -1
                }
            }
            ReadOffsetType::ReadFromStore => match self.read_local_offset() {
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
        let mut offset_serialize_wrapper = match self.read_local_offset() {
            Ok(value) => value.unwrap_or_default(),
            Err(e) => {
                error!("read local offset failed: {}", e);
                return;
            }
        };

        let offset_table = self.offset_table.lock().await;
        for (mq, offset) in offset_table.iter() {
            if mqs.contains(mq) {
                offset_serialize_wrapper
                    .offset_table
                    .insert(mq.clone(), AtomicI64::new(offset.get_offset()));
            }
        }

        let content = OffsetSerialize::from(offset_serialize_wrapper)
            .serialize_json_pretty()
            .expect("persistAll failed");
        if !content.is_empty() {
            if let Err(e) = file_utils::string_to_file(&content, self.store_path.as_str()) {
                error!("persistAll consumer offset Exception, {},{}", self.store_path, e);
            }
        }
    }

    async fn persist(&mut self, mq: &MessageQueue) {
        let offset_table = self.offset_table.lock().await;
        if let Some(offset) = offset_table.get(mq) {
            let mut offset_serialize_wrapper = match self.read_local_offset() {
                Ok(value) => value.unwrap_or_default(),
                Err(e) => {
                    error!("read local offset failed: {}", e);
                    return;
                }
            };
            offset_serialize_wrapper
                .offset_table
                .insert(mq.clone(), AtomicI64::new(offset.get_offset()));
            let content = OffsetSerialize::from(offset_serialize_wrapper)
                .serialize_json_pretty()
                .expect("persist failed");
            if !content.is_empty() {
                if let Err(e) = file_utils::string_to_file(&content, self.store_path.as_str()) {
                    error!("persist consumer offset Exception, {},{}", self.store_path, e);
                }
            }
        }
    }

    async fn remove_offset(&self, mq: &MessageQueue) {
        let mut offset_table = self.offset_table.lock().await;
        offset_table.remove(mq);
        info!(
            "remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}",
            mq,
            self.group_name,
            offset_table.len()
        );
    }

    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        let offset_table = self.offset_table.lock().await;
        offset_table
            .iter()
            .filter(|(mq, _)| topic.is_empty() || mq.get_topic() == topic)
            .map(|(mq, offset)| (mq.clone(), offset.get_offset()))
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
