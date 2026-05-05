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

use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_error::RocketMQError;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFile;
use crate::metadata::JsonMetadataStore;
use crate::metadata::TieredMetadataStore;
use crate::provider::TieredStoreProvider;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FlatFileKey {
    topic: String,
    queue_id: i32,
}

pub struct TieredFlatFileStore<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    metadata_store: Arc<JsonMetadataStore>,
    provider: P,
    files: DashMap<FlatFileKey, Arc<TieredFlatFile<P>>>,
}

impl<P> TieredFlatFileStore<P>
where
    P: TieredStoreProvider,
{
    pub fn new(config: Arc<TieredStoreConfig>, metadata_store: Arc<JsonMetadataStore>, provider: P) -> Self {
        Self {
            config,
            metadata_store,
            provider,
            files: DashMap::new(),
        }
    }

    pub async fn load(&self) -> Result<(), RocketMQError> {
        let queues = self.metadata_store.list_queues().await?;
        for queue in queues {
            self.get_or_create(queue.topic, queue.queue_id)?;
        }
        Ok(())
    }

    pub fn get(&self, topic: &str, queue_id: i32) -> Option<Arc<TieredFlatFile<P>>> {
        let key = FlatFileKey {
            topic: topic.to_owned(),
            queue_id,
        };
        self.files.get(&key).map(|entry| entry.value().clone())
    }

    pub fn get_or_create(&self, topic: String, queue_id: i32) -> Result<Arc<TieredFlatFile<P>>, RocketMQError> {
        let key = FlatFileKey {
            topic: topic.clone(),
            queue_id,
        };
        let entry = self.files.entry(key).or_insert_with(|| {
            Arc::new(TieredFlatFile::new(
                topic,
                queue_id,
                self.config.clone(),
                self.provider.clone(),
            ))
        });
        Ok(entry.value().clone())
    }

    pub async fn cleanup_expired(&self, now_millis: i64) -> Result<(), RocketMQError> {
        let _ = now_millis;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), RocketMQError> {
        Ok(())
    }

    pub async fn destroy(&self) -> Result<(), RocketMQError> {
        self.files.clear();
        Ok(())
    }
}
