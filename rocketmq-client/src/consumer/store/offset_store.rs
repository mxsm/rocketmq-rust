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

use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::store::local_file_offset_store::LocalFileOffsetStore;
use crate::consumer::store::read_offset_type::ReadOffsetType;
use crate::consumer::store::remote_broker_offset_store::RemoteBrokerOffsetStore;

pub struct OffsetStore {
    remote_broker_offset_store: Option<RemoteBrokerOffsetStore>,
    local_file_offset_store: Option<LocalFileOffsetStore>,
}

impl OffsetStore {
    pub fn new(
        remote_broker_offset_store: Option<RemoteBrokerOffsetStore>,
        local_file_offset_store: Option<LocalFileOffsetStore>,
    ) -> Self {
        Self {
            remote_broker_offset_store,
            local_file_offset_store,
        }
    }

    pub fn new_with_remote(remote_broker_offset_store: RemoteBrokerOffsetStore) -> Self {
        Self {
            remote_broker_offset_store: Some(remote_broker_offset_store),
            local_file_offset_store: None,
        }
    }

    pub fn new_with_local(local_file_offset_store: LocalFileOffsetStore) -> Self {
        Self {
            remote_broker_offset_store: None,
            local_file_offset_store: Some(local_file_offset_store),
        }
    }

    pub async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        if let Some(store) = &self.remote_broker_offset_store {
            store.load().await?;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.load().await?;
        }
        Ok(())
    }

    pub async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        if let Some(store) = &self.remote_broker_offset_store {
            store.update_offset(mq, offset, increase_only).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.update_offset(mq, offset, increase_only).await;
        }
    }

    pub async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        if let Some(store) = &self.remote_broker_offset_store {
            store.update_and_freeze_offset(mq, offset).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.update_and_freeze_offset(mq, offset).await;
        }
    }
    pub async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        if let Some(ref store) = self.remote_broker_offset_store {
            return store.read_offset(mq, type_).await;
        }

        if let Some(ref store) = self.local_file_offset_store {
            return store.read_offset(mq, type_).await;
        }
        0
    }

    pub async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>) {
        if let Some(ref mut store) = self.remote_broker_offset_store {
            store.persist_all(mqs).await;
        }
        if let Some(ref mut store) = self.local_file_offset_store {
            store.persist_all(mqs).await;
        }
    }

    pub async fn persist(&mut self, mq: &MessageQueue) {
        if let Some(ref mut store) = self.remote_broker_offset_store {
            store.persist(mq).await;
        }
        if let Some(ref mut store) = self.local_file_offset_store {
            store.persist(mq).await;
        }
    }

    pub async fn remove_offset(&self, mq: &MessageQueue) {
        if let Some(store) = &self.remote_broker_offset_store {
            store.remove_offset(mq).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            store.remove_offset(mq).await;
        }
    }
    pub async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        if let Some(store) = &self.remote_broker_offset_store {
            return store.clone_offset_table(topic).await;
        }
        if let Some(store) = &self.local_file_offset_store {
            return store.clone_offset_table(topic).await;
        }
        HashMap::new()
    }

    pub async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mut store) = self.remote_broker_offset_store {
            store.update_consume_offset_to_broker(mq, offset, is_oneway).await?;
        }
        if let Some(ref mut store) = self.local_file_offset_store {
            store.update_consume_offset_to_broker(mq, offset, is_oneway).await?;
        }
        Ok(())
    }
}

pub trait OffsetStoreTrait {
    async fn load(&self) -> rocketmq_error::RocketMQResult<()>;

    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool);

    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64);
    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64;

    async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>);

    async fn persist(&mut self, mq: &MessageQueue);

    async fn remove_offset(&self, mq: &MessageQueue);

    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64>;

    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()>;
}
