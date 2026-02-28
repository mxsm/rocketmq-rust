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

/// The active offset storage backend for a message consumer.
///
/// Exactly one variant is active during the lifetime of a consumer:
/// - [`Remote`][OffsetStore::Remote] is used in clustering mode, where offsets are coordinated
///   through the broker.
/// - [`Local`][OffsetStore::Local] is used in broadcasting mode, where offsets are stored in a
///   local file on the consumer host.
pub enum OffsetStore {
    /// Offsets persisted to and retrieved from the remote broker.
    Remote(RemoteBrokerOffsetStore),
    /// Offsets persisted to and retrieved from a local file.
    Local(LocalFileOffsetStore),
}

impl OffsetStore {
    /// Returns an [`OffsetStore`] backed by remote broker offset storage.
    pub fn new_with_remote(remote_broker_offset_store: RemoteBrokerOffsetStore) -> Self {
        Self::Remote(remote_broker_offset_store)
    }

    /// Returns an [`OffsetStore`] backed by local file offset storage.
    pub fn new_with_local(local_file_offset_store: LocalFileOffsetStore) -> Self {
        Self::Local(local_file_offset_store)
    }

    /// Asynchronously loads persisted offsets from the underlying storage backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage backend fails to read or deserialize
    /// the persisted offset data.
    pub async fn load(&self) -> rocketmq_error::RocketMQResult<()> {
        match self {
            Self::Remote(store) => store.load().await,
            Self::Local(store) => store.load().await,
        }
    }

    /// Asynchronously updates the stored offset for the given queue.
    ///
    /// When `increase_only` is `true`, the offset is updated only if the new value is
    /// greater than the currently stored value.
    pub async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        match self {
            Self::Remote(store) => store.update_offset(mq, offset, increase_only).await,
            Self::Local(store) => store.update_offset(mq, offset, increase_only).await,
        }
    }

    /// Asynchronously updates the stored offset for the given queue and prevents further
    /// updates until the freeze is lifted.
    pub async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64) {
        match self {
            Self::Remote(store) => store.update_and_freeze_offset(mq, offset).await,
            Self::Local(store) => store.update_and_freeze_offset(mq, offset).await,
        }
    }

    /// Asynchronously reads the current offset for the given queue according to the
    /// specified read strategy.
    ///
    /// Returns `0` if no offset has been recorded for the queue.
    pub async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64 {
        match self {
            Self::Remote(store) => store.read_offset(mq, type_).await,
            Self::Local(store) => store.read_offset(mq, type_).await,
        }
    }

    /// Asynchronously persists the offsets for all queues in `mqs` to the underlying
    /// storage backend.
    pub async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>) {
        match self {
            Self::Remote(store) => store.persist_all(mqs).await,
            Self::Local(store) => store.persist_all(mqs).await,
        }
    }

    /// Asynchronously persists the offset for the specified queue to the underlying
    /// storage backend.
    pub async fn persist(&mut self, mq: &MessageQueue) {
        match self {
            Self::Remote(store) => store.persist(mq).await,
            Self::Local(store) => store.persist(mq).await,
        }
    }

    /// Asynchronously removes the stored offset entry for the specified queue.
    pub async fn remove_offset(&self, mq: &MessageQueue) {
        match self {
            Self::Remote(store) => store.remove_offset(mq).await,
            Self::Local(store) => store.remove_offset(mq).await,
        }
    }

    /// Asynchronously returns a snapshot of the offset table filtered by the given topic.
    pub async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64> {
        match self {
            Self::Remote(store) => store.clone_offset_table(topic).await,
            Self::Local(store) => store.clone_offset_table(topic).await,
        }
    }

    /// Asynchronously pushes the given offset for the specified queue to the broker.
    ///
    /// When `is_oneway` is `true`, the request is sent without waiting for an acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker is unreachable or rejects the offset commit request.
    pub async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        match self {
            Self::Remote(store) => store.update_consume_offset_to_broker(mq, offset, is_oneway).await,
            Self::Local(store) => store.update_consume_offset_to_broker(mq, offset, is_oneway).await,
        }
    }
}

/// Defines the contract for a consumer offset storage backend.
///
/// Implementations are responsible for loading, updating, persisting, and querying
/// per-queue consume offsets. The two standard backends are
/// [`RemoteBrokerOffsetStore`] and [`LocalFileOffsetStore`].
pub trait OffsetStoreTrait {
    /// Asynchronously loads persisted offsets from the underlying storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage backend fails to read or deserialize
    /// the persisted offset data.
    async fn load(&self) -> rocketmq_error::RocketMQResult<()>;

    /// Asynchronously updates the stored offset for the given queue.
    ///
    /// When `increase_only` is `true`, the offset is updated only if the new value is
    /// greater than the currently stored value.
    async fn update_offset(&self, mq: &MessageQueue, offset: i64, increase_only: bool);

    /// Asynchronously updates the stored offset for the given queue and prevents further
    /// updates until the freeze is lifted.
    async fn update_and_freeze_offset(&self, mq: &MessageQueue, offset: i64);

    /// Asynchronously reads the current offset for the given queue according to the
    /// specified read strategy.
    ///
    /// Returns `0` if no offset has been recorded for the queue.
    async fn read_offset(&self, mq: &MessageQueue, type_: ReadOffsetType) -> i64;

    /// Asynchronously persists the offsets for all queues in `mqs` to the underlying storage.
    async fn persist_all(&mut self, mqs: &HashSet<MessageQueue>);

    /// Asynchronously persists the offset for the specified queue to the underlying storage.
    async fn persist(&mut self, mq: &MessageQueue);

    /// Asynchronously removes the stored offset entry for the specified queue.
    async fn remove_offset(&self, mq: &MessageQueue);

    /// Asynchronously returns a snapshot of the offset table filtered by the given topic.
    async fn clone_offset_table(&self, topic: &str) -> HashMap<MessageQueue, i64>;

    /// Asynchronously pushes the given offset for the specified queue to the broker.
    ///
    /// When `is_oneway` is `true`, the request is sent without waiting for an acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker is unreachable or rejects the offset commit request.
    async fn update_consume_offset_to_broker(
        &mut self,
        mq: &MessageQueue,
        offset: i64,
        is_oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()>;
}
