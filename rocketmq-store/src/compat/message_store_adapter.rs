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

//! Narrow projections from the frozen legacy `MessageStore` contract.

use std::convert::Infallible;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::OffsetIndex;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreLifecycle;

use crate::base::message_store::MessageStore;
use crate::capability::store_append_receipt;
use crate::capability::MessageReadRequest;
use crate::capability::MessageReadResult;
use crate::capability::MessageStoreHealthCapability;
use crate::capability::MessageStoreReadCapability;
use crate::capability::StoreAppendReceipt;
use crate::capability::StoreHealthSnapshot;
use crate::store_error::StoreError;

/// Temporary adapter that exposes only capabilities used by migrated callers.
///
/// New Broker paths must accept the capability traits directly. This adapter
/// exists solely to drain callers of the frozen `MessageStore` interface.
pub(crate) struct LegacyMessageStoreAdapter<'a, S> {
    store: &'a mut S,
}

impl<'a, S> LegacyMessageStoreAdapter<'a, S> {
    pub(crate) const fn new(store: &'a mut S) -> Self {
        Self { store }
    }
}

/// Logical queue range queried through the legacy compatibility adapter.
pub(crate) struct LegacyOffsetQuery {
    pub(crate) topic: CheetahString,
    pub(crate) queue_id: i32,
}

/// Minimum and maximum logical offsets observed as one projection.
pub(crate) struct LegacyOffsetRange {
    pub(crate) minimum: i64,
    pub(crate) maximum: i64,
}

impl<S> MessageAppender<MessageExtBrokerInner> for LegacyMessageStoreAdapter<'_, S>
where
    S: MessageStore,
{
    type Receipt = StoreAppendReceipt;
    type Error = Infallible;

    async fn append_message(&mut self, message: MessageExtBrokerInner) -> Result<Self::Receipt, Self::Error> {
        let result = MessageStore::put_message(self.store, message).await;
        Ok(store_append_receipt(
            result,
            MessageStore::get_max_phy_offset(self.store),
            MessageStore::get_confirm_offset(self.store),
        ))
    }
}

impl<S> MessageAppender<MessageExtBatch> for LegacyMessageStoreAdapter<'_, S>
where
    S: MessageStore,
{
    type Receipt = StoreAppendReceipt;
    type Error = Infallible;

    async fn append_message(&mut self, message: MessageExtBatch) -> Result<Self::Receipt, Self::Error> {
        let result = MessageStore::put_messages(self.store, message).await;
        Ok(store_append_receipt(
            result,
            MessageStore::get_max_phy_offset(self.store),
            MessageStore::get_confirm_offset(self.store),
        ))
    }
}

impl<S> MessageReader for LegacyMessageStoreAdapter<'_, S>
where
    S: MessageStore,
{
    type Request = MessageReadRequest;
    type Output = Option<MessageReadResult>;
    type Error = StoreError;

    async fn read(&self, request: Self::Request) -> Result<Self::Output, Self::Error> {
        MessageStoreReadCapability::new(&*self.store).read(request).await
    }
}

impl<S> StoreHealth for LegacyMessageStoreAdapter<'_, S>
where
    S: MessageStore,
{
    type Snapshot = StoreHealthSnapshot;

    fn health_snapshot(&self) -> Self::Snapshot {
        MessageStoreHealthCapability::new(&*self.store).health_snapshot()
    }
}

impl<S> OffsetIndex for LegacyMessageStoreAdapter<'_, S>
where
    S: MessageStore,
{
    type Query = LegacyOffsetQuery;
    type Output = LegacyOffsetRange;
    type Error = Infallible;

    fn query_offset(&self, query: &Self::Query) -> Result<Self::Output, Self::Error> {
        Ok(LegacyOffsetRange {
            minimum: MessageStore::get_min_offset_in_queue(&*self.store, &query.topic, query.queue_id),
            maximum: MessageStore::get_max_offset_in_queue(&*self.store, &query.topic, query.queue_id),
        })
    }
}

impl<S> StoreLifecycle for LegacyMessageStoreAdapter<'_, S>
where
    S: MessageStore,
{
    type Error = StoreError;

    async fn load(&mut self) -> Result<bool, Self::Error> {
        Ok(MessageStore::load(self.store).await)
    }

    async fn start(&mut self) -> Result<(), Self::Error> {
        MessageStore::start(self.store).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        MessageStore::shutdown_gracefully(self.store).await.map(|_| ())
    }
}
