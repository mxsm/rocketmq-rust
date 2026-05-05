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

use bytes::Bytes;
use rocketmq_error::RocketMQError;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TieredGetMessageStatus {
    Found,
    NoMatchedMessage,
    OffsetFoundNull,
    OffsetOverflowBadly,
    OffsetOverflowOne,
    OffsetTooSmall,
    #[default]
    NoMatchedLogicQueue,
}

#[derive(Debug, Default)]
pub struct TieredGetMessageResult {
    pub status: TieredGetMessageStatus,
    pub messages: Vec<Bytes>,
    pub min_offset: i64,
    pub max_offset: i64,
    pub next_begin_offset: i64,
}

#[derive(Debug, Default)]
pub struct TieredQueryResult<T> {
    pub values: Vec<T>,
}

#[allow(async_fn_in_trait)]
pub trait TieredMessageFetcher: Send + Sync {
    async fn get_message(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
        max_msg_nums: i32,
    ) -> Result<TieredGetMessageResult, RocketMQError>;

    async fn get_message_timestamp(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
    ) -> Result<i64, RocketMQError>;

    async fn get_offset_by_time(
        &self,
        topic: String,
        queue_id: i32,
        timestamp_millis: i64,
    ) -> Result<i64, RocketMQError>;

    async fn query_message(
        &self,
        topic: String,
        key: String,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<TieredQueryResult<Bytes>, RocketMQError>;
}

pub struct DefaultTieredMessageFetcher<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
}

impl<P> DefaultTieredMessageFetcher<P>
where
    P: TieredStoreProvider,
{
    pub fn new(config: Arc<TieredStoreConfig>, flat_file_store: Arc<TieredFlatFileStore<P>>) -> Self {
        Self {
            config,
            flat_file_store,
        }
    }
}

impl<P> TieredMessageFetcher for DefaultTieredMessageFetcher<P>
where
    P: TieredStoreProvider,
{
    async fn get_message(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
        max_msg_nums: i32,
    ) -> Result<TieredGetMessageResult, RocketMQError> {
        let Some(_flat_file) = self.flat_file_store.get(&topic, queue_id) else {
            return Ok(TieredGetMessageResult {
                status: TieredGetMessageStatus::NoMatchedLogicQueue,
                ..TieredGetMessageResult::default()
            });
        };

        let _ = queue_offset;
        let _ = max_msg_nums;
        let _ = &self.config;
        Ok(TieredGetMessageResult {
            status: TieredGetMessageStatus::NoMatchedMessage,
            ..TieredGetMessageResult::default()
        })
    }

    async fn get_message_timestamp(
        &self,
        topic: String,
        queue_id: i32,
        queue_offset: i64,
    ) -> Result<i64, RocketMQError> {
        let _ = topic;
        let _ = queue_id;
        let _ = queue_offset;
        Ok(-1)
    }

    async fn get_offset_by_time(
        &self,
        topic: String,
        queue_id: i32,
        timestamp_millis: i64,
    ) -> Result<i64, RocketMQError> {
        let _ = topic;
        let _ = queue_id;
        let _ = timestamp_millis;
        Ok(-1)
    }

    async fn query_message(
        &self,
        topic: String,
        key: String,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<TieredQueryResult<Bytes>, RocketMQError> {
        let _ = topic;
        let _ = key;
        let _ = max_num;
        let _ = begin;
        let _ = end;
        Ok(TieredQueryResult::default())
    }
}
