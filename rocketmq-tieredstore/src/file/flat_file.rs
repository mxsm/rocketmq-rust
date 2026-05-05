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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;

use crate::config::TieredStoreConfig;
use crate::error;
use crate::file::FileSegment;
use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::provider::TieredStoreProvider;

pub const CONSUME_QUEUE_UNIT_SIZE: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumeQueueUnit {
    pub commit_log_offset: i64,
    pub size: i32,
    pub tags_code: i64,
}

impl ConsumeQueueUnit {
    pub fn encode(self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(CONSUME_QUEUE_UNIT_SIZE);
        bytes.put_i64(self.commit_log_offset);
        bytes.put_i32(self.size);
        bytes.put_i64(self.tags_code);
        bytes.freeze()
    }
}

pub struct TieredFlatFile<P>
where
    P: TieredStoreProvider,
{
    topic: String,
    queue_id: i32,
    config: Arc<TieredStoreConfig>,
    provider: P,
    commit_log_segments: Mutex<Vec<Arc<TieredFileSegment<P>>>>,
    consume_queue_segments: Mutex<Vec<Arc<TieredFileSegment<P>>>>,
}

impl<P> TieredFlatFile<P>
where
    P: TieredStoreProvider,
{
    pub fn new(topic: String, queue_id: i32, config: Arc<TieredStoreConfig>, provider: P) -> Self {
        Self {
            topic,
            queue_id,
            config,
            provider,
            commit_log_segments: Mutex::new(Vec::new()),
            consume_queue_segments: Mutex::new(Vec::new()),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn queue_id(&self) -> i32 {
        self.queue_id
    }

    pub async fn append_commit_log(&self, message: Bytes, store_timestamp: i64) -> Result<u64, RocketMQError> {
        let segment = self
            .ensure_segment(FileSegmentType::CommitLog, self.config.commit_log_segment_size)
            .await?;
        let offset = segment.base_offset().saturating_add(segment.append_position());
        segment.append(message, store_timestamp).await?;
        Ok(offset)
    }

    pub async fn append_consume_queue(
        &self,
        unit: ConsumeQueueUnit,
        store_timestamp: i64,
    ) -> Result<(), RocketMQError> {
        let segment = self
            .ensure_segment(FileSegmentType::ConsumeQueue, self.config.consume_queue_segment_size)
            .await?;
        segment.append(unit.encode(), store_timestamp).await?;
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), RocketMQError> {
        let commit_log_segments = { self.commit_log_segments.lock().clone() };
        for segment in commit_log_segments {
            segment.commit().await?;
        }

        let consume_queue_segments = { self.consume_queue_segments.lock().clone() };
        for segment in consume_queue_segments {
            segment.commit().await?;
        }
        Ok(())
    }

    pub async fn recover(&self) -> Result<(), RocketMQError> {
        Ok(())
    }

    async fn ensure_segment(
        &self,
        segment_type: FileSegmentType,
        max_size: u64,
    ) -> Result<Arc<TieredFileSegment<P>>, RocketMQError> {
        let existing = match segment_type {
            FileSegmentType::CommitLog => self.commit_log_segments.lock().last().cloned(),
            FileSegmentType::ConsumeQueue => self.consume_queue_segments.lock().last().cloned(),
            FileSegmentType::Index => return Err(error::internal("index segment is managed by tiered index service")),
        };
        if let Some(segment) = existing {
            return Ok(segment);
        }

        let path = segment_path(&self.topic, self.queue_id, segment_type, 0);
        let segment = Arc::new(self.provider.create_segment(path, segment_type, 0, max_size).await?);
        match segment_type {
            FileSegmentType::CommitLog => {
                let mut segments = self.commit_log_segments.lock();
                if let Some(existing) = segments.last().cloned() {
                    return Ok(existing);
                }
                segments.push(segment.clone());
            }
            FileSegmentType::ConsumeQueue => {
                let mut segments = self.consume_queue_segments.lock();
                if let Some(existing) = segments.last().cloned() {
                    return Ok(existing);
                }
                segments.push(segment.clone());
            }
            FileSegmentType::Index => {}
        }
        Ok(segment)
    }
}

fn segment_path(topic: &str, queue_id: i32, segment_type: FileSegmentType, base_offset: u64) -> String {
    let segment_name = match segment_type {
        FileSegmentType::CommitLog => "commitlog",
        FileSegmentType::ConsumeQueue => "consumequeue",
        FileSegmentType::Index => "index",
    };
    format!("{topic}/{queue_id}/{segment_name}/{base_offset:020}")
}
