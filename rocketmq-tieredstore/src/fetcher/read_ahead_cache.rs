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
use std::collections::VecDeque;
use std::ops::Range;
use std::time::Duration;
use std::time::Instant;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;

use crate::file::FileSegment;
use crate::file::FileSegmentType;
use crate::file::TieredFileSegment;
use crate::provider::TieredStoreProvider;

pub(crate) const CONSUME_QUEUE_BLOCK_SIZE: usize = 64 * 1024;
pub(crate) const COMMIT_LOG_BLOCK_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ReadAheadCacheKey {
    path: String,
    generation: i64,
    segment_type: FileSegmentType,
    block_offset: u64,
}

struct ReadAheadCacheEntry {
    bytes: Bytes,
    last_access: Instant,
}

#[derive(Default)]
struct ReadAheadCacheState {
    entries: HashMap<ReadAheadCacheKey, ReadAheadCacheEntry>,
    lru: VecDeque<ReadAheadCacheKey>,
    bytes: usize,
}

pub(crate) struct ReadAheadCache {
    enabled: bool,
    max_bytes: usize,
    expire: Duration,
    state: Mutex<ReadAheadCacheState>,
}

impl ReadAheadCache {
    pub(crate) fn new(enabled: bool, max_bytes: usize, expire: Duration) -> Self {
        Self {
            enabled: enabled && max_bytes > 0 && !expire.is_zero(),
            max_bytes,
            expire,
            state: Mutex::new(ReadAheadCacheState::default()),
        }
    }

    pub(crate) async fn read<P>(
        &self,
        segment: &TieredFileSegment<P>,
        range: Range<u64>,
        block_size: usize,
    ) -> Result<Bytes, RocketMQError>
    where
        P: TieredStoreProvider,
    {
        if !self.enabled || block_size == 0 {
            return segment.read(range).await;
        }

        let commit_position = segment.commit_position();
        if range.start >= commit_position || range.end <= range.start {
            return segment.read(range).await;
        }
        let requested_end = range.end.min(commit_position);
        let block_size = block_size as u64;
        let metadata = segment.metadata();
        let mut chunks = Vec::new();
        let mut block_offset = range.start / block_size * block_size;
        while block_offset < requested_end {
            let block_end = block_offset.saturating_add(block_size).min(commit_position);
            let key = ReadAheadCacheKey {
                path: metadata.path.clone(),
                generation: metadata.create_timestamp,
                segment_type: metadata.segment_type,
                block_offset,
            };
            let required_len =
                usize::try_from(requested_end.min(block_end).saturating_sub(block_offset)).unwrap_or(usize::MAX);
            let block = match self.get(&key, required_len) {
                Some(bytes) => bytes,
                None => {
                    let bytes = segment.read(block_offset..block_end).await?;
                    self.insert(key, bytes.clone());
                    bytes
                }
            };
            let slice_start =
                usize::try_from(range.start.max(block_offset).saturating_sub(block_offset)).unwrap_or(block.len());
            let slice_end =
                usize::try_from(requested_end.min(block_end).saturating_sub(block_offset)).unwrap_or(block.len());
            if slice_start > slice_end || slice_end > block.len() {
                return Err(RocketMQError::storage_read_failed(
                    metadata.path.clone(),
                    "tiered read-ahead block is shorter than the committed range",
                ));
            }
            chunks.push(block.slice(slice_start..slice_end));
            block_offset = block_end;
        }

        if chunks.len() == 1 {
            if let Some(chunk) = chunks.pop() {
                return Ok(chunk);
            }
        }
        let total_len = chunks.iter().map(Bytes::len).sum();
        let mut joined = BytesMut::with_capacity(total_len);
        for chunk in chunks {
            joined.put(chunk);
        }
        Ok(joined.freeze())
    }

    pub(crate) fn invalidate_path(&self, path: &str) {
        let mut state = self.state.lock();
        let keys = state
            .entries
            .keys()
            .filter(|key| key.path == path)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            Self::remove(&mut state, &key);
        }
    }

    pub(crate) fn clear(&self) {
        *self.state.lock() = ReadAheadCacheState::default();
    }

    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.state.lock().bytes
    }

    fn get(&self, key: &ReadAheadCacheKey, required_len: usize) -> Option<Bytes> {
        let mut state = self.state.lock();
        let now = Instant::now();
        let usable = state.entries.get(key).is_some_and(|entry| {
            now.duration_since(entry.last_access) <= self.expire && entry.bytes.len() >= required_len
        });
        if !usable {
            Self::remove(&mut state, key);
            return None;
        }
        let entry = state.entries.get_mut(key)?;
        entry.last_access = now;
        let bytes = entry.bytes.clone();
        state.lru.retain(|candidate| candidate != key);
        state.lru.push_back(key.clone());
        Some(bytes)
    }

    fn insert(&self, key: ReadAheadCacheKey, bytes: Bytes) {
        if bytes.is_empty() || bytes.len() > self.max_bytes {
            return;
        }
        let mut state = self.state.lock();
        Self::remove(&mut state, &key);
        while state.bytes.saturating_add(bytes.len()) > self.max_bytes {
            let Some(oldest) = state.lru.pop_front() else {
                break;
            };
            if let Some(entry) = state.entries.remove(&oldest) {
                state.bytes = state.bytes.saturating_sub(entry.bytes.len());
            }
        }
        state.bytes = state.bytes.saturating_add(bytes.len());
        state.entries.insert(
            key.clone(),
            ReadAheadCacheEntry {
                bytes,
                last_access: Instant::now(),
            },
        );
        state.lru.push_back(key);
    }

    fn remove(state: &mut ReadAheadCacheState, key: &ReadAheadCacheKey) {
        if let Some(entry) = state.entries.remove(key) {
            state.bytes = state.bytes.saturating_sub(entry.bytes.len());
        }
        state.lru.retain(|candidate| candidate != key);
    }
}

pub(crate) const fn block_size(segment_type: FileSegmentType) -> usize {
    match segment_type {
        FileSegmentType::CommitLog => COMMIT_LOG_BLOCK_SIZE,
        FileSegmentType::ConsumeQueue => CONSUME_QUEUE_BLOCK_SIZE,
        FileSegmentType::Index => CONSUME_QUEUE_BLOCK_SIZE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_evicts_by_total_bytes() {
        let cache = ReadAheadCache::new(true, 6, Duration::from_secs(10));
        let first = ReadAheadCacheKey {
            path: "topic/0/commitlog/0".to_owned(),
            generation: 1,
            segment_type: FileSegmentType::CommitLog,
            block_offset: 0,
        };
        let second = ReadAheadCacheKey {
            path: "topic/0/commitlog/4".to_owned(),
            generation: 2,
            segment_type: FileSegmentType::CommitLog,
            block_offset: 0,
        };

        cache.insert(first.clone(), Bytes::from_static(b"abcd"));
        cache.insert(second.clone(), Bytes::from_static(b"efgh"));

        assert!(cache.get(&first, 4).is_none());
        assert_eq!(cache.get(&second, 4), Some(Bytes::from_static(b"efgh")));
        assert_eq!(cache.retained_bytes(), 4);
    }

    #[test]
    fn cache_isolates_generations_and_invalidates_the_whole_segment_path() {
        let cache = ReadAheadCache::new(true, 8, Duration::from_secs(10));
        let first = ReadAheadCacheKey {
            path: "topic/0/commitlog/0".to_owned(),
            generation: 1,
            segment_type: FileSegmentType::CommitLog,
            block_offset: 0,
        };
        let second = ReadAheadCacheKey {
            generation: 2,
            ..first.clone()
        };

        cache.insert(first.clone(), Bytes::from_static(b"old!"));
        cache.insert(second.clone(), Bytes::from_static(b"new!"));

        assert_eq!(cache.get(&first, 4), Some(Bytes::from_static(b"old!")));
        assert_eq!(cache.get(&second, 4), Some(Bytes::from_static(b"new!")));
        assert_eq!(cache.retained_bytes(), 8);
        cache.invalidate_path(&first.path);
        assert_eq!(cache.retained_bytes(), 0);
    }
}
