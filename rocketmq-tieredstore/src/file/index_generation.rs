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

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use tokio::sync::Mutex;

use crate::error;
use crate::provider::TieredStoreProvider;

const CURRENT_MAGIC: u32 = 0x5449_4743;
const GENERATION_MAGIC: u32 = 0x5449_474D;
const FORMAT_VERSION: u16 = 1;
const CURRENT_SIZE: usize = 40;
const GENERATION_METADATA_SIZE: usize = 64;
const NO_GENERATION: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexGenerationMetadata {
    pub(crate) generation: u64,
    pub(crate) entry_count: u64,
    pub(crate) min_timestamp: i64,
    pub(crate) max_timestamp: i64,
    pub(crate) min_commit_log_offset: u64,
    pub(crate) max_commit_log_offset: u64,
    pub(crate) content_crc: u32,
}

impl IndexGenerationMetadata {
    pub(crate) fn empty(generation: u64) -> Self {
        Self {
            generation,
            entry_count: 0,
            min_timestamp: 0,
            max_timestamp: 0,
            min_commit_log_offset: 0,
            max_commit_log_offset: 0,
            content_crc: crc32(&[]),
        }
    }
}

#[derive(Debug)]
pub(crate) struct IndexGeneration {
    id: u64,
    path: String,
    metadata: Option<IndexGenerationMetadata>,
}

impl IndexGeneration {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn metadata(&self) -> Option<IndexGenerationMetadata> {
        self.metadata
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IndexGenerationLease {
    generation: Arc<IndexGeneration>,
}

impl IndexGenerationLease {
    pub(crate) fn id(&self) -> u64 {
        self.generation.id()
    }

    pub(crate) fn path(&self) -> &str {
        self.generation.path()
    }

    pub(crate) fn metadata(&self) -> Option<IndexGenerationMetadata> {
        self.generation.metadata()
    }
}

#[derive(Debug)]
struct IndexGenerationState {
    initialized: bool,
    current: Arc<IndexGeneration>,
    previous: Option<Arc<IndexGeneration>>,
    retired: Vec<Arc<IndexGeneration>>,
    next_generation: u64,
}

#[derive(Debug)]
pub(crate) struct IndexGenerationManager<P>
where
    P: TieredStoreProvider,
{
    directory: String,
    provider: P,
    state: RwLock<IndexGenerationState>,
    initialize_lock: Mutex<()>,
}

impl<P> IndexGenerationManager<P>
where
    P: TieredStoreProvider,
{
    pub(crate) fn new(directory: String, provider: P) -> Self {
        let legacy = Arc::new(IndexGeneration {
            id: 0,
            path: directory.clone(),
            metadata: None,
        });
        Self {
            directory,
            provider,
            state: RwLock::new(IndexGenerationState {
                initialized: false,
                current: legacy,
                previous: None,
                retired: Vec::new(),
                next_generation: 1,
            }),
            initialize_lock: Mutex::new(()),
        }
    }

    pub(crate) async fn current(&self) -> Result<IndexGenerationLease, RocketMQError> {
        self.ensure_initialized().await?;
        Ok(IndexGenerationLease {
            generation: self.state.read().current.clone(),
        })
    }

    pub(crate) async fn previous(&self) -> Result<Option<IndexGenerationLease>, RocketMQError> {
        self.ensure_initialized().await?;
        Ok(self
            .state
            .read()
            .previous
            .clone()
            .map(|generation| IndexGenerationLease { generation }))
    }

    pub(crate) async fn next_generation(&self) -> Result<u64, RocketMQError> {
        self.ensure_initialized().await?;
        Ok(self.state.read().next_generation)
    }

    pub(crate) fn temporary_path(&self, generation: u64) -> String {
        format!("{}/generations/gen-{generation}.tmp", self.directory)
    }

    pub(crate) fn generation_path(&self, generation: u64) -> String {
        if generation == 0 {
            self.directory.clone()
        } else {
            format!("{}/generations/gen-{generation}", self.directory)
        }
    }

    pub(crate) fn metadata_path(&self, generation_path: &str) -> String {
        format!("{generation_path}/GENERATION")
    }

    pub(crate) async fn publish(&self, metadata: IndexGenerationMetadata) -> Result<(), RocketMQError> {
        self.ensure_initialized().await?;
        let (old_current_id, next_generation) = {
            let state = self.state.read();
            if metadata.generation != state.next_generation {
                return Err(error::storage_write_failed(
                    self.current_path(),
                    format!(
                        "generation publication out of order: expected {}, got {}",
                        state.next_generation, metadata.generation
                    ),
                ));
            }
            (state.current.id(), state.next_generation.saturating_add(1))
        };
        let pointer = CurrentPointer {
            current: metadata.generation,
            previous: Some(old_current_id),
            next_generation,
        };
        self.provider
            .atomic_write(self.current_path(), encode_current(pointer))
            .await?;

        let mut state = self.state.write();
        let old_current = state.current.clone();
        if let Some(old_previous) = state.previous.replace(old_current) {
            state.retired.push(old_previous);
        }
        state.current = Arc::new(IndexGeneration {
            id: metadata.generation,
            path: self.generation_path(metadata.generation),
            metadata: Some(metadata),
        });
        state.next_generation = next_generation;
        Ok(())
    }

    pub(crate) async fn rollback_to_previous(&self) -> Result<bool, RocketMQError> {
        self.ensure_initialized().await?;
        let (current, previous, next_generation) = {
            let state = self.state.read();
            let Some(previous) = state.previous.clone() else {
                return Ok(false);
            };
            (state.current.clone(), previous, state.next_generation)
        };
        self.provider
            .atomic_write(
                self.current_path(),
                encode_current(CurrentPointer {
                    current: previous.id(),
                    previous: Some(current.id()),
                    next_generation,
                }),
            )
            .await?;
        let mut state = self.state.write();
        state.current = previous;
        state.previous = Some(current);
        Ok(true)
    }

    pub(crate) async fn rollback_to_validated_previous(&self) -> Result<bool, RocketMQError> {
        self.ensure_initialized().await?;
        let (current, previous, next_generation) = {
            let state = self.state.read();
            let Some(previous) = state.previous.clone() else {
                return Ok(false);
            };
            (state.current.clone(), previous, state.next_generation)
        };
        self.provider
            .atomic_write(
                self.current_path(),
                encode_current(CurrentPointer {
                    current: previous.id(),
                    previous: None,
                    next_generation,
                }),
            )
            .await?;
        let mut state = self.state.write();
        state.current = previous;
        state.previous = None;
        state.retired.push(current);
        Ok(true)
    }

    pub(crate) async fn cleanup_retired(&self) -> Result<(), RocketMQError> {
        let deletable = {
            let mut state = self.state.write();
            let mut deletable = Vec::new();
            state.retired.retain(|generation| {
                if Arc::strong_count(generation) == 1 {
                    if generation.id() > 0 {
                        deletable.push(generation.path().to_owned());
                    }
                    false
                } else {
                    true
                }
            });
            deletable
        };
        for path in deletable {
            self.provider.delete_prefix(path).await?;
        }
        Ok(())
    }

    async fn ensure_initialized(&self) -> Result<(), RocketMQError> {
        if self.state.read().initialized {
            return Ok(());
        }
        let _guard = self.initialize_lock.lock().await;
        if self.state.read().initialized {
            return Ok(());
        }

        let pointer = self.load_current_pointer().await?;
        let (current, previous, next_generation) = match pointer {
            Some(pointer) => {
                let current = self.load_generation(pointer.current).await?;
                let previous = match pointer.previous {
                    Some(generation) => Some(self.load_generation(generation).await?),
                    None => None,
                };
                (current, previous, pointer.next_generation)
            }
            None => (
                Arc::new(IndexGeneration {
                    id: 0,
                    path: self.directory.clone(),
                    metadata: None,
                }),
                None,
                1,
            ),
        };
        {
            let mut state = self.state.write();
            state.current = current;
            state.previous = previous;
            state.next_generation = next_generation.max(1);
            state.initialized = true;
        }
        self.cleanup_orphans_after_restart().await
    }

    async fn load_current_pointer(&self) -> Result<Option<CurrentPointer>, RocketMQError> {
        let path = self.current_path();
        let size = self.provider.segment_size(path.clone()).await?;
        if size == 0 {
            return Ok(None);
        }
        let bytes = self.provider.read(path.clone(), 0, size as usize).await?;
        decode_current(&bytes, &path).map(Some)
    }

    async fn load_generation(&self, generation: u64) -> Result<Arc<IndexGeneration>, RocketMQError> {
        if generation == 0 {
            return Ok(Arc::new(IndexGeneration {
                id: 0,
                path: self.directory.clone(),
                metadata: None,
            }));
        }
        let path = self.generation_path(generation);
        let metadata_path = self.metadata_path(&path);
        let size = self.provider.segment_size(metadata_path.clone()).await?;
        if size != GENERATION_METADATA_SIZE as u64 {
            return Err(error::storage_corrupted(metadata_path));
        }
        let bytes = self.provider.read(metadata_path.clone(), 0, size as usize).await?;
        let metadata = decode_generation_metadata(&bytes, &metadata_path)?;
        if metadata.generation != generation {
            return Err(error::storage_corrupted(metadata_path));
        }
        Ok(Arc::new(IndexGeneration {
            id: generation,
            path,
            metadata: Some(metadata),
        }))
    }

    async fn cleanup_orphans_after_restart(&self) -> Result<(), RocketMQError> {
        let prefix = format!("{}/generations", self.directory);
        let paths = self.provider.list(prefix.clone()).await?;
        if paths.is_empty() {
            return Ok(());
        }
        let (current, previous) = {
            let state = self.state.read();
            (
                state.current.id(),
                state.previous.as_ref().map(|generation| generation.id()),
            )
        };
        let mut generation_roots = BTreeSet::new();
        let marker = format!("{prefix}/");
        for path in paths {
            let Some(relative) = path.strip_prefix(&marker) else {
                continue;
            };
            let Some(component) = relative.split('/').next() else {
                continue;
            };
            if component.starts_with("gen-") {
                generation_roots.insert(format!("{prefix}/{component}"));
            }
        }
        for root in generation_roots {
            let keep = parse_generation_id(&root)
                .is_some_and(|generation| generation == current || Some(generation) == previous);
            if !keep {
                self.provider.delete_prefix(root).await?;
            }
        }
        Ok(())
    }

    fn current_path(&self) -> String {
        format!("{}/CURRENT", self.directory)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CurrentPointer {
    current: u64,
    previous: Option<u64>,
    next_generation: u64,
}

pub(crate) fn encode_generation_metadata(metadata: IndexGenerationMetadata) -> Bytes {
    let mut bytes = BytesMut::with_capacity(GENERATION_METADATA_SIZE);
    bytes.put_u32(GENERATION_MAGIC);
    bytes.put_u16(FORMAT_VERSION);
    bytes.put_u16(0);
    bytes.put_u64(metadata.generation);
    bytes.put_u64(metadata.entry_count);
    bytes.put_i64(metadata.min_timestamp);
    bytes.put_i64(metadata.max_timestamp);
    bytes.put_u64(metadata.min_commit_log_offset);
    bytes.put_u64(metadata.max_commit_log_offset);
    bytes.put_u32(metadata.content_crc);
    let checksum = crc32(&bytes);
    bytes.put_u32(checksum);
    bytes.freeze()
}

pub(crate) fn decode_generation_metadata(bytes: &Bytes, path: &str) -> Result<IndexGenerationMetadata, RocketMQError> {
    if bytes.len() != GENERATION_METADATA_SIZE {
        return Err(error::storage_corrupted(path));
    }
    let expected_checksum = u32::from_be_bytes(bytes[GENERATION_METADATA_SIZE - 4..].try_into().unwrap_or_default());
    if crc32(&bytes[..GENERATION_METADATA_SIZE - 4]) != expected_checksum {
        return Err(error::storage_corrupted(path));
    }
    let mut cursor = bytes.clone();
    if cursor.get_u32() != GENERATION_MAGIC || cursor.get_u16() != FORMAT_VERSION {
        return Err(error::storage_corrupted(path));
    }
    let _reserved = cursor.get_u16();
    let metadata = IndexGenerationMetadata {
        generation: cursor.get_u64(),
        entry_count: cursor.get_u64(),
        min_timestamp: cursor.get_i64(),
        max_timestamp: cursor.get_i64(),
        min_commit_log_offset: cursor.get_u64(),
        max_commit_log_offset: cursor.get_u64(),
        content_crc: cursor.get_u32(),
    };
    if metadata.entry_count == 0 {
        if metadata.min_timestamp != 0
            || metadata.max_timestamp != 0
            || metadata.min_commit_log_offset != 0
            || metadata.max_commit_log_offset != 0
        {
            return Err(error::storage_corrupted(path));
        }
    } else if metadata.min_timestamp > metadata.max_timestamp
        || metadata.min_commit_log_offset > metadata.max_commit_log_offset
    {
        return Err(error::storage_corrupted(path));
    }
    Ok(metadata)
}

fn encode_current(pointer: CurrentPointer) -> Bytes {
    let mut bytes = BytesMut::with_capacity(CURRENT_SIZE);
    bytes.put_u32(CURRENT_MAGIC);
    bytes.put_u16(FORMAT_VERSION);
    bytes.put_u16(0);
    bytes.put_u64(pointer.current);
    bytes.put_u64(pointer.previous.unwrap_or(NO_GENERATION));
    bytes.put_u64(pointer.next_generation);
    let checksum = crc32(&bytes);
    bytes.put_u32(checksum);
    bytes.put_u32(0);
    bytes.freeze()
}

fn decode_current(bytes: &Bytes, path: &str) -> Result<CurrentPointer, RocketMQError> {
    if bytes.len() != CURRENT_SIZE {
        return Err(error::storage_corrupted(path));
    }
    let expected_checksum = u32::from_be_bytes(bytes[32..36].try_into().unwrap_or_default());
    if crc32(&bytes[..32]) != expected_checksum {
        return Err(error::storage_corrupted(path));
    }
    let mut cursor = bytes.clone();
    if cursor.get_u32() != CURRENT_MAGIC || cursor.get_u16() != FORMAT_VERSION {
        return Err(error::storage_corrupted(path));
    }
    let _reserved = cursor.get_u16();
    let current = cursor.get_u64();
    let previous = match cursor.get_u64() {
        NO_GENERATION => None,
        generation => Some(generation),
    };
    let next_generation = cursor.get_u64();
    if next_generation <= current || previous == Some(current) {
        return Err(error::storage_corrupted(path));
    }
    Ok(CurrentPointer {
        current,
        previous,
        next_generation,
    })
}

fn parse_generation_id(path: &str) -> Option<u64> {
    let component = path.rsplit('/').next()?;
    if component.ends_with(".tmp") {
        return None;
    }
    component.strip_prefix("gen-")?.parse().ok()
}

pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_metadata_round_trips_and_rejects_corruption() {
        let metadata = IndexGenerationMetadata {
            generation: 7,
            entry_count: 3,
            min_timestamp: 10,
            max_timestamp: 30,
            min_commit_log_offset: 100,
            max_commit_log_offset: 300,
            content_crc: 42,
        };
        let encoded = encode_generation_metadata(metadata);
        assert_eq!(decode_generation_metadata(&encoded, "GENERATION").unwrap(), metadata);

        let mut corrupted = encoded.to_vec();
        corrupted[24] ^= 1;
        assert!(decode_generation_metadata(&Bytes::from(corrupted), "GENERATION").is_err());
    }

    #[test]
    fn current_pointer_round_trips_and_rejects_corruption() {
        let pointer = CurrentPointer {
            current: 8,
            previous: Some(7),
            next_generation: 9,
        };
        let encoded = encode_current(pointer);
        assert_eq!(decode_current(&encoded, "CURRENT").unwrap(), pointer);

        let mut corrupted = encoded.to_vec();
        corrupted[8] ^= 1;
        assert!(decode_current(&Bytes::from(corrupted), "CURRENT").is_err());
    }
}
