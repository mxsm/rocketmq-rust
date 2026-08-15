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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::utils::cleanup_policy_utils::get_delete_policy_arc_mut;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::select_result::SelectMappedBufferCacheState;
use crate::base::select_result::SelectMappedBufferResult;
use crate::kv::compaction_generation;
use crate::kv::compaction_generation::CompactionPayload;
use crate::kv::compaction_generation::CompactionQueueKey;
use crate::kv::compaction_generation::CompactionQueues;
use crate::kv::compaction_generation::CompactionRecord;
use crate::kv::compaction_generation::CurrentPointer;
use crate::kv::compaction_generation::GenerationMetadata;
use crate::log_file::mapped_file::MappedFile;
use crate::runtime::StoreRuntimeScope;

type PayloadResolver = dyn Fn(i64, i32) -> Option<Bytes> + Send + Sync;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CompactionReadState {
    Ready {
        generation: u64,
        durable_watermark: i64,
    },
    Recovering {
        generation: u64,
        durable_watermark: i64,
        required_wal_position: i64,
    },
}

#[derive(Debug)]
struct CompactionGeneration {
    metadata: GenerationMetadata,
    queues: CompactionQueues,
}

impl CompactionGeneration {
    fn empty() -> Self {
        Self {
            metadata: compaction_generation::empty_metadata(0),
            queues: CompactionQueues::new(),
        }
    }
}

#[derive(Debug)]
struct CompactionState {
    current: Arc<CompactionGeneration>,
    previous: Option<Arc<CompactionGeneration>>,
    retired: Vec<Arc<CompactionGeneration>>,
    delta: CompactionQueues,
    next_generation: u64,
    read_state: CompactionReadState,
    corrupted: bool,
    inactive_topics: HashSet<CheetahString>,
}

pub struct CompactionStore {
    root: Option<PathBuf>,
    runtime_scope: Option<StoreRuntimeScope>,
    state: RwLock<CompactionState>,
    payload_resolver: RwLock<Option<Arc<PayloadResolver>>>,
    generation_lock: tokio::sync::Mutex<()>,
    #[cfg(test)]
    fault_stage: std::sync::atomic::AtomicU8,
}

impl Default for CompactionStore {
    fn default() -> Self {
        let current = Arc::new(CompactionGeneration::empty());
        Self {
            root: None,
            runtime_scope: None,
            state: RwLock::new(CompactionState {
                current,
                previous: None,
                retired: Vec::new(),
                delta: CompactionQueues::new(),
                next_generation: 1,
                read_state: CompactionReadState::Ready {
                    generation: 0,
                    durable_watermark: 0,
                },
                corrupted: false,
                inactive_topics: HashSet::new(),
            }),
            payload_resolver: RwLock::new(None),
            generation_lock: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            fault_stage: std::sync::atomic::AtomicU8::new(0),
        }
    }
}

impl CompactionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_root(root: PathBuf, runtime_scope: StoreRuntimeScope) -> Self {
        let store = Self {
            root: Some(root),
            runtime_scope: Some(runtime_scope),
            ..Self::default()
        };
        store.state.write().read_state = CompactionReadState::Recovering {
            generation: 0,
            durable_watermark: 0,
            required_wal_position: 0,
        };
        store
    }

    pub(crate) fn set_payload_resolver<F>(&self, resolver: F)
    where
        F: Fn(i64, i32) -> Option<Bytes> + Send + Sync + 'static,
    {
        *self.payload_resolver.write() = Some(Arc::new(resolver));
    }

    pub(crate) async fn load(&self) -> Result<(), RocketMQError> {
        let Some(root) = self.root.clone() else {
            return Ok(());
        };
        let runtime_scope = self.runtime_scope()?;
        let _generation_guard = self.generation_lock.lock().await;
        let Some(pointer) = compaction_generation::read_current(runtime_scope, root.clone()).await? else {
            compaction_generation::cleanup_orphans(runtime_scope, root, 0, None).await?;
            return Ok(());
        };

        let (current, previous, effective_pointer) =
            match compaction_generation::load_generation(runtime_scope, root.clone(), pointer.current).await {
                Ok(current) => {
                    let previous = match pointer.previous {
                        Some(generation) => {
                            compaction_generation::load_generation(runtime_scope, root.clone(), generation)
                                .await
                                .ok()
                                .map(Self::generation_from_loaded)
                        }
                        None => None,
                    };
                    let effective_pointer = CurrentPointer {
                        current: pointer.current,
                        previous: previous.as_ref().map(|generation| generation.metadata.generation),
                        next_generation: pointer.next_generation,
                    };
                    if effective_pointer != pointer {
                        compaction_generation::write_current(runtime_scope, root.clone(), effective_pointer).await?;
                    }
                    (Self::generation_from_loaded(current), previous, effective_pointer)
                }
                Err(current_error) => {
                    let Some(previous_id) = pointer.previous else {
                        return Err(current_error);
                    };
                    let previous =
                        compaction_generation::load_generation(runtime_scope, root.clone(), previous_id).await?;
                    let rollback = CurrentPointer {
                        current: previous_id,
                        previous: None,
                        next_generation: pointer.next_generation,
                    };
                    compaction_generation::write_current(runtime_scope, root.clone(), rollback).await?;
                    (Self::generation_from_loaded(previous), None, rollback)
                }
            };

        let durable_watermark = current.metadata.max_physical_offset;
        {
            let mut state = self.state.write();
            state.current = current;
            state.previous = previous;
            state.retired.clear();
            state.delta.clear();
            state.next_generation = effective_pointer
                .next_generation
                .max(effective_pointer.current.saturating_add(1));
            state.read_state = CompactionReadState::Recovering {
                generation: effective_pointer.current,
                durable_watermark,
                required_wal_position: durable_watermark,
            };
            state.corrupted = false;
            state.inactive_topics.clear();
        }
        compaction_generation::cleanup_orphans(
            runtime_scope,
            root,
            effective_pointer.current,
            effective_pointer.previous,
        )
        .await
    }

    pub(crate) fn begin_recovery(&self, required_wal_position: i64) {
        let mut state = self.state.write();
        state.read_state = CompactionReadState::Recovering {
            generation: state.current.metadata.generation,
            durable_watermark: state.current.metadata.max_physical_offset,
            required_wal_position: required_wal_position.max(0),
        };
    }

    pub(crate) fn finish_recovery(&self, recovered_wal_position: i64) {
        let mut state = self.state.write();
        let delta_watermark = state
            .delta
            .values()
            .flat_map(BTreeMap::values)
            .map(CompactionRecord::end_physical_offset)
            .max()
            .unwrap_or_default();
        state.read_state = CompactionReadState::Ready {
            generation: state.current.metadata.generation,
            durable_watermark: state
                .current
                .metadata
                .max_physical_offset
                .max(delta_watermark)
                .max(recovered_wal_position.max(0)),
        };
    }

    pub(crate) fn read_state(&self) -> CompactionReadState {
        self.state.read().read_state.clone()
    }

    pub(crate) fn durable_dispatch_offset(&self, commit_log_min_offset: i64) -> i64 {
        self.state
            .read()
            .current
            .metadata
            .max_physical_offset
            .max(commit_log_min_offset)
    }

    pub(crate) fn reconcile_topic_policies(
        &self,
        topic_config_table: &dashmap::DashMap<CheetahString, Arc<TopicConfig>>,
    ) {
        let mut state = self.state.write();
        let topics = state
            .current
            .queues
            .keys()
            .chain(state.delta.keys())
            .map(|key| key.topic.clone())
            .collect::<HashSet<_>>();
        for topic in topics {
            let is_compaction = topic_config_table
                .get(&topic)
                .is_some_and(|config| get_delete_policy_arc_mut(Some(config.value())) == CleanupPolicy::COMPACTION);
            if !is_compaction {
                state.inactive_topics.insert(topic);
            }
        }
    }

    pub(crate) fn deactivate_topics(&self, topics: &[CheetahString]) {
        self.state.write().inactive_topics.extend(topics.iter().cloned());
    }

    pub fn put_message(&self, topic: &CheetahString, queue_id: i32, queue_offset: i64, batch_num: i32, payload: Bytes) {
        self.put_message_with_key(topic, queue_id, queue_offset, batch_num, None, payload);
    }

    pub(crate) fn put_dispatch_message(&self, dispatch_request: &DispatchRequest) {
        let key = canonical_key(Some(dispatch_request.keys.clone()));
        let Some(key) = key else {
            tracing::warn!(topic = %dispatch_request.topic, "compaction projection rejected a message without a non-blank key");
            return;
        };
        self.put_reference(
            &dispatch_request.topic,
            dispatch_request.queue_id,
            dispatch_request.consume_queue_offset,
            dispatch_request.batch_size as i32,
            Some(key),
            dispatch_request.commit_log_offset,
            dispatch_request.msg_size,
            dispatch_request.body_size == 0,
        );
    }

    pub(crate) fn put_message_with_key(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
        batch_num: i32,
        message_key: Option<CheetahString>,
        payload: Bytes,
    ) {
        let Some(message_key) = canonical_key(message_key) else {
            return;
        };
        if queue_offset < 0 {
            return;
        }
        let source_size = payload.len() as i32;
        let tombstone = payload.is_empty();
        self.insert_record(
            topic,
            queue_id,
            CompactionRecord {
                queue_offset,
                batch_num: batch_num.max(1),
                key: Some(message_key),
                source_physical_offset: queue_offset,
                source_size,
                tombstone,
                payload: CompactionPayload::Inline(payload),
            },
        );
    }

    fn put_reference(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
        batch_num: i32,
        message_key: Option<CheetahString>,
        physical_offset: i64,
        size: i32,
        tombstone: bool,
    ) {
        if queue_offset < 0 || physical_offset < 0 || size <= 0 {
            return;
        }
        self.insert_record(
            topic,
            queue_id,
            CompactionRecord {
                queue_offset,
                batch_num: batch_num.max(1),
                key: message_key,
                source_physical_offset: physical_offset,
                source_size: size,
                tombstone,
                payload: CompactionPayload::CommitLog,
            },
        );
    }

    fn insert_record(&self, topic: &CheetahString, queue_id: i32, record: CompactionRecord) {
        let mut state = self.state.write();
        if state.inactive_topics.remove(topic) {
            let mut queues = state.current.queues.clone();
            queues.retain(|key, _| key.topic != *topic);
            state.current = Arc::new(CompactionGeneration {
                metadata: state.current.metadata,
                queues,
            });
            state.delta.retain(|key, _| key.topic != *topic);
        }
        let queue_key = CompactionQueueKey::new(topic, queue_id);
        let existing = state
            .delta
            .get(&queue_key)
            .and_then(|queue| queue.get(&record.queue_offset))
            .or_else(|| {
                state
                    .current
                    .queues
                    .get(&queue_key)
                    .and_then(|queue| queue.get(&record.queue_offset))
            });
        if let Some(existing) = existing {
            if !same_record_identity(existing, &record) {
                state.corrupted = true;
                tracing::error!(topic = %topic, queue_id, queue_offset = record.queue_offset, "conflicting compaction replay detected");
            }
            return;
        }
        state
            .delta
            .entry(queue_key)
            .or_default()
            .insert(record.queue_offset, record);
    }

    pub fn dispatch_from_commit_log<MF: MappedFile>(
        &self,
        dispatch_request: &DispatchRequest,
        commit_log_file: &MF,
    ) -> bool {
        if !dispatch_request.success
            || dispatch_request.msg_size <= 0
            || dispatch_request.commit_log_offset < 0
            || dispatch_request.consume_queue_offset < 0
        {
            return false;
        }
        let file_from_offset = commit_log_file.get_file_from_offset() as i64;
        if dispatch_request.commit_log_offset < file_from_offset {
            return false;
        }
        let pos = (dispatch_request.commit_log_offset - file_from_offset) as usize;
        if commit_log_file
            .get_data(pos, dispatch_request.msg_size as usize)
            .is_none()
        {
            return false;
        }
        self.put_dispatch_message(dispatch_request);
        true
    }

    pub async fn compact_once(&self) -> Result<usize, RocketMQError> {
        let _generation_guard = self.generation_lock.lock().await;
        self.cleanup_retired().await?;
        let (base_generation, delta_snapshot, inactive_snapshot, merged, generation, before) = {
            let state = self.state.read();
            if state.corrupted {
                return Err(RocketMQError::StorageCorrupted {
                    path: self.root_display(),
                });
            }
            if state.delta.is_empty() && state.inactive_topics.is_empty() {
                return Ok(0);
            }
            let mut source = merged_queues(&state.current.queues, &state.delta);
            let before = merged_count(&source);
            source.retain(|key, _| !state.inactive_topics.contains(&key.topic));
            (
                state.current.metadata.generation,
                state.delta.clone(),
                state.inactive_topics.clone(),
                compact_queues(source),
                state.next_generation,
                before,
            )
        };
        let removed = before.saturating_sub(merged_count(&merged));

        self.fail_if(FaultStage::Build)?;
        let (metadata, published_queues) = if let Some(root) = self.root.clone() {
            let runtime_scope = self.runtime_scope()?;
            let materialized = self.materialize_queues(&merged).await?;
            let metadata = compaction_generation::write_temporary_generation(
                runtime_scope,
                root.clone(),
                generation,
                materialized,
            )
            .await?;
            self.fail_if(FaultStage::Sync)?;
            compaction_generation::sync_temporary_generation(runtime_scope, root.clone(), generation).await?;
            compaction_generation::validate_temporary_generation(runtime_scope, root.clone(), generation, metadata)
                .await?;
            self.fail_if(FaultStage::Rename)?;
            compaction_generation::rename_generation(runtime_scope, root.clone(), generation).await?;
            let loaded = compaction_generation::load_generation(runtime_scope, root.clone(), generation).await?;
            self.fail_if(FaultStage::Current)?;
            compaction_generation::write_current(
                runtime_scope,
                root,
                CurrentPointer {
                    current: generation,
                    previous: Some(base_generation),
                    next_generation: generation.saturating_add(1),
                },
            )
            .await?;
            (loaded.metadata, loaded.queues)
        } else {
            (metadata_for_memory(generation, &merged), merged)
        };

        {
            let mut state = self.state.write();
            if state.current.metadata.generation != base_generation {
                return Err(RocketMQError::storage_write_failed(
                    self.root_display(),
                    "compaction generation changed during publication",
                ));
            }
            let old_current = state.current.clone();
            if let Some(old_previous) = state.previous.replace(old_current) {
                state.retired.push(old_previous);
            }
            state.current = Arc::new(CompactionGeneration {
                metadata,
                queues: published_queues,
            });
            remove_snapshot_from_delta(&mut state.delta, &delta_snapshot);
            state.inactive_topics.retain(|topic| !inactive_snapshot.contains(topic));
            state.next_generation = generation.saturating_add(1);
            state.read_state = match state.read_state {
                CompactionReadState::Ready { durable_watermark, .. } => CompactionReadState::Ready {
                    generation,
                    durable_watermark: durable_watermark.max(metadata.max_physical_offset),
                },
                CompactionReadState::Recovering {
                    required_wal_position, ..
                } => CompactionReadState::Recovering {
                    generation,
                    durable_watermark: metadata.max_physical_offset,
                    required_wal_position,
                },
            };
        }
        self.fail_if(FaultStage::Cleanup)?;
        self.cleanup_retired().await?;
        Ok(removed)
    }

    pub(crate) async fn rollback_to_previous_generation(&self) -> Result<bool, RocketMQError> {
        let _generation_guard = self.generation_lock.lock().await;
        let (current, previous, next_generation) = {
            let state = self.state.read();
            let Some(previous) = state.previous.clone() else {
                return Ok(false);
            };
            (state.current.clone(), previous, state.next_generation)
        };
        if let Some(root) = self.root.clone() {
            compaction_generation::write_current(
                self.runtime_scope()?,
                root,
                CurrentPointer {
                    current: previous.metadata.generation,
                    previous: Some(current.metadata.generation),
                    next_generation,
                },
            )
            .await?;
        }
        let mut state = self.state.write();
        state.current = previous;
        state.previous = Some(current);
        state.read_state = CompactionReadState::Recovering {
            generation: state.current.metadata.generation,
            durable_watermark: state.current.metadata.max_physical_offset,
            required_wal_position: state.current.metadata.max_physical_offset,
        };
        Ok(true)
    }

    async fn materialize_queues(&self, queues: &CompactionQueues) -> Result<CompactionQueues, RocketMQError> {
        let mut materialized = queues.clone();
        for queue in materialized.values_mut() {
            for record in queue.values_mut() {
                if record.tombstone && record.source_size == 0 {
                    record.payload = CompactionPayload::Inline(Bytes::new());
                    continue;
                }
                let Some(payload) = self.resolve_payload(record).await? else {
                    return Err(RocketMQError::storage_read_failed(
                        self.root_display(),
                        format!(
                            "compaction source payload is unavailable at physical offset {}",
                            record.source_physical_offset
                        ),
                    ));
                };
                if payload.len() != record.source_size as usize {
                    return Err(RocketMQError::StorageCorrupted {
                        path: self.root_display(),
                    });
                }
                record.payload = CompactionPayload::Inline(payload);
            }
        }
        Ok(materialized)
    }

    async fn cleanup_retired(&self) -> Result<(), RocketMQError> {
        let deletable = {
            let state = self.state.read();
            state
                .retired
                .iter()
                .filter(|generation| Arc::strong_count(generation) == 1)
                .map(|generation| generation.metadata.generation)
                .filter(|generation| *generation > 0)
                .collect::<Vec<_>>()
        };
        if let Some(root) = self.root.clone() {
            let runtime_scope = self.runtime_scope()?;
            for generation in &deletable {
                compaction_generation::delete_generation(runtime_scope, root.clone(), *generation).await?;
            }
        }
        if !deletable.is_empty() {
            self.state
                .write()
                .retired
                .retain(|generation| !deletable.contains(&generation.metadata.generation));
        }
        Ok(())
    }

    pub async fn get_message(
        &self,
        _group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
    ) -> Option<GetMessageResult> {
        let (current_lease, delta, read_state, corrupted, inactive) = {
            let state = self.state.read();
            (
                state.current.clone(),
                state
                    .delta
                    .get(&CompactionQueueKey::new(topic, queue_id))
                    .cloned()
                    .unwrap_or_default(),
                state.read_state.clone(),
                state.corrupted,
                state.inactive_topics.contains(topic),
            )
        };
        if corrupted || matches!(read_state, CompactionReadState::Recovering { .. }) {
            // Preserve the stable Java-compatible public enum while making the internal state explicit.
            return Some(status_result(GetMessageStatus::MessageWasRemoving, offset, 0, 0));
        }
        if inactive {
            return Some(status_result(GetMessageStatus::NoMatchedLogicQueue, 0, 0, 0));
        }

        let key = CompactionQueueKey::new(topic, queue_id);
        let mut queue = current_lease.queues.get(&key).cloned().unwrap_or_default();
        queue.extend(delta);
        let compacted_queue = compact_queue(queue);
        let queue = compacted_queue
            .iter()
            .filter(|(_, record)| !record.tombstone)
            .map(|(offset, record)| (*offset, record.clone()))
            .collect::<BTreeMap<_, _>>();
        if queue.is_empty() {
            return Some(status_result(GetMessageStatus::NoMatchedLogicQueue, 0, 0, 0));
        }
        let min_offset = *queue.keys().next().unwrap_or(&0);
        let max_offset = compacted_queue
            .values()
            .map(|message| message.queue_offset + i64::from(message.batch_num))
            .max()
            .unwrap_or(min_offset);
        if offset < min_offset {
            return Some(status_result(
                GetMessageStatus::OffsetTooSmall,
                min_offset,
                min_offset,
                max_offset,
            ));
        }
        if offset == max_offset {
            return Some(status_result(
                GetMessageStatus::OffsetOverflowOne,
                offset,
                min_offset,
                max_offset,
            ));
        }
        if offset > max_offset {
            return Some(status_result(
                GetMessageStatus::OffsetOverflowBadly,
                max_offset,
                min_offset,
                max_offset,
            ));
        }

        let mut result = GetMessageResult::new_result_size(max_msg_nums.max(1) as usize);
        let max_msg_nums = max_msg_nums.max(1);
        let max_total_msg_size = max_total_msg_size.max(1);
        let mut next_begin_offset = offset;
        for (&queue_offset, message) in queue.range(offset..) {
            if result.message_count() >= max_msg_nums {
                break;
            }
            let payload = match self.resolve_payload(message).await {
                Ok(Some(payload)) => payload,
                Ok(None) => {
                    tracing::warn!(
                        physical_offset = message.source_physical_offset,
                        "compaction payload is unavailable at its exact location"
                    );
                    return Some(status_result(
                        GetMessageStatus::OffsetFoundNull,
                        offset,
                        min_offset,
                        max_offset,
                    ));
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        generation = current_lease.metadata.generation,
                        "compaction generation payload read failed"
                    );
                    return Some(status_result(
                        GetMessageStatus::OffsetFoundNull,
                        offset,
                        min_offset,
                        max_offset,
                    ));
                }
            };
            let Ok(payload_size) = i32::try_from(payload.len()) else {
                tracing::warn!(
                    physical_offset = message.source_physical_offset,
                    payload_size = payload.len(),
                    "compaction payload exceeds the selected-buffer size limit"
                );
                return Some(status_result(
                    GetMessageStatus::OffsetFoundNull,
                    offset,
                    min_offset,
                    max_offset,
                ));
            };
            if result.buffer_total_size() > 0 && result.buffer_total_size() + payload_size > max_total_msg_size {
                break;
            }
            let physical_offset = message.physical_offset();
            let Some(selected) = SelectMappedBufferResult::from_bytes_with_metadata(
                physical_offset.max(0) as u64,
                physical_offset.max(0) as u64,
                payload,
                true,
                SelectMappedBufferCacheState::Unknown,
            ) else {
                return Some(status_result(
                    GetMessageStatus::OffsetFoundNull,
                    offset,
                    min_offset,
                    max_offset,
                ));
            };
            result.add_message(selected, queue_offset as u64, message.batch_num);
            next_begin_offset = message.queue_offset + i64::from(message.batch_num);
        }
        if result.message_count() == 0 {
            return Some(status_result(
                GetMessageStatus::NoMatchedMessage,
                max_offset,
                min_offset,
                max_offset,
            ));
        }
        result.set_status(Some(GetMessageStatus::Found));
        result.set_next_begin_offset(next_begin_offset);
        result.set_min_offset(min_offset);
        result.set_max_offset(max_offset);
        Some(result)
    }

    async fn resolve_payload(&self, message: &CompactionRecord) -> Result<Option<Bytes>, RocketMQError> {
        match &message.payload {
            CompactionPayload::Inline(payload) => Ok(Some(payload.clone())),
            CompactionPayload::CommitLog => Ok(self
                .payload_resolver
                .read()
                .as_ref()
                .and_then(|resolver| resolver(message.source_physical_offset, message.source_size))),
            CompactionPayload::Generation {
                generation,
                position,
                size,
            } => {
                let Some(root) = self.root.clone() else {
                    return Ok(None);
                };
                compaction_generation::read_generation_payload(
                    self.runtime_scope()?,
                    root,
                    *generation,
                    *position,
                    *size,
                )
                .await
                .map(Some)
            }
        }
    }

    fn runtime_scope(&self) -> Result<&StoreRuntimeScope, RocketMQError> {
        self.runtime_scope.as_ref().ok_or_else(|| {
            RocketMQError::storage_write_failed(
                self.root_display(),
                "durable compaction store requires a service runtime scope",
            )
        })
    }

    pub fn message_count(&self, topic: &CheetahString, queue_id: i32) -> usize {
        let state = self.state.read();
        if state.inactive_topics.contains(topic) {
            return 0;
        }
        let key = CompactionQueueKey::new(topic, queue_id);
        let mut queue = state.current.queues.get(&key).cloned().unwrap_or_default();
        if let Some(delta) = state.delta.get(&key) {
            queue.extend(delta.clone());
        }
        if state.corrupted {
            return 0;
        }
        visible_queue(queue).len()
    }

    pub(crate) fn current_generation_id(&self) -> u64 {
        self.state.read().current.metadata.generation
    }

    fn generation_from_loaded(loaded: compaction_generation::LoadedGeneration) -> Arc<CompactionGeneration> {
        Arc::new(CompactionGeneration {
            metadata: loaded.metadata,
            queues: loaded.queues,
        })
    }

    fn root_display(&self) -> String {
        self.root
            .as_ref()
            .map(|root| root.to_string_lossy().into_owned())
            .unwrap_or_else(|| "memory-compaction".to_owned())
    }

    #[cfg(test)]
    fn set_fault(&self, stage: FaultStage) {
        self.fault_stage
            .store(stage as u8, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    fn current_lease(&self) -> Arc<CompactionGeneration> {
        self.state.read().current.clone()
    }

    #[cfg(test)]
    fn fail_if(&self, stage: FaultStage) -> Result<(), RocketMQError> {
        if self
            .fault_stage
            .compare_exchange(
                stage as u8,
                0,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            return Err(RocketMQError::storage_write_failed(
                self.root_display(),
                format!("injected compaction {stage:?} failure"),
            ));
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn fail_if(&self, _stage: FaultStage) -> Result<(), RocketMQError> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
enum FaultStage {
    Build = 1,
    Sync = 2,
    Rename = 3,
    Current = 4,
    Cleanup = 5,
}

fn merged_queues(current: &CompactionQueues, delta: &CompactionQueues) -> CompactionQueues {
    let mut merged = current.clone();
    for (key, queue) in delta {
        merged.entry(key.clone()).or_default().extend(queue.clone());
    }
    merged
}

fn compact_queues(mut queues: CompactionQueues) -> CompactionQueues {
    queues.retain(|_, queue| {
        *queue = compact_queue(std::mem::take(queue));
        !queue.is_empty()
    });
    queues
}

fn compact_queue(mut queue: BTreeMap<i64, CompactionRecord>) -> BTreeMap<i64, CompactionRecord> {
    let mut latest = HashMap::<CheetahString, i64>::new();
    for (&queue_offset, record) in &queue {
        let Some(key) = record.key.as_ref() else {
            continue;
        };
        latest
            .entry(key.clone())
            .and_modify(|current| *current = (*current).max(queue_offset))
            .or_insert(queue_offset);
    }
    queue.retain(|queue_offset, record| {
        record
            .key
            .as_ref()
            .is_some_and(|key| latest.get(key).is_some_and(|latest| latest == queue_offset))
    });
    queue
}

fn visible_queue(queue: BTreeMap<i64, CompactionRecord>) -> BTreeMap<i64, CompactionRecord> {
    compact_queue(queue)
        .into_iter()
        .filter(|(_, record)| !record.tombstone)
        .collect()
}

fn canonical_key(key: Option<CheetahString>) -> Option<CheetahString> {
    key.filter(|key| !key.is_empty() && !key.chars().all(char::is_whitespace))
}

fn merged_count(queues: &CompactionQueues) -> usize {
    queues.values().map(BTreeMap::len).sum()
}

fn remove_snapshot_from_delta(delta: &mut CompactionQueues, snapshot: &CompactionQueues) {
    delta.retain(|queue_key, queue| {
        if let Some(included) = snapshot.get(queue_key) {
            queue.retain(|offset, record| {
                !included
                    .get(offset)
                    .is_some_and(|included| same_record_identity(record, included))
            });
        }
        !queue.is_empty()
    });
}

fn same_record_identity(left: &CompactionRecord, right: &CompactionRecord) -> bool {
    left.queue_offset == right.queue_offset
        && left.batch_num == right.batch_num
        && left.key == right.key
        && left.tombstone == right.tombstone
        && left.physical_offset() == right.physical_offset()
        && left.end_physical_offset() == right.end_physical_offset()
        && match (&left.payload, &right.payload) {
            (CompactionPayload::Inline(left), CompactionPayload::Inline(right)) => left == right,
            // A durable generation record and its CommitLog replay intentionally use different payload owners.
            // Their exact physical range and metadata above are the stable replay identity.
            _ => true,
        }
}

fn metadata_for_memory(generation: u64, queues: &CompactionQueues) -> GenerationMetadata {
    let mut metadata = compaction_generation::empty_metadata(generation);
    metadata.record_count = merged_count(queues) as u64;
    if metadata.record_count == 0 {
        return metadata;
    }
    metadata.min_queue_offset = queues
        .values()
        .flat_map(BTreeMap::values)
        .map(|record| record.queue_offset)
        .min()
        .unwrap_or_default();
    metadata.max_queue_offset = queues
        .values()
        .flat_map(BTreeMap::values)
        .map(|record| record.queue_offset)
        .max()
        .unwrap_or_default();
    metadata.min_physical_offset = queues
        .values()
        .flat_map(BTreeMap::values)
        .map(CompactionRecord::physical_offset)
        .min()
        .unwrap_or_default();
    metadata.max_physical_offset = queues
        .values()
        .flat_map(BTreeMap::values)
        .map(CompactionRecord::end_physical_offset)
        .max()
        .unwrap_or_default();
    metadata
}

fn status_result(
    status: GetMessageStatus,
    next_begin_offset: i64,
    min_offset: i64,
    max_offset: i64,
) -> GetMessageResult {
    let mut result = GetMessageResult::new_result_size(0);
    result.set_status(Some(status));
    result.set_next_begin_offset(next_begin_offset);
    result.set_min_offset(min_offset);
    result.set_max_offset(max_offset);
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use parking_lot::RwLock;
    use serde::Deserialize;

    use super::CompactionReadState;
    use super::CompactionStore;
    use super::FaultStage;
    use crate::base::message_status_enum::GetMessageStatus;
    use crate::kv::compaction_generation;

    fn durable_store(root: PathBuf) -> CompactionStore {
        CompactionStore::with_root(root, crate::runtime::test_scope("compaction-store-test"))
    }

    #[derive(Deserialize)]
    struct JavaCompactionContract {
        records: Vec<JavaCompactionRecord>,
        replay: JavaReplayContract,
        retention: JavaRetentionContract,
    }

    #[derive(Deserialize)]
    struct JavaCompactionRecord {
        queue_offset: i64,
        raw_key: String,
        body: String,
        tombstone: bool,
    }

    #[derive(Deserialize)]
    struct JavaReplayContract {
        identical_same_offset: String,
        conflicting_same_offset: String,
        winner: String,
    }

    #[derive(Deserialize)]
    struct JavaRetentionContract {
        retain_tombstone_until_atomic_generation_covers_older_records: bool,
        restart_must_not_resurrect_deleted_value: bool,
    }

    #[tokio::test]
    async fn java_55_fixture_drives_batch_tombstone_replay_and_retention_contracts() {
        let fixture: JavaCompactionContract = serde_json::from_str(include_str!(
            "../../../scripts/fixtures/java-5.5-compaction-contract.json"
        ))
        .expect("valid Java 5.5 compaction fixture");
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("java-fixture-topic");
        let group = CheetahString::from_static_str("java-fixture-group");

        for record in fixture.records {
            assert_eq!(record.tombstone, record.body.is_empty());
            store.put_message_with_key(
                &topic,
                0,
                record.queue_offset,
                1,
                Some(CheetahString::from_string(record.raw_key)),
                Bytes::from(record.body),
            );
        }

        assert_eq!(store.message_count(&topic, 0), 1);
        let result = store.get_message(&group, &topic, 0, 11, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.next_begin_offset(), 12);
        assert_eq!(fixture.replay.identical_same_offset, "idempotent");
        assert_eq!(fixture.replay.conflicting_same_offset, "fail-closed");
        assert_eq!(fixture.replay.winner, "maximum-queue-offset");
        assert!(
            fixture
                .retention
                .retain_tombstone_until_atomic_generation_covers_older_records
        );
        assert!(fixture.retention.restart_must_not_resurrect_deleted_value);
    }

    #[tokio::test]
    async fn get_message_returns_java_style_status_for_missing_queue() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-missing-topic");
        let group = CheetahString::from_static_str("compaction-missing-group");
        let result = store
            .get_message(&group, &topic, 0, 0, 32, 1024)
            .await
            .expect("compaction store should return a status result");
        assert_eq!(result.status(), Some(GetMessageStatus::NoMatchedLogicQueue));
        assert_eq!(result.message_count(), 0);
    }

    #[tokio::test]
    async fn put_and_get_message_round_trips_in_compaction_store() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-topic");
        let group = CheetahString::from_static_str("compaction-group");
        store.put_message_with_key(
            &topic,
            0,
            7,
            1,
            Some(CheetahString::from_static_str("key")),
            Bytes::from_static(b"compacted-message"),
        );
        let result = store
            .get_message(&group, &topic, 0, 7, 32, 1024)
            .await
            .expect("compaction result");
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.message_count(), 1);
        assert_eq!(result.next_begin_offset(), 8);
        assert_eq!(
            result.message_mapped_list()[0].get_bytes_ref().unwrap().as_ref(),
            b"compacted-message"
        );
    }

    #[tokio::test]
    async fn compact_once_keeps_latest_physical_offset_for_same_key() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-topic");
        let group = CheetahString::from_static_str("compaction-group");
        let key = CheetahString::from_static_str("same-key");
        store.put_message_with_key(&topic, 0, 0, 1, Some(key.clone()), Bytes::from_static(b"old-message"));
        store.put_message_with_key(&topic, 0, 1, 1, Some(key), Bytes::from_static(b"latest-message"));
        assert_eq!(store.message_count(&topic, 0), 1);
        assert_eq!(store.compact_once().await.unwrap(), 1);
        assert_eq!(store.current_generation_id(), 1);
        let latest_result = store
            .get_message(&group, &topic, 0, 1, 32, 1024)
            .await
            .expect("latest result");
        assert_eq!(latest_result.status(), Some(GetMessageStatus::Found));
        assert_eq!(
            latest_result.message_mapped_list()[0].get_bytes_ref().unwrap(),
            &Bytes::from_static(b"latest-message")
        );
    }

    #[tokio::test]
    async fn latest_queue_offset_wins_even_when_physical_offsets_are_not_monotonic() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-order-topic");
        let group = CheetahString::from_static_str("compaction-order-group");
        let key = CheetahString::from_static_str("same-key");
        let payloads = Arc::new(RwLock::new(HashMap::from([
            (100_i64, Bytes::from_static(b"old")),
            (50_i64, Bytes::from_static(b"new")),
        ])));
        install_resolver(&store, payloads);

        store.put_reference(&topic, 0, 10, 1, Some(key.clone()), 100, 3, false);
        store.put_reference(&topic, 0, 11, 1, Some(key), 50, 3, false);

        let result = store.get_message(&group, &topic, 0, 11, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(
            result.message_mapped_list()[0].get_bytes_ref().unwrap().as_ref(),
            b"new"
        );
    }

    #[tokio::test]
    async fn empty_body_tombstone_survives_generation_restart_without_resurrection() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("compaction-tombstone");
        let topic = CheetahString::from_static_str("compaction-tombstone-topic");
        let group = CheetahString::from_static_str("compaction-tombstone-group");
        let key = CheetahString::from_static_str("deleted-key");

        let store = durable_store(root.clone());
        store.load().await.unwrap();
        store.finish_recovery(0);
        store.put_message_with_key(&topic, 0, 0, 1, Some(key.clone()), Bytes::from_static(b"value"));
        store.compact_once().await.unwrap();
        store.put_message_with_key(&topic, 0, 1, 1, Some(key), Bytes::new());
        assert_eq!(store.message_count(&topic, 0), 0);
        store.compact_once().await.unwrap();
        drop(store);

        let restarted = durable_store(root);
        restarted.load().await.unwrap();
        restarted.finish_recovery(1);
        assert_eq!(restarted.message_count(&topic, 0), 0);
        let result = restarted.get_message(&group, &topic, 0, 0, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::NoMatchedLogicQueue));
    }

    #[tokio::test]
    async fn same_offset_replay_is_idempotent_but_conflicting_replay_fails_closed() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-replay-topic");
        let group = CheetahString::from_static_str("compaction-replay-group");
        let key = CheetahString::from_static_str("key");

        store.put_message_with_key(&topic, 0, 4, 1, Some(key.clone()), Bytes::from_static(b"same"));
        store.put_message_with_key(&topic, 0, 4, 1, Some(key.clone()), Bytes::from_static(b"same"));
        assert_eq!(store.message_count(&topic, 0), 1);

        store.put_message_with_key(&topic, 0, 4, 1, Some(key), Bytes::from_static(b"different"));
        assert_eq!(store.message_count(&topic, 0), 0);
        let result = store.get_message(&group, &topic, 0, 4, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::MessageWasRemoving));
        assert!(store.compact_once().await.is_err());
    }

    #[tokio::test]
    async fn raw_multi_token_key_is_compacted_as_one_key_without_splitting() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-raw-key-topic");
        let group = CheetahString::from_static_str("compaction-raw-key-group");
        let key = CheetahString::from_static_str("a b");

        store.put_message_with_key(&topic, 0, 0, 1, Some(key.clone()), Bytes::from_static(b"old"));
        store.put_message_with_key(&topic, 0, 1, 1, Some(key), Bytes::from_static(b"new"));
        assert_eq!(store.message_count(&topic, 0), 1);
        let result = store.get_message(&group, &topic, 0, 1, 1, 1024).await.unwrap();
        assert_eq!(
            result.message_mapped_list()[0].get_bytes_ref().unwrap().as_ref(),
            b"new"
        );
    }

    #[tokio::test]
    async fn tombstone_only_tail_advances_the_logical_read_frontier() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-tombstone-frontier-topic");
        let group = CheetahString::from_static_str("compaction-tombstone-frontier-group");

        store.put_message_with_key(
            &topic,
            0,
            0,
            1,
            Some(CheetahString::from_static_str("visible")),
            Bytes::from_static(b"value"),
        );
        store.put_message_with_key(
            &topic,
            0,
            2,
            1,
            Some(CheetahString::from_static_str("deleted")),
            Bytes::new(),
        );

        let result = store.get_message(&group, &topic, 0, 1, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::NoMatchedMessage));
        assert_eq!(result.next_begin_offset(), 3);
        assert_eq!(result.max_offset(), 3);
    }

    #[tokio::test]
    async fn missing_or_blank_keys_never_create_compaction_projection_state() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-key-contract-topic");
        let group = CheetahString::from_static_str("compaction-key-contract-group");

        store.put_message_with_key(&topic, 0, 0, 1, None, Bytes::from_static(b"missing"));
        store.put_message_with_key(
            &topic,
            0,
            1,
            1,
            Some(CheetahString::from_static_str("   \t")),
            Bytes::from_static(b"blank"),
        );

        assert_eq!(store.message_count(&topic, 0), 0);
        let result = store
            .get_message(&group, &topic, 0, 0, 32, 1024)
            .await
            .expect("compaction status");
        assert_eq!(result.status(), Some(GetMessageStatus::NoMatchedLogicQueue));
    }

    #[tokio::test]
    async fn recovering_state_fails_closed_without_raw_commit_log_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let store = durable_store(temp.path().join("compaction"));
        store.begin_recovery(100);
        let topic = CheetahString::from_static_str("topic");
        let group = CheetahString::from_static_str("group");
        let result = store.get_message(&group, &topic, 0, 0, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::MessageWasRemoving));
        assert!(matches!(store.read_state(), CompactionReadState::Recovering { .. }));
    }

    #[tokio::test]
    async fn generation_kill_points_recover_and_never_reexpose_stale_key() {
        for stage in [
            FaultStage::Build,
            FaultStage::Sync,
            FaultStage::Rename,
            FaultStage::Current,
            FaultStage::Cleanup,
        ] {
            run_kill_point(stage).await;
        }
    }

    #[tokio::test]
    async fn corrupt_current_rolls_back_only_to_validated_previous_and_stays_recovering() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("compaction-corrupt-current");
        let topic = CheetahString::from_static_str("topic");
        let group = CheetahString::from_static_str("group");
        let key = CheetahString::from_static_str("key");
        let payloads = Arc::new(RwLock::new(HashMap::from([
            (10_i64, Bytes::from_static(b"old")),
            (20_i64, Bytes::from_static(b"new")),
        ])));

        let store = durable_store(root.clone());
        install_resolver(&store, payloads.clone());
        store.load().await.unwrap();
        store.finish_recovery(0);
        store.put_reference(&topic, 0, 0, 1, Some(key.clone()), 10, 3, false);
        store.compact_once().await.unwrap();
        assert_eq!(store.durable_dispatch_offset(0), 13);
        store.put_reference(&topic, 0, 1, 1, Some(key.clone()), 20, 3, false);
        store.compact_once().await.unwrap();
        assert_eq!(store.current_generation_id(), 2);
        drop(store);

        std::fs::write(
            compaction_generation::generation_path(&root, 2).join("records"),
            b"corrupt",
        )
        .unwrap();
        let restarted = durable_store(root);
        install_resolver(&restarted, payloads);
        restarted.load().await.unwrap();
        assert_eq!(restarted.current_generation_id(), 1);
        assert!(matches!(restarted.read_state(), CompactionReadState::Recovering { .. }));
        assert_eq!(
            restarted
                .get_message(&group, &topic, 0, 0, 32, 1024)
                .await
                .unwrap()
                .status(),
            Some(GetMessageStatus::MessageWasRemoving)
        );

        restarted.put_reference(&topic, 0, 1, 1, Some(key), 20, 3, false);
        restarted.finish_recovery(23);
        assert_eq!(
            restarted
                .get_message(&group, &topic, 0, 0, 32, 1024)
                .await
                .unwrap()
                .status(),
            Some(GetMessageStatus::OffsetTooSmall)
        );
    }

    #[tokio::test]
    async fn published_generation_reads_without_commit_log_payload() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("compaction-independent-generation");
        let topic = CheetahString::from_static_str("topic");
        let group = CheetahString::from_static_str("group");
        let payloads = Arc::new(RwLock::new(HashMap::from([(10_i64, Bytes::from_static(b"live"))])));

        let store = durable_store(root.clone());
        install_resolver(&store, payloads.clone());
        store.load().await.unwrap();
        store.finish_recovery(0);
        store.put_reference(
            &topic,
            0,
            0,
            1,
            Some(CheetahString::from_static_str("key")),
            10,
            4,
            false,
        );
        store.compact_once().await.unwrap();
        payloads.write().clear();

        let result = store.get_message(&group, &topic, 0, 0, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(
            result.message_mapped_list()[0].get_bytes_ref().unwrap(),
            &Bytes::from_static(b"live")
        );
        drop(store);

        let restarted = durable_store(root);
        restarted.load().await.unwrap();
        restarted.finish_recovery(14);
        let result = restarted.get_message(&group, &topic, 0, 0, 1, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(
            result.message_mapped_list()[0].get_bytes_ref().unwrap(),
            &Bytes::from_static(b"live")
        );
    }

    async fn run_kill_point(stage: FaultStage) {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(format!("compaction-{stage:?}"));
        let topic = CheetahString::from_static_str("topic");
        let group = CheetahString::from_static_str("group");
        let key = CheetahString::from_static_str("key");
        let payloads = Arc::new(RwLock::new(HashMap::from([
            (10_i64, Bytes::from_static(b"old")),
            (20_i64, Bytes::from_static(b"new")),
        ])));

        let store = durable_store(root.clone());
        install_resolver(&store, payloads.clone());
        store.load().await.unwrap();
        store.finish_recovery(0);
        store.put_reference(&topic, 0, 0, 1, Some(key.clone()), 10, 3, false);
        store.compact_once().await.unwrap();
        store.put_reference(&topic, 0, 1, 1, Some(key.clone()), 20, 3, false);
        store.set_fault(stage);
        assert!(store.compact_once().await.is_err());
        let expected_generation = if matches!(stage, FaultStage::Cleanup) { 2 } else { 1 };
        assert_eq!(store.current_generation_id(), expected_generation);
        drop(store);

        let restarted = durable_store(root);
        install_resolver(&restarted, payloads);
        restarted.load().await.unwrap();
        assert!(matches!(restarted.read_state(), CompactionReadState::Recovering { .. }));
        let recovering = restarted.get_message(&group, &topic, 0, 0, 32, 1024).await.unwrap();
        assert_eq!(recovering.status(), Some(GetMessageStatus::MessageWasRemoving));

        restarted.put_reference(&topic, 0, 1, 1, Some(key), 20, 3, false);
        restarted.finish_recovery(23);
        let result = restarted.get_message(&group, &topic, 0, 0, 32, 1024).await.unwrap();
        assert_eq!(result.status(), Some(GetMessageStatus::OffsetTooSmall));
        let latest = restarted.get_message(&group, &topic, 0, 1, 32, 1024).await.unwrap();
        assert_eq!(latest.status(), Some(GetMessageStatus::Found));
        assert_eq!(
            latest.message_mapped_list()[0].get_bytes_ref().unwrap(),
            &Bytes::from_static(b"new")
        );
    }

    #[tokio::test]
    async fn reader_lease_delays_retired_generation_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("compaction");
        let topic = CheetahString::from_static_str("topic");
        let store = durable_store(root.clone());
        install_resolver(
            &store,
            Arc::new(RwLock::new(HashMap::from([
                (10_i64, Bytes::from_static(b"one")),
                (20_i64, Bytes::from_static(b"two")),
                (30_i64, Bytes::from_static(b"three")),
            ]))),
        );
        store.load().await.unwrap();
        store.finish_recovery(0);
        let key = CheetahString::from_static_str("lease-key");
        store.put_reference(&topic, 0, 0, 1, Some(key.clone()), 10, 3, false);
        store.compact_once().await.unwrap();
        let generation_one_lease = store.current_lease();
        store.put_reference(&topic, 0, 1, 1, Some(key.clone()), 20, 3, false);
        store.compact_once().await.unwrap();
        store.put_reference(&topic, 0, 2, 1, Some(key), 30, 5, false);
        store.compact_once().await.unwrap();
        assert!(compaction_generation::generation_path(&root, 1).exists());
        drop(generation_one_lease);
        store.compact_once().await.unwrap();
        assert!(!compaction_generation::generation_path(&root, 1).exists());
    }

    fn install_resolver(store: &CompactionStore, payloads: Arc<RwLock<HashMap<i64, Bytes>>>) {
        store.set_payload_resolver(move |offset, _size| payloads.read().get(&offset).cloned());
    }
}
