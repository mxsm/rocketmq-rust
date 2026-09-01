// Copyright 2026 The RocketMQ Rust Authors
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
use std::collections::VecDeque;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSnapshotFile;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

use crate::timer::partition_manifest::PartitionManifestFailure;
use crate::timer::partition_manifest::TimerPayloadPartitionKey;
use crate::timer::partition_manifest::TimerPayloadPartitionManifest;
use crate::timer::partition_manifest::TimerPayloadPartitionState;
use crate::timer::payload_record::TimerPayloadRecordV1;
use crate::timer::payload_record::TimerPayloadRecordViolation;

const SEGMENT_NAME_WIDTH: usize = 20;

/// Capacity and file-layout limits for the long-horizon payload store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerPayloadStoreConfig {
    /// Root containing UTC-day/lane partitions.
    pub root: PathBuf,
    /// Maximum bytes in one partition segment.
    pub segment_bytes: u64,
    /// Maximum cached append handles.
    pub max_open_handles: usize,
    /// Maximum encoded bytes accepted by one append batch.
    pub batch_bytes: usize,
    /// Maximum encoded bytes accepted for one record.
    pub max_record_bytes: usize,
    /// Maximum durable bytes in one due-day/lane partition.
    pub max_partition_live_bytes: u64,
}

impl TimerPayloadStoreConfig {
    /// Creates production-oriented defaults below the message-store root.
    pub fn for_store_root(store_root: impl AsRef<Path>) -> Self {
        Self {
            root: store_root.as_ref().join("timer-extended").join("payload-v1"),
            segment_bytes: 256 * 1024 * 1024,
            max_open_handles: 64,
            batch_bytes: 8 * 1024 * 1024,
            max_record_bytes: 4 * 1024 * 1024,
            max_partition_live_bytes: 64 * 1024 * 1024 * 1024,
        }
    }

    fn validate(&self) -> Result<(), TimerPayloadStoreFailure> {
        if self.root.as_os_str().is_empty()
            || self.segment_bytes == 0
            || self.max_open_handles == 0
            || self.batch_bytes == 0
            || self.max_record_bytes == 0
            || self.max_partition_live_bytes == 0
            || self.max_record_bytes as u64 > self.segment_bytes
        {
            return Err(TimerPayloadStoreFailure::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PartitionRuntime {
    manifest: TimerPayloadPartitionManifest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct HandleKey {
    partition: TimerPayloadPartitionKey,
    segment_id: u64,
}

#[derive(Default)]
struct PayloadStoreState {
    partitions: HashMap<TimerPayloadPartitionKey, PartitionRuntime>,
    handles: HashMap<HandleKey, File>,
    handle_order: VecDeque<HandleKey>,
}

/// Append-only, due-day/lane partitioned store for complete Timer messages.
pub struct TimerPayloadStore {
    config: TimerPayloadStoreConfig,
    state: Mutex<PayloadStoreState>,
}

impl TimerPayloadStore {
    /// Creates a payload store. Call [`Self::load`] before use.
    ///
    /// # Errors
    ///
    /// Returns an error when capacity limits are inconsistent.
    fn new_checked(config: TimerPayloadStoreConfig) -> Result<Self, TimerPayloadStoreFailure> {
        config.validate()?;
        Ok(Self {
            config,
            state: Mutex::new(PayloadStoreState::default()),
        })
    }

    /// Recovers every existing partition and truncates only incomplete active tails.
    fn load_checked(&self) -> Result<(), TimerPayloadStoreFailure> {
        std::fs::create_dir_all(&self.config.root)?;
        let mut discovered = Vec::new();
        for day_entry in std::fs::read_dir(&self.config.root)? {
            let day_entry = day_entry?;
            if !day_entry.file_type()?.is_dir() {
                continue;
            }
            let Some(day) = day_entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("day-"))
                .and_then(|value| value.parse::<i32>().ok())
            else {
                continue;
            };
            for lane_entry in std::fs::read_dir(day_entry.path())? {
                let lane_entry = lane_entry?;
                if !lane_entry.file_type()?.is_dir() {
                    continue;
                }
                let Some(lane) = lane_entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("lane-"))
                    .and_then(|value| value.parse::<u16>().ok())
                else {
                    continue;
                };
                discovered.push(TimerPayloadPartitionKey { due_day_utc: day, lane });
            }
        }

        let mut recovered = HashMap::with_capacity(discovered.len());
        for key in discovered {
            let manifest = self.recover_partition(key)?;
            recovered.insert(key, PartitionRuntime { manifest });
        }
        let mut state = self.state.lock();
        state.partitions = recovered;
        state.handles.clear();
        state.handle_order.clear();
        Ok(())
    }

    /// Appends and synchronizes one bounded batch before returning durable locators.
    ///
    /// A failure may leave unreferenced payload records. Replaying the source is idempotent at the
    /// Timeline layer, and orphan GC removes records not referenced by any non-terminal state.
    fn append_batch_checked(
        &self,
        records: &[TimerPayloadRecordV1],
    ) -> Result<Vec<TimerPayloadStoreLocator>, TimerPayloadStoreFailure> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut prepared = Vec::with_capacity(records.len());
        let mut batch_bytes = 0usize;
        for record in records {
            let encoded = record.encode_checked()?;
            if encoded.len() > self.config.max_record_bytes {
                return Err(TimerPayloadStoreFailure::RecordLimitExceeded(encoded.len()));
            }
            batch_bytes = batch_bytes.saturating_add(encoded.len());
            if batch_bytes > self.config.batch_bytes {
                return Err(TimerPayloadStoreFailure::BatchLimitExceeded(batch_bytes));
            }
            prepared.push((
                TimerPayloadPartitionKey {
                    due_day_utc: record.due_day_utc_checked()?,
                    lane: record.lane,
                },
                encoded,
            ));
        }

        let mut state = self.state.lock();
        let mut touched_handles = HashSet::new();
        let mut touched_partitions = HashSet::new();
        let mut locators = Vec::with_capacity(prepared.len());
        for (partition, encoded) in prepared {
            self.ensure_partition_loaded(&mut state, partition)?;
            let current = state
                .partitions
                .get(&partition)
                .copied()
                .ok_or(TimerPayloadStoreFailure::PartitionMissing)?;
            if current.manifest.state != TimerPayloadPartitionState::Open {
                return Err(TimerPayloadStoreFailure::PartitionNotOpen(partition));
            }
            let encoded_len = encoded.len() as u64;
            if current.manifest.live_bytes.saturating_add(encoded_len) > self.config.max_partition_live_bytes {
                return Err(TimerPayloadStoreFailure::PartitionLimitExceeded(partition));
            }

            let mut segment_id = current.manifest.active_segment_id;
            let mut segment_len = current.manifest.active_segment_len;
            if segment_len > 0 && segment_len.saturating_add(encoded_len) > self.config.segment_bytes {
                let previous = HandleKey { partition, segment_id };
                if let Some(handle) = state.handles.get_mut(&previous) {
                    handle.sync_data()?;
                }
                segment_id = segment_id.saturating_add(1);
                segment_len = 0;
            }

            let handle_key = HandleKey { partition, segment_id };
            let path = self.segment_path(partition, segment_id);
            let handle = self.append_handle(&mut state, handle_key, &path)?;
            handle.seek(SeekFrom::Start(segment_len))?;
            handle.write_all(&encoded)?;
            touched_handles.insert(handle_key);
            touched_partitions.insert(partition);

            let checksum = TimerPayloadRecordV1::checksum(&encoded)?;
            let locator = TimerPayloadStoreLocator::try_new(
                partition.due_day_utc,
                partition.lane,
                segment_id,
                segment_len,
                u32::try_from(encoded.len())
                    .map_err(|_| TimerPayloadStoreFailure::RecordLimitExceeded(encoded.len()))?,
                checksum,
            )
            .map_err(|_| TimerPayloadStoreFailure::RecordLimitExceeded(encoded.len()))?;
            let runtime = state
                .partitions
                .get_mut(&partition)
                .ok_or(TimerPayloadStoreFailure::PartitionMissing)?;
            runtime.manifest.active_segment_id = segment_id;
            runtime.manifest.active_segment_len = segment_len.saturating_add(encoded_len);
            runtime.manifest.record_count = runtime.manifest.record_count.saturating_add(1);
            runtime.manifest.live_bytes = runtime.manifest.live_bytes.saturating_add(encoded_len);
            locators.push(locator);
        }

        for key in touched_handles {
            if let Some(handle) = state.handles.get_mut(&key) {
                handle.sync_data()?;
            }
        }
        for partition in touched_partitions {
            let path = self.partition_path(partition);
            state
                .partitions
                .get_mut(&partition)
                .ok_or(TimerPayloadStoreFailure::PartitionMissing)?
                .manifest
                .persist(&path)?;
        }
        Ok(locators)
    }

    /// Reads and verifies one durable payload locator.
    fn read_checked(
        &self,
        locator: TimerPayloadStoreLocator,
    ) -> Result<TimerPayloadRecordV1, TimerPayloadStoreFailure> {
        let partition = TimerPayloadPartitionKey {
            due_day_utc: locator.due_day_utc(),
            lane: locator.lane(),
        };
        let path = self.segment_path(partition, locator.segment_id());
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(locator.offset()))?;
        let mut encoded = vec![0u8; locator.length() as usize];
        file.read_exact(&mut encoded)?;
        if TimerPayloadRecordV1::checksum(&encoded)? != locator.checksum() {
            return Err(TimerPayloadStoreFailure::LocatorChecksumMismatch);
        }
        let record = TimerPayloadRecordV1::decode_checked(&encoded)?;
        if record.due_day_utc_checked()? != partition.due_day_utc || record.lane != partition.lane {
            return Err(TimerPayloadStoreFailure::LocatorPartitionMismatch);
        }
        Ok(record)
    }

    /// Seals one partition after its UTC day is closed for new materialization.
    fn seal_partition_checked(&self, key: TimerPayloadPartitionKey) -> Result<(), TimerPayloadStoreFailure> {
        self.transition_partition(
            key,
            TimerPayloadPartitionState::Open,
            TimerPayloadPartitionState::Sealed,
        )
    }

    /// Marks a sealed partition eligible for whole-partition GC.
    fn mark_gc_eligible_checked(&self, key: TimerPayloadPartitionKey) -> Result<(), TimerPayloadStoreFailure> {
        self.transition_partition(
            key,
            TimerPayloadPartitionState::Sealed,
            TimerPayloadPartitionState::GcEligible,
        )
    }

    /// Deletes a GC-eligible partition only after state, snapshot, and replication fences agree.
    fn gc_partition_checked(
        &self,
        key: TimerPayloadPartitionKey,
        no_live_timeline_references: bool,
        snapshot_safe: bool,
        replication_safe: bool,
    ) -> Result<bool, TimerPayloadStoreFailure> {
        if !no_live_timeline_references || !snapshot_safe || !replication_safe {
            return Ok(false);
        }
        let partition_path = self.partition_path(key);
        let canonical_root = self.config.root.canonicalize()?;
        let canonical_partition = partition_path.canonicalize()?;
        if !canonical_partition.starts_with(&canonical_root) || canonical_partition == canonical_root {
            return Err(TimerPayloadStoreFailure::UnsafeGcPath);
        }
        let mut state = self.state.lock();
        let runtime = state
            .partitions
            .get(&key)
            .copied()
            .ok_or(TimerPayloadStoreFailure::PartitionMissing)?;
        if runtime.manifest.state != TimerPayloadPartitionState::GcEligible {
            return Ok(false);
        }
        let handles: Vec<_> = state
            .handles
            .keys()
            .copied()
            .filter(|handle| handle.partition == key)
            .collect();
        for handle in handles {
            if let Some(file) = state.handles.remove(&handle) {
                file.sync_data()?;
            }
            state.handle_order.retain(|candidate| *candidate != handle);
        }
        std::fs::remove_dir_all(&canonical_partition)?;
        state.partitions.remove(&key);
        Ok(true)
    }

    /// Returns the number of cached append handles.
    pub fn open_handle_count(&self) -> usize {
        self.state.lock().handles.len()
    }

    /// Returns a copy of one loaded partition manifest.
    pub fn partition_manifest(&self, key: TimerPayloadPartitionKey) -> Option<TimerPayloadPartitionManifest> {
        self.state.lock().partitions.get(&key).map(|runtime| runtime.manifest)
    }

    /// Returns an allocation-free snapshot of currently loaded payload partitions.
    pub fn metrics(&self) -> TimerPayloadStoreMetrics {
        let state = self.state.lock();
        let mut metrics = TimerPayloadStoreMetrics {
            partition_count: state.partitions.len(),
            open_handle_count: state.handles.len(),
            ..TimerPayloadStoreMetrics::default()
        };
        for runtime in state.partitions.values() {
            metrics.live_bytes = metrics.live_bytes.saturating_add(runtime.manifest.live_bytes);
            metrics.record_count = metrics.record_count.saturating_add(runtime.manifest.record_count);
        }
        metrics
    }

    /// Flushes and copies one immutable payload snapshot while pinning every partition manifest.
    ///
    /// Only one artifact generation may be active at a time. A failed copy intentionally leaves
    /// its durable pins in place so GC cannot invalidate a partially published artifact.
    fn create_snapshot_files_checked(
        &self,
        target_root: &Path,
        generation: u64,
    ) -> Result<Vec<TimerSnapshotFile>, TimerPayloadStoreFailure> {
        if generation == 0 {
            return Err(TimerPayloadStoreFailure::InvalidSnapshotGeneration);
        }
        let mut state = self.state.lock();
        if state
            .partitions
            .values()
            .any(|partition| partition.manifest.snapshot_pin_generation != 0)
        {
            return Err(TimerPayloadStoreFailure::SnapshotAlreadyPinned);
        }
        for handle in state.handles.values_mut() {
            handle.sync_data()?;
        }
        for runtime in state.partitions.values_mut() {
            runtime.manifest.snapshot_pin_generation = generation;
            runtime.manifest.persist(&self.partition_path(runtime.manifest.key))?;
        }

        let mut files = Vec::new();
        for runtime in state.partitions.values() {
            let key = runtime.manifest.key;
            let source_partition = self.partition_path(key);
            let relative_partition =
                PathBuf::from(format!("day-{:010}", key.due_day_utc)).join(format!("lane-{:05}", key.lane));
            for name in ["manifest.a", "manifest.b"] {
                let source = source_partition.join(name);
                if source.exists() {
                    files.push(copy_snapshot_file(
                        &source,
                        &target_root.join(&relative_partition).join(name),
                        &relative_partition.join(name),
                        source.metadata()?.len(),
                    )?);
                }
            }
            for segment_id in 0..=runtime.manifest.active_segment_id {
                let name = format!("{segment_id:020}");
                let source = self.segment_path(key, segment_id);
                if !source.exists() {
                    continue;
                }
                let length = if segment_id == runtime.manifest.active_segment_id {
                    runtime.manifest.active_segment_len
                } else {
                    source.metadata()?.len()
                };
                if length > 0 {
                    files.push(copy_snapshot_file(
                        &source,
                        &target_root.join(&relative_partition).join(&name),
                        &relative_partition.join(&name),
                        length,
                    )?);
                }
            }
        }
        Ok(files)
    }

    /// Releases one successfully copied snapshot generation from every partition.
    fn release_snapshot_pin_checked(&self, generation: u64) -> Result<(), TimerPayloadStoreFailure> {
        let mut state = self.state.lock();
        for runtime in state.partitions.values_mut() {
            if runtime.manifest.snapshot_pin_generation == generation {
                runtime.manifest.snapshot_pin_generation = 0;
                runtime.manifest.persist(&self.partition_path(runtime.manifest.key))?;
            }
        }
        Ok(())
    }

    fn transition_partition(
        &self,
        key: TimerPayloadPartitionKey,
        expected: TimerPayloadPartitionState,
        next: TimerPayloadPartitionState,
    ) -> Result<(), TimerPayloadStoreFailure> {
        let mut state = self.state.lock();
        self.ensure_partition_loaded(&mut state, key)?;
        let handle_keys: Vec<_> = state
            .handles
            .keys()
            .copied()
            .filter(|handle| handle.partition == key)
            .collect();
        for handle_key in handle_keys {
            if let Some(handle) = state.handles.get_mut(&handle_key) {
                handle.sync_data()?;
            }
        }
        let runtime = state
            .partitions
            .get_mut(&key)
            .ok_or(TimerPayloadStoreFailure::PartitionMissing)?;
        if runtime.manifest.state != expected {
            return Err(TimerPayloadStoreFailure::InvalidPartitionTransition {
                current: runtime.manifest.state,
                requested: next,
            });
        }
        runtime.manifest.state = next;
        runtime.manifest.persist(&self.partition_path(key))?;
        Ok(())
    }

    fn ensure_partition_loaded(
        &self,
        state: &mut PayloadStoreState,
        key: TimerPayloadPartitionKey,
    ) -> Result<(), TimerPayloadStoreFailure> {
        if state.partitions.contains_key(&key) {
            return Ok(());
        }
        let manifest = self.recover_partition(key)?;
        state.partitions.insert(key, PartitionRuntime { manifest });
        Ok(())
    }

    fn recover_partition(
        &self,
        key: TimerPayloadPartitionKey,
    ) -> Result<TimerPayloadPartitionManifest, TimerPayloadStoreFailure> {
        let directory = self.partition_path(key);
        std::fs::create_dir_all(&directory)?;
        let mut manifest = TimerPayloadPartitionManifest::load(&directory, key)?;
        if manifest.state == TimerPayloadPartitionState::Deleted {
            return Err(TimerPayloadStoreFailure::PartitionNotOpen(key));
        }
        let mut segments = Vec::new();
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(segment_id) = entry
                .file_name()
                .to_str()
                .filter(|name| name.len() == SEGMENT_NAME_WIDTH && name.bytes().all(|byte| byte.is_ascii_digit()))
                .and_then(|name| name.parse::<u64>().ok())
            else {
                continue;
            };
            segments.push(segment_id);
        }
        segments.sort_unstable();
        for pair in segments.windows(2) {
            if pair[1] != pair[0].saturating_add(1) {
                return Err(TimerPayloadStoreFailure::SegmentHole {
                    expected: pair[0].saturating_add(1),
                    actual: pair[1],
                });
            }
        }
        if segments.is_empty() {
            segments.push(0);
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(self.segment_path(key, 0))?;
        }

        let mut total_records = 0u64;
        let mut total_bytes = 0u64;
        let mut active_len = 0u64;
        for (index, segment_id) in segments.iter().copied().enumerate() {
            let last = index + 1 == segments.len();
            let (records, bytes) = self.recover_segment(key, segment_id, last)?;
            total_records = total_records.saturating_add(records);
            total_bytes = total_bytes.saturating_add(bytes);
            if last {
                active_len = bytes;
            }
        }
        let active_segment_id = *segments.last().ok_or(TimerPayloadStoreFailure::PartitionMissing)?;
        if manifest.active_segment_id != active_segment_id
            || manifest.active_segment_len != active_len
            || manifest.record_count != total_records
            || manifest.live_bytes != total_bytes
        {
            manifest.active_segment_id = active_segment_id;
            manifest.active_segment_len = active_len;
            manifest.record_count = total_records;
            manifest.live_bytes = total_bytes;
            manifest.persist(&directory)?;
        }
        Ok(manifest)
    }

    fn recover_segment(
        &self,
        partition: TimerPayloadPartitionKey,
        segment_id: u64,
        active: bool,
    ) -> Result<(u64, u64), TimerPayloadStoreFailure> {
        let path = self.segment_path(partition, segment_id);
        let mut file = OpenOptions::new().read(true).write(active).open(&path)?;
        let file_len = file.metadata()?.len();
        let mut cursor = 0u64;
        let mut records = 0u64;
        while cursor < file_len {
            let remaining = file_len - cursor;
            if remaining < TimerPayloadRecordV1::header_size() as u64 {
                if active {
                    file.set_len(cursor)?;
                    file.sync_data()?;
                    break;
                }
                return Err(TimerPayloadStoreFailure::CorruptSealedSegment(segment_id));
            }
            file.seek(SeekFrom::Start(cursor))?;
            let mut header = vec![0u8; TimerPayloadRecordV1::header_size()];
            file.read_exact(&mut header)?;
            let declared_len = match TimerPayloadRecordV1::declared_len(&header) {
                Ok(length) => length,
                Err(error) => return Err(TimerPayloadStoreFailure::Record(error)),
            };
            if declared_len < TimerPayloadRecordV1::header_size() + 4
                || declared_len > self.config.max_record_bytes
                || cursor.saturating_add(declared_len as u64) > file_len
            {
                if active {
                    file.set_len(cursor)?;
                    file.sync_data()?;
                    break;
                }
                return Err(TimerPayloadStoreFailure::CorruptSealedSegment(segment_id));
            }
            let mut encoded = vec![0u8; declared_len];
            encoded[..header.len()].copy_from_slice(&header);
            file.read_exact(&mut encoded[header.len()..])?;
            TimerPayloadRecordV1::decode_checked(&encoded)?;
            cursor = cursor.saturating_add(declared_len as u64);
            records = records.saturating_add(1);
        }
        Ok((records, cursor))
    }

    fn append_handle<'a>(
        &self,
        state: &'a mut PayloadStoreState,
        key: HandleKey,
        path: &Path,
    ) -> Result<&'a mut File, TimerPayloadStoreFailure> {
        if !state.handles.contains_key(&key) {
            while state.handles.len() >= self.config.max_open_handles {
                let Some(oldest) = state.handle_order.pop_front() else {
                    break;
                };
                if let Some(handle) = state.handles.remove(&oldest) {
                    handle.sync_data()?;
                }
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let handle = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(path)?;
            state.handles.insert(key, handle);
        }
        state.handle_order.retain(|candidate| *candidate != key);
        state.handle_order.push_back(key);
        state
            .handles
            .get_mut(&key)
            .ok_or(TimerPayloadStoreFailure::PartitionMissing)
    }

    fn partition_path(&self, key: TimerPayloadPartitionKey) -> PathBuf {
        self.config
            .root
            .join(format!("day-{:010}", key.due_day_utc))
            .join(format!("lane-{:05}", key.lane))
    }

    fn segment_path(&self, key: TimerPayloadPartitionKey, segment_id: u64) -> PathBuf {
        self.partition_path(key).join(format!("{segment_id:020}"))
    }
}

impl TimerPayloadStore {
    /// Creates a payload store.
    ///
    /// Returns `Ok(None)` when the configured limits are inconsistent.
    pub fn new(config: TimerPayloadStoreConfig) -> Result<Option<Self>, StoreError> {
        TimerPayloadStoreFailure::into_public(Self::new_checked(config), StoreOperation::Start)
    }

    /// Recovers every existing payload partition.
    pub fn load(&self) -> Result<(), StoreError> {
        self.load_checked()
            .map_err(|error| error.into_store_error(StoreOperation::Load))
    }

    /// Appends and synchronizes one bounded payload batch.
    ///
    /// Returns `Ok(None)` when a record or configured batch limit rejects the input before I/O.
    ///
    /// # Errors
    ///
    /// Returns a storage error when durable payload or manifest I/O fails.
    pub fn append_batch(
        &self,
        records: &[TimerPayloadRecordV1],
    ) -> Result<Option<Vec<TimerPayloadStoreLocator>>, StoreError> {
        TimerPayloadStoreFailure::into_public(self.append_batch_checked(records), StoreOperation::Append)
    }

    /// Reads and verifies one durable payload locator.
    pub fn read(&self, locator: TimerPayloadStoreLocator) -> Result<TimerPayloadRecordV1, StoreError> {
        self.read_checked(locator)
            .map_err(|error| error.into_store_error(StoreOperation::Read))
    }

    /// Seals one payload partition.
    pub fn seal_partition(&self, key: TimerPayloadPartitionKey) -> Result<(), StoreError> {
        self.seal_partition_checked(key)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }

    /// Marks one sealed payload partition eligible for collection.
    pub fn mark_gc_eligible(&self, key: TimerPayloadPartitionKey) -> Result<(), StoreError> {
        self.mark_gc_eligible_checked(key)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }

    /// Collects one eligible payload partition when every fence permits it.
    pub fn gc_partition(
        &self,
        key: TimerPayloadPartitionKey,
        no_live_timeline_references: bool,
        snapshot_safe: bool,
        replication_safe: bool,
    ) -> Result<bool, StoreError> {
        self.gc_partition_checked(key, no_live_timeline_references, snapshot_safe, replication_safe)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }

    /// Creates and pins one immutable payload snapshot.
    pub fn create_snapshot_files(
        &self,
        target_root: &Path,
        generation: u64,
    ) -> Result<Vec<TimerSnapshotFile>, StoreError> {
        self.create_snapshot_files_checked(target_root, generation)
            .map_err(|error| error.into_store_error(StoreOperation::Flush))
    }

    /// Releases one payload snapshot pin.
    pub fn release_snapshot_pin(&self, generation: u64) -> Result<(), StoreError> {
        self.release_snapshot_pin_checked(generation)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }
}

fn copy_snapshot_file(
    source_path: &Path,
    target_path: &Path,
    relative_path: &Path,
    length: u64,
) -> Result<TimerSnapshotFile, TimerPayloadStoreFailure> {
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut source = File::open(source_path)?;
    let mut target = OpenOptions::new().create_new(true).write(true).open(target_path)?;
    let mut remaining = length;
    let mut buffer = vec![0u8; 64 * 1024];
    let mut hasher = Sha256::new();
    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        source.read_exact(&mut buffer[..chunk])?;
        target.write_all(&buffer[..chunk])?;
        hasher.update(&buffer[..chunk]);
        remaining -= chunk as u64;
    }
    target.sync_data()?;
    Ok(TimerSnapshotFile {
        relative_path: relative_path.to_string_lossy().replace('\\', "/"),
        length,
        sha256: hex::encode(hasher.finalize()),
    })
}

/// Bounded operational snapshot for the independent timer payload store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerPayloadStoreMetrics {
    /// Durable encoded bytes referenced by loaded partition manifests.
    pub live_bytes: u64,
    /// Durable payload records referenced by loaded partition manifests.
    pub record_count: u64,
    /// Loaded due-day/lane partitions.
    pub partition_count: usize,
    /// Cached append handles, always bounded by configuration.
    pub open_handle_count: usize,
}

/// Long-horizon payload-store error.
#[derive(Debug, Error)]
pub(crate) enum TimerPayloadStoreFailure {
    /// Underlying filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Record codec failed.
    #[error("timer payload record codec failed: {0}")]
    Record(
        #[from]
        #[source]
        TimerPayloadRecordViolation,
    ),
    /// Partition manifest failed.
    #[error(transparent)]
    Manifest(#[from] PartitionManifestFailure),
    /// Capacity values are inconsistent.
    #[error("invalid timer payload-store configuration")]
    InvalidConfig,
    /// One record exceeds the configured maximum.
    #[error("timer payload record exceeds configured limit: {0}")]
    RecordLimitExceeded(usize),
    /// One batch exceeds the configured maximum.
    #[error("timer payload batch exceeds configured limit: {0}")]
    BatchLimitExceeded(usize),
    /// One partition exceeds its configured live-byte limit.
    #[error("timer payload partition exceeds its live-byte limit: {0:?}")]
    PartitionLimitExceeded(TimerPayloadPartitionKey),
    /// Partition is not open for appends.
    #[error("timer payload partition is not open: {0:?}")]
    PartitionNotOpen(TimerPayloadPartitionKey),
    /// In-memory partition state is missing after recovery.
    #[error("timer payload partition state is missing")]
    PartitionMissing,
    /// Snapshot generation zero is reserved for the unpinned state.
    #[error("timer payload snapshot generation must be non-zero")]
    InvalidSnapshotGeneration,
    /// The first production format keeps one durable snapshot artifact pinned at a time.
    #[error("timer payload snapshot is already pinned")]
    SnapshotAlreadyPinned,
    /// Segment ids are not contiguous.
    #[error("timer payload segment hole: expected={expected}, actual={actual}")]
    SegmentHole {
        /// Expected segment id.
        expected: u64,
        /// Actual segment id.
        actual: u64,
    },
    /// An immutable segment is corrupt; only an active torn tail may be truncated.
    #[error("timer payload sealed segment is corrupt: {0}")]
    CorruptSealedSegment(u64),
    /// Locator CRC does not match the encoded record.
    #[error("timer payload locator checksum mismatch")]
    LocatorChecksumMismatch,
    /// Locator partition differs from the decoded record deadline/lane.
    #[error("timer payload locator partition mismatch")]
    LocatorPartitionMismatch,
    /// Lifecycle transition does not match the current state.
    #[error("invalid timer payload partition transition: {current:?} -> {requested:?}")]
    InvalidPartitionTransition {
        /// Current persisted state.
        current: TimerPayloadPartitionState,
        /// Requested next state.
        requested: TimerPayloadPartitionState,
    },
    /// Recursive GC target escaped the configured payload root.
    #[error("refusing unsafe timer payload GC path")]
    UnsafeGcPath,
}

impl TimerPayloadStoreFailure {
    fn is_contract_violation(&self, operation: StoreOperation) -> bool {
        matches!((operation, self), (StoreOperation::Start, Self::InvalidConfig))
            || matches!(
                (operation, self),
                (
                    StoreOperation::Append,
                    Self::Record(_) | Self::RecordLimitExceeded(_) | Self::BatchLimitExceeded(_)
                )
            )
    }

    fn into_public<T>(result: Result<T, Self>, operation: StoreOperation) -> Result<Option<T>, StoreError> {
        match result {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.is_contract_violation(operation) => Ok(None),
            Err(error) => Err(error.into_store_error(operation)),
        }
    }

    fn into_store_error(self, operation: StoreOperation) -> StoreError {
        match self {
            error @ Self::Io(_) => StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
                .in_component(StoreComponent::Store)
                .with_source(error),
            error @ Self::PartitionLimitExceeded(_) => {
                StoreError::new(&rocketmq_error::STORAGE_CAPACITY_EXHAUSTED, operation)
                    .in_component(StoreComponent::Store)
                    .with_source(error)
            }
            error @ (Self::Manifest(_)
            | Self::Record(_)
            | Self::InvalidConfig
            | Self::RecordLimitExceeded(_)
            | Self::BatchLimitExceeded(_)
            | Self::PartitionNotOpen(_)
            | Self::PartitionMissing
            | Self::InvalidSnapshotGeneration
            | Self::SnapshotAlreadyPinned
            | Self::SegmentHole { .. }
            | Self::CorruptSealedSegment(_)
            | Self::LocatorChecksumMismatch
            | Self::LocatorPartitionMismatch
            | Self::InvalidPartitionTransition { .. }
            | Self::UnsafeGcPath) => StoreError::new(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation)
                .in_component(StoreComponent::Store)
                .with_source(error),
        }
    }
}

#[cfg(test)]
mod error_projection_tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn direct_record_contract_is_none_but_persisted_corruption_keeps_owning_source() {
        let direct = TimerPayloadStoreFailure::into_public::<()>(
            Err(TimerPayloadStoreFailure::Record(TimerPayloadRecordViolation::BadMagic)),
            StoreOperation::Append,
        )
        .expect("direct codec rejection is not operational");
        assert!(direct.is_none());

        let error = TimerPayloadStoreFailure::Record(TimerPayloadRecordViolation::BadMagic)
            .into_store_error(StoreOperation::Read);
        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_STATE_CORRUPTED);
        let owner = error
            .source()
            .and_then(|source| source.downcast_ref::<TimerPayloadStoreFailure>())
            .expect("StoreError retains the payload-store owner");
        assert!(owner
            .source()
            .and_then(|source| source.downcast_ref::<TimerPayloadRecordViolation>())
            .is_some());
    }
}
