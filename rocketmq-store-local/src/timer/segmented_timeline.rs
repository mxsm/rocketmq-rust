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

use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use parking_lot::Mutex;
use rocketmq_store_api::TimerSnapshotFile;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

use crate::timer::timeline_manifest::TimelineManifestError;
use crate::timer::timeline_manifest::TimelineManifestV1;
use crate::timer::timeline_segment::inspect_timeline_run;
use crate::timer::timeline_segment::write_timeline_run;
use crate::timer::timeline_segment::TimelinePartitionKey;
use crate::timer::timeline_segment::TimelineRunDescriptor;
use crate::timer::timeline_segment::TimelineRunKind;
use crate::timer::timeline_segment::TimelineRunReader;
use crate::timer::timeline_segment::TimelineSegmentError;
use crate::timer::timeline_segment::TimelineSegmentKey;
use crate::timer::timeline_segment::TimelineSegmentRecord;

const TIMELINE_DIRECTORY: &str = "timer-extended/timeline-segments-v1";

/// Bounded native Timeline storage limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentedTimelineConfig {
    /// Maximum simultaneously open run readers.
    pub max_open_runs: usize,
    /// Delta-run count that makes a partition eligible for merge.
    pub merge_delta_runs: usize,
    /// Maximum runs consumed by one merge.
    pub merge_max_input_runs: usize,
    /// Maximum logical bytes emitted by one merge.
    pub merge_max_output_bytes: usize,
}

impl Default for SegmentedTimelineConfig {
    fn default() -> Self {
        Self {
            max_open_runs: 64,
            merge_delta_runs: 8,
            merge_max_input_runs: 16,
            merge_max_output_bytes: 64 * 1024 * 1024,
        }
    }
}

impl SegmentedTimelineConfig {
    fn validate(self) -> Result<Self, SegmentedTimelineError> {
        if self.max_open_runs == 0
            || self.merge_delta_runs < 2
            || self.merge_max_input_runs < 2
            || self.merge_max_input_runs > self.max_open_runs
            || self.merge_max_output_bytes < TimelineSegmentRecord::encoded_size()
        {
            return Err(SegmentedTimelineError::InvalidConfiguration);
        }
        Ok(self)
    }
}

/// Durable proof returned only after run fsync and manifest publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeWriteReceipt {
    /// Native manifest generation containing every appended entry.
    pub manifest_generation: u64,
    /// Monotonic cumulative native bytes made durable.
    pub durable_end: u64,
    /// Stable hash of the logical source records.
    pub record_hash: u64,
    /// CRC32C of the published manifest.
    pub manifest_checksum: u32,
}

/// Complete native continuation. The full key prevents same-millisecond loss.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentedTimelineContinuation {
    /// Manifest view used to derive run positions.
    pub manifest_generation: u64,
    /// Physical partition being scanned.
    pub partition: TimelinePartitionKey,
    /// Records consumed from each run in this partition.
    pub run_positions: Vec<(u64, u64)>,
    /// Last full key emitted or de-duplicated.
    pub last_key: Option<TimelineSegmentKey>,
}

/// Bounded, stable native range page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SegmentedTimelinePage {
    /// Records ordered by `(due_ms, lane, timer_id, generation)`.
    pub records: Vec<TimelineSegmentRecord>,
    /// Cursor for the next page.
    pub continuation: Option<SegmentedTimelineContinuation>,
    /// Fixed-record logical bytes retained by this page.
    pub retained_bytes: usize,
}

/// Pin pairing a snapshot id to an immutable manifest generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeSnapshotPin {
    /// Shared Extended snapshot generation.
    pub snapshot_generation: u64,
    /// Native manifest generation protected from GC.
    pub manifest_generation: u64,
    /// Cumulative durable end protected by the pin.
    pub durable_end: u64,
    /// Manifest checksum used by bootstrap validation.
    pub manifest_checksum: u32,
}

/// Result of a bounded merge attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeMergeResult {
    /// Input runs replaced by a new base.
    pub merged_runs: usize,
    /// Unique records retained in the new base.
    pub output_records: usize,
    /// True when the selected partition exceeded the current I/O budget.
    pub deferred: bool,
    /// Published generation, or zero when no merge occurred.
    pub manifest_generation: u64,
}

/// Immutable-run long-horizon index. All mutation is serialized at manifest publication.
pub struct SegmentedTimeline {
    root: PathBuf,
    config: SegmentedTimelineConfig,
    manifest: Mutex<TimelineManifestV1>,
}

impl SegmentedTimeline {
    /// Opens the native index, validates only active run headers/footers, and retains orphan runs
    /// for explicit reconciliation.
    /// Reports failures through the canonical storage facade.
    pub fn open(store_root: impl AsRef<Path>, config: SegmentedTimelineConfig) -> Result<Self, StoreError> {
        Self::open_typed(store_root, config).map_err(|error| error.into_store_error(StoreOperation::Load))
    }

    fn open_typed(
        store_root: impl AsRef<Path>,
        config: SegmentedTimelineConfig,
    ) -> Result<Self, SegmentedTimelineError> {
        let config = config.validate()?;
        let root = store_root.as_ref().join(TIMELINE_DIRECTORY);
        let manifest = TimelineManifestV1::load(&root)?;
        for descriptor in &manifest.active_runs {
            let inspected = inspect_timeline_run(&root, &descriptor.relative_path)?;
            if inspected != *descriptor {
                return Err(SegmentedTimelineError::ManifestRunMismatch);
            }
        }
        Ok(Self {
            root,
            config,
            manifest: Mutex::new(manifest),
        })
    }

    /// Returns the current published manifest without exposing the mutation lock.
    pub fn manifest(&self) -> TimelineManifestV1 {
        self.manifest.lock().clone()
    }

    /// Verifies that an overlay checkpoint cannot reference future or non-durable native bytes.
    /// Reports failures through the canonical storage facade.
    pub fn validate_overlay_checkpoint(
        &self,
        manifest_generation: u64,
        durable_end: u64,
        manifest_checksum: u32,
    ) -> Result<(), StoreError> {
        self.validate_overlay_checkpoint_typed(manifest_generation, durable_end, manifest_checksum)
            .map_err(|error| error.into_store_error(StoreOperation::Load))
    }

    fn validate_overlay_checkpoint_typed(
        &self,
        manifest_generation: u64,
        durable_end: u64,
        manifest_checksum: u32,
    ) -> Result<(), SegmentedTimelineError> {
        let manifest = self.manifest.lock();
        if manifest_generation == 0
            || manifest_generation > manifest.generation
            || durable_end == 0
            || durable_end > manifest.durable_end
            || manifest_checksum == 0
            || manifest_generation == manifest.generation && manifest.checksum()? != manifest_checksum
        {
            return Err(SegmentedTimelineError::OverlayCheckpointMismatch);
        }
        Ok(())
    }

    /// Loads and verifies the exact archived manifest protected by a snapshot pin.
    /// Reports failures through the canonical storage facade.
    pub fn validate_snapshot_pin(&self, pin: NativeSnapshotPin) -> Result<(), StoreError> {
        self.validate_snapshot_pin_typed(pin)
            .map_err(|error| error.into_store_error(StoreOperation::Read))
    }

    fn validate_snapshot_pin_typed(&self, pin: NativeSnapshotPin) -> Result<(), SegmentedTimelineError> {
        let manifest = self.manifest.lock();
        if manifest.snapshot_pins.get(&pin.snapshot_generation).copied() != Some(pin.manifest_generation) {
            return Err(SegmentedTimelineError::UnknownSnapshotPin);
        }
        let archived = TimelineManifestV1::load_archive(&self.root, pin.manifest_generation)?;
        if archived.durable_end != pin.durable_end || archived.checksum()? != pin.manifest_checksum {
            return Err(SegmentedTimelineError::SnapshotManifestMismatch);
        }
        Ok(())
    }

    /// Copies every run reachable from a pinned manifest plus the immutable manifest itself.
    ///
    /// The returned file identities are relative to `target_root` and can be embedded directly in
    /// a cross-media snapshot manifest.
    /// Reports failures through the canonical storage facade.
    pub fn create_snapshot_files(
        &self,
        target_root: &Path,
        pin: NativeSnapshotPin,
    ) -> Result<Vec<TimerSnapshotFile>, StoreError> {
        self.create_snapshot_files_typed(target_root, pin)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }

    fn create_snapshot_files_typed(
        &self,
        target_root: &Path,
        pin: NativeSnapshotPin,
    ) -> Result<Vec<TimerSnapshotFile>, SegmentedTimelineError> {
        self.validate_snapshot_pin_typed(pin)?;
        let manifest = TimelineManifestV1::load_archive(&self.root, pin.manifest_generation)?;
        let mut files = Vec::with_capacity(manifest.active_runs.len().saturating_add(1));
        let manifest_relative = PathBuf::from("manifests").join(format!("{:020}.manifest", pin.manifest_generation));
        files.push(copy_snapshot_file(
            &self.root.join(&manifest_relative),
            &target_root.join(&manifest_relative),
            &manifest_relative,
        )?);
        for run in &manifest.active_runs {
            let relative = PathBuf::from(&run.relative_path);
            files.push(copy_snapshot_file(
                &self.root.join(&relative),
                &target_root.join(&relative),
                &relative,
            )?);
        }
        Ok(files)
    }

    /// Appends records as one or more partition-local delta runs.
    ///
    /// Existing identical keys are reused. A conflicting replay fails closed. Newly created run
    /// files are synced before one A/B manifest publication makes them reachable.
    /// Reports failures through the canonical storage facade.
    pub fn append_batch(&self, records: &[TimelineSegmentRecord]) -> Result<NativeWriteReceipt, StoreError> {
        self.append_batch_typed(records)
            .map_err(|error| error.into_store_error(StoreOperation::Append))
    }

    fn append_batch_typed(
        &self,
        records: &[TimelineSegmentRecord],
    ) -> Result<NativeWriteReceipt, SegmentedTimelineError> {
        if records.is_empty() {
            return Err(SegmentedTimelineError::EmptyBatch);
        }
        let record_hash = hash_records(records);
        let mut manifest = self.manifest.lock();
        let mut grouped = group_and_validate(records)?;
        self.remove_idempotent_records(&manifest, &mut grouped)?;
        if grouped.values().all(Vec::is_empty) {
            return Ok(NativeWriteReceipt {
                manifest_generation: manifest.generation,
                durable_end: manifest.durable_end,
                record_hash,
                manifest_checksum: manifest.checksum()?,
            });
        }

        let created_generation = manifest
            .generation
            .checked_add(1)
            .ok_or(SegmentedTimelineError::GenerationExhausted)?;
        let mut candidate = manifest.clone();
        for (partition, partition_records) in grouped {
            if partition_records.is_empty() {
                continue;
            }
            let run_hash = hash_records(&partition_records);
            let run_id = stable_run_id(run_hash, candidate.next_run_id);
            let relative_path = run_path(created_generation, partition, TimelineRunKind::Delta, run_id);
            let descriptor = match inspect_timeline_run(&self.root, &relative_path) {
                Ok(existing) => {
                    validate_existing_run(&self.root, &existing, &partition_records)?;
                    existing
                }
                Err(TimelineSegmentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    write_timeline_run(
                        &self.root,
                        &relative_path,
                        TimelineRunKind::Delta,
                        partition,
                        run_id,
                        created_generation,
                        &partition_records,
                    )?
                }
                Err(error) => return Err(error.into()),
            };
            candidate.durable_end = candidate
                .durable_end
                .checked_add(run_physical_bytes(&descriptor)?)
                .ok_or(SegmentedTimelineError::LengthOverflow)?;
            candidate.next_run_id = candidate.next_run_id.saturating_add(1).max(1);
            candidate.active_runs.push(descriptor);
        }
        candidate.active_runs.sort_by(run_order);
        let published = candidate.publish_next(&self.root)?;
        let receipt = NativeWriteReceipt {
            manifest_generation: published.generation,
            durable_end: published.durable_end,
            record_hash,
            manifest_checksum: published.checksum()?,
        };
        *manifest = published;
        Ok(receipt)
    }

    /// Reads one bounded page. At most one partition's runs are open simultaneously.
    /// Reports failures through the canonical storage facade.
    pub fn scan_due(
        &self,
        from_exclusive: Option<TimelineSegmentKey>,
        due_exclusive_ms: i64,
        max_records: usize,
        max_bytes: usize,
        continuation: Option<SegmentedTimelineContinuation>,
    ) -> Result<SegmentedTimelinePage, StoreError> {
        self.scan_due_typed(from_exclusive, due_exclusive_ms, max_records, max_bytes, continuation)
            .map_err(|error| error.into_store_error(StoreOperation::Read))
    }

    fn scan_due_typed(
        &self,
        from_exclusive: Option<TimelineSegmentKey>,
        due_exclusive_ms: i64,
        max_records: usize,
        max_bytes: usize,
        continuation: Option<SegmentedTimelineContinuation>,
    ) -> Result<SegmentedTimelinePage, SegmentedTimelineError> {
        if max_records == 0 || max_bytes < TimelineSegmentRecord::encoded_size() || due_exclusive_ms < 0 {
            return Err(SegmentedTimelineError::InvalidScanBudget);
        }
        let manifest = self.manifest.lock().clone();
        let lower_due = from_exclusive.map_or(0, |key| key.due_time_ms.max(0));
        let mut partitions = manifest
            .runs_in_range(lower_due, due_exclusive_ms)
            .into_iter()
            .map(|run| run.partition)
            .collect::<Vec<_>>();
        partitions.sort_unstable();
        partitions.dedup();
        if let Some(cursor) = continuation.as_ref() {
            partitions.retain(|partition| *partition >= cursor.partition);
        } else if let Some(from) = from_exclusive {
            let from_partition = from.partition()?;
            partitions.retain(|partition| *partition >= from_partition);
        }
        for (partition_index, partition) in partitions.iter().copied().enumerate() {
            let runs = manifest.partition_runs(partition);
            if runs.len() > self.config.max_open_runs {
                return Err(SegmentedTimelineError::ReadAmplificationLimit {
                    runs: runs.len(),
                    limit: self.config.max_open_runs,
                });
            }
            let positions = continuation
                .as_ref()
                .filter(|cursor| cursor.manifest_generation == manifest.generation && cursor.partition == partition)
                .map(|cursor| cursor.run_positions.iter().copied().collect::<HashMap<_, _>>())
                .unwrap_or_default();
            let last_key = continuation
                .as_ref()
                .filter(|cursor| cursor.partition == partition)
                .and_then(|cursor| cursor.last_key)
                .filter(|key| key.partition().ok() == Some(partition))
                .or_else(|| from_exclusive.filter(|key| key.partition().ok() == Some(partition)));
            let mut page = self.scan_partition(
                &manifest,
                partition,
                runs,
                positions,
                last_key,
                due_exclusive_ms,
                max_records,
                max_bytes,
            )?;
            if !page.records.is_empty() || page.continuation.is_some() {
                if page.continuation.is_none() && partition_index + 1 < partitions.len() {
                    page.continuation = Some(SegmentedTimelineContinuation {
                        manifest_generation: manifest.generation,
                        partition: partitions[partition_index + 1],
                        run_positions: Vec::new(),
                        last_key: page.records.last().map(|record| record.key),
                    });
                }
                return Ok(page);
            }
        }
        Ok(SegmentedTimelinePage::default())
    }

    /// Returns the exact active record for one full key, if present.
    /// Reports failures through the canonical storage facade.
    pub fn get(&self, key: TimelineSegmentKey) -> Result<Option<TimelineSegmentRecord>, StoreError> {
        self.get_typed(key)
            .map_err(|error| error.into_store_error(StoreOperation::Read))
    }

    fn get_typed(&self, key: TimelineSegmentKey) -> Result<Option<TimelineSegmentRecord>, SegmentedTimelineError> {
        let manifest = self.manifest.lock().clone();
        let mut found = None;
        for descriptor in manifest.partition_runs(key.partition()?) {
            if key.due_time_ms < descriptor.min_due_time_ms || key.due_time_ms > descriptor.max_due_time_ms {
                continue;
            }
            let mut reader = TimelineRunReader::open(&self.root, descriptor)?;
            while let Some(record) = reader.read_next()? {
                match record.key.cmp(&key) {
                    Ordering::Less => continue,
                    Ordering::Greater => break,
                    Ordering::Equal => match found {
                        None => found = Some(record),
                        Some(existing) if existing == record => {}
                        Some(_) => return Err(SegmentedTimelineError::ConflictingDuplicate),
                    },
                }
            }
        }
        Ok(found)
    }

    /// Pins the current native generation for one shared Extended snapshot.
    /// Reports failures through the canonical storage facade.
    pub fn pin_snapshot(&self, snapshot_generation: u64) -> Result<NativeSnapshotPin, StoreError> {
        self.pin_snapshot_typed(snapshot_generation)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }

    fn pin_snapshot_typed(&self, snapshot_generation: u64) -> Result<NativeSnapshotPin, SegmentedTimelineError> {
        if snapshot_generation == 0 {
            return Err(SegmentedTimelineError::InvalidSnapshotGeneration);
        }
        let mut manifest = self.manifest.lock();
        if manifest.snapshot_pins.contains_key(&snapshot_generation) {
            return Err(SegmentedTimelineError::SnapshotAlreadyPinned);
        }
        let pinned_generation = manifest.generation;
        manifest.archive(&self.root)?;
        let mut candidate = manifest.clone();
        candidate.snapshot_pins.insert(snapshot_generation, pinned_generation);
        let published = candidate.publish_next(&self.root)?;
        let pin = NativeSnapshotPin {
            snapshot_generation,
            manifest_generation: pinned_generation,
            durable_end: manifest.durable_end,
            manifest_checksum: manifest.checksum()?,
        };
        *manifest = published;
        Ok(pin)
    }

    /// Releases a previously persisted snapshot pin and then reclaims unreachable runs.
    /// Reports failures through the canonical storage facade.
    pub fn release_snapshot(&self, pin: NativeSnapshotPin) -> Result<usize, StoreError> {
        self.release_snapshot_typed(pin)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }

    fn release_snapshot_typed(&self, pin: NativeSnapshotPin) -> Result<usize, SegmentedTimelineError> {
        let mut manifest = self.manifest.lock();
        if manifest.snapshot_pins.get(&pin.snapshot_generation).copied() != Some(pin.manifest_generation) {
            return Err(SegmentedTimelineError::UnknownSnapshotPin);
        }
        let mut candidate = manifest.clone();
        candidate.snapshot_pins.remove(&pin.snapshot_generation);
        let published = candidate.publish_next(&self.root)?;
        *manifest = published;
        self.collect_garbage_locked(&mut manifest)
    }

    /// Merges one eligible partition within explicit run and output-byte budgets.
    ///
    /// `retain` is evaluated only after a stable de-duplicated merge. The caller must enforce
    /// terminal, replication, grace-period, and snapshot fences before returning `false`.
    #[allow(
        dead_code,
        reason = "exercised by the in-crate merge scenarios; production merge scheduling arrives with the store merge driver"
    )]
    pub(crate) fn merge_one<F>(&self, mut retain: F) -> Result<NativeMergeResult, SegmentedTimelineError>
    where
        F: FnMut(&TimelineSegmentRecord) -> bool,
    {
        self.merge_one_prioritized(&mut retain, || false)
    }

    /// Merges one bounded partition unless due delivery currently has priority.
    ///
    /// A yielded merge publishes no run or manifest. Its immutable inputs remain reachable, so a
    /// later invocation resumes safely without a separate recovery protocol.
    #[allow(
        dead_code,
        reason = "exercised by the in-crate merge scenarios; production merge scheduling arrives with the store merge driver"
    )]
    pub(crate) fn merge_one_prioritized<F, P>(
        &self,
        mut retain: F,
        mut due_delivery_pending: P,
    ) -> Result<NativeMergeResult, SegmentedTimelineError>
    where
        F: FnMut(&TimelineSegmentRecord) -> bool,
        P: FnMut() -> bool,
    {
        if due_delivery_pending() {
            return Ok(NativeMergeResult {
                deferred: true,
                ..NativeMergeResult::default()
            });
        }
        let mut manifest = self.manifest.lock();
        let mut partitions = BTreeMap::<TimelinePartitionKey, Vec<TimelineRunDescriptor>>::new();
        for run in &manifest.active_runs {
            partitions.entry(run.partition).or_default().push(run.clone());
        }
        let Some((partition, mut runs)) = partitions.into_iter().find(|(_, runs)| {
            runs.iter().filter(|run| run.kind == TimelineRunKind::Delta).count() >= self.config.merge_delta_runs
        }) else {
            return Ok(NativeMergeResult::default());
        };
        runs.sort_by(run_order);
        if runs.len() > self.config.merge_max_input_runs {
            runs.truncate(self.config.merge_max_input_runs);
        }
        let input_bytes = runs.iter().try_fold(0usize, |sum, run| {
            usize::try_from(run.logical_bytes)
                .ok()
                .and_then(|bytes| sum.checked_add(bytes))
                .ok_or(SegmentedTimelineError::LengthOverflow)
        })?;
        if input_bytes > self.config.merge_max_output_bytes {
            return Ok(NativeMergeResult {
                deferred: true,
                ..NativeMergeResult::default()
            });
        }
        let records = self.read_merged_runs(&manifest, &runs, None, i64::MAX, usize::MAX, usize::MAX)?;
        if due_delivery_pending() {
            return Ok(NativeMergeResult {
                deferred: true,
                ..NativeMergeResult::default()
            });
        }
        let retained = records.into_iter().filter(|record| retain(record)).collect::<Vec<_>>();
        if retained.is_empty() {
            return Err(SegmentedTimelineError::EmptyMergeOutput);
        }
        let created_generation = manifest
            .generation
            .checked_add(1)
            .ok_or(SegmentedTimelineError::GenerationExhausted)?;
        let run_id = stable_run_id(hash_records(&retained), manifest.next_run_id);
        let relative_path = run_path(created_generation, partition, TimelineRunKind::Base, run_id);
        let descriptor = write_timeline_run(
            &self.root,
            &relative_path,
            TimelineRunKind::Base,
            partition,
            run_id,
            created_generation,
            &retained,
        )?;
        let replaced: Vec<_> = runs.iter().map(|run| run.relative_path.clone()).collect();
        let mut candidate = manifest.clone();
        candidate
            .active_runs
            .retain(|run| !replaced.iter().any(|path| path == &run.relative_path));
        candidate.active_runs.push(descriptor.clone());
        candidate.active_runs.sort_by(run_order);
        candidate.garbage_runs.extend(replaced);
        candidate.next_run_id = candidate.next_run_id.saturating_add(1).max(1);
        candidate.durable_end = candidate
            .durable_end
            .checked_add(run_physical_bytes(&descriptor)?)
            .ok_or(SegmentedTimelineError::LengthOverflow)?;
        let published = candidate.publish_next(&self.root)?;
        let result = NativeMergeResult {
            merged_runs: runs.len(),
            output_records: retained.len(),
            deferred: false,
            manifest_generation: published.generation,
        };
        *manifest = published;
        let _ = self.collect_garbage_locked(&mut manifest)?;
        let result = NativeMergeResult {
            manifest_generation: manifest.generation,
            ..result
        };
        Ok(result)
    }

    /// Deletes a whole partition only when no snapshot generation is pinned.
    /// Reports failures through the canonical storage facade.
    pub fn delete_partition(&self, partition: TimelinePartitionKey) -> Result<usize, StoreError> {
        self.delete_partition_typed(partition)
            .map_err(|error| error.into_store_error(StoreOperation::Admin))
    }

    fn delete_partition_typed(&self, partition: TimelinePartitionKey) -> Result<usize, SegmentedTimelineError> {
        let mut manifest = self.manifest.lock();
        if !manifest.snapshot_pins.is_empty() {
            return Err(SegmentedTimelineError::SnapshotPinned);
        }
        let removed: Vec<_> = manifest
            .active_runs
            .iter()
            .filter(|run| run.partition == partition)
            .map(|run| run.relative_path.clone())
            .collect();
        if removed.is_empty() {
            return Ok(0);
        }
        let mut candidate = manifest.clone();
        candidate.active_runs.retain(|run| run.partition != partition);
        candidate.garbage_runs.extend(removed);
        let published = candidate.publish_next(&self.root)?;
        *manifest = published;
        self.collect_garbage_locked(&mut manifest)
    }

    /// Lists sealed or partial run files not reachable from the current manifest.
    #[allow(
        dead_code,
        reason = "exercised by the in-crate merge scenarios; production merge scheduling arrives with the store merge driver"
    )]
    pub(crate) fn orphan_runs(&self) -> Result<Vec<PathBuf>, SegmentedTimelineError> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let manifest = self.manifest.lock();
        let reachable = manifest
            .active_runs
            .iter()
            .map(|run| self.root.join(&run.relative_path))
            .collect::<Vec<_>>();
        let garbage = manifest
            .garbage_runs
            .iter()
            .map(|path| self.root.join(path))
            .collect::<Vec<_>>();
        let mut orphans = Vec::new();
        collect_run_paths(&self.root, &mut orphans)?;
        orphans.retain(|path| !reachable.contains(path) && !garbage.contains(path));
        orphans.sort();
        Ok(orphans)
    }

    fn remove_idempotent_records(
        &self,
        manifest: &TimelineManifestV1,
        grouped: &mut BTreeMap<TimelinePartitionKey, Vec<TimelineSegmentRecord>>,
    ) -> Result<(), SegmentedTimelineError> {
        for (partition, requested) in grouped {
            if requested.is_empty() {
                continue;
            }
            let mut existing = BTreeMap::new();
            for descriptor in manifest.partition_runs(*partition) {
                let mut reader = TimelineRunReader::open(&self.root, descriptor)?;
                while let Some(record) = reader.read_next()? {
                    if let Some(previous) = existing.insert(record.key, record) {
                        if previous != record {
                            return Err(SegmentedTimelineError::ConflictingDuplicate);
                        }
                    }
                }
            }
            requested.retain(|record| match existing.get(&record.key) {
                Some(value) if value == record => false,
                Some(_) => true,
                None => true,
            });
            if requested
                .iter()
                .any(|record| existing.get(&record.key).is_some_and(|value| value != record))
            {
                return Err(SegmentedTimelineError::ConflictingDuplicate);
            }
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "range scan receives independent correctness and resource boundaries"
    )]
    fn scan_partition(
        &self,
        manifest: &TimelineManifestV1,
        partition: TimelinePartitionKey,
        runs: Vec<TimelineRunDescriptor>,
        positions: HashMap<u64, u64>,
        last_key: Option<TimelineSegmentKey>,
        due_exclusive_ms: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<SegmentedTimelinePage, SegmentedTimelineError> {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();
        let mut consumed = BTreeMap::new();
        for descriptor in runs {
            let run_id = descriptor.run_id;
            let mut reader = TimelineRunReader::open(&self.root, descriptor)?;
            let skip = positions.get(&run_id).copied().unwrap_or_default();
            reader.seek_to(skip)?;
            consumed.insert(run_id, skip);
            let reader_index = readers.len();
            if let Some(record) = next_after(&mut reader, last_key, due_exclusive_ms)? {
                consumed.insert(run_id, reader.position().saturating_sub(1));
                heap.push(HeapRecord {
                    record,
                    reader_index,
                    run_id,
                });
            }
            readers.push(reader);
        }
        let mut page = SegmentedTimelinePage::default();
        let mut last_seen = last_key;
        while !heap.is_empty() {
            let next_bytes = page
                .retained_bytes
                .saturating_add(TimelineSegmentRecord::encoded_size());
            if page.records.len() >= max_records || next_bytes > max_bytes {
                break;
            }
            let head = heap.pop().ok_or(SegmentedTimelineError::HeapInvariant)?;
            consumed.insert(head.run_id, readers[head.reader_index].position());
            match last_seen {
                Some(key) if key == head.record.key => {
                    if page.records.last().is_some_and(|record| *record != head.record) {
                        return Err(SegmentedTimelineError::ConflictingDuplicate);
                    }
                }
                Some(key) if key > head.record.key => return Err(SegmentedTimelineError::OrderingViolation),
                _ => {
                    page.retained_bytes = next_bytes;
                    page.records.push(head.record);
                    last_seen = Some(head.record.key);
                }
            }
            if let Some(record) = next_after(&mut readers[head.reader_index], last_seen, due_exclusive_ms)? {
                consumed.insert(head.run_id, readers[head.reader_index].position().saturating_sub(1));
                heap.push(HeapRecord {
                    record,
                    reader_index: head.reader_index,
                    run_id: head.run_id,
                });
            }
        }
        if !heap.is_empty() {
            page.continuation = Some(SegmentedTimelineContinuation {
                manifest_generation: manifest.generation,
                partition,
                run_positions: consumed.into_iter().collect(),
                last_key: last_seen,
            });
        }
        Ok(page)
    }

    #[allow(
        dead_code,
        reason = "exercised by the in-crate merge scenarios; production merge scheduling arrives with the store merge driver"
    )]
    fn read_merged_runs(
        &self,
        manifest: &TimelineManifestV1,
        runs: &[TimelineRunDescriptor],
        from_exclusive: Option<TimelineSegmentKey>,
        due_exclusive_ms: i64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<TimelineSegmentRecord>, SegmentedTimelineError> {
        let partition = runs
            .first()
            .map(|run| run.partition)
            .ok_or(SegmentedTimelineError::EmptyBatch)?;
        if runs.iter().any(|run| run.partition != partition) {
            return Err(SegmentedTimelineError::PartitionMismatch);
        }
        Ok(self
            .scan_partition(
                manifest,
                partition,
                runs.to_vec(),
                HashMap::new(),
                from_exclusive,
                due_exclusive_ms,
                max_records,
                max_bytes,
            )?
            .records)
    }

    fn collect_garbage_locked(&self, manifest: &mut TimelineManifestV1) -> Result<usize, SegmentedTimelineError> {
        if !manifest.snapshot_pins.is_empty() || manifest.garbage_runs.is_empty() {
            return Ok(0);
        }
        let mut deleted = 0usize;
        let garbage = manifest.garbage_runs.clone();
        for relative_path in &garbage {
            validate_relative_path(relative_path)?;
            match std::fs::remove_file(self.root.join(relative_path)) {
                Ok(()) => deleted = deleted.saturating_add(1),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let mut candidate = manifest.clone();
        candidate.garbage_runs.clear();
        *manifest = candidate.publish_next(&self.root)?;
        Ok(deleted)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HeapRecord {
    record: TimelineSegmentRecord,
    reader_index: usize,
    run_id: u64,
}

impl Ord for HeapRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .key
            .cmp(&self.record.key)
            .then_with(|| other.run_id.cmp(&self.run_id))
    }
}

impl PartialOrd for HeapRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn next_after(
    reader: &mut TimelineRunReader,
    last_key: Option<TimelineSegmentKey>,
    due_exclusive_ms: i64,
) -> Result<Option<TimelineSegmentRecord>, SegmentedTimelineError> {
    while let Some(record) = reader.read_next()? {
        if record.key.due_time_ms >= due_exclusive_ms {
            return Ok(None);
        }
        if last_key.is_some_and(|key| record.key <= key) {
            continue;
        }
        return Ok(Some(record));
    }
    Ok(None)
}

fn group_and_validate(
    records: &[TimelineSegmentRecord],
) -> Result<BTreeMap<TimelinePartitionKey, Vec<TimelineSegmentRecord>>, SegmentedTimelineError> {
    let mut grouped = BTreeMap::<TimelinePartitionKey, Vec<TimelineSegmentRecord>>::new();
    for record in records {
        grouped.entry(record.key.partition()?).or_default().push(*record);
    }
    for partition_records in grouped.values_mut() {
        partition_records.sort_unstable_by_key(|record| record.key);
        for pair in partition_records.windows(2) {
            if pair[0].key == pair[1].key && pair[0] != pair[1] {
                return Err(SegmentedTimelineError::ConflictingDuplicate);
            }
        }
        partition_records.dedup();
    }
    Ok(grouped)
}

fn validate_existing_run(
    root: &Path,
    descriptor: &TimelineRunDescriptor,
    expected: &[TimelineSegmentRecord],
) -> Result<(), SegmentedTimelineError> {
    if descriptor.record_count != u64::try_from(expected.len()).unwrap_or(u64::MAX) {
        return Err(SegmentedTimelineError::RunIdCollision);
    }
    let mut reader = TimelineRunReader::open(root, descriptor.clone())?;
    for expected_record in expected {
        if reader.read_next()? != Some(*expected_record) {
            return Err(SegmentedTimelineError::RunIdCollision);
        }
    }
    if reader.read_next()?.is_some() {
        return Err(SegmentedTimelineError::RunIdCollision);
    }
    Ok(())
}

fn stable_run_id(content_hash: u64, fallback: u64) -> u64 {
    if content_hash == 0 {
        fallback.max(1)
    } else {
        content_hash
    }
}

fn hash_records(records: &[TimelineSegmentRecord]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for record in records {
        for byte in record
            .key
            .due_time_ms
            .to_be_bytes()
            .into_iter()
            .chain(record.key.lane.to_be_bytes())
            .chain(record.key.timer_id.get().to_be_bytes())
            .chain(record.key.generation.get().to_be_bytes())
            .chain(record.source_cq_offset.get().to_be_bytes())
            .chain(record.source_physical_offset.to_be_bytes())
            .chain(record.payload.checksum().to_be_bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
    }
    hash
}

fn run_path(generation: u64, partition: TimelinePartitionKey, kind: TimelineRunKind, run_id: u64) -> String {
    let prefix = match kind {
        TimelineRunKind::Base => "base",
        TimelineRunKind::Delta => "delta",
    };
    format!(
        "generations/{generation:020}/day={:010}/hour={:02}/lane={:05}/{prefix}-{run_id:020}.run",
        partition.due_day_utc, partition.due_hour_utc, partition.lane
    )
}

fn run_physical_bytes(descriptor: &TimelineRunDescriptor) -> Result<u64, SegmentedTimelineError> {
    descriptor
        .logical_bytes
        .checked_add(184)
        .ok_or(SegmentedTimelineError::LengthOverflow)
}

fn run_order(left: &TimelineRunDescriptor, right: &TimelineRunDescriptor) -> Ordering {
    left.partition
        .cmp(&right.partition)
        .then_with(|| left.kind.cmp(&right.kind))
        .then_with(|| left.run_id.cmp(&right.run_id))
}

#[allow(dead_code, reason = "used by the in-crate merge scenarios")]
fn collect_run_paths(directory: &Path, output: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_run_paths(&entry.path(), output)?;
        } else if entry.path().extension().is_some_and(|extension| extension == "run") {
            output.push(entry.path());
        }
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<(), SegmentedTimelineError> {
    if path.is_empty() || path.starts_with('/') || path.contains("..") {
        return Err(SegmentedTimelineError::UnsafePath);
    }
    Ok(())
}

fn copy_snapshot_file(
    source_path: &Path,
    target_path: &Path,
    relative_path: &Path,
) -> Result<TimerSnapshotFile, SegmentedTimelineError> {
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut source = File::open(source_path)?;
    let length = source.metadata()?.len();
    let mut target = OpenOptions::new().create_new(true).write(true).open(target_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        target.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
    }
    target.sync_data()?;
    Ok(TimerSnapshotFile {
        relative_path: relative_path.to_string_lossy().replace('\\', "/"),
        length,
        sha256: hex::encode(hasher.finalize()),
    })
}

/// Native segmented Timeline failure.
#[derive(Debug, Error)]
pub(crate) enum SegmentedTimelineError {
    /// Underlying filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Immutable run codec failed.
    #[error(transparent)]
    Segment(#[from] TimelineSegmentError),
    /// A/B manifest failed.
    #[error(transparent)]
    Manifest(#[from] TimelineManifestError),
    /// Resource limits are zero or inconsistent.
    #[error("invalid segmented Timeline configuration")]
    InvalidConfiguration,
    /// Write batches must contain at least one record.
    #[error("segmented Timeline write batch is empty")]
    EmptyBatch,
    /// Page limits cannot retain one fixed record.
    #[error("invalid segmented Timeline scan budget")]
    InvalidScanBudget,
    /// One partition exceeds the hard open-reader limit and must be merged first.
    #[error("segmented Timeline partition has {runs} runs, exceeding reader limit {limit}")]
    ReadAmplificationLimit {
        /// Active run count.
        runs: usize,
        /// Configured reader limit.
        limit: usize,
    },
    /// Manifest metadata does not match a reachable sealed run.
    #[error("segmented Timeline manifest/run mismatch")]
    ManifestRunMismatch,
    /// Replayed full key differs from its durable value.
    #[error("conflicting duplicate native Timeline key")]
    ConflictingDuplicate,
    /// Content-derived run id resolved to different bytes.
    #[error("native Timeline run id collision")]
    RunIdCollision,
    /// Merge candidates unexpectedly span partitions.
    #[error("native Timeline merge partition mismatch")]
    #[allow(dead_code, reason = "reported by the in-crate merge scenarios")]
    PartitionMismatch,
    /// A merge cannot publish an empty base without an explicit partition tombstone protocol.
    #[error("native Timeline merge produced an empty base")]
    #[allow(dead_code, reason = "reported by the in-crate merge scenarios")]
    EmptyMergeOutput,
    /// Ordering invariant was violated while merging runs.
    #[error("native Timeline merge ordering violation")]
    OrderingViolation,
    /// Heap unexpectedly became empty after a successful peek.
    #[error("native Timeline merge heap invariant violated")]
    HeapInvariant,
    /// Snapshot ids are non-zero.
    #[error("invalid native Timeline snapshot generation")]
    InvalidSnapshotGeneration,
    /// Snapshot id is already pinned.
    #[error("native Timeline snapshot is already pinned")]
    SnapshotAlreadyPinned,
    /// Snapshot pin does not match the persisted manifest.
    #[error("unknown native Timeline snapshot pin")]
    UnknownSnapshotPin,
    /// Destructive maintenance is fenced by a snapshot.
    #[error("native Timeline snapshot pin prevents reclamation")]
    SnapshotPinned,
    /// Relative run path failed validation.
    #[error("unsafe native Timeline relative path")]
    UnsafePath,
    /// Generation cannot advance.
    #[error("native Timeline generation exhausted")]
    GenerationExhausted,
    /// Byte counters overflowed.
    #[error("native Timeline length overflow")]
    LengthOverflow,
    /// Overlay checkpoint references future native bytes or an inconsistent current manifest.
    #[error("native Timeline overlay checkpoint mismatch")]
    OverlayCheckpointMismatch,
    /// Archived snapshot manifest does not match its pin.
    #[error("native Timeline snapshot manifest mismatch")]
    SnapshotManifestMismatch,
}

impl SegmentedTimelineError {
    /// Promotes this leaf into the canonical storage facade exactly once.
    ///
    /// Filesystem faults keep their typed I/O source, nested segment or
    /// manifest evidence is corrupted state, and the remaining typed failures
    /// follow the owning operation. The complete leaf is preserved as the
    /// typed source.
    pub(crate) fn into_store_error(self, operation: StoreOperation) -> StoreError {
        let descriptor = match (&self, operation) {
            (Self::Io(_), _) => &rocketmq_error::STORAGE_IO_FAILED,
            (Self::Segment(_) | Self::Manifest(_), _) => &rocketmq_error::STORAGE_STATE_CORRUPTED,
            (_, StoreOperation::Load | StoreOperation::Read | StoreOperation::QueryOffset) => {
                &rocketmq_error::STORAGE_READ_FAILED
            }
            (_, StoreOperation::Append | StoreOperation::Flush | StoreOperation::AppendDerived) => {
                &rocketmq_error::STORAGE_WRITE_FAILED
            }
            _ => &rocketmq_error::STORAGE_INTERNAL_FAILURE,
        };
        StoreError::new(descriptor, operation).with_source(self)
    }
}
