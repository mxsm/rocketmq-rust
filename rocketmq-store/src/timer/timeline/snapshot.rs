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

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_store_api::path_to_file_uri;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerSnapshotManifest;
use rocketmq_store_api::TimerTimelineIndexKind;
use rocketmq_store_api::TIMER_SNAPSHOT_SCHEMA_VERSION;
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
use rocketmq_store_local::timer::segmented_timeline::NativeSnapshotPin;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline;
use rocketmq_store_rocksdb::timer::checkpoint::TimelineCheckpointKind;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineSnapshotPin;
use thiserror::Error;

use super::ShadowTimelineMaterializer;
use super::TimelineCompletionReconciler;
use crate::timer::clock::TimerClockSafety;
use crate::timer::clock::TimerClockState;
use crate::timer::role::TimerRoleState;

const MANIFEST_NAME: &str = "timer-snapshot-manifest.json";

/// Consistent payload/Timeline artifact creator. Calls are serialized per store.
pub(crate) struct TimelineSnapshotManager {
    snapshot_root: PathBuf,
    timeline: Arc<RocksDbTimelineIndex>,
    payload: Arc<TimerPayloadStore>,
    materializer: Arc<ShadowTimelineMaterializer>,
    completion: Arc<TimelineCompletionReconciler>,
    role: Arc<TimerRoleState>,
    clock: Arc<TimerClockSafety>,
    activation_epoch: TimerEngineEpoch,
    native_timeline: Option<Arc<SegmentedTimeline>>,
    create_lock: Mutex<()>,
}

impl TimelineSnapshotManager {
    #[allow(
        clippy::too_many_arguments,
        reason = "snapshot consistency requires each independent durable capability"
    )]
    pub(crate) fn new(
        store_root: impl AsRef<Path>,
        timeline: Arc<RocksDbTimelineIndex>,
        payload: Arc<TimerPayloadStore>,
        materializer: Arc<ShadowTimelineMaterializer>,
        completion: Arc<TimelineCompletionReconciler>,
        role: Arc<TimerRoleState>,
        clock: Arc<TimerClockSafety>,
        activation_epoch: TimerEngineEpoch,
    ) -> Self {
        Self {
            snapshot_root: store_root.as_ref().join("timer-extended").join("snapshots-v1"),
            timeline,
            payload,
            materializer,
            completion,
            role,
            clock,
            activation_epoch,
            native_timeline: None,
            create_lock: Mutex::new(()),
        }
    }

    /// Enables a segmented owner while retaining the RocksDB overlay checkpoint.
    pub(crate) fn with_segmented_timeline(mut self, native_timeline: Arc<SegmentedTimeline>) -> Self {
        self.native_timeline = Some(native_timeline);
        self
    }

    /// Creates and atomically publishes one artifact. Pins remain until explicit release.
    pub(crate) fn create(&self) -> Result<TimerSnapshotManifest, TimelineSnapshotError> {
        let _create_guard = self.create_lock.lock();
        if self.clock.observe().state == TimerClockState::Unsafe {
            return Err(TimelineSnapshotError::ClockUnsafe);
        }
        let role_epoch = self.role.epoch();
        if role_epoch == 0 || self.activation_epoch.get() == 0 {
            return Err(TimelineSnapshotError::MissingEpoch);
        }
        std::fs::create_dir_all(&self.snapshot_root)?;
        let generation = self.next_generation()?;
        let building = self.snapshot_root.join(format!(".snapshot-{generation:020}.building"));
        let published = self.snapshot_root.join(format!("snapshot-{generation:020}"));
        if building.exists() || published.exists() {
            return Err(TimelineSnapshotError::ArtifactExists(generation));
        }
        std::fs::create_dir(&building)?;

        let barrier = self.materializer.snapshot_barrier();
        let _snapshot_guard = barrier.write();
        let gc_fence = TimelineKeyV1 {
            due_time_ms: i64::MIN,
            lane: 0,
            timer_id: TimerId::new(0),
            generation: TimerGeneration::new(0),
        };
        let native_pin = match self.native_timeline.as_ref() {
            Some(native) => Some(
                native
                    .pin_snapshot(generation)?
                    .expect("snapshot generation is generated as non-zero"),
            ),
            None => None,
        };
        let pin = self.timeline.pin_snapshot_generation(gc_fence, generation)?;
        let mut native_files = match (self.native_timeline.as_ref(), native_pin) {
            (Some(native), Some(native_pin)) => native.create_snapshot_files(&building.join("native"), native_pin)?,
            _ => Vec::new(),
        };
        for file in &mut native_files {
            file.relative_path = format!("native/{}", file.relative_path);
        }
        let payload_root = building.join("payload");
        let mut payload_files = self.payload.create_snapshot_files(&payload_root, generation)?;
        for file in &mut payload_files {
            file.relative_path = format!("payload/{}", file.relative_path);
        }
        let (source_cq_cursor, source_physical_cursor, format_fingerprint) =
            self.materializer.snapshot_source_cursors()?;
        let due_time_cursor_ms = self
            .timeline
            .checkpoint(TimelineCheckpointKind::Due, 0)?
            .map(|checkpoint| checkpoint.due_cursor.due_time_ms())
            .unwrap_or_default()
            .max(0);
        let completion_physical_cursor = self.completion.completion_physical_cursor()?.max(0);
        let timeline_sequence = self.timeline.store().latest_sequence_number()?.max(1);
        let checkpoint_dir = building.join("timeline");
        self.timeline.store().create_checkpoint_blocking(checkpoint_dir)?;

        let checkpoint_uri = path_to_file_uri(&published.join("timeline"));
        let mut manifest = TimerSnapshotManifest {
            schema_version: TIMER_SNAPSHOT_SCHEMA_VERSION,
            generation,
            source_cq_cursor,
            source_physical_cursor,
            due_time_cursor_ms,
            completion_physical_cursor,
            timeline_sequence,
            timeline_index_kind: if native_pin.is_some() {
                TimerTimelineIndexKind::Segmented
            } else {
                TimerTimelineIndexKind::RocksDb
            },
            native_manifest_generation: native_pin.map(|pin| pin.manifest_generation),
            native_durable_end: native_pin.map(|pin| pin.durable_end),
            native_manifest_checksum: native_pin.map(|pin| pin.manifest_checksum),
            native_files,
            role_epoch,
            activation_epoch: self.activation_epoch.get(),
            format_fingerprint,
            timeline_checkpoint_uri: checkpoint_uri,
            payload_files,
            checksum: String::new(),
        };
        manifest.seal()?;
        manifest.validate_artifact_files(&building)?;
        write_manifest(&building.join(MANIFEST_NAME), &manifest)?;
        std::fs::rename(&building, &published)?;
        debug_assert_eq!(pin.generation, generation);
        Ok(manifest)
    }

    /// Loads the newest atomically published local artifact, if one exists.
    pub(crate) fn latest_published(&self) -> Result<Option<TimerSnapshotManifest>, TimelineSnapshotError> {
        if !self.snapshot_root.exists() {
            return Ok(None);
        }
        let mut generations = Vec::new();
        for entry in std::fs::read_dir(&self.snapshot_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let Some(generation) = name
                .to_str()
                .and_then(|name| name.strip_prefix("snapshot-"))
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            generations.push((generation, entry.path()));
        }
        generations.sort_unstable_by_key(|(generation, _)| *generation);
        let Some((_, directory)) = generations.pop() else {
            return Ok(None);
        };
        let bytes = std::fs::read(directory.join(MANIFEST_NAME))?;
        let manifest: TimerSnapshotManifest = serde_json::from_slice(&bytes)?;
        manifest.validate_artifact_files(&directory)?;
        Ok(Some(manifest))
    }

    /// Releases Timeline and payload GC pins after artifact replication/installation is confirmed.
    pub(crate) fn release(&self, manifest: &TimerSnapshotManifest) -> Result<(), TimelineSnapshotError> {
        manifest.validate()?;
        let pin = TimelineSnapshotPin {
            generation: manifest.generation,
            gc_fence: TimelineKeyV1 {
                due_time_ms: i64::MIN,
                lane: 0,
                timer_id: TimerId::new(0),
                generation: TimerGeneration::new(0),
            },
        };
        let native_release = if let (Some(native), TimerTimelineIndexKind::Segmented) =
            (self.native_timeline.as_ref(), manifest.timeline_index_kind)
        {
            let native_pin = native_pin_from_manifest(manifest)?;
            native.validate_snapshot_pin(native_pin)?;
            Some((native, native_pin))
        } else {
            None
        };
        self.payload.release_snapshot_pin(manifest.generation)?;
        self.timeline.release_snapshot(pin)?;
        if let Some((native, native_pin)) = native_release {
            native.release_snapshot(native_pin)?;
        }
        Ok(())
    }

    fn next_generation(&self) -> Result<u64, TimelineSnapshotError> {
        let mut maximum = 0u64;
        for entry in std::fs::read_dir(&self.snapshot_root)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            let digits = name.trim_start_matches('.').trim_start_matches("snapshot-");
            let digits = digits.trim_end_matches(".building");
            if let Ok(generation) = digits.parse::<u64>() {
                maximum = maximum.max(generation);
            }
        }
        maximum.checked_add(1).ok_or(TimelineSnapshotError::GenerationExhausted)
    }
}

fn native_pin_from_manifest(manifest: &TimerSnapshotManifest) -> Result<NativeSnapshotPin, TimelineSnapshotError> {
    Ok(NativeSnapshotPin {
        snapshot_generation: manifest.generation,
        manifest_generation: manifest
            .native_manifest_generation
            .ok_or(TimelineSnapshotError::MissingNativeBinding)?,
        durable_end: manifest
            .native_durable_end
            .ok_or(TimelineSnapshotError::MissingNativeBinding)?,
        manifest_checksum: manifest
            .native_manifest_checksum
            .ok_or(TimelineSnapshotError::MissingNativeBinding)?,
    })
}

fn write_manifest(path: &Path, manifest: &TimerSnapshotManifest) -> Result<(), TimelineSnapshotError> {
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_data()?;
    Ok(())
}

#[derive(Debug, Error)]
pub(crate) enum TimelineSnapshotError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Timeline(#[from] rocketmq_error::RocketMQError),
    #[error(transparent)]
    Materializer(#[from] super::TimelineMaterializerError),
    #[error(transparent)]
    Completion(#[from] super::TimelineCompletionError),
    #[error(transparent)]
    Artifact(#[from] rocketmq_store_api::StoreError),
    #[error(transparent)]
    Manifest(#[from] rocketmq_store_api::StoreContractViolation),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("CLOCK_UNSAFE prevents Extended snapshot publication")]
    ClockUnsafe,
    #[error("Extended snapshot requires non-zero role and activation epochs")]
    MissingEpoch,
    #[error("Extended snapshot generation is exhausted")]
    GenerationExhausted,
    #[error("Extended snapshot artifact generation {0} already exists")]
    ArtifactExists(u64),
    #[error("segmented snapshot manifest is missing its native binding")]
    MissingNativeBinding,
}
