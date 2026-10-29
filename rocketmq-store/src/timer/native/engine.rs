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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;

use parking_lot::Mutex;
use rocketmq_store_local::timer::native_format::native_key_hash;
use rocketmq_store_local::timer::native_format::native_lane;
use rocketmq_store_local::timer::native_format::NativeBucketId;
use rocketmq_store_local::timer::native_format::NativeBucketKind;
use rocketmq_store_local::timer::native_format::NativeCancelRecord;
use rocketmq_store_local::timer::native_format::NativeCheckpoint;
use rocketmq_store_local::timer::native_format::NativeIndexEntry;
use rocketmq_store_local::timer::native_format::NativeLocator;
use rocketmq_store_local::timer::native_storage::NativeStorageError;
use rocketmq_store_local::timer::native_storage::NativeTimerStorage;
use thiserror::Error;

use super::cancel_set::CancelSet;
use super::cancel_set::NativeRecall;
use super::config::NativeTimerConfig;
use super::near_wheel::NearWheel;

/// A `native` timer engine that cannot open, index or load its timers.
#[derive(Debug, Error)]
pub(crate) enum NativeEngineError {
    #[error("native timer setting {0} is invalid")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    Storage(#[from] NativeStorageError),
    #[error("native bucket {0:?} was written by a newer engine")]
    UnsupportedBucket(NativeBucketId),
}

/// One Timer CQ record for the engine to index, its properties already read.
#[derive(Clone, Debug)]
pub(crate) struct NativeTimerSource {
    pub(crate) cq_offset: i64,
    pub(crate) commit_log_offset: i64,
    pub(crate) size: i32,
    /// The original deadline.
    pub(crate) due_ms: i64,
    pub(crate) generation: u64,
    pub(crate) real_topic: String,
    pub(crate) real_queue_id: i32,
    pub(crate) kind: NativeSourceKind,
}

#[derive(Clone, Debug)]
pub(crate) enum NativeSourceKind {
    /// A timer, which recalls find by its unique key.
    Timer { unique_key: String },
    /// A recall, carrying the key and the deadline of the timer it cancels.
    Recall { key: String },
}

/// Why the engine did not index a source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeRejection {
    InvalidDeadline,
    /// Later than the maximum delay.
    BeyondMaxDelay,
    /// Later than the mid horizon. Such a timer needs a payload copy in a day bucket, which
    /// this engine does not write yet, so admission must not send it here.
    BeyondMidHorizon,
}

/// What one [`NativeTimerEngine::ingest`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NativeIngestReport {
    pub(crate) indexed: usize,
    pub(crate) recalls: usize,
    /// Sources not indexed, by Timer CQ offset.
    pub(crate) rejected: Vec<(i64, NativeRejection)>,
}

/// A due timer and the recalls that may cancel it, to check once its message is read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeDueTimer {
    pub(crate) entry: NativeIndexEntry,
    pub(crate) recalls: Vec<NativeRecall>,
}

struct EngineState {
    wheel: NearWheel,
    cancels: CancelSet,
    /// Every hour bucket with a file.
    hours: BTreeSet<NativeBucketId>,
    /// Hour buckets starting before this are in the wheel; a timer indexed into one of them
    /// goes into the wheel as well.
    loaded_until_ms: i64,
}

/// The `native` engine's index for timers due within its mid horizon: hour buckets on disk
/// that point into the CommitLog, and the near wheel in memory that delivery takes due timers
/// from. An hour bucket is loaded into the wheel whole once its hour starts within the near
/// window.
///
/// The engine is synchronous and does file I/O under its lock; callers run it off the async
/// runtime. Bucket files are appended before the wheel changes, so the wheel never holds a
/// timer that a restart could lose to an unsynced append (once a checkpoint covers it).
pub(crate) struct NativeTimerEngine {
    config: NativeTimerConfig,
    storage: NativeTimerStorage,
    state: Mutex<EngineState>,
}

impl NativeTimerEngine {
    /// Opens the engine's files under `store_root`, cut back to their last checkpoint, which it
    /// returns along with the engine; the Timer CQ is replayed from there. The recalls on disk
    /// are loaded, and the hour buckets wait in their files until their hour comes near.
    ///
    /// # Errors
    ///
    /// Returns [`NativeEngineError`] for an invalid setting, a storage failure, or a bucket
    /// kind this engine does not read.
    pub(crate) fn open(
        store_root: impl AsRef<Path>,
        config: NativeTimerConfig,
        now_ms: i64,
    ) -> Result<(Self, Option<NativeCheckpoint>), NativeEngineError> {
        config.validate().map_err(NativeEngineError::InvalidConfig)?;
        let (storage, checkpoint) = NativeTimerStorage::open(store_root, config.lane_count, now_ms)?;
        let mut hours = BTreeSet::new();
        for bucket in storage.buckets() {
            match bucket.bucket.kind() {
                NativeBucketKind::Hour => {
                    hours.insert(bucket.bucket);
                }
                NativeBucketKind::Day | NativeBucketKind::Minute => {
                    return Err(NativeEngineError::UnsupportedBucket(bucket.bucket));
                }
            }
        }
        let mut cancels = CancelSet::default();
        for record in storage.read_cancels()? {
            cancels.insert(&record);
        }
        let engine = Self {
            state: Mutex::new(EngineState {
                wheel: NearWheel::new(config.lane_count),
                cancels,
                hours,
                loaded_until_ms: i64::MIN,
            }),
            config,
            storage,
        };
        Ok((engine, checkpoint))
    }

    /// Indexes `sources`, read in Timer CQ order at `now_ms`: timers into their hour buckets,
    /// and into the wheel when their hour is loaded; recalls into the cancel log and the cancel
    /// set. Nothing is synced.
    ///
    /// # Errors
    ///
    /// Returns a storage error when a bucket or the cancel log cannot be appended. Part of the
    /// batch may then be in its files; indexing the batch again repeats those entries, which
    /// the wheel adds once, keyed by their cursors.
    pub(crate) fn ingest(
        &self,
        sources: &[NativeTimerSource],
        now_ms: i64,
    ) -> Result<NativeIngestReport, NativeEngineError> {
        let mut report = NativeIngestReport::default();
        let mut timers: BTreeMap<NativeBucketId, Vec<NativeIndexEntry>> = BTreeMap::new();
        let mut recalls = Vec::new();
        for source in sources {
            let bucket = match self.hour_of(source, now_ms) {
                Ok(bucket) => bucket,
                Err(rejection) => {
                    report.rejected.push((source.cq_offset, rejection));
                    continue;
                }
            };
            match &source.kind {
                NativeSourceKind::Timer { unique_key } => timers.entry(bucket).or_default().push(NativeIndexEntry {
                    due_ms: source.due_ms,
                    cq_offset: source.cq_offset,
                    locator: NativeLocator::CommitLog {
                        offset: source.commit_log_offset,
                    },
                    generation: source.generation,
                    key_hash: native_key_hash(unique_key.as_bytes()),
                    size: source.size,
                    lane: native_lane(&source.real_topic, source.real_queue_id, self.config.lane_count),
                }),
                NativeSourceKind::Recall { key } => recalls.push(NativeCancelRecord {
                    due_ms: source.due_ms,
                    tombstone_cq_offset: source.cq_offset,
                    generation: source.generation,
                    key: key.as_bytes().to_vec(),
                }),
            }
        }

        let mut state = self.state.lock();
        for (bucket, entries) in &timers {
            self.storage.append_entries(*bucket, entries)?;
        }
        if !recalls.is_empty() {
            self.storage.append_cancels(&recalls)?;
        }
        for (bucket, entries) in timers {
            state.hours.insert(bucket);
            report.indexed += entries.len();
            if bucket.start_ms() < state.loaded_until_ms {
                for entry in entries {
                    state.wheel.insert(entry);
                }
            }
        }
        for recall in &recalls {
            state.cancels.insert(recall);
        }
        report.recalls = recalls.len();
        Ok(report)
    }

    /// Takes up to `max` timers of `lane` due at or before `now_ms`, in `(due_ms, cq_offset)`
    /// order, after loading the hour buckets that have come near.
    ///
    /// # Errors
    ///
    /// Returns a storage error when a bucket cannot be loaded; nothing is taken then.
    pub(crate) fn poll_due(
        &self,
        lane: u16,
        now_ms: i64,
        max: usize,
    ) -> Result<Vec<NativeDueTimer>, NativeEngineError> {
        let mut state = self.state.lock();
        self.load_near_hours(&mut state, now_ms)?;
        let due = state.wheel.take_due(lane, now_ms, max);
        Ok(due
            .into_iter()
            .map(|entry| NativeDueTimer {
                recalls: state.cancels.candidates(&entry),
                entry,
            })
            .collect())
    }

    /// When the engine next has work: the earliest deadline in the wheel, rounded up to the
    /// precision, or the time the next hour bucket comes near enough to load.
    pub(crate) fn next_deadline_ms(&self) -> Option<i64> {
        let state = self.state.lock();
        let precision = self.config.precision_ms;
        let due = state.wheel.next_due_ms().map(|due| {
            due.saturating_add(precision - 1)
                .div_euclid(precision)
                .saturating_mul(precision)
        });
        // An hour bucket loads once its start is within the near window.
        let load = state
            .hours
            .iter()
            .find(|bucket| bucket.start_ms() >= state.loaded_until_ms)
            .map(|bucket| bucket.start_ms().saturating_sub(self.config.near_window_ms));
        match (due, load) {
            (Some(due), Some(load)) => Some(due.min(load)),
            (due, load) => due.or(load),
        }
    }

    #[cfg(test)]
    pub(crate) fn storage(&self) -> &NativeTimerStorage {
        &self.storage
    }

    /// Timers in the wheel, recalls in the cancel set, and hour buckets on disk.
    pub(crate) fn counts(&self) -> (usize, usize, usize) {
        let state = self.state.lock();
        (state.wheel.len(), state.cancels.len(), state.hours.len())
    }

    /// The hour bucket of a source, or why it is not indexed.
    fn hour_of(&self, source: &NativeTimerSource, now_ms: i64) -> Result<NativeBucketId, NativeRejection> {
        if source.due_ms > now_ms.saturating_add(self.config.max_delay_ms) {
            return Err(NativeRejection::BeyondMaxDelay);
        }
        if source.due_ms >= now_ms.saturating_add(self.config.mid_horizon_ms) {
            return Err(NativeRejection::BeyondMidHorizon);
        }
        NativeBucketId::containing(NativeBucketKind::Hour, source.due_ms).map_err(|_| NativeRejection::InvalidDeadline)
    }

    /// Loads, oldest first, the hour buckets starting before the end of the hour the near
    /// window reaches. Past the memory budget, only hours that have already started load, so
    /// due timers are never held back by later ones.
    fn load_near_hours(&self, state: &mut EngineState, now_ms: i64) -> Result<(), NativeEngineError> {
        let hour_ms = NativeBucketKind::Hour.window_ms();
        let reach = now_ms.saturating_add(self.config.near_window_ms);
        let target = reach.div_euclid(hour_ms).saturating_add(1).saturating_mul(hour_ms);
        if target <= state.loaded_until_ms {
            return Ok(());
        }
        let pending: Vec<_> = state
            .hours
            .iter()
            .copied()
            .filter(|bucket| bucket.start_ms() >= state.loaded_until_ms && bucket.start_ms() < target)
            .collect();
        for bucket in pending {
            if bucket.start_ms() > now_ms && state.wheel.memory_bytes() >= self.config.near_wheel_memory_bytes {
                state.loaded_until_ms = bucket.start_ms();
                return Ok(());
            }
            let wheel = &mut state.wheel;
            self.storage.visit_entries(bucket, |entries| {
                for entry in entries {
                    wheel.insert(*entry);
                }
                Ok(())
            })?;
            state.loaded_until_ms = bucket.end_ms();
        }
        state.loaded_until_ms = target;
        Ok(())
    }
}
