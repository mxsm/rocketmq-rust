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

use bytes::Bytes;
use parking_lot::Mutex;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_local::timer::native_format::native_key_hash;
use rocketmq_store_local::timer::native_format::native_lane;
use rocketmq_store_local::timer::native_format::NativeBucketId;
use rocketmq_store_local::timer::native_format::NativeBucketKind;
use rocketmq_store_local::timer::native_format::NativeCancelRecord;
use rocketmq_store_local::timer::native_format::NativeCheckpoint;
use rocketmq_store_local::timer::native_format::NativeDueCursor;
use rocketmq_store_local::timer::native_format::NativeIndexEntry;
use rocketmq_store_local::timer::native_format::NativeLocator;
use rocketmq_store_local::timer::native_format::NativePayloadSegment;
use rocketmq_store_local::timer::native_storage::NativePayloadLocation;
use rocketmq_store_local::timer::native_storage::NativeStorageConfig;
use rocketmq_store_local::timer::native_storage::NativeStorageError;
use rocketmq_store_local::timer::native_storage::NativeTimerStorage;
use rocketmq_store_local::timer::payload_record::TimerPayloadRecordV1;
use thiserror::Error;

use super::cancel_set::CancelSet;
use super::cancel_set::NativeRecall;
use super::config::NativeTimerConfig;
use super::near_wheel::NearWheel;

mod commit;
mod promote;

/// A `native` timer engine that cannot open, index or load its timers.
#[derive(Debug, Error)]
pub(crate) enum NativeEngineError {
    #[error("native timer setting {0} is invalid")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    Storage(#[from] NativeStorageError),
    #[error("native checkpoint has {found} lane watermarks for {lanes} lanes")]
    WatermarkCount { found: usize, lanes: u16 },
    #[error("native hour bucket {0:?} comes before an hour already split into minute runs")]
    UnsplitHour(NativeBucketId),
    #[error("native timer {0} has no readable copy")]
    NotCopied(i64),
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
    /// The complete CommitLog record, copied for a timer due beyond the mid horizon.
    pub(crate) frame: Bytes,
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
    /// Due beyond the mid horizon, but its record cannot be copied: an empty frame or real
    /// topic, or one too large for a payload segment.
    InvalidPayload,
}

/// What one [`NativeTimerEngine::ingest`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NativeIngestReport {
    pub(crate) indexed: usize,
    pub(crate) recalls: usize,
    /// Sources not indexed, by Timer CQ offset.
    pub(crate) rejected: Vec<(i64, NativeRejection)>,
}

/// What a [`NativeTimerEngine`] holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct NativeEngineCounts {
    /// Timers in the near wheel.
    pub(crate) wheel: usize,
    /// Recalls in the cancel set.
    pub(crate) recalls: usize,
    /// Day buckets not yet moved into hours.
    pub(crate) days: usize,
    /// Hour buckets not yet split.
    pub(crate) hours: usize,
    /// Minute runs.
    pub(crate) minutes: usize,
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
    /// Day buckets not yet moved into hours.
    days: BTreeSet<NativeBucketId>,
    /// Days starting before this are moved into hours: a copied timer due in one of them is
    /// indexed with the timers that point into the CommitLog.
    days_promoted_until_ms: i64,
    /// Hour buckets not yet split into minute runs.
    hours: BTreeSet<NativeBucketId>,
    /// Minute runs with a file.
    minutes: BTreeSet<NativeBucketId>,
    /// Hours starting before this are split into minute runs: a timer due in one of them is
    /// indexed into its minute run.
    promoted_until_ms: i64,
    /// Day buckets moved into hours, and hour buckets split into minute runs. The next
    /// checkpoint leaves them out, and then they are deleted, so a restart finds each timer's
    /// entry in exactly one bucket.
    retired: BTreeSet<NativeBucketId>,
    /// Minute runs starting before this are in the wheel; a timer indexed into one of them goes
    /// into the wheel as well.
    loaded_until_ms: i64,
    /// Per lane, the last timer delivered in an unbroken run of the lane's order. A restart
    /// takes every timer at or below it for delivered.
    watermarks: Vec<NativeDueCursor>,
    /// Per lane, the last timer taken for delivery. A batch in flight moves the watermark up
    /// to it once written, so a timer indexed later at or below it would fall below the
    /// watermark.
    taken: Vec<NativeDueCursor>,
    /// Every Timer CQ record visible before this is indexed, recalls included, so a timer due
    /// before it cannot be passed over by a recall still on its way. Nothing is delivered
    /// until the Source stage reports how far it got after the engine opens.
    visible_through_ms: i64,
    /// The Timer CQ offset after the last batch indexed.
    source_frontier: i64,
    /// The lowest CommitLog offset of each hour bucket written to since the engine opened.
    bucket_min_offsets: BTreeMap<NativeBucketId, i64>,
    /// The fence of the checkpoint the engine opened from. It stands in for the buckets found
    /// on disk, whose lowest offsets were not read.
    opened_fence: Option<i64>,
}

/// The `native` engine's index of timers: day buckets for timers due beyond the mid horizon,
/// whose records are copied, hour buckets and minute runs for the rest, and the near wheel in
/// memory that delivery takes due timers from. A day bucket is moved into hours once its day
/// starts within the mid horizon, an hour bucket is split into minute runs once its hour starts
/// within the near window, and a minute run is loaded into the wheel once its minute does.
///
/// The engine is synchronous and does file I/O under its lock; callers run it off the async
/// runtime. Bucket files are appended before the wheel changes, so the wheel never holds a
/// timer that a restart could lose to an unsynced append (once a checkpoint covers it).
pub(crate) struct NativeTimerEngine {
    config: NativeTimerConfig,
    storage: NativeTimerStorage,
    state: Mutex<EngineState>,
    /// Serializes group commits.
    commit: Mutex<()>,
}

impl NativeTimerEngine {
    /// Opens the engine's files under `store_root`, cut back to their last checkpoint, which it
    /// returns along with the engine. The recalls on disk are loaded, the lane watermarks
    /// restored, and the hour buckets wait in their files until their hour comes near; the
    /// caller replays the Timer CQ from [`Self::source_frontier`].
    ///
    /// # Errors
    ///
    /// Returns [`NativeEngineError`] for an invalid setting, a storage failure, a bucket kind
    /// this engine does not read, or a checkpoint whose watermarks do not match the lanes.
    pub(crate) fn open(
        store_root: impl AsRef<Path>,
        config: NativeTimerConfig,
        now_ms: i64,
    ) -> Result<(Self, Option<NativeCheckpoint>), NativeEngineError> {
        config.validate().map_err(NativeEngineError::InvalidConfig)?;
        let storage_config = NativeStorageConfig {
            payload_segment_bytes: config.payload_segment_bytes,
            ..NativeStorageConfig::new(config.lane_count)
        };
        let (storage, checkpoint) = NativeTimerStorage::open(store_root, storage_config, now_ms)?;
        let mut days = BTreeSet::new();
        let mut hours = BTreeSet::new();
        let mut minutes = BTreeSet::new();
        for bucket in storage.buckets() {
            match bucket.bucket.kind() {
                NativeBucketKind::Day => days.insert(bucket.bucket),
                NativeBucketKind::Hour => hours.insert(bucket.bucket),
                NativeBucketKind::Minute => minutes.insert(bucket.bucket),
            };
        }
        // Hours are split in order, and an hour bucket is deleted only after a checkpoint that
        // no longer names it, so no hour bucket comes before a minute run's hour.
        let promoted_until_ms = minutes
            .iter()
            .map(|minute| hour_end_ms(minute.start_ms()))
            .max()
            .unwrap_or(i64::MIN);
        if let Some(hour) = hours.iter().find(|hour| hour.start_ms() < promoted_until_ms) {
            return Err(NativeEngineError::UnsplitHour(*hour));
        }
        let mut cancels = CancelSet::default();
        for record in storage.read_cancels()? {
            cancels.insert(&record);
        }
        let lanes = usize::from(config.lane_count);
        let watermarks = match &checkpoint {
            Some(checkpoint) if checkpoint.lane_watermarks.len() != lanes => {
                return Err(NativeEngineError::WatermarkCount {
                    found: checkpoint.lane_watermarks.len(),
                    lanes: config.lane_count,
                });
            }
            Some(checkpoint) => checkpoint.lane_watermarks.clone(),
            None => vec![NativeDueCursor::START; lanes],
        };
        let engine = Self {
            state: Mutex::new(EngineState {
                wheel: NearWheel::new(config.lane_count),
                cancels,
                loaded_until_ms: i64::MIN,
                taken: watermarks.clone(),
                watermarks,
                visible_through_ms: i64::MIN,
                source_frontier: checkpoint.as_ref().map_or(0, |checkpoint| checkpoint.source_cq_offset),
                bucket_min_offsets: BTreeMap::new(),
                opened_fence: checkpoint
                    .as_ref()
                    .filter(|_| !hours.is_empty() || !minutes.is_empty())
                    .map(|checkpoint| checkpoint.commit_log_fence),
                days,
                days_promoted_until_ms: i64::MIN,
                hours,
                minutes,
                promoted_until_ms,
                retired: BTreeSet::new(),
            }),
            config,
            storage,
            commit: Mutex::new(()),
        };
        Ok((engine, checkpoint))
    }

    /// The Timer CQ offset to read the next source from: after the last batch indexed, or,
    /// right after opening, the checkpointed offset to replay from.
    pub(crate) fn source_frontier(&self) -> i64 {
        self.state.lock().source_frontier
    }

    /// Indexes `sources`, read in Timer CQ order at `now_ms` up to `next_source_offset`:
    /// timers into their hour buckets, or into their minute runs once their hour is split, and
    /// into the wheel when their minute is loaded; recalls into the cancel log and the cancel
    /// set. A timer due beyond the mid horizon is copied first, since the CommitLog may drop
    /// its source before then, and indexed in its day bucket. Nothing is synced.
    ///
    /// A timer at or below the last timer its lane took for delivery would be taken for
    /// delivered after a restart, once that batch moves the watermark, so it is indexed at that
    /// timer's deadline instead, right after it in the lane's order. That only delivers it
    /// later than its deadline, as it already is.
    ///
    /// # Errors
    ///
    /// Returns a storage error when a bucket or the cancel log cannot be appended; the frontier
    /// then stays where it was. Part of the batch may be in its files; indexing the batch again
    /// repeats those entries, which the wheel adds once, keyed by their cursors.
    pub(crate) fn ingest(
        &self,
        sources: &[NativeTimerSource],
        next_source_offset: i64,
        now_ms: i64,
    ) -> Result<NativeIngestReport, NativeEngineError> {
        let mut report = NativeIngestReport::default();
        let mut timers: BTreeMap<NativeBucketId, Vec<NativeIndexEntry>> = BTreeMap::new();
        let mut copies: BTreeMap<i32, Vec<(Vec<u8>, NativeIndexEntry)>> = BTreeMap::new();
        let mut recalls = Vec::new();
        let mut state = self.state.lock();
        for source in sources {
            if let Err(rejection) = self.admit(source.due_ms, now_ms) {
                report.rejected.push((source.cq_offset, rejection));
                continue;
            }
            match &source.kind {
                NativeSourceKind::Timer { unique_key } => {
                    let lane = native_lane(&source.real_topic, source.real_queue_id, self.config.lane_count);
                    let taken = state.taken_through(lane);
                    let cursor = NativeDueCursor {
                        due_ms: source.due_ms,
                        cq_offset: source.cq_offset,
                    };
                    let due_ms = if cursor <= taken { taken.due_ms } else { source.due_ms };
                    let entry = NativeIndexEntry {
                        due_ms,
                        cq_offset: source.cq_offset,
                        locator: NativeLocator::CommitLog {
                            offset: source.commit_log_offset,
                        },
                        generation: source.generation,
                        key_hash: native_key_hash(unique_key.as_bytes()),
                        size: source.size,
                        lane,
                    };
                    // Due after the CommitLog may have dropped its source: copy the record.
                    if due_ms >= now_ms.saturating_add(self.config.mid_horizon_ms) {
                        match self.copy_of(source, &entry) {
                            Ok((day, record)) => copies.entry(day).or_default().push((record, entry)),
                            Err(rejection) => report.rejected.push((source.cq_offset, rejection)),
                        }
                        continue;
                    }
                    let Some(bucket) = state.near_bucket(due_ms) else {
                        report
                            .rejected
                            .push((source.cq_offset, NativeRejection::InvalidDeadline));
                        continue;
                    };
                    timers.entry(bucket).or_default().push(entry);
                }
                NativeSourceKind::Recall { key } => recalls.push(NativeCancelRecord {
                    due_ms: source.due_ms,
                    tombstone_cq_offset: source.cq_offset,
                    generation: source.generation,
                    key: key.as_bytes().to_vec(),
                }),
            }
        }

        // Copies first, so every entry that points at one is written after it.
        for (day, day_copies) in copies {
            let (records, entries): (Vec<_>, Vec<_>) = day_copies.into_iter().unzip();
            let locations = self.storage.append_payloads(day, &records)?;
            for (mut entry, location) in entries.into_iter().zip(locations) {
                entry.locator = NativeLocator::Payload {
                    segment_id: location.segment.segment_id,
                    offset: location.offset,
                };
                // A copy is at most a segment, which `copy_of` checked fits an `i32`.
                entry.size = location.length as i32;
                if let Some(bucket) = state.copy_bucket(entry.due_ms) {
                    timers.entry(bucket).or_default().push(entry);
                }
            }
        }
        for (bucket, entries) in &mut timers {
            if bucket.kind() == NativeBucketKind::Minute {
                // Minute runs are written as sorted frames.
                entries.sort_by_key(NativeIndexEntry::cursor);
            }
            self.storage.append_entries(*bucket, entries)?;
        }
        if !recalls.is_empty() {
            self.storage.append_cancels(&recalls)?;
        }
        for (bucket, entries) in timers {
            report.indexed += entries.len();
            state.add_written(bucket, &entries);
            if bucket.kind() == NativeBucketKind::Minute && bucket.start_ms() < state.loaded_until_ms {
                for entry in entries {
                    state.insert_undelivered(entry);
                }
            }
        }
        for recall in &recalls {
            state.cancels.insert(recall);
        }
        report.recalls = recalls.len();
        let after_last = sources.iter().map(|source| source.cq_offset.saturating_add(1)).max();
        state.source_frontier = state
            .source_frontier
            .max(next_source_offset)
            .max(after_last.unwrap_or(i64::MIN));
        Ok(report)
    }

    /// Takes up to `max` timers of `lane` due at or before `now_ms` and before the visibility
    /// watermark, in `(due_ms, cq_offset)` order, after loading the buckets that have come
    /// near. A taken timer counts as delivered once [`Self::advance_delivered`] passes it; one
    /// that must wait goes back with [`Self::requeue`]. A lane takes one batch at a time.
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
        self.load_near(&mut state, now_ms)?;
        let through_ms = now_ms.min(state.visible_through_ms);
        let due = state.wheel.take_due(lane, through_ms, max);
        if let (Some(last), Some(taken)) = (due.last(), state.taken.get_mut(usize::from(lane))) {
            *taken = (*taken).max(last.cursor());
        }
        Ok(due
            .into_iter()
            .map(|entry| NativeDueTimer {
                recalls: state.cancels.candidates(&entry),
                entry,
            })
            .collect())
    }

    /// Records that every Timer CQ record visible before `visible_ms` is indexed, so timers due
    /// before it may be delivered. The watermark never moves back.
    pub(crate) fn set_visible_through(&self, visible_ms: i64) {
        let mut state = self.state.lock();
        state.visible_through_ms = state.visible_through_ms.max(visible_ms);
    }

    /// Puts taken timers back, for a later batch of their lanes.
    pub(crate) fn requeue(&self, entries: impl IntoIterator<Item = NativeIndexEntry>) {
        let mut state = self.state.lock();
        for entry in entries {
            state.insert_undelivered(entry);
        }
    }

    /// Reads the copy a copied timer's entry points at.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the entry points nowhere readable.
    pub(crate) fn read_payload(&self, entry: &NativeIndexEntry) -> Result<TimerPayloadRecordV1, NativeEngineError> {
        let NativeLocator::Payload { segment_id, offset } = entry.locator else {
            return Err(NativeEngineError::NotCopied(entry.cq_offset));
        };
        let due_day_utc =
            NativePayloadSegment::due_day_of(entry.due_ms).ok_or(NativeEngineError::NotCopied(entry.cq_offset))?;
        let location = NativePayloadLocation {
            segment: NativePayloadSegment {
                due_day_utc,
                segment_id,
            },
            offset,
            length: u32::try_from(entry.size).map_err(|_| NativeEngineError::NotCopied(entry.cq_offset))?,
        };
        Ok(self.storage.read_payload(location)?)
    }

    /// Records that `lane` delivered every timer up to `through`, in its order. The watermark
    /// never moves back.
    pub(crate) fn advance_delivered(&self, lane: u16, through: NativeDueCursor) {
        let mut state = self.state.lock();
        if let Some(watermark) = state.watermarks.get_mut(usize::from(lane)) {
            *watermark = (*watermark).max(through);
        }
    }

    /// When the engine next has work: the earliest deadline in the wheel, rounded up to the
    /// precision, or the time the next hour bucket or minute run comes near enough to split or
    /// load.
    pub(crate) fn next_deadline_ms(&self) -> Option<i64> {
        let state = self.state.lock();
        let precision = self.config.precision_ms;
        let due = state.wheel.next_due_ms().map(|due| {
            due.saturating_add(precision - 1)
                .div_euclid(precision)
                .saturating_mul(precision)
        });
        // Day buckets move into hours once their start is within the mid horizon.
        let promote = state
            .days
            .first()
            .map(|bucket| bucket.start_ms().saturating_sub(self.config.mid_horizon_ms));
        // Hour buckets and minute runs come in once their start is within the near window.
        let split = state.hours.first().map(|bucket| bucket.start_ms());
        let load = state
            .minutes
            .iter()
            .find(|bucket| bucket.start_ms() >= state.loaded_until_ms)
            .map(|bucket| bucket.start_ms());
        let near = [split, load]
            .into_iter()
            .flatten()
            .min()
            .map(|start| start.saturating_sub(self.config.near_window_ms));
        [due, near, promote].into_iter().flatten().min()
    }

    #[cfg(test)]
    pub(crate) fn storage(&self) -> &NativeTimerStorage {
        &self.storage
    }

    /// What the engine holds.
    pub(crate) fn counts(&self) -> NativeEngineCounts {
        let state = self.state.lock();
        NativeEngineCounts {
            wheel: state.wheel.len(),
            recalls: state.cancels.len(),
            days: state.days.len(),
            hours: state.hours.len(),
            minutes: state.minutes.len(),
        }
    }

    /// Why a source due at `due_ms` is not indexed, if it is not.
    fn admit(&self, due_ms: i64, now_ms: i64) -> Result<(), NativeRejection> {
        if due_ms < 0 {
            return Err(NativeRejection::InvalidDeadline);
        }
        if due_ms > now_ms.saturating_add(self.config.max_delay_ms) {
            return Err(NativeRejection::BeyondMaxDelay);
        }
        Ok(())
    }

    /// The UTC due day and the encoded copy of the timer `entry` indexes.
    fn copy_of(&self, source: &NativeTimerSource, entry: &NativeIndexEntry) -> Result<(i32, Vec<u8>), NativeRejection> {
        let day = NativePayloadSegment::due_day_of(entry.due_ms).ok_or(NativeRejection::InvalidDeadline)?;
        let record = TimerPayloadRecordV1 {
            due_time_ms: entry.due_ms,
            lane: entry.lane,
            timer_id: TimerId::new(u128::from(entry.cq_offset.unsigned_abs())),
            generation: TimerGeneration::new(entry.generation),
            source_cq_offset: TimerSourceCqOffset::new(entry.cq_offset),
            source_physical_offset: source.commit_log_offset,
            real_queue_id: source.real_queue_id,
            real_topic: source.real_topic.clone(),
            frame: source.frame.to_vec(),
        }
        .encode()
        .ok_or(NativeRejection::InvalidPayload)?;
        let fits = record.len() as u64 <= self.config.payload_segment_bytes && i32::try_from(record.len()).is_ok();
        if !fits {
            return Err(NativeRejection::InvalidPayload);
        }
        Ok((day, record))
    }
}

impl EngineState {
    /// The bucket a timer due at `due_ms` is indexed in among the timers that point into the
    /// CommitLog: its minute run once its hour is split, else its hour bucket.
    fn near_bucket(&self, due_ms: i64) -> Option<NativeBucketId> {
        let kind = if due_ms < self.promoted_until_ms {
            NativeBucketKind::Minute
        } else {
            NativeBucketKind::Hour
        };
        NativeBucketId::containing(kind, due_ms).ok()
    }

    /// The bucket a copied timer due at `due_ms` is indexed in: its day bucket, or once its
    /// day is moved into hours, the bucket [`Self::near_bucket`] picks.
    fn copy_bucket(&self, due_ms: i64) -> Option<NativeBucketId> {
        if due_ms >= self.days_promoted_until_ms {
            NativeBucketId::containing(NativeBucketKind::Day, due_ms).ok()
        } else {
            self.near_bucket(due_ms)
        }
    }

    /// Records `entries`, just written to `bucket`, among the files and their lowest offsets.
    fn add_written(&mut self, bucket: NativeBucketId, entries: &[NativeIndexEntry]) {
        match bucket.kind() {
            NativeBucketKind::Day => self.days.insert(bucket),
            NativeBucketKind::Hour => self.hours.insert(bucket),
            NativeBucketKind::Minute => self.minutes.insert(bucket),
        };
        if let Some(lowest) = entries.iter().filter_map(commit_log_offset).min() {
            let min = self.bucket_min_offsets.entry(bucket).or_insert(lowest);
            *min = (*min).min(lowest);
        }
    }

    fn watermark(&self, lane: u16) -> NativeDueCursor {
        self.watermarks
            .get(usize::from(lane))
            .copied()
            .unwrap_or(NativeDueCursor::START)
    }

    /// The last timer `lane` took for delivery, or delivered.
    fn taken_through(&self, lane: u16) -> NativeDueCursor {
        let taken = self.taken.get(usize::from(lane)).copied();
        taken.map_or(self.watermark(lane), |taken| taken.max(self.watermark(lane)))
    }

    /// Adds `entry` to the wheel unless its lane delivered it.
    fn insert_undelivered(&mut self, entry: NativeIndexEntry) {
        if entry.cursor() > self.watermark(entry.lane) {
            self.wheel.insert(entry);
        }
    }
}

/// The end of the hour holding `time_ms`.
fn hour_end_ms(time_ms: i64) -> i64 {
    let hour_ms = NativeBucketKind::Hour.window_ms();
    time_ms.div_euclid(hour_ms).saturating_add(1).saturating_mul(hour_ms)
}

fn commit_log_offset(entry: &NativeIndexEntry) -> Option<i64> {
    match entry.locator {
        NativeLocator::CommitLog { offset } => Some(offset),
        NativeLocator::Payload { .. } => None,
    }
}
