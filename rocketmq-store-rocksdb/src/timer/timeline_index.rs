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
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_error::RocketMQError;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;

use crate::batch::RocksDbWriteBatch;
use crate::config::RocksDbConfig;
use crate::error::codec_error;
use crate::iterator::RocksDbRangeScanOptions;
use crate::store::KeyValueStore;
use crate::store::RocksDbStore;
use crate::timer::checkpoint::encode_checkpoint_key;
use crate::timer::checkpoint::TimelineCheckpointKind;
use crate::timer::checkpoint::TimelineCheckpointV1;
use crate::timer::codec::crc32c;
use crate::timer::codec::TimelineKeyV1;
use crate::timer::codec::TimelineRecordV1;
use crate::timer::state_index::RocksDbTimelineStateIndex;
use crate::timer::BUCKET_SUMMARY_CF;
use crate::timer::CHECKPOINT_CF;
use crate::timer::SHADOW_OBSERVATION_CF;
use crate::timer::SHADOW_TIMELINE_CF;
use crate::timer::TIMELINE_CF;

const SHADOW_KEY_VERSION: u8 = 1;
const SHADOW_KEY_SIZE: usize = 25;
const SHADOW_OBSERVATION_KEY_VERSION: u8 = 2;
const ACTIVE_SNAPSHOT_PIN_KEY: &[u8] = b"timer-active-snapshot-pin-v1";
const SNAPSHOT_PIN_VALUE_SIZE: usize = 8 + 35 + 4;

/// Durable, non-delivering shadow observation type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ShadowObservationKind {
    /// The source payload was durably materialized.
    Materialized = 0,
    /// The shadow scanner observed the record as due.
    Due = 1,
    /// Java-compatible Recall cancelled the source generation.
    Cancelled = 2,
    /// A bounded comparison sample was retained.
    Difference = 3,
}

/// One decoded Timeline entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimelineIndexEntry {
    /// Ordered key.
    pub key: TimelineKeyV1,
    /// Small payload/state reference.
    pub record: TimelineRecordV1,
}

/// Bounded page from a Timeline range scan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimelineIndexPage {
    /// Decoded records in key order.
    pub entries: Vec<TimelineIndexEntry>,
    /// Last returned key when more records may remain.
    pub continuation: Option<TimelineKeyV1>,
    /// Encoded key/value bytes retained by this page.
    pub retained_bytes: usize,
}

/// Logical snapshot pin preventing GC from crossing a reader fence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimelineSnapshotPin {
    /// Monotonic local pin generation.
    pub generation: u64,
    /// Exclusive upper Timeline key that GC must preserve.
    pub gc_fence: TimelineKeyV1,
}

/// Dedicated Extended Timeline database.
pub struct RocksDbTimelineIndex {
    store: Arc<RocksDbStore>,
    next_snapshot_generation: AtomicU64,
    snapshot_pins: Mutex<BTreeMap<u64, TimelineKeyV1>>,
    state_transition_lock: Arc<Mutex<()>>,
}

impl RocksDbTimelineIndex {
    /// Opens a physically isolated Timeline DB below `store_root`.
    ///
    /// # Errors
    ///
    /// Returns an error when RocksDB cannot open with the required WAL/sync profile.
    pub fn open(store_root: impl AsRef<Path>) -> Result<Self, RocketMQError> {
        let config = RocksDbConfig::timer_timeline(store_root);
        debug_assert!(config.wal_enabled && config.sync_write);
        let store = Arc::new(RocksDbStore::open(config)?);
        let index = Self::from_store(store);
        index.restore_snapshot_pin()?;
        Ok(index)
    }

    /// Creates an index over an already opened dedicated Timeline DB.
    pub fn from_store(store: Arc<RocksDbStore>) -> Self {
        Self {
            store,
            next_snapshot_generation: AtomicU64::new(0),
            snapshot_pins: Mutex::new(BTreeMap::new()),
            state_transition_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Returns the dedicated database wrapper for state/outbox composition.
    pub fn store(&self) -> Arc<RocksDbStore> {
        Arc::clone(&self.store)
    }

    /// Returns a state view sharing this Timeline's serialized CAS domain.
    pub fn state_index(&self) -> RocksDbTimelineStateIndex {
        RocksDbTimelineStateIndex::with_transition_lock(
            Arc::clone(&self.store),
            Arc::clone(&self.state_transition_lock),
        )
    }

    /// Writes Timeline entries and an optional source checkpoint in one sync-WAL batch.
    pub fn put_batch(
        &self,
        entries: &[TimelineIndexEntry],
        checkpoint: Option<(TimelineCheckpointKind, u16, TimelineCheckpointV1)>,
    ) -> Result<usize, RocketMQError> {
        let mut batch = RocksDbWriteBatch::with_capacity(entries.len().saturating_add(1));
        for entry in entries {
            Self::append_entry(&mut batch, entry)?;
        }
        if let Some((kind, lane, checkpoint)) = checkpoint {
            Self::append_checkpoint(&mut batch, kind, lane, checkpoint);
        }
        self.store.write_batch(&batch)?;
        Ok(entries.len())
    }

    /// Appends one formal or shadow entry to an existing atomic batch.
    pub fn append_entry(batch: &mut RocksDbWriteBatch, entry: &TimelineIndexEntry) -> Result<(), RocketMQError> {
        let value = entry.record.encode();
        if entry.record.shadow_only {
            batch.put_cf(
                SHADOW_TIMELINE_CF,
                encode_shadow_key(
                    entry.record.source_cq_offset.get(),
                    entry.record.source_physical_offset,
                    entry.key.generation,
                )?,
                value,
            );
            batch.put_cf(SHADOW_OBSERVATION_CF, entry.key.encode(), value);
        } else {
            batch.put_cf(TIMELINE_CF, entry.key.encode(), value);
        }
        Ok(())
    }

    /// Appends a checkpoint update to an existing atomic batch.
    pub fn append_checkpoint(
        batch: &mut RocksDbWriteBatch,
        kind: TimelineCheckpointKind,
        lane: u16,
        checkpoint: TimelineCheckpointV1,
    ) {
        batch.put_cf(CHECKPOINT_CF, encode_checkpoint_key(kind, lane), checkpoint.encode());
    }

    /// Reads one idempotency marker from the shadow source-identity index.
    pub fn get_shadow(
        &self,
        source_cq_offset: i64,
        source_physical_offset: i64,
        generation: TimerGeneration,
    ) -> Result<Option<TimelineRecordV1>, RocketMQError> {
        let key = encode_shadow_key(source_cq_offset, source_physical_offset, generation)?;
        self.store
            .get_cf(SHADOW_TIMELINE_CF, &key)?
            .map(|value| TimelineRecordV1::decode(&value))
            .transpose()
    }

    /// Reads and verifies one formal Timeline record.
    pub fn get(&self, key: TimelineKeyV1) -> Result<Option<TimelineRecordV1>, RocketMQError> {
        self.store
            .get_cf(TIMELINE_CF, &key.encode())?
            .map(|value| TimelineRecordV1::decode(&value))
            .transpose()
    }

    /// Scans `[start_due_ms, end_due_exclusive_ms)` under message and byte limits.
    ///
    /// `continuation` is exclusive; callers can pass the previous page token directly.
    pub fn range_scan(
        &self,
        start_due_ms: i64,
        end_due_exclusive_ms: i64,
        continuation: Option<TimelineKeyV1>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<TimelineIndexPage, RocketMQError> {
        self.range_scan_cf(
            TIMELINE_CF,
            start_due_ms,
            end_due_exclusive_ms,
            continuation,
            max_messages,
            max_bytes,
        )
    }

    /// Scans due-ordered shadow candidates without exposing them as formal records.
    pub fn range_scan_shadow(
        &self,
        start_due_ms: i64,
        end_due_exclusive_ms: i64,
        continuation: Option<TimelineKeyV1>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<TimelineIndexPage, RocketMQError> {
        self.range_scan_cf(
            SHADOW_OBSERVATION_CF,
            start_due_ms,
            end_due_exclusive_ms,
            continuation,
            max_messages,
            max_bytes,
        )
    }

    fn range_scan_cf(
        &self,
        cf: &'static str,
        start_due_ms: i64,
        end_due_exclusive_ms: i64,
        continuation: Option<TimelineKeyV1>,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<TimelineIndexPage, RocketMQError> {
        let minimum_record_bytes = TimelineKeyV1::encoded_size().saturating_add(TimelineRecordV1::encoded_size());
        if max_messages == 0 || max_bytes < minimum_record_bytes || end_due_exclusive_ms <= start_due_ms {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "timer.timeline.scan_budget",
                value: format!("messages={max_messages}, bytes={max_bytes}"),
                reason: "scan budget must fit at least one record and a non-empty time range".to_string(),
            });
        }
        let first = TimelineKeyV1 {
            due_time_ms: start_due_ms,
            lane: 0,
            timer_id: TimerId::new(0),
            generation: TimerGeneration::new(0),
        };
        let end = TimelineKeyV1 {
            due_time_ms: end_due_exclusive_ms,
            lane: 0,
            timer_id: TimerId::new(0),
            generation: TimerGeneration::new(0),
        };
        let scan_start = continuation.unwrap_or(first);
        let raw = self.store.range_scan(&RocksDbRangeScanOptions::new(
            cf,
            scan_start.encode(),
            end.encode(),
            max_messages.saturating_add(2),
        ))?;
        let mut page = TimelineIndexPage::default();
        let mut has_more = false;
        for item in raw {
            let key = TimelineKeyV1::decode(&item.key)?;
            if continuation == Some(key) {
                continue;
            }
            let retained = item.key.len().saturating_add(item.value.len());
            if page.entries.len() >= max_messages || page.retained_bytes.saturating_add(retained) > max_bytes {
                has_more = true;
                break;
            }
            page.retained_bytes = page.retained_bytes.saturating_add(retained);
            page.entries.push(TimelineIndexEntry {
                key,
                record: TimelineRecordV1::decode(&item.value)?,
            });
        }
        if has_more {
            page.continuation = page.entries.last().map(|entry| entry.key).or(continuation);
        }
        Ok(page)
    }

    /// Appends a durable shadow-only observation marker to an existing batch.
    pub fn append_shadow_observation(
        batch: &mut RocksDbWriteBatch,
        source_cq_offset: i64,
        source_physical_offset: i64,
        generation: TimerGeneration,
        kind: ShadowObservationKind,
        value: impl Into<Vec<u8>>,
    ) -> Result<(), RocketMQError> {
        batch.put_cf(
            SHADOW_OBSERVATION_CF,
            encode_shadow_observation_key(source_cq_offset, source_physical_offset, generation, kind)?,
            value,
        );
        Ok(())
    }

    /// Removes a shadow source from the due-ordered candidate set without touching its source marker.
    pub fn delete_shadow_due_candidate(batch: &mut RocksDbWriteBatch, key: TimelineKeyV1) {
        batch.delete_cf(SHADOW_OBSERVATION_CF, key.encode());
    }

    /// Reads a durable shadow-only observation marker.
    pub fn shadow_observation(
        &self,
        source_cq_offset: i64,
        source_physical_offset: i64,
        generation: TimerGeneration,
        kind: ShadowObservationKind,
    ) -> Result<Option<Vec<u8>>, RocketMQError> {
        let key = encode_shadow_observation_key(source_cq_offset, source_physical_offset, generation, kind)?;
        Ok(self
            .store
            .get_cf(SHADOW_OBSERVATION_CF, &key)?
            .map(|value| value.to_vec()))
    }

    /// Reads one durable checkpoint.
    pub fn checkpoint(
        &self,
        kind: TimelineCheckpointKind,
        lane: u16,
    ) -> Result<Option<TimelineCheckpointV1>, RocketMQError> {
        self.store
            .get_cf(CHECKPOINT_CF, &encode_checkpoint_key(kind, lane))?
            .map(|value| TimelineCheckpointV1::decode(&value))
            .transpose()
    }

    /// Atomically writes arbitrary Timeline/state/outbox operations.
    pub fn write_batch(&self, batch: &RocksDbWriteBatch) -> Result<(), RocketMQError> {
        self.store.write_batch(batch)
    }

    /// Pins a logical GC fence for snapshot/replication readers.
    pub fn pin_snapshot(&self, gc_fence: TimelineKeyV1) -> Result<TimelineSnapshotPin, RocketMQError> {
        let generation = self
            .next_snapshot_generation
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        self.pin_snapshot_generation(gc_fence, generation)
    }

    /// Persists a snapshot pin using the generation shared with PayloadStore.
    pub fn pin_snapshot_generation(
        &self,
        gc_fence: TimelineKeyV1,
        generation: u64,
    ) -> Result<TimelineSnapshotPin, RocketMQError> {
        if generation == 0 {
            return Err(codec_error("Timeline snapshot generation must be non-zero"));
        }
        let mut pins = self.snapshot_pins.lock().map_err(|error| {
            RocketMQError::storage_write_failed("timer-timeline", format!("snapshot pin lock poisoned: {error}"))
        })?;
        if !pins.is_empty() {
            return Err(codec_error("Timeline snapshot is already pinned"));
        }
        let value = encode_snapshot_pin(generation, gc_fence);
        self.store.put_cf(CHECKPOINT_CF, ACTIVE_SNAPSHOT_PIN_KEY, &value)?;
        pins.insert(generation, gc_fence);
        self.next_snapshot_generation.fetch_max(generation, Ordering::Release);
        Ok(TimelineSnapshotPin { generation, gc_fence })
    }

    /// Releases a logical snapshot pin.
    pub fn release_snapshot(&self, pin: TimelineSnapshotPin) -> Result<(), RocketMQError> {
        let mut pins = self.snapshot_pins.lock().map_err(|error| {
            RocketMQError::storage_write_failed("timer-timeline", format!("snapshot pin lock poisoned: {error}"))
        })?;
        match pins.remove(&pin.generation) {
            Some(fence) if fence == pin.gc_fence => self.store.delete_cf(CHECKPOINT_CF, ACTIVE_SNAPSHOT_PIN_KEY),
            _ => Err(codec_error("unknown or mismatched Timeline snapshot pin")),
        }
    }

    fn restore_snapshot_pin(&self) -> Result<(), RocketMQError> {
        let Some(value) = self.store.get_cf(CHECKPOINT_CF, ACTIVE_SNAPSHOT_PIN_KEY)? else {
            return Ok(());
        };
        let pin = decode_snapshot_pin(&value)?;
        self.next_snapshot_generation.store(pin.generation, Ordering::Release);
        self.snapshot_pins
            .lock()
            .map_err(|error| {
                RocketMQError::storage_read_failed("timer-timeline", format!("snapshot pin lock poisoned: {error}"))
            })?
            .insert(pin.generation, pin.gc_fence);
        Ok(())
    }

    /// Deletes at most `max_records` formal Timeline records below the effective GC fence.
    pub fn gc(&self, requested_fence: TimelineKeyV1, max_records: usize) -> Result<usize, RocketMQError> {
        if max_records == 0 {
            return Ok(0);
        }
        let effective_fence = {
            let pins = self.snapshot_pins.lock().map_err(|error| {
                RocketMQError::storage_write_failed("timer-timeline", format!("snapshot pin lock poisoned: {error}"))
            })?;
            pins.values()
                .copied()
                .min()
                .map_or(requested_fence, |pin| pin.min(requested_fence))
        };
        let raw = self.store.range_scan(&RocksDbRangeScanOptions::new(
            TIMELINE_CF,
            TimelineKeyV1 {
                due_time_ms: i64::MIN,
                lane: 0,
                timer_id: TimerId::new(0),
                generation: TimerGeneration::new(0),
            }
            .encode(),
            effective_fence.encode(),
            max_records,
        ))?;
        let mut batch = RocksDbWriteBatch::with_capacity(raw.len());
        for item in &raw {
            batch.delete_cf(TIMELINE_CF, item.key.to_vec());
        }
        self.store.write_batch(&batch)?;
        Ok(raw.len())
    }

    /// Returns a bounded formal GC page below all active snapshot pins without deleting it.
    pub fn gc_candidates(
        &self,
        requested_fence: TimelineKeyV1,
        max_records: usize,
    ) -> Result<Vec<TimelineIndexEntry>, RocketMQError> {
        if max_records == 0 {
            return Ok(Vec::new());
        }
        let effective_fence = {
            let pins = self.snapshot_pins.lock().map_err(|error| {
                RocketMQError::storage_read_failed("timer-timeline", format!("snapshot pin lock poisoned: {error}"))
            })?;
            pins.values()
                .copied()
                .min()
                .map_or(requested_fence, |pin| pin.min(requested_fence))
        };
        self.store
            .range_scan(&RocksDbRangeScanOptions::new(
                TIMELINE_CF,
                TimelineKeyV1 {
                    due_time_ms: i64::MIN,
                    lane: 0,
                    timer_id: TimerId::new(0),
                    generation: TimerGeneration::new(0),
                }
                .encode(),
                effective_fence.encode(),
                max_records,
            ))?
            .into_iter()
            .map(|item| {
                Ok(TimelineIndexEntry {
                    key: TimelineKeyV1::decode(&item.key)?,
                    record: TimelineRecordV1::decode(&item.value)?,
                })
            })
            .collect()
    }

    /// Appends one formal Timeline deletion to a larger terminal-GC batch.
    pub fn append_delete_entry(batch: &mut RocksDbWriteBatch, key: TimelineKeyV1) {
        batch.delete_cf(TIMELINE_CF, key.encode());
    }

    /// Stores an 8-byte bucket count/byte summary under a caller-defined ordered bucket key.
    pub fn put_bucket_summary(&self, key: &[u8], count: u64, bytes: u64) -> Result<(), RocketMQError> {
        let mut value = [0u8; 16];
        value[..8].copy_from_slice(&count.to_be_bytes());
        value[8..].copy_from_slice(&bytes.to_be_bytes());
        self.store.put_cf(BUCKET_SUMMARY_CF, key, &value)
    }

    /// Appends one bucket summary replacement to an existing atomic Timeline batch.
    pub fn append_bucket_summary(batch: &mut RocksDbWriteBatch, key: Vec<u8>, count: u64, bytes: u64) {
        let mut value = [0u8; 16];
        value[..8].copy_from_slice(&count.to_be_bytes());
        value[8..].copy_from_slice(&bytes.to_be_bytes());
        batch.put_cf(BUCKET_SUMMARY_CF, key, value);
    }

    /// Reads one bucket count/byte summary.
    pub fn bucket_summary(&self, key: &[u8]) -> Result<Option<(u64, u64)>, RocketMQError> {
        self.store
            .get_cf(BUCKET_SUMMARY_CF, key)?
            .map(|value| {
                if value.len() != 16 {
                    return Err(codec_error("invalid Timeline bucket summary"));
                }
                let count = u64::from_be_bytes(value[..8].try_into().map_err(|_| codec_error("invalid count"))?);
                let bytes = u64::from_be_bytes(value[8..].try_into().map_err(|_| codec_error("invalid bytes"))?);
                Ok((count, bytes))
            })
            .transpose()
    }

    /// Closes the dedicated DB after all lifecycle-owned workers have stopped.
    pub fn close(&self) {
        self.store.close();
    }
}

fn encode_snapshot_pin(generation: u64, gc_fence: TimelineKeyV1) -> [u8; SNAPSHOT_PIN_VALUE_SIZE] {
    let mut value = [0u8; SNAPSHOT_PIN_VALUE_SIZE];
    value[..8].copy_from_slice(&generation.to_be_bytes());
    value[8..43].copy_from_slice(&gc_fence.encode());
    let checksum = crc32c(&value[..43]);
    value[43..].copy_from_slice(&checksum.to_be_bytes());
    value
}

fn decode_snapshot_pin(value: &[u8]) -> Result<TimelineSnapshotPin, RocketMQError> {
    if value.len() != SNAPSHOT_PIN_VALUE_SIZE
        || crc32c(&value[..43])
            != u32::from_be_bytes(
                value[43..47]
                    .try_into()
                    .map_err(|_| codec_error("invalid Timeline snapshot pin"))?,
            )
    {
        return Err(codec_error("invalid Timeline snapshot pin"));
    }
    let generation = u64::from_be_bytes(
        value[..8]
            .try_into()
            .map_err(|_| codec_error("invalid Timeline snapshot generation"))?,
    );
    if generation == 0 {
        return Err(codec_error("invalid Timeline snapshot generation"));
    }
    Ok(TimelineSnapshotPin {
        generation,
        gc_fence: TimelineKeyV1::decode(&value[8..43])?,
    })
}

/// Encodes a Java-compatible shadow key by durable source identity.
pub fn encode_shadow_key(
    source_cq_offset: i64,
    source_physical_offset: i64,
    generation: TimerGeneration,
) -> Result<[u8; SHADOW_KEY_SIZE], RocketMQError> {
    if source_cq_offset < 0 || source_physical_offset < 0 {
        return Err(codec_error("shadow source identity must be non-negative"));
    }
    let mut output = [0u8; SHADOW_KEY_SIZE];
    output[0] = SHADOW_KEY_VERSION;
    output[1..9].copy_from_slice(&source_cq_offset.to_be_bytes());
    output[9..17].copy_from_slice(&source_physical_offset.to_be_bytes());
    output[17..25].copy_from_slice(&generation.get().to_be_bytes());
    Ok(output)
}

fn encode_shadow_observation_key(
    source_cq_offset: i64,
    source_physical_offset: i64,
    generation: TimerGeneration,
    kind: ShadowObservationKind,
) -> Result<[u8; SHADOW_KEY_SIZE + 1], RocketMQError> {
    let source = encode_shadow_key(source_cq_offset, source_physical_offset, generation)?;
    let mut output = [0u8; SHADOW_KEY_SIZE + 1];
    output[0] = SHADOW_OBSERVATION_KEY_VERSION;
    output[1..SHADOW_KEY_SIZE].copy_from_slice(&source[1..]);
    output[SHADOW_KEY_SIZE] = kind as u8;
    Ok(output)
}
