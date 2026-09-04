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

use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;

use crate::batch::RocksDbWriteBatch;
use crate::error::codec_contract;
use crate::error::codec_corrupted;
use crate::error::state_corrupted_source;
use crate::iterator::RocksDbRangeScanOptions;
use crate::store::KeyValueStore;
use crate::store::RocksDbStore;
use crate::timer::codec::crc32c;
use crate::timer::codec::decode_engine;
use crate::timer::codec::encode_engine;
use crate::timer::STATE_CF;

const STATE_KEY_VERSION: u8 = 1;
const STATE_KEY_SIZE: usize = 25;
const LEGACY_STATE_VALUE_FIXED_SIZE: usize = 45;
const STATE_VALUE_FIXED_SIZE: usize = 71;

/// Durable Extended Timeline state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TimelineState {
    /// Payload handoff has not completed.
    SourceOnly = 0,
    /// Payload and Timeline record are durable.
    Pending = 1,
    /// Durable ready outbox contains this generation.
    Ready = 2,
    /// A delivery worker owns the generation.
    Delivering = 3,
    /// Final CommitLog put succeeded and completion is being recorded.
    Committing = 4,
    /// Final delivery fact is durable.
    Delivered = 5,
    /// Recall cancelled this generation before delivery started.
    Cancelled = 6,
    /// Corruption prevents automatic progress.
    Quarantined = 7,
}

impl TimelineState {
    fn decode(operation: StoreOperation, value: u8) -> Result<Self, StoreError> {
        match value {
            0 => Ok(Self::SourceOnly),
            1 => Ok(Self::Pending),
            2 => Ok(Self::Ready),
            3 => Ok(Self::Delivering),
            4 => Ok(Self::Committing),
            5 => Ok(Self::Delivered),
            6 => Ok(Self::Cancelled),
            7 => Ok(Self::Quarantined),
            _ => Err(codec_corrupted(operation)),
        }
    }

    /// Returns true when Recall may atomically cancel this state.
    pub const fn is_recallable(self) -> bool {
        matches!(self, Self::Pending | Self::Ready)
    }

    /// Returns true when no automatic transition may leave this state.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Delivered | Self::Cancelled | Self::Quarantined)
    }

    /// Returns true only for state-machine edges defined by the V1 delivery protocol.
    pub const fn allows_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::SourceOnly, Self::Pending)
                | (Self::SourceOnly, Self::Quarantined)
                | (Self::Pending, Self::Ready)
                | (Self::Pending, Self::Cancelled)
                | (Self::Pending, Self::Quarantined)
                | (Self::Ready, Self::Delivering)
                | (Self::Ready, Self::Cancelled)
                | (Self::Ready, Self::Quarantined)
                | (Self::Delivering, Self::Committing)
                | (Self::Delivering, Self::Ready)
                | (Self::Delivering, Self::Quarantined)
                | (Self::Committing, Self::Delivered)
                | (Self::Committing, Self::Ready)
                | (Self::Committing, Self::Quarantined)
        )
    }
}

/// State and immutable owner route for one timer generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelineStateRecordV1 {
    /// Current state.
    pub state: TimelineState,
    /// Monotonic compare-and-set version.
    pub state_version: u64,
    /// Immutable owner and delivery-token route.
    pub route: PersistedTimerRoute,
    /// Activation epoch captured when the timer was admitted.
    pub admission_epoch: TimerEngineEpoch,
    /// Engine-owner epoch fencing stale workers.
    pub owner_epoch: TimerEngineEpoch,
    /// Monotonic claim sequence within the owner epoch.
    pub claim_seq: u64,
    /// Exact unrounded deadline retained for lease recovery.
    pub due_time_ms: i64,
    /// Stable lane retained for ready-outbox reconstruction.
    pub lane: u16,
    /// Wall-clock time when DELIVERED or CANCELLED became durable.
    ///
    /// Legacy records decode as zero and are not automatically reclaimed.
    pub terminal_at_ms: i64,
    /// Shadow records never enter formal delivery states.
    pub shadow_only: bool,
}

impl TimelineStateRecordV1 {
    /// Encodes a state record with a trailing CRC32C.
    ///
    /// # Errors
    ///
    /// Returns an error if the delivery token exceeds the V1 length field.
    pub fn encode(&self, operation: StoreOperation) -> Result<Vec<u8>, StoreError> {
        let token = self.route.delivery_token().as_bytes();
        let token_len = u16::try_from(token.len()).map_err(|_| codec_contract(operation))?;
        let mut output = Vec::with_capacity(STATE_VALUE_FIXED_SIZE + token.len());
        output.extend_from_slice(&EXTENDED_TIMELINE_FORMAT_VERSION.to_be_bytes());
        output.push(self.state as u8);
        output.extend_from_slice(&self.state_version.to_be_bytes());
        output.extend_from_slice(&self.admission_epoch.get().to_be_bytes());
        output.extend_from_slice(&self.owner_epoch.get().to_be_bytes());
        output.push(encode_engine(self.route.engine_id()));
        output.extend_from_slice(&self.route.format_version().to_be_bytes());
        output.extend_from_slice(&self.route.normalization_policy_fingerprint().to_be_bytes());
        output.extend_from_slice(&token_len.to_be_bytes());
        output.extend_from_slice(token);
        output.push(u8::from(self.shadow_only));
        output.extend_from_slice(&self.claim_seq.to_be_bytes());
        output.extend_from_slice(&self.due_time_ms.to_be_bytes());
        output.extend_from_slice(&self.lane.to_be_bytes());
        output.extend_from_slice(&self.terminal_at_ms.to_be_bytes());
        let checksum = crc32c(&output);
        output.extend_from_slice(&checksum.to_be_bytes());
        Ok(output)
    }

    /// Decodes a state value using the generation from its key.
    pub fn decode(operation: StoreOperation, bytes: &[u8], generation: TimerGeneration) -> Result<Self, StoreError> {
        if bytes.len() < LEGACY_STATE_VALUE_FIXED_SIZE
            || read_u16(operation, bytes, 0)? != EXTENDED_TIMELINE_FORMAT_VERSION
            || crc32c(&bytes[..bytes.len() - 4]) != read_u32(operation, bytes, bytes.len() - 4)?
        {
            return Err(codec_corrupted(operation));
        }
        let token_len = usize::from(read_u16(operation, bytes, 38)?);
        let token_start = 40usize;
        let token_end = token_start.saturating_add(token_len);
        let legacy = token_end.saturating_add(5) == bytes.len();
        let claim_only = token_end.saturating_add(13) == bytes.len();
        let recovery_fields = token_end.saturating_add(23) == bytes.len();
        let current = token_end.saturating_add(31) == bytes.len();
        if token_len == 0 || (!legacy && !claim_only && !recovery_fields && !current) || bytes[token_end] > 1 {
            return Err(codec_corrupted(operation));
        }
        let token = std::str::from_utf8(&bytes[token_start..token_end])
            .map_err(|source| state_corrupted_source(operation, source))?;
        let route = PersistedTimerRoute::try_new(
            decode_engine(operation, bytes[27])?,
            read_u16(operation, bytes, 28)?,
            read_u64(operation, bytes, 30)?,
            generation,
            token,
        )
        .map_err(|source| state_corrupted_source(operation, source))?;
        Ok(Self {
            state: TimelineState::decode(operation, bytes[2])?,
            state_version: read_u64(operation, bytes, 3)?,
            route,
            admission_epoch: TimerEngineEpoch::new(read_u64(operation, bytes, 11)?),
            owner_epoch: TimerEngineEpoch::new(read_u64(operation, bytes, 19)?),
            claim_seq: if claim_only || recovery_fields || current {
                read_u64(operation, bytes, token_end + 1)?
            } else {
                0
            },
            due_time_ms: if recovery_fields || current {
                read_i64(operation, bytes, token_end + 9)?
            } else {
                0
            },
            lane: if recovery_fields || current {
                read_u16(operation, bytes, token_end + 17)?
            } else {
                0
            },
            terminal_at_ms: if current {
                read_i64(operation, bytes, token_end + 19)?
            } else {
                0
            },
            shadow_only: bytes[token_end] == 1,
        })
    }
}

/// Result of one serialized state compare-and-set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateTransitionResult {
    /// The requested transition was persisted.
    Applied(TimelineStateRecordV1),
    /// The record exists but its version or state no longer matches.
    Conflict(TimelineStateRecordV1),
    /// No state exists for this timer generation.
    Missing,
}

/// Decoded state entry returned by bounded recovery scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelineStateEntry {
    /// Timer identity from the state key.
    pub timer_id: TimerId,
    /// Generation from the state key.
    pub generation: TimerGeneration,
    /// Verified state value.
    pub record: TimelineStateRecordV1,
}

/// Bounded state scan page with an exclusive continuation identity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimelineStatePage {
    /// Verified entries in key order.
    pub entries: Vec<TimelineStateEntry>,
    /// Last identity when another page may remain.
    pub continuation: Option<(TimerId, TimerGeneration)>,
}

/// Serialized state-index operations over the dedicated Timeline database.
pub struct RocksDbTimelineStateIndex {
    store: Arc<RocksDbStore>,
    transition_lock: Arc<Mutex<()>>,
}

impl RocksDbTimelineStateIndex {
    /// Creates a state-index view over an already opened dedicated Timeline DB.
    pub fn new(store: Arc<RocksDbStore>) -> Self {
        Self {
            store,
            transition_lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn with_transition_lock(store: Arc<RocksDbStore>, transition_lock: Arc<Mutex<()>>) -> Self {
        Self { store, transition_lock }
    }

    /// Reads and verifies one state record.
    pub fn get(
        &self,
        timer_id: TimerId,
        generation: TimerGeneration,
    ) -> Result<Option<TimelineStateRecordV1>, StoreError> {
        self.store
            .get_cf(
                rocketmq_store_api::StoreOperation::Read,
                STATE_CF,
                &encode_state_key(timer_id, generation),
            )?
            .map(|bytes| TimelineStateRecordV1::decode(StoreOperation::Read, &bytes, generation))
            .transpose()
    }

    /// Reads a bounded group of state records with one RocksDB multi-get.
    pub fn get_many(
        &self,
        keys: &[(TimerId, TimerGeneration)],
    ) -> Result<Vec<Option<TimelineStateRecordV1>>, StoreError> {
        let encoded = keys
            .iter()
            .map(|(timer_id, generation)| encode_state_key(*timer_id, *generation).to_vec())
            .collect::<Vec<_>>();
        self.store
            .multi_get_cf(rocketmq_store_api::StoreOperation::Read, STATE_CF, &encoded)?
            .into_iter()
            .zip(keys)
            .map(|(value, (_, generation))| {
                value
                    .map(|value| TimelineStateRecordV1::decode(StoreOperation::Read, &value, *generation))
                    .transpose()
            })
            .collect()
    }

    /// Scans a bounded state page for restart/promotion lease recovery.
    pub fn scan(&self, max_records: usize) -> Result<Vec<TimelineStateEntry>, StoreError> {
        Ok(self.scan_after(None, max_records)?.entries)
    }

    /// Scans after an exclusive timer/generation identity for bounded full-state recovery.
    pub fn scan_after(
        &self,
        continuation: Option<(TimerId, TimerGeneration)>,
        max_records: usize,
    ) -> Result<TimelineStatePage, StoreError> {
        if max_records == 0 {
            return Ok(TimelineStatePage::default());
        }
        let start = continuation.map_or_else(
            || {
                let mut start = [0u8; STATE_KEY_SIZE];
                start[0] = STATE_KEY_VERSION;
                start
            },
            |(timer_id, generation)| encode_state_key(timer_id, generation),
        );
        let end = [STATE_KEY_VERSION.saturating_add(1)];
        let scan_limit = max_records.saturating_add(1);
        let mut entries = self
            .store
            .range_scan(
                rocketmq_store_api::StoreOperation::Read,
                &RocksDbRangeScanOptions::new(STATE_CF, start, end, scan_limit),
            )?
            .into_iter()
            .map(|item| {
                let (timer_id, generation) = decode_state_key(StoreOperation::Read, &item.key)?;
                Ok(TimelineStateEntry {
                    timer_id,
                    generation,
                    record: TimelineStateRecordV1::decode(StoreOperation::Read, &item.value, generation)?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let scan_was_full = entries.len() == scan_limit;
        if let Some(continuation) = continuation {
            entries.retain(|entry| (entry.timer_id, entry.generation) > continuation);
        }
        let has_more = scan_was_full || entries.len() > max_records;
        entries.truncate(max_records);
        let continuation = has_more.then(|| {
            entries
                .last()
                .map(|entry| (entry.timer_id, entry.generation))
                .unwrap_or(continuation.unwrap_or((TimerId::new(0), TimerGeneration::new(0))))
        });
        Ok(TimelineStatePage { entries, continuation })
    }

    /// Writes one state record.
    pub fn put(
        &self,
        timer_id: TimerId,
        generation: TimerGeneration,
        record: &TimelineStateRecordV1,
    ) -> Result<(), StoreError> {
        self.store.put_cf(
            StoreOperation::AppendDerived,
            STATE_CF,
            &encode_state_key(timer_id, generation),
            &record.encode(StoreOperation::AppendDerived)?,
        )
    }

    /// Appends one state record to an existing atomic materialization batch.
    pub fn append_state(
        batch: &mut RocksDbWriteBatch,
        timer_id: TimerId,
        generation: TimerGeneration,
        record: &TimelineStateRecordV1,
    ) -> Result<(), StoreError> {
        batch.put_cf(
            STATE_CF,
            encode_state_key(timer_id, generation),
            record.encode(StoreOperation::AppendDerived)?,
        );
        Ok(())
    }

    /// Appends state deletion after all terminal GC fences have been checked.
    pub fn append_delete(batch: &mut RocksDbWriteBatch, timer_id: TimerId, generation: TimerGeneration) {
        batch.delete_cf(STATE_CF, encode_state_key(timer_id, generation));
    }

    /// Applies a state transition only when both state and state version match.
    pub fn compare_and_set(
        &self,
        timer_id: TimerId,
        generation: TimerGeneration,
        expected_state: TimelineState,
        expected_version: u64,
        next_state: TimelineState,
        mut side_effects: RocksDbWriteBatch,
    ) -> Result<StateTransitionResult, StoreError> {
        let _guard = self
            .transition_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        let Some(current) = self.get(timer_id, generation)? else {
            return Ok(StateTransitionResult::Missing);
        };
        if current.state != expected_state || current.state_version != expected_version {
            return Ok(StateTransitionResult::Conflict(current));
        }
        if !current.state.allows_transition_to(next_state) {
            return Err(codec_contract(StoreOperation::AppendDerived));
        }
        let mut next = current;
        next.state = next_state;
        next.state_version = next.state_version.saturating_add(1);
        if matches!(next_state, TimelineState::Delivered | TimelineState::Cancelled) {
            next.terminal_at_ms = current_time_millis();
        }
        side_effects.put_cf(
            STATE_CF,
            encode_state_key(timer_id, generation),
            next.encode(StoreOperation::AppendDerived)?,
        );
        self.store
            .write_batch(rocketmq_store_api::StoreOperation::AppendDerived, &side_effects)?;
        Ok(StateTransitionResult::Applied(next))
    }

    /// Claims one READY generation for an epoch-fenced delivery worker.
    ///
    /// The owner epoch and claim sequence are committed in the same batch as the
    /// READY-to-DELIVERING transition, making this the Recall/delivery linearization point.
    pub fn claim_ready(
        &self,
        timer_id: TimerId,
        generation: TimerGeneration,
        expected_version: u64,
        owner_epoch: TimerEngineEpoch,
        claim_seq: u64,
        mut side_effects: RocksDbWriteBatch,
    ) -> Result<StateTransitionResult, StoreError> {
        if owner_epoch.get() == 0 || claim_seq == 0 {
            return Err(codec_contract(StoreOperation::AppendDerived));
        }
        let _guard = self
            .transition_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        let Some(current) = self.get(timer_id, generation)? else {
            return Ok(StateTransitionResult::Missing);
        };
        if current.state != TimelineState::Ready || current.state_version != expected_version {
            return Ok(StateTransitionResult::Conflict(current));
        }
        let mut next = current;
        next.state = TimelineState::Delivering;
        next.state_version = next.state_version.saturating_add(1);
        next.owner_epoch = owner_epoch;
        next.claim_seq = claim_seq;
        side_effects.put_cf(
            STATE_CF,
            encode_state_key(timer_id, generation),
            next.encode(StoreOperation::AppendDerived)?,
        );
        self.store
            .write_batch(rocketmq_store_api::StoreOperation::AppendDerived, &side_effects)?;
        Ok(StateTransitionResult::Applied(next))
    }

    /// Applies a replicated final fact and its side effects atomically.
    ///
    /// READY is accepted only after a previous claim (`claim_seq > 0`), covering the narrow
    /// case where lease recovery raced with delayed CommitLog replay. A never-claimed READY or
    /// a cancelled generation fails closed.
    pub fn complete_from_receipt(
        &self,
        timer_id: TimerId,
        generation: TimerGeneration,
        delivery_token: &str,
        owner_epoch: TimerEngineEpoch,
        mut side_effects: RocksDbWriteBatch,
    ) -> Result<StateTransitionResult, StoreError> {
        let _guard = self
            .transition_lock
            .lock()
            .map_err(|_| crate::error::internal_failure(rocketmq_store_api::StoreOperation::AppendDerived))?;
        let Some(current) = self.get(timer_id, generation)? else {
            return Ok(StateTransitionResult::Missing);
        };
        if current.route.delivery_token() != delivery_token || current.owner_epoch != owner_epoch {
            return Ok(StateTransitionResult::Conflict(current));
        }
        if current.state == TimelineState::Delivered {
            return Ok(StateTransitionResult::Conflict(current));
        }
        let receipt_proves_delivery = matches!(current.state, TimelineState::Delivering | TimelineState::Committing)
            || (current.state == TimelineState::Ready && current.claim_seq > 0);
        if !receipt_proves_delivery {
            return Ok(StateTransitionResult::Conflict(current));
        }
        let mut next = current;
        next.state = TimelineState::Delivered;
        next.state_version = next.state_version.saturating_add(1);
        next.terminal_at_ms = current_time_millis();
        side_effects.put_cf(
            STATE_CF,
            encode_state_key(timer_id, generation),
            next.encode(StoreOperation::AppendDerived)?,
        );
        self.store
            .write_batch(rocketmq_store_api::StoreOperation::AppendDerived, &side_effects)?;
        Ok(StateTransitionResult::Applied(next))
    }
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

/// Encodes a fixed state key.
pub fn encode_state_key(timer_id: TimerId, generation: TimerGeneration) -> [u8; STATE_KEY_SIZE] {
    let mut output = [0u8; STATE_KEY_SIZE];
    output[0] = STATE_KEY_VERSION;
    output[1..17].copy_from_slice(&timer_id.get().to_be_bytes());
    output[17..25].copy_from_slice(&generation.get().to_be_bytes());
    output
}

/// Decodes a fixed state key.
pub fn decode_state_key(operation: StoreOperation, bytes: &[u8]) -> Result<(TimerId, TimerGeneration), StoreError> {
    if bytes.len() != STATE_KEY_SIZE || bytes[0] != STATE_KEY_VERSION {
        return Err(codec_corrupted(operation));
    }
    Ok((
        TimerId::new(read_u128(operation, bytes, 1)?),
        TimerGeneration::new(read_u64(operation, bytes, 17)?),
    ))
}

fn read_array<const N: usize>(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<[u8; N], StoreError> {
    let value = bytes
        .get(offset..offset.saturating_add(N))
        .ok_or_else(|| codec_corrupted(operation))?;
    value
        .try_into()
        .map_err(|source| state_corrupted_source(operation, source))
}

fn read_u16(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u16, StoreError> {
    Ok(u16::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_u32(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u32, StoreError> {
    Ok(u32::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_u64(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u64, StoreError> {
    Ok(u64::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_i64(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<i64, StoreError> {
    Ok(i64::from_be_bytes(read_array(operation, bytes, offset)?))
}

fn read_u128(operation: StoreOperation, bytes: &[u8], offset: usize) -> Result<u128, StoreError> {
    Ok(u128::from_be_bytes(read_array(operation, bytes, offset)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(state: TimelineState) -> TimelineStateRecordV1 {
        TimelineStateRecordV1 {
            state,
            state_version: 9,
            route: PersistedTimerRoute::try_new(
                rocketmq_store_api::TimerEngineId::ExtendedTimeline,
                EXTENDED_TIMELINE_FORMAT_VERSION,
                17,
                TimerGeneration::new(3),
                "stable-token",
            )
            .expect("route"),
            admission_epoch: TimerEngineEpoch::new(5),
            owner_epoch: TimerEngineEpoch::new(7),
            claim_seq: 11,
            due_time_ms: 1_000,
            lane: 3,
            terminal_at_ms: 0,
            shadow_only: false,
        }
    }

    #[test]
    fn state_codec_round_trips_and_rejects_crc_damage() {
        let expected = record(TimelineState::Pending);
        let mut encoded = expected.encode(StoreOperation::AppendDerived).expect("encode");
        assert_eq!(
            TimelineStateRecordV1::decode(StoreOperation::Read, &encoded, TimerGeneration::new(3)).expect("decode"),
            expected
        );
        encoded[3] ^= 1;
        assert!(TimelineStateRecordV1::decode(StoreOperation::Read, &encoded, TimerGeneration::new(3)).is_err());
    }

    #[test]
    fn state_codec_reads_the_pre_terminal_timestamp_layout_fail_closed() {
        let expected = record(TimelineState::Delivered);
        let mut legacy = expected
            .encode(StoreOperation::AppendDerived)
            .expect("encode current state");
        let checksum_start = legacy.len() - 4;
        legacy.drain(checksum_start - 8..checksum_start);
        legacy.truncate(legacy.len() - 4);
        legacy.extend_from_slice(&crc32c(&legacy).to_be_bytes());

        let decoded = TimelineStateRecordV1::decode(StoreOperation::Read, &legacy, TimerGeneration::new(3))
            .expect("decode prior recovery layout");
        assert_eq!(decoded.state, TimelineState::Delivered);
        assert_eq!(decoded.terminal_at_ms, 0);
    }
}
