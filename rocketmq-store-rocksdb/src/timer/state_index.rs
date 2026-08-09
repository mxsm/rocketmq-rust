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

use rocketmq_error::RocketMQError;
use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;

use crate::batch::RocksDbWriteBatch;
use crate::error::codec_error;
use crate::store::KeyValueStore;
use crate::store::RocksDbStore;
use crate::timer::codec::crc32c;
use crate::timer::codec::decode_engine;
use crate::timer::codec::encode_engine;
use crate::timer::STATE_CF;

const STATE_KEY_VERSION: u8 = 1;
const STATE_KEY_SIZE: usize = 25;
const STATE_VALUE_FIXED_SIZE: usize = 45;

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
    fn decode(value: u8) -> Result<Self, RocketMQError> {
        match value {
            0 => Ok(Self::SourceOnly),
            1 => Ok(Self::Pending),
            2 => Ok(Self::Ready),
            3 => Ok(Self::Delivering),
            4 => Ok(Self::Committing),
            5 => Ok(Self::Delivered),
            6 => Ok(Self::Cancelled),
            7 => Ok(Self::Quarantined),
            _ => Err(codec_error("unknown extended timer state")),
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
    /// Shadow records never enter formal delivery states.
    pub shadow_only: bool,
}

impl TimelineStateRecordV1 {
    /// Encodes a state record with a trailing CRC32C.
    ///
    /// # Errors
    ///
    /// Returns an error if the delivery token exceeds the V1 length field.
    pub fn encode(&self) -> Result<Vec<u8>, RocketMQError> {
        let token = self.route.delivery_token().as_bytes();
        let token_len = u16::try_from(token.len()).map_err(|_| codec_error("delivery token is too long"))?;
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
        let checksum = crc32c(&output);
        output.extend_from_slice(&checksum.to_be_bytes());
        Ok(output)
    }

    /// Decodes a state value using the generation from its key.
    pub fn decode(bytes: &[u8], generation: TimerGeneration) -> Result<Self, RocketMQError> {
        if bytes.len() < STATE_VALUE_FIXED_SIZE
            || read_u16(bytes, 0)? != EXTENDED_TIMELINE_FORMAT_VERSION
            || crc32c(&bytes[..bytes.len() - 4]) != read_u32(bytes, bytes.len() - 4)?
        {
            return Err(codec_error("invalid extended timer state version, length, or CRC"));
        }
        let token_len = usize::from(read_u16(bytes, 38)?);
        let token_start = 40usize;
        let token_end = token_start.saturating_add(token_len);
        if token_len == 0 || token_end.saturating_add(5) != bytes.len() || bytes[token_end] > 1 {
            return Err(codec_error("invalid extended timer state token or flags"));
        }
        let token = std::str::from_utf8(&bytes[token_start..token_end])
            .map_err(|_| codec_error("delivery token is not UTF-8"))?;
        let route = PersistedTimerRoute::try_new(
            decode_engine(bytes[27])?,
            read_u16(bytes, 28)?,
            read_u64(bytes, 30)?,
            generation,
            token,
        )
        .map_err(|error| codec_error(error.to_string()))?;
        Ok(Self {
            state: TimelineState::decode(bytes[2])?,
            state_version: read_u64(bytes, 3)?,
            route,
            admission_epoch: TimerEngineEpoch::new(read_u64(bytes, 11)?),
            owner_epoch: TimerEngineEpoch::new(read_u64(bytes, 19)?),
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
    ) -> Result<Option<TimelineStateRecordV1>, RocketMQError> {
        self.store
            .get_cf(STATE_CF, &encode_state_key(timer_id, generation))?
            .map(|bytes| TimelineStateRecordV1::decode(&bytes, generation))
            .transpose()
    }

    /// Writes one state record.
    pub fn put(
        &self,
        timer_id: TimerId,
        generation: TimerGeneration,
        record: &TimelineStateRecordV1,
    ) -> Result<(), RocketMQError> {
        self.store
            .put_cf(STATE_CF, &encode_state_key(timer_id, generation), &record.encode()?)
    }

    /// Appends one state record to an existing atomic materialization batch.
    pub fn append_state(
        batch: &mut RocksDbWriteBatch,
        timer_id: TimerId,
        generation: TimerGeneration,
        record: &TimelineStateRecordV1,
    ) -> Result<(), RocketMQError> {
        batch.put_cf(STATE_CF, encode_state_key(timer_id, generation), record.encode()?);
        Ok(())
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
    ) -> Result<StateTransitionResult, RocketMQError> {
        let _guard = self.transition_lock.lock().map_err(|error| {
            RocketMQError::storage_write_failed("timer-timeline", format!("state transition lock poisoned: {error}"))
        })?;
        let Some(current) = self.get(timer_id, generation)? else {
            return Ok(StateTransitionResult::Missing);
        };
        if current.state != expected_state || current.state_version != expected_version {
            return Ok(StateTransitionResult::Conflict(current));
        }
        if !current.state.allows_transition_to(next_state) {
            return Err(codec_error("illegal Extended Timeline state transition"));
        }
        let mut next = current;
        next.state = next_state;
        next.state_version = next.state_version.saturating_add(1);
        side_effects.put_cf(STATE_CF, encode_state_key(timer_id, generation), next.encode()?);
        self.store.write_batch(&side_effects)?;
        Ok(StateTransitionResult::Applied(next))
    }
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
pub fn decode_state_key(bytes: &[u8]) -> Result<(TimerId, TimerGeneration), RocketMQError> {
    if bytes.len() != STATE_KEY_SIZE || bytes[0] != STATE_KEY_VERSION {
        return Err(codec_error("invalid extended timer state key"));
    }
    Ok((
        TimerId::new(read_u128(bytes, 1)?),
        TimerGeneration::new(read_u64(bytes, 17)?),
    ))
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], RocketMQError> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| codec_error("truncated extended timer state record"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, RocketMQError> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, RocketMQError> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, RocketMQError> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u128(bytes: &[u8], offset: usize) -> Result<u128, RocketMQError> {
    Ok(u128::from_be_bytes(read_array(bytes, offset)?))
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
            shadow_only: false,
        }
    }

    #[test]
    fn state_codec_round_trips_and_rejects_crc_damage() {
        let expected = record(TimelineState::Pending);
        let mut encoded = expected.encode().expect("encode");
        assert_eq!(
            TimelineStateRecordV1::decode(&encoded, TimerGeneration::new(3)).expect("decode"),
            expected
        );
        encoded[3] ^= 1;
        assert!(TimelineStateRecordV1::decode(&encoded, TimerGeneration::new(3)).is_err());
    }
}
