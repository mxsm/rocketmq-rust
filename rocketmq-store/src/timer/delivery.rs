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

#[cfg(feature = "extended_timeline")]
use std::collections::HashMap;
#[cfg(feature = "extended_timeline")]
use std::sync::atomic::AtomicU64;
#[cfg(feature = "extended_timeline")]
use std::sync::atomic::Ordering;
#[cfg(feature = "extended_timeline")]
use std::sync::Arc;

#[cfg(feature = "extended_timeline")]
use bytes::Bytes;
#[cfg(feature = "extended_timeline")]
use cheetah_string::CheetahString;
#[cfg(feature = "extended_timeline")]
use parking_lot::Mutex;
#[cfg(feature = "extended_timeline")]
use rocketmq_model::common::message::message_accessor::MessageAccessor;
#[cfg(feature = "extended_timeline")]
use rocketmq_model::common::message::message_ext::MessageExt;
#[cfg(feature = "extended_timeline")]
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
#[cfg(feature = "extended_timeline")]
use rocketmq_model::common::message::message_single;
#[cfg(feature = "extended_timeline")]
use rocketmq_model::common::message::MessageConst;
#[cfg(feature = "extended_timeline")]
use rocketmq_model::common::message::MessageTrait;
#[cfg(feature = "extended_timeline")]
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
#[cfg(feature = "extended_timeline")]
use rocketmq_runtime::common::time_utils::current_millis;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_api::TimerEngineEpoch;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_api::TimerEngineId;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_api::TimerGeneration;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_api::TimerId;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_local::timer::payload_record::TimerPayloadRecordV1;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::timer::state_index::StateTransitionResult;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
#[cfg(feature = "extended_timeline")]
use thiserror::Error;

use crate::base::message_status_enum::PutMessageStatus;
#[cfg(feature = "extended_timeline")]
use crate::message_store::local_file_message_store::TimerMessageWriteHandle;
#[cfg(feature = "extended_timeline")]
use crate::timer::clock::TimerClockSafety;
#[cfg(feature = "extended_timeline")]
use crate::timer::clock::TimerClockState;
use crate::timer::error::CorruptionReason;
use crate::timer::error::RetryClass;
use crate::timer::error::TimerWorkResult;
#[cfg(feature = "extended_timeline")]
use crate::timer::role::TimerRoleState;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineCompletionReconciler;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineReadyOutbox;
#[cfg(feature = "extended_timeline")]
use crate::timer::timeline::TimelineReceiptStore;

pub(crate) fn classify_delivery_status(status: PutMessageStatus) -> TimerWorkResult {
    match status {
        PutMessageStatus::PutOk
        | PutMessageStatus::FlushDiskTimeout
        | PutMessageStatus::FlushSlaveTimeout
        | PutMessageStatus::SlaveNotAvailable => TimerWorkResult::Complete,
        PutMessageStatus::ServiceNotAvailable
        | PutMessageStatus::CreateMappedFileFailed
        | PutMessageStatus::OsPageCacheBusy
        | PutMessageStatus::InSyncReplicasNotEnough
        | PutMessageStatus::PutToRemoteBrokerFail
        | PutMessageStatus::WheelTimerFlowControl => TimerWorkResult::Retry(RetryClass::DeliveryRejected),
        PutMessageStatus::MessageIllegal
        | PutMessageStatus::PropertiesSizeExceeded
        | PutMessageStatus::UnknownError
        | PutMessageStatus::LmqConsumeQueueNumExceeded
        | PutMessageStatus::WheelTimerMsgIllegal
        | PutMessageStatus::WheelTimerNotEnable => TimerWorkResult::Quarantine(CorruptionReason::UnsupportedRecord),
    }
}

#[cfg(feature = "extended_timeline")]
pub(crate) fn delivery_shard(topic: &str, queue_id: i32, shard_count: usize) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in topic.as_bytes().iter().chain(queue_id.to_be_bytes().iter()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    (hash as usize) % shard_count.max(1)
}

/// Epoch-fenced Extended Timeline delivery owner.
#[cfg(feature = "extended_timeline")]
pub(crate) struct TimelineDeliveryCoordinator {
    timeline: Arc<RocksDbTimelineIndex>,
    state: RocksDbTimelineStateIndex,
    payload_store: Arc<TimerPayloadStore>,
    writer: TimerMessageWriteHandle,
    role: Arc<TimerRoleState>,
    clock: Arc<TimerClockSafety>,
    completion: Arc<TimelineCompletionReconciler>,
    activation_epoch: TimerEngineEpoch,
    lane_count: usize,
    page_messages: usize,
    page_bytes: usize,
    lease_ms: u64,
    next_claim_seq: AtomicU64,
    leases: Mutex<HashMap<(TimerId, TimerGeneration), DeliveryLease>>,
    recovery_cursor: Mutex<Option<(TimerId, TimerGeneration)>>,
}

#[cfg(feature = "extended_timeline")]
impl TimelineDeliveryCoordinator {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor mirrors independent durable delivery capabilities"
    )]
    pub(crate) fn new(
        timeline: Arc<RocksDbTimelineIndex>,
        payload_store: Arc<TimerPayloadStore>,
        writer: TimerMessageWriteHandle,
        role: Arc<TimerRoleState>,
        clock: Arc<TimerClockSafety>,
        completion: Arc<TimelineCompletionReconciler>,
        activation_epoch: TimerEngineEpoch,
        lane_count: usize,
        page_messages: usize,
        page_bytes: usize,
        lease_ms: u64,
    ) -> Result<Self, TimelineDeliveryError> {
        if activation_epoch.get() == 0 || lane_count == 0 || page_messages == 0 || page_bytes == 0 || lease_ms == 0 {
            return Err(TimelineDeliveryError::InvalidConfiguration);
        }
        Ok(Self {
            state: timeline.state_index(),
            timeline,
            payload_store,
            writer,
            role,
            clock,
            completion,
            activation_epoch,
            lane_count,
            page_messages,
            page_bytes,
            lease_ms,
            next_claim_seq: AtomicU64::new(0),
            leases: Mutex::new(HashMap::new()),
            recovery_cursor: Mutex::new(None),
        })
    }

    /// Reconciles final facts first, then claims and delivers one bounded ready page.
    pub(crate) async fn run_once(&self) -> Result<TimelineDeliveryRun, TimelineDeliveryError> {
        let observation = self.clock.observe();
        if observation.state == TimerClockState::Unsafe {
            return Ok(TimelineDeliveryRun {
                clock_unsafe: true,
                ..TimelineDeliveryRun::default()
            });
        }
        let Some(epoch) = self.role.capture_delivery_epoch() else {
            return Ok(TimelineDeliveryRun::default());
        };
        self.completion
            .run_once(self.page_messages.saturating_mul(4), self.page_bytes.saturating_mul(4))?;
        if self.completion.is_caught_up()? {
            self.recover_stale_claims(epoch)?;
        }
        let mut result = TimelineDeliveryRun::default();
        let outbox = TimelineReadyOutbox::new(Arc::clone(&self.timeline));
        for lane in 0..self.lane_count {
            if result.examined >= self.page_messages || result.bytes >= self.page_bytes {
                break;
            }
            let lane = u16::try_from(lane).map_err(|_| TimelineDeliveryError::InvalidConfiguration)?;
            let remaining = self.page_messages.saturating_sub(result.examined);
            for key in outbox.scan_ready(lane, remaining)? {
                result.examined = result.examined.saturating_add(1);
                let outcome = self.deliver_key(key, epoch, observation.wall_time_ms).await?;
                result.bytes = result.bytes.saturating_add(outcome.payload_bytes);
                match outcome.disposition {
                    DeliveryDisposition::Committed => result.committed = result.committed.saturating_add(1),
                    DeliveryDisposition::Recovered => result.recovered = result.recovered.saturating_add(1),
                    DeliveryDisposition::Skipped => {}
                }
                if result.examined >= self.page_messages || result.bytes >= self.page_bytes {
                    break;
                }
            }
        }
        Ok(result)
    }

    async fn deliver_key(
        &self,
        key: TimelineKeyV1,
        epoch: u64,
        wall_time_ms: i64,
    ) -> Result<DeliveryOutcome, TimelineDeliveryError> {
        let Some(current) = self.state.get(key.timer_id, key.generation)? else {
            return Err(TimelineDeliveryError::MissingState);
        };
        if current.state != TimelineState::Ready {
            return Ok(DeliveryOutcome::skipped());
        }
        if current.shadow_only
            || current.route.engine_id() != TimerEngineId::ExtendedTimeline
            || current.admission_epoch < self.activation_epoch
            || key.due_time_ms > wall_time_ms
        {
            return Ok(DeliveryOutcome::skipped());
        }
        if let Some(receipt) =
            TimelineReceiptStore::new(Arc::clone(&self.timeline)).get(current.route.delivery_token())?
        {
            if receipt.timer_id != key.timer_id || receipt.generation != key.generation {
                return Err(TimelineDeliveryError::ReceiptIdentityMismatch);
            }
            let mut side_effects = RocksDbWriteBatch::with_capacity(1);
            TimelineReadyOutbox::delete_ready(&mut side_effects, key);
            return match self.state.complete_from_receipt(
                key.timer_id,
                key.generation,
                current.route.delivery_token(),
                receipt.owner_epoch,
                side_effects,
            )? {
                StateTransitionResult::Applied(_) => Ok(DeliveryOutcome {
                    disposition: DeliveryDisposition::Recovered,
                    payload_bytes: 0,
                }),
                StateTransitionResult::Conflict(existing) if existing.state == TimelineState::Delivered => {
                    let mut cleanup = RocksDbWriteBatch::with_capacity(1);
                    TimelineReadyOutbox::delete_ready(&mut cleanup, key);
                    self.timeline.write_batch(&cleanup)?;
                    Ok(DeliveryOutcome {
                        disposition: DeliveryDisposition::Recovered,
                        payload_bytes: 0,
                    })
                }
                StateTransitionResult::Conflict(_) | StateTransitionResult::Missing => {
                    Err(TimelineDeliveryError::ReceiptStateConflict)
                }
            };
        }

        let claim_seq = self.next_claim_seq.fetch_add(1, Ordering::AcqRel).saturating_add(1);
        let owner_epoch = TimerEngineEpoch::new(epoch);
        let mut claim_side_effects = RocksDbWriteBatch::with_capacity(1);
        TimelineReadyOutbox::delete_ready(&mut claim_side_effects, key);
        let claimed = match self.state.claim_ready(
            key.timer_id,
            key.generation,
            current.state_version,
            owner_epoch,
            claim_seq,
            claim_side_effects,
        )? {
            StateTransitionResult::Applied(claimed) => claimed,
            StateTransitionResult::Conflict(_) | StateTransitionResult::Missing => {
                return Ok(DeliveryOutcome::skipped());
            }
        };
        let deadline = self.clock.observe().monotonic_time_ms.saturating_add(self.lease_ms);
        self.leases.lock().insert(
            (key.timer_id, key.generation),
            DeliveryLease {
                owner_epoch,
                claim_seq,
                deadline_monotonic_ms: deadline,
            },
        );

        let result = self.deliver_claimed(key, claimed, wall_time_ms).await;
        if result.is_err() {
            self.leases.lock().remove(&(key.timer_id, key.generation));
        }
        result
    }

    fn recover_stale_claims(&self, current_epoch: u64) -> Result<(), TimelineDeliveryError> {
        let continuation = *self.recovery_cursor.lock();
        let page = self.state.scan_after(continuation, self.page_messages)?;
        let receipts = TimelineReceiptStore::new(Arc::clone(&self.timeline));
        for entry in page.entries {
            if !matches!(
                entry.record.state,
                TimelineState::Delivering | TimelineState::Committing
            ) || entry.record.owner_epoch.get() == current_epoch
            {
                continue;
            }
            if entry.record.due_time_ms <= 0 {
                self.state.compare_and_set(
                    entry.timer_id,
                    entry.generation,
                    entry.record.state,
                    entry.record.state_version,
                    TimelineState::Quarantined,
                    RocksDbWriteBatch::with_capacity(0),
                )?;
                continue;
            }
            let key = TimelineKeyV1 {
                due_time_ms: entry.record.due_time_ms,
                lane: entry.record.lane,
                timer_id: entry.timer_id,
                generation: entry.generation,
            };
            if let Some(receipt) = receipts.get(entry.record.route.delivery_token())? {
                let mut side_effects = RocksDbWriteBatch::with_capacity(1);
                TimelineReadyOutbox::delete_ready(&mut side_effects, key);
                self.state.complete_from_receipt(
                    entry.timer_id,
                    entry.generation,
                    entry.record.route.delivery_token(),
                    receipt.owner_epoch,
                    side_effects,
                )?;
            } else {
                let mut side_effects = RocksDbWriteBatch::with_capacity(1);
                TimelineReadyOutbox::append_ready(&mut side_effects, key, entry.record.state_version.saturating_add(1));
                self.state.compare_and_set(
                    entry.timer_id,
                    entry.generation,
                    entry.record.state,
                    entry.record.state_version,
                    TimelineState::Ready,
                    side_effects,
                )?;
            }
        }
        *self.recovery_cursor.lock() = page.continuation;
        Ok(())
    }

    async fn deliver_claimed(
        &self,
        key: TimelineKeyV1,
        claimed: rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1,
        initial_wall_time_ms: i64,
    ) -> Result<DeliveryOutcome, TimelineDeliveryError> {
        let record = self.timeline.get(key)?.ok_or(TimelineDeliveryError::MissingTimeline)?;
        if record.shadow_only || record.owner_engine != TimerEngineId::ExtendedTimeline {
            self.quarantine_claim(key, claimed.state_version)?;
            return Err(TimelineDeliveryError::OwnerMismatch);
        }
        let payload = match self.payload_store.read(record.payload) {
            Ok(payload) => payload,
            Err(error) => {
                self.quarantine_claim(key, claimed.state_version)?;
                return Err(error.into());
            }
        };
        if let Err(error) = validate_payload(key, &payload) {
            self.quarantine_claim(key, claimed.state_version)?;
            return Err(error);
        }
        let payload_bytes = payload.frame.len();
        let message = match build_final_message(&payload, &claimed, key) {
            Ok(message) => message,
            Err(error) => {
                self.quarantine_claim(key, claimed.state_version)?;
                return Err(error);
            }
        };

        let final_observation = self.clock.observe();
        let lease_current = self
            .leases
            .lock()
            .get(&(key.timer_id, key.generation))
            .copied()
            .is_some_and(|lease| {
                lease.owner_epoch == claimed.owner_epoch
                    && lease.claim_seq == claimed.claim_seq
                    && final_observation.monotonic_time_ms <= lease.deadline_monotonic_ms
            });
        if final_observation.state == TimerClockState::Unsafe
            || final_observation.wall_time_ms < key.due_time_ms
            || initial_wall_time_ms < key.due_time_ms
            || !lease_current
            || !self.role.is_current_delivery_epoch(claimed.owner_epoch.get())
        {
            self.return_claim_to_ready(key, claimed.state_version)?;
            return Ok(DeliveryOutcome::skipped());
        }
        let current = self
            .state
            .get(key.timer_id, key.generation)?
            .ok_or(TimelineDeliveryError::MissingState)?;
        if current.state != TimelineState::Delivering
            || current.state_version != claimed.state_version
            || current.owner_epoch != claimed.owner_epoch
            || current.claim_seq != claimed.claim_seq
        {
            return Ok(DeliveryOutcome::skipped());
        }
        let committing = match self.state.compare_and_set(
            key.timer_id,
            key.generation,
            TimelineState::Delivering,
            current.state_version,
            TimelineState::Committing,
            RocksDbWriteBatch::with_capacity(0),
        )? {
            StateTransitionResult::Applied(committing) => committing,
            StateTransitionResult::Conflict(_) | StateTransitionResult::Missing => {
                return Ok(DeliveryOutcome::skipped());
            }
        };
        if !self.role.is_current_delivery_epoch(committing.owner_epoch.get())
            || self.clock.state() == TimerClockState::Unsafe
            || self.clock.observe().wall_time_ms < key.due_time_ms
        {
            self.return_committing_to_ready(key, committing.state_version)?;
            return Ok(DeliveryOutcome::skipped());
        }

        let put_result = self.writer.put_message(message).await;
        self.leases.lock().remove(&(key.timer_id, key.generation));
        if put_result.is_ok() {
            return Ok(DeliveryOutcome {
                disposition: DeliveryDisposition::Committed,
                payload_bytes,
            });
        }
        self.return_committing_to_ready(key, committing.state_version)?;
        Ok(DeliveryOutcome::skipped())
    }

    fn quarantine_claim(&self, key: TimelineKeyV1, state_version: u64) -> Result<(), TimelineDeliveryError> {
        self.state.compare_and_set(
            key.timer_id,
            key.generation,
            TimelineState::Delivering,
            state_version,
            TimelineState::Quarantined,
            RocksDbWriteBatch::with_capacity(0),
        )?;
        Ok(())
    }

    fn return_claim_to_ready(&self, key: TimelineKeyV1, state_version: u64) -> Result<(), TimelineDeliveryError> {
        let mut side_effects = RocksDbWriteBatch::with_capacity(1);
        TimelineReadyOutbox::append_ready(&mut side_effects, key, state_version.saturating_add(1));
        self.state.compare_and_set(
            key.timer_id,
            key.generation,
            TimelineState::Delivering,
            state_version,
            TimelineState::Ready,
            side_effects,
        )?;
        self.leases.lock().remove(&(key.timer_id, key.generation));
        Ok(())
    }

    fn return_committing_to_ready(&self, key: TimelineKeyV1, state_version: u64) -> Result<(), TimelineDeliveryError> {
        let mut side_effects = RocksDbWriteBatch::with_capacity(1);
        TimelineReadyOutbox::append_ready(&mut side_effects, key, state_version.saturating_add(1));
        self.state.compare_and_set(
            key.timer_id,
            key.generation,
            TimelineState::Committing,
            state_version,
            TimelineState::Ready,
            side_effects,
        )?;
        Ok(())
    }
}

#[cfg(feature = "extended_timeline")]
#[derive(Clone, Copy, Debug)]
struct DeliveryLease {
    owner_epoch: TimerEngineEpoch,
    claim_seq: u64,
    deadline_monotonic_ms: u64,
}

#[cfg(feature = "extended_timeline")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryDisposition {
    Committed,
    Recovered,
    Skipped,
}

#[cfg(feature = "extended_timeline")]
#[derive(Clone, Copy, Debug)]
struct DeliveryOutcome {
    disposition: DeliveryDisposition,
    payload_bytes: usize,
}

#[cfg(feature = "extended_timeline")]
impl DeliveryOutcome {
    const fn skipped() -> Self {
        Self {
            disposition: DeliveryDisposition::Skipped,
            payload_bytes: 0,
        }
    }
}

#[cfg(feature = "extended_timeline")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TimelineDeliveryRun {
    pub(crate) examined: usize,
    pub(crate) committed: usize,
    pub(crate) recovered: usize,
    pub(crate) bytes: usize,
    pub(crate) clock_unsafe: bool,
}

#[cfg(feature = "extended_timeline")]
fn validate_payload(key: TimelineKeyV1, payload: &TimerPayloadRecordV1) -> Result<(), TimelineDeliveryError> {
    if payload.timer_id != key.timer_id
        || payload.generation != key.generation
        || payload.due_time_ms != key.due_time_ms
        || payload.lane != key.lane
    {
        return Err(TimelineDeliveryError::PayloadIdentityMismatch);
    }
    Ok(())
}

#[cfg(feature = "extended_timeline")]
fn build_final_message(
    payload: &TimerPayloadRecordV1,
    state: &rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1,
    key: TimelineKeyV1,
) -> Result<MessageExtBrokerInner, TimelineDeliveryError> {
    let mut frame = Bytes::copy_from_slice(&payload.frame);
    let source = MessageDecoder::decode(&mut frame, true, false, false, false, false)
        .ok_or(TimelineDeliveryError::PayloadFrameInvalid)?;
    let mut final_message = copy_message(source);
    final_message.set_topic(CheetahString::from_string(payload.real_topic.clone()));
    final_message.message_ext_inner.queue_id = payload.real_queue_id;
    for property in [
        MessageConst::PROPERTY_REAL_TOPIC,
        MessageConst::PROPERTY_REAL_QUEUE_ID,
        MessageConst::PROPERTY_TIMER_OUT_MS,
        MessageConst::PROPERTY_TIMER_DELAY_SEC,
        MessageConst::PROPERTY_TIMER_DELAY_MS,
        MessageConst::PROPERTY_TIMER_DELIVER_MS,
        MessageConst::PROPERTY_TIMER_ROLL_TIMES,
        MessageConst::PROPERTY_TIMER_ROLL_LABEL,
        MessageConst::PROPERTY_TIMER_DEL_UNIQKEY,
    ] {
        MessageAccessor::clear_property(&mut final_message, property);
    }
    for (property, value) in [
        (
            MessageConst::PROPERTY_TIMER_DELIVERY_TOKEN,
            state.route.delivery_token().to_owned(),
        ),
        (MessageConst::PROPERTY_TIMER_ID, key.timer_id.get().to_string()),
        (
            MessageConst::PROPERTY_TIMER_GENERATION,
            key.generation.get().to_string(),
        ),
        (
            MessageConst::TIMER_ENGINE_TYPE,
            TimerEngineId::ExtendedTimeline.as_str().to_owned(),
        ),
        (
            MessageConst::PROPERTY_TIMER_OWNER_EPOCH,
            state.owner_epoch.get().to_string(),
        ),
        (MessageConst::PROPERTY_TIMER_LANE, key.lane.to_string()),
        (
            MessageConst::PROPERTY_TIMER_ORIGINAL_DELIVER_MS,
            key.due_time_ms.to_string(),
        ),
    ] {
        MessageAccessor::put_property(
            &mut final_message,
            CheetahString::from_static_str(property),
            CheetahString::from_string(value),
        );
    }
    final_message.properties_string = MessageDecoder::message_properties_to_string(final_message.get_properties());
    Ok(final_message)
}

#[cfg(feature = "extended_timeline")]
fn copy_message(source: MessageExt) -> MessageExtBrokerInner {
    let sys_flag = source.sys_flag();
    let born_timestamp = source.born_timestamp();
    let born_host = source.born_host();
    let store_host = source.store_host();
    let reconsume_times = source.reconsume_times();
    let store_timestamp = source.store_timestamp();
    let source_message = source.message;
    let mut inner = MessageExtBrokerInner::default();
    if let Some(body) = source_message.get_body() {
        inner.set_body(body.clone());
    }
    inner.set_flag(source_message.flag());
    MessageAccessor::set_properties(&mut inner, source_message.properties().as_map().clone());
    MessageAccessor::put_property(
        &mut inner,
        CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_ENQUEUE_MS),
        CheetahString::from_string(store_timestamp.to_string()),
    );
    MessageAccessor::put_property(
        &mut inner,
        CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DEQUEUE_MS),
        CheetahString::from_string((current_millis() as i64).to_string()),
    );
    inner.message_ext_inner.sys_flag = sys_flag;
    inner.message_ext_inner.born_timestamp = born_timestamp;
    inner.message_ext_inner.born_host = born_host;
    inner.message_ext_inner.store_host = store_host;
    inner.message_ext_inner.reconsume_times = reconsume_times;
    inner.set_wait_store_msg_ok(false);
    let topic_filter_type = message_single::parse_topic_filter_type(inner.sys_flag());
    inner.tags_code = MessageExtBrokerInner::tags_string2tags_code(
        &topic_filter_type,
        inner.tags().as_ref().unwrap_or(&CheetahString::empty()),
    );
    inner
}

#[cfg(feature = "extended_timeline")]
#[derive(Debug, Error)]
pub(crate) enum TimelineDeliveryError {
    #[error("invalid Extended delivery configuration")]
    InvalidConfiguration,
    #[error("Timeline storage failure: {0}")]
    Timeline(#[from] rocketmq_error::RocketMQError),
    #[error("payload storage failure: {0}")]
    Payload(#[from] crate::store_error::StoreError),
    #[error("completion replay failure: {0}")]
    Completion(#[from] crate::timer::timeline::TimelineCompletionError),
    #[error("formal Timeline entry has no state")]
    MissingState,
    #[error("formal Timeline entry is missing")]
    MissingTimeline,
    #[error("formal Timeline owner does not match Extended")]
    OwnerMismatch,
    #[error("payload identity does not match Timeline key")]
    PayloadIdentityMismatch,
    #[error("completion receipt identity does not match Timeline key")]
    ReceiptIdentityMismatch,
    #[error("completion receipt conflicts with the durable timer state")]
    ReceiptStateConflict,
    #[error("payload CommitLog frame cannot be decoded")]
    PayloadFrameInvalid,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_delivery_errors_have_one_explicit_policy() {
        assert_eq!(
            classify_delivery_status(PutMessageStatus::OsPageCacheBusy),
            TimerWorkResult::Retry(RetryClass::DeliveryRejected)
        );
        assert_eq!(
            classify_delivery_status(PutMessageStatus::MessageIllegal),
            TimerWorkResult::Quarantine(CorruptionReason::UnsupportedRecord)
        );
    }

    #[test]
    #[cfg(feature = "extended_timeline")]
    fn timer_same_fifo_group_always_uses_the_same_shard() {
        assert_eq!(delivery_shard("orders", 7, 8), delivery_shard("orders", 7, 8));
    }
}
