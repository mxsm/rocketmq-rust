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

use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::timer::codec::RecallLookupKeyV1;
use rocketmq_store_rocksdb::timer::codec::RecallLookupValueV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::StateTransitionResult;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::LOOKUP_CF;

use super::TimelineReadyOutbox;

/// Durable, generation-fenced Recall transitions for the formal Extended Timeline.
pub(crate) struct TimelineRecallService {
    timeline: Arc<RocksDbTimelineIndex>,
    state: RocksDbTimelineStateIndex,
}

impl TimelineRecallService {
    pub(crate) fn new(timeline: Arc<RocksDbTimelineIndex>) -> Self {
        Self {
            state: timeline.state_index(),
            timeline,
        }
    }

    /// Adds the active-generation lookup to a materialization batch.
    pub(crate) fn append_lookup(
        batch: &mut RocksDbWriteBatch,
        key: &RecallLookupKeyV1,
        value: RecallLookupValueV1,
    ) -> Result<(), StoreError> {
        batch.put_cf(LOOKUP_CF, key.encode(StoreOperation::AppendDerived)?, value.encode());
        Ok(())
    }

    /// Cancels PENDING/READY only. Delivery states are immutable RecallTooLate outcomes.
    pub(crate) fn recall(&self, lookup: &RecallLookupKeyV1) -> Result<RecallResult, StoreError> {
        let Some(encoded) =
            self.timeline
                .store()
                .get_cf(StoreOperation::Read, LOOKUP_CF, &lookup.encode(StoreOperation::Read)?)?
        else {
            return Ok(RecallResult::NotFound);
        };
        let target = RecallLookupValueV1::decode(StoreOperation::Read, &encoded)?;
        let Some(current) = self.state.get(target.timer_id, target.generation)? else {
            return Ok(RecallResult::NotFound);
        };
        if current.route.engine_id() != lookup.engine || current.route.generation() != target.generation {
            return Ok(RecallResult::StaleGeneration);
        }
        match current.state {
            TimelineState::Pending | TimelineState::Ready => {
                let timeline_key = TimelineKeyV1 {
                    due_time_ms: target.due_time_ms,
                    lane: target.lane,
                    timer_id: target.timer_id,
                    generation: target.generation,
                };
                let mut side_effects = RocksDbWriteBatch::with_capacity(2);
                TimelineReadyOutbox::delete_ready(&mut side_effects, timeline_key);
                TimelineReadyOutbox::delete_late_ready(&mut side_effects, timeline_key);
                match self.state.compare_and_set(
                    target.timer_id,
                    target.generation,
                    current.state,
                    current.state_version,
                    TimelineState::Cancelled,
                    side_effects,
                )? {
                    StateTransitionResult::Applied(next) => Ok(RecallResult::Cancelled {
                        generation: target.generation.get(),
                        state_version: next.state_version,
                    }),
                    StateTransitionResult::Conflict(next) => Ok(result_for_state(next.state)),
                    StateTransitionResult::Missing => Ok(RecallResult::NotFound),
                }
            }
            state => Ok(result_for_state(state)),
        }
    }
}

fn result_for_state(state: TimelineState) -> RecallResult {
    match state {
        TimelineState::Cancelled => RecallResult::AlreadyCancelled,
        TimelineState::Delivering | TimelineState::Committing | TimelineState::Delivered => RecallResult::TooLate,
        TimelineState::Quarantined => RecallResult::Quarantined,
        TimelineState::SourceOnly | TimelineState::Pending | TimelineState::Ready => RecallResult::Retry,
    }
}

/// Stable Recall outcome used by adapters and tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecallResult {
    Cancelled { generation: u64, state_version: u64 },
    AlreadyCancelled,
    TooLate,
    StaleGeneration,
    Quarantined,
    Retry,
    NotFound,
}

#[cfg(test)]
mod tests {
    use rocketmq_store_api::PersistedTimerRoute;
    use rocketmq_store_api::TimerEngineEpoch;
    use rocketmq_store_api::TimerGeneration;
    use rocketmq_store_api::TimerId;
    use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
    use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
    use tempfile::tempdir;

    use super::*;

    fn state(generation: TimerGeneration, state: TimelineState) -> TimelineStateRecordV1 {
        TimelineStateRecordV1 {
            state,
            state_version: 4,
            route: PersistedTimerRoute::try_new(
                rocketmq_store_api::TimerEngineId::ExtendedTimeline,
                EXTENDED_TIMELINE_FORMAT_VERSION,
                1,
                generation,
                format!("token-{}", generation.get()),
            )
            .expect("route"),
            admission_epoch: TimerEngineEpoch::new(1),
            owner_epoch: TimerEngineEpoch::new(1),
            claim_seq: 0,
            due_time_ms: 1_000,
            lane: 0,
            terminal_at_ms: 0,
            shadow_only: false,
        }
    }

    #[test]
    fn recall_cancels_only_the_lookup_generation() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open")
                .expect("valid Timeline configuration"),
        );
        let timer_id = TimerId::new(7);
        let active = TimerGeneration::new(2);
        let old = TimerGeneration::new(1);
        let lookup = RecallLookupKeyV1 {
            engine: rocketmq_store_api::TimerEngineId::ExtendedTimeline,
            topic: "orders".to_string(),
            unique_key: "key".to_string(),
        };
        let mut batch = RocksDbWriteBatch::with_capacity(3);
        RocksDbTimelineStateIndex::append_state(&mut batch, timer_id, old, &state(old, TimelineState::Ready))
            .expect("old state");
        RocksDbTimelineStateIndex::append_state(&mut batch, timer_id, active, &state(active, TimelineState::Pending))
            .expect("active state");
        TimelineRecallService::append_lookup(
            &mut batch,
            &lookup,
            RecallLookupValueV1 {
                timer_id,
                generation: active,
                due_time_ms: 10_000,
                lane: 1,
                state_version: 4,
            },
        )
        .expect("lookup");
        timeline.write_batch(&batch).expect("write");

        let result = TimelineRecallService::new(Arc::clone(&timeline))
            .recall(&lookup)
            .expect("recall");
        assert!(matches!(result, RecallResult::Cancelled { generation: 2, .. }));
        let states = RocksDbTimelineStateIndex::new(timeline.store());
        let cancelled = states.get(timer_id, active).expect("active").expect("state");
        assert_eq!(cancelled.state, TimelineState::Cancelled);
        assert!(cancelled.terminal_at_ms > 0);
        assert_eq!(
            states.get(timer_id, old).expect("old").expect("state").state,
            TimelineState::Ready
        );
    }

    #[test]
    fn delivering_recall_is_too_late() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("open")
                .expect("valid Timeline configuration"),
        );
        let timer_id = TimerId::new(9);
        let generation = TimerGeneration::new(1);
        let lookup = RecallLookupKeyV1 {
            engine: rocketmq_store_api::TimerEngineId::ExtendedTimeline,
            topic: "orders".to_string(),
            unique_key: "late".to_string(),
        };
        let mut batch = RocksDbWriteBatch::with_capacity(2);
        RocksDbTimelineStateIndex::append_state(
            &mut batch,
            timer_id,
            generation,
            &state(generation, TimelineState::Delivering),
        )
        .expect("state");
        TimelineRecallService::append_lookup(
            &mut batch,
            &lookup,
            RecallLookupValueV1 {
                timer_id,
                generation,
                due_time_ms: 10_000,
                lane: 1,
                state_version: 4,
            },
        )
        .expect("lookup");
        timeline.write_batch(&batch).expect("write");

        assert_eq!(
            TimelineRecallService::new(timeline).recall(&lookup).expect("recall"),
            RecallResult::TooLate
        );
    }
}
