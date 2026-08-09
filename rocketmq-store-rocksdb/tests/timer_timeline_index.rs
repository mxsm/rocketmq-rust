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

use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::config::RocksDbConfig;
use rocketmq_store_rocksdb::timer::codec::RecallLookupKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::StateTransitionResult;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use std::sync::Arc;
use std::sync::Barrier;

fn entry(id: u128, due_time_ms: i64, lane: u16, source_offset: i64, shadow_only: bool) -> TimelineIndexEntry {
    TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms,
            lane,
            timer_id: TimerId::new(id),
            generation: TimerGeneration::new(0),
        },
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(
                i32::try_from(due_time_ms.div_euclid(86_400_000)).expect("due day"),
                lane,
                1,
                source_offset as u64 * 100,
                64,
                id as u32,
            )
            .expect("payload locator"),
            source_cq_offset: TimerSourceCqOffset::new(source_offset),
            source_physical_offset: source_offset * 1_000,
            source_size: 64,
            state_version: 1,
            owner_engine: TimerEngineId::JavaCompat,
            shadow_only,
        },
    }
}

#[test]
fn timeline_config_is_physically_isolated_and_sync_wal_protected() {
    let directory = tempfile::tempdir().expect("root");
    let config = RocksDbConfig::timer_timeline(directory.path());
    assert!(config.path.ends_with("timer-extended/timeline-v1"));
    assert!(config.wal_enabled);
    assert!(config.sync_write);
    assert!(config.column_families.iter().any(|cf| cf.name == "timeline"));
    assert!(config.column_families.iter().any(|cf| cf.name == "shadow_timeline"));
}

#[test]
fn timeline_codec_preserves_deadline_lane_and_generation_order() {
    let key = |due_time_ms, lane, generation| {
        TimelineKeyV1 {
            due_time_ms,
            lane,
            timer_id: TimerId::new(7),
            generation: TimerGeneration::new(generation),
        }
        .encode()
    };
    assert!(key(7_999, 0, 0) < key(8_000, 0, 0));
    assert!(key(8_000, 0, 0) < key(8_000, 1, 0));
    assert!(key(8_000, 1, 0) < key(8_000, 1, 1));
    assert!(key(8_000, 1, 1) < key(8_001, 0, 0));

    let recall_a = RecallLookupKeyV1 {
        engine: TimerEngineId::ExtendedTimeline,
        topic: "a+b".to_string(),
        unique_key: "c".to_string(),
    }
    .encode()
    .expect("lookup");
    let recall_b = RecallLookupKeyV1 {
        engine: TimerEngineId::ExtendedTimeline,
        topic: "a".to_string(),
        unique_key: "b+c".to_string(),
    }
    .encode()
    .expect("lookup");
    assert_ne!(recall_a, recall_b, "length prefixes must prevent delimiter collisions");
}

#[test]
fn timeline_range_scan_is_bounded_and_continuable() {
    let directory = tempfile::tempdir().expect("root");
    let index = RocksDbTimelineIndex::open(directory.path()).expect("open");
    let records = [
        entry(1, 7_999, 0, 1, false),
        entry(2, 8_000, 1, 2, false),
        entry(3, 8_001, 0, 3, false),
    ];
    assert_eq!(index.put_batch(&records, None).expect("put"), 3);

    let first = index.range_scan(7_000, 9_000, None, 1, 1_024).expect("first page");
    assert_eq!(first.entries.len(), 1);
    assert!(first.continuation.is_some());
    let second = index
        .range_scan(7_000, 9_000, first.continuation, 2, 1_024)
        .expect("second page");
    assert_eq!(second.entries.len(), 2);
    assert_eq!(second.entries[0].key.due_time_ms, 8_000);
    assert!(second.retained_bytes <= 1_024);
}

#[test]
fn shadow_namespace_cannot_be_returned_by_formal_due_scan() {
    let directory = tempfile::tempdir().expect("root");
    let index = RocksDbTimelineIndex::open(directory.path()).expect("open");
    assert_eq!(
        index
            .put_batch(&[entry(1, 8_000, 0, 1, true)], None)
            .expect("put shadow"),
        1
    );
    assert!(index
        .range_scan(0, 9_000, None, 10, 4_096)
        .expect("scan")
        .entries
        .is_empty());
}

#[test]
fn state_compare_and_set_never_overwrites_a_conflict() {
    let directory = tempfile::tempdir().expect("root");
    let index = RocksDbTimelineIndex::open(directory.path()).expect("open");
    let state = RocksDbTimelineStateIndex::new(index.store());
    let timer_id = TimerId::new(7);
    let generation = TimerGeneration::new(3);
    let record = TimelineStateRecordV1 {
        state: TimelineState::Pending,
        state_version: 11,
        route: PersistedTimerRoute::try_new(
            TimerEngineId::ExtendedTimeline,
            EXTENDED_TIMELINE_FORMAT_VERSION,
            13,
            generation,
            "stable-token",
        )
        .expect("route"),
        admission_epoch: TimerEngineEpoch::new(17),
        owner_epoch: TimerEngineEpoch::new(19),
        claim_seq: 0,
        due_time_ms: 1_000,
        lane: 3,
        terminal_at_ms: 0,
        shadow_only: false,
    };
    state.put(timer_id, generation, &record).expect("put state");
    assert!(matches!(
        state
            .compare_and_set(
                timer_id,
                generation,
                TimelineState::Pending,
                11,
                TimelineState::Ready,
                RocksDbWriteBatch::default(),
            )
            .expect("transition"),
        StateTransitionResult::Applied(_)
    ));
    assert!(matches!(
        state
            .compare_and_set(
                timer_id,
                generation,
                TimelineState::Pending,
                11,
                TimelineState::Cancelled,
                RocksDbWriteBatch::default(),
            )
            .expect("conflict"),
        StateTransitionResult::Conflict(_)
    ));
}

#[test]
fn state_views_from_one_timeline_share_a_single_cas_domain() {
    let directory = tempfile::tempdir().expect("root");
    let index = RocksDbTimelineIndex::open(directory.path()).expect("open");
    let ready_view = index.state_index();
    let recall_view = index.state_index();
    let timer_id = TimerId::new(11);
    let generation = TimerGeneration::new(5);
    ready_view
        .put(
            timer_id,
            generation,
            &TimelineStateRecordV1 {
                state: TimelineState::Pending,
                state_version: 0,
                route: PersistedTimerRoute::try_new(
                    TimerEngineId::ExtendedTimeline,
                    EXTENDED_TIMELINE_FORMAT_VERSION,
                    1,
                    generation,
                    "race-token",
                )
                .expect("route"),
                admission_epoch: TimerEngineEpoch::new(1),
                owner_epoch: TimerEngineEpoch::new(1),
                claim_seq: 0,
                due_time_ms: 1_000,
                lane: 3,
                terminal_at_ms: 0,
                shadow_only: false,
            },
        )
        .expect("state");
    let barrier = Arc::new(Barrier::new(3));
    let ready_barrier = Arc::clone(&barrier);
    let ready = std::thread::spawn(move || {
        ready_barrier.wait();
        ready_view
            .compare_and_set(
                timer_id,
                generation,
                TimelineState::Pending,
                0,
                TimelineState::Ready,
                RocksDbWriteBatch::default(),
            )
            .expect("ready CAS")
    });
    let recall_barrier = Arc::clone(&barrier);
    let recall = std::thread::spawn(move || {
        recall_barrier.wait();
        recall_view
            .compare_and_set(
                timer_id,
                generation,
                TimelineState::Pending,
                0,
                TimelineState::Cancelled,
                RocksDbWriteBatch::default(),
            )
            .expect("recall CAS")
    });
    barrier.wait();
    let outcomes = [
        ready.join().expect("ready worker"),
        recall.join().expect("recall worker"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, StateTransitionResult::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, StateTransitionResult::Conflict(_)))
            .count(),
        1
    );
}
