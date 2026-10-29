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

use rocketmq_store_local::timer::native_format::native_lane;
use rocketmq_store_local::timer::native_format::NativeCheckpoint;
use rocketmq_store_local::timer::native_format::NativeDueCursor;
use tempfile::tempdir;
use tempfile::TempDir;

use super::config::NativeTimerConfig;
use super::engine::NativeDueTimer;
use super::engine::NativeEngineError;
use super::engine::NativeRejection;
use super::engine::NativeSourceKind;
use super::engine::NativeTimerEngine;
use super::engine::NativeTimerSource;
use super::near_wheel::ENTRY_MEMORY_BYTES;

const HOUR_MS: i64 = 3_600_000;
const MINUTE_MS: i64 = 60_000;
/// The start of an hour.
const H0: i64 = 1_700_000_000_000 - 1_700_000_000_000 % HOUR_MS;
const LANES: u16 = 4;
const TOPIC: &str = "TopicA";

fn config() -> NativeTimerConfig {
    NativeTimerConfig {
        lane_count: LANES,
        ..NativeTimerConfig::default()
    }
}

fn open(root: &TempDir, config: NativeTimerConfig, now_ms: i64) -> NativeTimerEngine {
    NativeTimerEngine::open(root.path(), config, now_ms).unwrap().0
}

/// Timer `cq_offset` of queue 0, due at `due_ms`.
fn timer(cq_offset: i64, due_ms: i64) -> NativeTimerSource {
    NativeTimerSource {
        cq_offset,
        commit_log_offset: cq_offset * 100,
        size: 100,
        due_ms,
        generation: 0,
        real_topic: TOPIC.to_owned(),
        real_queue_id: 0,
        kind: NativeSourceKind::Timer {
            unique_key: format!("key-{cq_offset}"),
        },
    }
}

/// A recall of timer `target`, carried by Timer CQ record `cq_offset`.
fn recall(cq_offset: i64, target: i64, due_ms: i64) -> NativeTimerSource {
    NativeTimerSource {
        kind: NativeSourceKind::Recall {
            key: format!("{TOPIC}_key-{target}"),
        },
        ..timer(cq_offset, due_ms)
    }
}

fn lane() -> u16 {
    native_lane(TOPIC, 0, LANES)
}

fn due(engine: &NativeTimerEngine, now_ms: i64) -> Vec<NativeDueTimer> {
    engine.poll_due(lane(), now_ms, 1_000).unwrap()
}

fn offsets(timers: &[NativeDueTimer]) -> Vec<i64> {
    timers.iter().map(|timer| timer.entry.cq_offset).collect()
}

#[test]
fn due_timers_come_out_in_deadline_then_timer_cq_order_and_never_early() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let report = engine
        .ingest(
            &[
                timer(1, H0 + 2_000),
                timer(2, H0 + 1_000),
                timer(3, H0 + 2_000),
                timer(4, H0 + 1_500),
            ],
            H0,
        )
        .unwrap();
    assert_eq!((report.indexed, report.rejected.len()), (4, 0));

    assert!(due(&engine, H0 + 999).is_empty());
    assert_eq!(engine.next_deadline_ms(), Some(H0 + 1_000));
    assert_eq!(offsets(&due(&engine, H0 + 1_500)), vec![2, 4]);
    // Rounded up to the precision.
    assert_eq!(engine.next_deadline_ms(), Some(H0 + 2_000));
    assert_eq!(offsets(&due(&engine, H0 + 5_000)), vec![1, 3]);
    assert_eq!(engine.next_deadline_ms(), None);
}

#[test]
fn an_hour_bucket_loads_once_its_hour_comes_within_the_near_window() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    engine
        .ingest(&[timer(1, H0 + 5 * MINUTE_MS), timer(2, H0 + 3 * HOUR_MS + 1)], H0)
        .unwrap();
    assert_eq!(offsets(&due(&engine, H0 + 5 * MINUTE_MS)), vec![1]);
    assert_eq!(engine.counts(), (0, 0, 2));
    // The later bucket waits in its file until ten minutes before its hour.
    assert_eq!(engine.next_deadline_ms(), Some(H0 + 3 * HOUR_MS - 10 * MINUTE_MS));
    assert!(due(&engine, H0 + 3 * HOUR_MS - 10 * MINUTE_MS - 1).is_empty());
    assert_eq!(engine.counts().0, 0);
    assert!(due(&engine, H0 + 3 * HOUR_MS - 10 * MINUTE_MS).is_empty());
    assert_eq!(engine.counts().0, 1);

    // A timer indexed into a loaded hour goes straight to the wheel.
    engine.ingest(&[timer(3, H0 + 3 * HOUR_MS)], H0 + 2 * HOUR_MS).unwrap();
    assert_eq!(offsets(&due(&engine, H0 + 3 * HOUR_MS + 1)), vec![3, 2]);
}

#[test]
fn a_due_timer_carries_the_recalls_that_may_cancel_it() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let report = engine
        .ingest(
            &[
                timer(1, H0 + 1_000),
                timer(2, H0 + 1_000),
                recall(3, 1, H0 + 1_000),
                // Recalls the right key, but for another hour.
                recall(4, 2, H0 + HOUR_MS + 1_000),
            ],
            H0,
        )
        .unwrap();
    assert_eq!((report.indexed, report.recalls), (2, 2));
    let due = due(&engine, H0 + 1_000);
    assert_eq!(offsets(&due), vec![1, 2]);
    assert_eq!(due[0].recalls.len(), 1);
    assert_eq!(due[0].recalls[0].key, format!("{TOPIC}_key-1").into_bytes());
    assert!(due[1].recalls.is_empty());
}

#[test]
fn sources_past_the_mid_horizon_or_the_maximum_delay_are_not_indexed() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let report = engine
        .ingest(
            &[
                timer(1, -5),
                timer(2, H0 + 48 * HOUR_MS),
                timer(3, H0 + 401 * 24 * HOUR_MS),
                recall(4, 9, H0 + 49 * HOUR_MS),
                timer(5, H0 + 48 * HOUR_MS - 1),
            ],
            H0,
        )
        .unwrap();
    assert_eq!(report.indexed, 1);
    assert_eq!(
        report.rejected,
        vec![
            (1, NativeRejection::InvalidDeadline),
            (2, NativeRejection::BeyondMidHorizon),
            (3, NativeRejection::BeyondMaxDelay),
            (4, NativeRejection::BeyondMidHorizon),
        ]
    );
}

#[test]
fn past_the_memory_budget_only_started_hours_load() {
    let root = tempdir().unwrap();
    let config = NativeTimerConfig {
        near_window_ms: 3 * HOUR_MS,
        near_wheel_memory_bytes: 2 * ENTRY_MEMORY_BYTES,
        ..config()
    };
    let engine = open(&root, config, H0);
    let mut sources: Vec<_> = (1..=3).map(|cq| timer(cq, H0 + HOUR_MS + cq)).collect();
    sources.extend((4..=6).map(|cq| timer(cq, H0 + 2 * HOUR_MS + cq)));
    engine.ingest(&sources, H0).unwrap();

    // Both hours are within the window, but the first fills the budget.
    assert!(due(&engine, H0 + 10 * MINUTE_MS).is_empty());
    assert_eq!(engine.counts().0, 3);
    assert!(due(&engine, H0 + HOUR_MS - 1).is_empty());
    assert_eq!(engine.counts().0, 3);
    // Once its hour has started, a bucket loads whatever the budget says, so undelivered
    // earlier timers never hold back due ones.
    assert_eq!(offsets(&due(&engine, H0 + 2 * HOUR_MS + 10)), vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn delivered_timers_free_the_budget_for_the_next_hour() {
    let root = tempdir().unwrap();
    let config = NativeTimerConfig {
        near_window_ms: 3 * HOUR_MS,
        near_wheel_memory_bytes: 2 * ENTRY_MEMORY_BYTES,
        ..config()
    };
    let engine = open(&root, config, H0);
    let mut sources: Vec<_> = (1..=3).map(|cq| timer(cq, H0 + HOUR_MS + cq)).collect();
    sources.extend((4..=6).map(|cq| timer(cq, H0 + 2 * HOUR_MS + cq)));
    engine.ingest(&sources, H0).unwrap();

    assert_eq!(offsets(&due(&engine, H0 + HOUR_MS + 10)), vec![1, 2, 3]);
    assert!(due(&engine, H0 + HOUR_MS + 20).is_empty());
    assert_eq!(engine.counts().0, 3);
}

#[test]
fn reopening_after_a_checkpoint_keeps_timers_and_recalls() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    engine
        .ingest(
            &[timer(1, H0 + 1_000), timer(2, H0 + 2_000), recall(3, 2, H0 + 2_000)],
            H0,
        )
        .unwrap();
    let synced = engine.storage().sync().unwrap();
    let checkpoint = NativeCheckpoint {
        sequence: 1,
        written_ms: H0,
        source_cq_offset: 4,
        commit_log_fence: 0,
        cancel_log_length: synced.cancel_log_length,
        lane_watermarks: vec![NativeDueCursor::START; usize::from(LANES)],
        buckets: synced.buckets,
        payload_segments: Vec::new(),
    };
    engine.storage().write_checkpoint(&checkpoint).unwrap();
    // Not covered by the checkpoint: replayed from the Timer CQ instead.
    engine.ingest(&[timer(4, H0 + 3_000)], H0).unwrap();
    drop(engine);

    let (engine, recovered) = NativeTimerEngine::open(root.path(), config(), H0).unwrap();
    assert_eq!(recovered, Some(checkpoint));
    assert_eq!(engine.counts(), (0, 1, 1));
    let due = due(&engine, H0 + 5_000);
    assert_eq!(offsets(&due), vec![1, 2]);
    assert_eq!(due[1].recalls.len(), 1);
}

#[test]
fn an_invalid_setting_is_named() {
    let root = tempdir().unwrap();
    let config = NativeTimerConfig {
        lane_count: 0,
        ..config()
    };
    assert!(matches!(
        NativeTimerEngine::open(root.path(), config, H0),
        Err(NativeEngineError::InvalidConfig("laneCount"))
    ));
}
