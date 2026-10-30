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

use bytes::Bytes;
use rocketmq_store_local::timer::native_format::native_lane;
use rocketmq_store_local::timer::native_format::NativeBucketId;
use rocketmq_store_local::timer::native_format::NativeBucketKind;
use rocketmq_store_local::timer::native_format::NativeDueCursor;
use rocketmq_store_local::timer::native_format::NativeLocator;
use rocketmq_store_local::timer::native_format::NativePayloadSegment;
use rocketmq_store_local::timer::native_storage::NativePayloadLocation;
use rocketmq_store_local::timer::payload_record::TimerPayloadRecordV1;
use tempfile::tempdir;
use tempfile::TempDir;

use super::config::NativeTimerConfig;
use super::engine::NativeDueTimer;
use super::engine::NativeEngineError;
use super::engine::NativeIngestReport;
use super::engine::NativeRejection;
use super::engine::NativeSourceKind;
use super::engine::NativeTimerEngine;
use super::engine::NativeTimerSource;
use super::near_wheel::ENTRY_MEMORY_BYTES;

mod collect;
mod delivery;
mod recall;

const HOUR_MS: i64 = 3_600_000;
const MINUTE_MS: i64 = 60_000;
const DAY_MS: i64 = 24 * HOUR_MS;
/// The start of an hour.
const H0: i64 = 1_700_000_000_000 - 1_700_000_000_000 % HOUR_MS;
const LANES: u16 = 4;
const TOPIC: &str = "TopicA";
/// Past every CommitLog offset the tests use.
const COMMIT_LOG_END: i64 = 1_000_000;

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
        frame: Bytes::from(format!("frame-{cq_offset}").into_bytes()),
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

/// Indexes `sources` as one batch ending after the last of them.
fn ingest(engine: &NativeTimerEngine, sources: &[NativeTimerSource], now_ms: i64) -> NativeIngestReport {
    let next = sources.iter().map(|source| source.cq_offset + 1).max().unwrap_or(0);
    engine.ingest(sources, next, now_ms).unwrap()
}

fn lane() -> u16 {
    native_lane(TOPIC, 0, LANES)
}

/// Takes the due timers, everything before `now_ms` indexed.
fn due(engine: &NativeTimerEngine, now_ms: i64) -> Vec<NativeDueTimer> {
    engine.set_visible_through(now_ms);
    engine.poll_due(lane(), now_ms, 1_000).unwrap()
}

/// Takes the due timers and records them delivered.
fn deliver(engine: &NativeTimerEngine, now_ms: i64) -> Vec<i64> {
    let due = due(engine, now_ms);
    if let Some(last) = due.last() {
        engine.advance_delivered(lane(), last.entry.cursor());
    }
    offsets(&due)
}

fn offsets(timers: &[NativeDueTimer]) -> Vec<i64> {
    timers.iter().map(|timer| timer.entry.cq_offset).collect()
}

#[test]
fn due_timers_come_out_in_deadline_then_timer_cq_order_and_never_early() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let report = ingest(
        &engine,
        &[
            timer(1, H0 + 2_000),
            timer(2, H0 + 1_000),
            timer(3, H0 + 2_000),
            timer(4, H0 + 1_500),
        ],
        H0,
    );
    assert_eq!((report.indexed, report.rejected.len()), (4, 0));
    assert_eq!(engine.source_frontier(), 5);

    assert!(due(&engine, H0 + 999).is_empty());
    assert_eq!(engine.next_deadline_ms(), Some(H0 + 1_000));
    assert_eq!(offsets(&due(&engine, H0 + 1_500)), vec![2, 4]);
    // Rounded up to the precision.
    assert_eq!(engine.next_deadline_ms(), Some(H0 + 2_000));
    assert_eq!(offsets(&due(&engine, H0 + 5_000)), vec![1, 3]);
    assert_eq!(engine.next_deadline_ms(), None);
}

#[test]
fn an_hour_is_split_into_minute_runs_and_a_minute_loads_once_it_comes_near() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &[
            timer(1, H0 + 5 * MINUTE_MS),
            timer(2, H0 + 3 * HOUR_MS + 1),
            timer(4, H0 + 3 * HOUR_MS + 30 * MINUTE_MS),
        ],
        H0,
    );
    assert_eq!(offsets(&due(&engine, H0 + 5 * MINUTE_MS)), vec![1]);
    let counts = engine.counts();
    assert_eq!((counts.wheel, counts.hours, counts.minutes), (0, 1, 1));
    // The later hour waits in its bucket until ten minutes before it starts.
    assert_eq!(engine.next_deadline_ms(), Some(H0 + 3 * HOUR_MS - 10 * MINUTE_MS));
    assert!(due(&engine, H0 + 3 * HOUR_MS - 10 * MINUTE_MS - 1).is_empty());
    assert_eq!(engine.counts().hours, 1);
    assert!(due(&engine, H0 + 3 * HOUR_MS - 10 * MINUTE_MS).is_empty());
    // Split into its two minutes; only the first is near enough to load.
    let counts = engine.counts();
    assert_eq!((counts.wheel, counts.hours, counts.minutes), (1, 0, 3));
    assert_eq!(engine.next_deadline_ms(), Some(H0 + 3 * HOUR_MS + 1_000));

    // A timer indexed into a loaded minute goes straight to the wheel.
    ingest(&engine, &[timer(3, H0 + 3 * HOUR_MS)], H0 + 2 * HOUR_MS);
    assert_eq!(offsets(&due(&engine, H0 + 3 * HOUR_MS + 1)), vec![3, 2]);
    assert_eq!(engine.counts().wheel, 0);
    assert_eq!(offsets(&due(&engine, H0 + 3 * HOUR_MS + 30 * MINUTE_MS)), vec![4]);
}

#[test]
fn a_due_timer_carries_the_recalls_that_may_cancel_it() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let report = ingest(
        &engine,
        &[
            timer(1, H0 + 1_000),
            timer(2, H0 + 1_000),
            recall(3, 1, H0 + 1_000),
            recall(4, 9, H0 + 1_000),
        ],
        H0,
    );
    assert_eq!((report.indexed, report.recalls), (2, 2));
    let due = due(&engine, H0 + 1_000);
    assert_eq!(offsets(&due), vec![1, 2]);
    assert_eq!(due[0].recalls.len(), 1);
    assert_eq!(due[0].recalls[0].key, format!("{TOPIC}_key-1").into_bytes());
    assert!(due[1].recalls.is_empty());
}

#[test]
fn sources_past_the_maximum_delay_or_without_a_copyable_record_are_not_indexed() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let mut empty_frame = timer(4, H0 + 3 * DAY_MS);
    empty_frame.frame = Bytes::new();
    let report = ingest(
        &engine,
        &[
            timer(1, -5),
            timer(2, H0 + 3 * DAY_MS),
            timer(3, H0 + 401 * DAY_MS),
            empty_frame,
            timer(5, H0 + 48 * HOUR_MS - 1),
            recall(6, 2, H0 + 3 * DAY_MS),
        ],
        H0,
    );
    assert_eq!((report.indexed, report.recalls), (2, 1));
    assert_eq!(
        report.rejected,
        vec![
            (1, NativeRejection::InvalidDeadline),
            (3, NativeRejection::BeyondMaxDelay),
            (4, NativeRejection::InvalidPayload),
        ]
    );
    // Rejected sources are passed all the same.
    assert_eq!(engine.source_frontier(), 7);
    let counts = engine.counts();
    assert_eq!((counts.days, counts.hours), (1, 1));
}

#[test]
fn past_the_memory_budget_only_started_minutes_load() {
    let root = tempdir().unwrap();
    let config = NativeTimerConfig {
        near_window_ms: 3 * HOUR_MS,
        near_wheel_memory_bytes: 2 * ENTRY_MEMORY_BYTES,
        ..config()
    };
    let engine = open(&root, config, H0);
    let mut sources: Vec<_> = (1..=3).map(|cq| timer(cq, H0 + HOUR_MS + cq)).collect();
    sources.extend((4..=6).map(|cq| timer(cq, H0 + 2 * HOUR_MS + cq)));
    ingest(&engine, &sources, H0);

    // Both minutes are within the window, but the first fills the budget.
    assert!(due(&engine, H0 + 10 * MINUTE_MS).is_empty());
    assert_eq!(engine.counts().wheel, 3);
    assert!(due(&engine, H0 + HOUR_MS - 1).is_empty());
    assert_eq!(engine.counts().wheel, 3);
    // Once its minute has started, a run loads whatever the budget says, so undelivered
    // earlier timers never hold back due ones.
    assert_eq!(offsets(&due(&engine, H0 + 2 * HOUR_MS + 10)), vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn delivered_timers_free_the_budget_for_the_next_minute() {
    let root = tempdir().unwrap();
    let config = NativeTimerConfig {
        near_window_ms: 3 * HOUR_MS,
        near_wheel_memory_bytes: 2 * ENTRY_MEMORY_BYTES,
        ..config()
    };
    let engine = open(&root, config, H0);
    let mut sources: Vec<_> = (1..=3).map(|cq| timer(cq, H0 + HOUR_MS + cq)).collect();
    sources.extend((4..=6).map(|cq| timer(cq, H0 + 2 * HOUR_MS + cq)));
    ingest(&engine, &sources, H0);

    assert_eq!(offsets(&due(&engine, H0 + HOUR_MS + 10)), vec![1, 2, 3]);
    assert!(due(&engine, H0 + HOUR_MS + 20).is_empty());
    assert_eq!(engine.counts().wheel, 3);
}

#[test]
fn a_crash_replays_the_sources_after_the_checkpoint_and_redelivers_no_delivered_timer() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &[
            timer(0, H0 + 1_000),
            timer(1, H0 + 2_000),
            timer(2, H0 + 3_000),
            timer(3, H0 + 4_000),
            recall(4, 3, H0 + 4_000),
        ],
        H0,
    );
    assert_eq!(deliver(&engine, H0 + 2_000), vec![0, 1]);
    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0 + 2_000).unwrap();
    assert_eq!(checkpoint.sequence, 1);
    assert_eq!(checkpoint.source_cq_offset, 5);
    // Not covered by the checkpoint: indexed, and timer 2 delivered.
    ingest(&engine, &[timer(5, H0 + 5_000), timer(6, H0 + 6_000)], H0 + 2_000);
    assert_eq!(deliver(&engine, H0 + 3_000), vec![2]);
    drop(engine);

    let (engine, recovered) = NativeTimerEngine::open(root.path(), config(), H0 + 3_000).unwrap();
    assert_eq!(recovered, Some(checkpoint));
    assert_eq!(engine.source_frontier(), 5);
    assert_eq!(engine.counts().recalls, 1);
    // The replay of the Timer CQ from the checkpoint brings back what it did not cover.
    ingest(&engine, &[timer(5, H0 + 5_000), timer(6, H0 + 6_000)], H0 + 3_000);
    let due = due(&engine, H0 + 10_000);
    // Timer 2 was delivered after the checkpoint, so it comes again; 0 and 1 do not.
    assert_eq!(offsets(&due), vec![2, 3, 5, 6]);
    assert_eq!(due[1].recalls.len(), 1);
}

#[test]
fn a_timer_behind_its_lane_watermark_still_survives_a_crash() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 5_000)], H0);
    assert_eq!(deliver(&engine, H0 + 5_000), vec![1]);
    // Due before the timer the lane delivered last; it reached the engine late.
    ingest(&engine, &[timer(2, H0 + 1_000)], H0 + 5_000);
    engine.checkpoint(COMMIT_LOG_END, H0 + 5_000).unwrap();
    drop(engine);

    let engine = open(&root, config(), H0 + 5_000);
    let due = due(&engine, H0 + 5_000);
    assert_eq!(offsets(&due), vec![2]);
    // Indexed right after the watermark, not before its own deadline.
    assert_eq!(due[0].entry.due_ms, H0 + 5_000);
}

#[test]
fn the_fence_keeps_the_lowest_commit_log_offset_still_needed() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(7, H0 + 1_000), timer(3, H0 + 2_000)], H0);
    // The pending timers point at offsets 700 and 300; the next source is at 900.
    assert_eq!(engine.checkpoint(900, H0).unwrap().commit_log_fence, 300);
    assert_eq!(engine.checkpoint(200, H0).unwrap().commit_log_fence, 200);
    drop(engine);

    // After a restart the buckets found on disk keep the fence they had.
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(9, H0 + 3_000)], H0);
    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0).unwrap();
    assert_eq!((checkpoint.sequence, checkpoint.commit_log_fence), (3, 200));
}

#[test]
fn a_lane_watermark_never_moves_back() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let ahead = NativeDueCursor {
        due_ms: H0 + 5_000,
        cq_offset: 4,
    };
    engine.advance_delivered(lane(), ahead);
    engine.advance_delivered(
        lane(),
        NativeDueCursor {
            due_ms: H0,
            cq_offset: 9,
        },
    );
    assert_eq!(
        engine.checkpoint(0, H0).unwrap().lane_watermarks[usize::from(lane())],
        ahead
    );
}

/// The file of the hour bucket starting at `start_ms`.
fn hour_file(engine: &NativeTimerEngine, start_ms: i64) -> std::path::PathBuf {
    let hour = NativeBucketId::starting_at(NativeBucketKind::Hour, start_ms).unwrap();
    engine.storage().root().join(hour.relative_path())
}

#[test]
fn a_split_hour_bucket_is_deleted_only_once_a_checkpoint_leaves_it_out() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &[timer(1, H0 + HOUR_MS + 1_000), timer(2, H0 + HOUR_MS + 61_000)],
        H0,
    );
    assert!(due(&engine, H0 + HOUR_MS - 10 * MINUTE_MS).is_empty());
    assert_eq!(engine.counts().minutes, 2);
    // Split and retired, but the last checkpoint may still name it.
    assert!(hour_file(&engine, H0 + HOUR_MS).exists());

    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0).unwrap();
    assert!(checkpoint
        .buckets
        .iter()
        .all(|bucket| bucket.bucket.kind() == NativeBucketKind::Minute));
    assert_eq!(checkpoint.buckets.len(), 2);
    assert!(!hour_file(&engine, H0 + HOUR_MS).exists());
    drop(engine);

    let engine = open(&root, config(), H0 + HOUR_MS);
    let counts = engine.counts();
    assert_eq!((counts.hours, counts.minutes), (0, 2));
    assert_eq!(offsets(&due(&engine, H0 + HOUR_MS + 61_000)), vec![1, 2]);
}

#[test]
fn a_crash_before_the_checkpoint_that_drops_a_split_hour_recovers_the_hour_bucket() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &[timer(1, H0 + HOUR_MS + 1_000), timer(2, H0 + HOUR_MS + 61_000)],
        H0,
    );
    engine.checkpoint(COMMIT_LOG_END, H0).unwrap();
    // Split after the checkpoint, then lost.
    assert!(due(&engine, H0 + HOUR_MS - 10 * MINUTE_MS).is_empty());
    drop(engine);

    let engine = open(&root, config(), H0 + HOUR_MS);
    let counts = engine.counts();
    assert_eq!((counts.hours, counts.minutes), (1, 0));
    // Split again, and every timer comes out once.
    assert_eq!(offsets(&due(&engine, H0 + HOUR_MS + 61_000)), vec![1, 2]);
}

#[test]
fn a_timer_of_a_split_hour_goes_to_its_minute_run() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 1_000)], H0);
    assert_eq!(offsets(&due(&engine, H0 + 1_000)), vec![1]);
    // The hour is split, but this minute is beyond the near window.
    ingest(&engine, &[timer(2, H0 + 40 * MINUTE_MS)], H0);
    let counts = engine.counts();
    assert_eq!((counts.wheel, counts.hours, counts.minutes), (0, 0, 2));
    assert!(due(&engine, H0 + 30 * MINUTE_MS - 1).is_empty());
    assert!(due(&engine, H0 + 30 * MINUTE_MS).is_empty());
    assert_eq!(engine.counts().wheel, 1);
    assert_eq!(offsets(&due(&engine, H0 + 40 * MINUTE_MS)), vec![2]);
}

/// The start of the UTC day three days after `H0`.
const D3: i64 = (H0 / DAY_MS + 3) * DAY_MS;

/// The copied record of a due timer that points at one.
fn copy_of(engine: &NativeTimerEngine, timer: &NativeDueTimer) -> TimerPayloadRecordV1 {
    let NativeLocator::Payload { segment_id, offset } = timer.entry.locator else {
        panic!("timer {} was not copied", timer.entry.cq_offset);
    };
    let location = NativePayloadLocation {
        segment: NativePayloadSegment {
            due_day_utc: NativePayloadSegment::due_day_of(timer.entry.due_ms).unwrap(),
            segment_id,
        },
        offset,
        length: timer.entry.size as u32,
    };
    engine.storage().read_payload(location).unwrap()
}

#[test]
fn a_timer_beyond_the_mid_horizon_is_copied_and_moves_into_hours_on_its_way() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let due_ms = D3 + 5 * MINUTE_MS;
    ingest(&engine, &[timer(1, due_ms)], H0);
    let counts = engine.counts();
    assert_eq!((counts.days, counts.hours), (1, 0));
    assert_eq!(engine.next_deadline_ms(), Some(D3 - 48 * HOUR_MS));

    // Its day moves into hours once the day starts within the mid horizon.
    assert!(due(&engine, D3 - 48 * HOUR_MS - 1).is_empty());
    assert_eq!(engine.counts().days, 1);
    assert!(due(&engine, D3 - 48 * HOUR_MS).is_empty());
    let counts = engine.counts();
    assert_eq!((counts.days, counts.hours), (0, 1));

    let due = due(&engine, due_ms);
    assert_eq!(offsets(&due), vec![1]);
    let copy = copy_of(&engine, &due[0]);
    assert_eq!(copy.frame, b"frame-1");
    assert_eq!(copy.source_cq_offset.get(), 1);
    assert_eq!(copy.real_topic, TOPIC);
}

#[test]
fn a_moved_day_bucket_is_deleted_only_once_a_checkpoint_leaves_it_out() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, D3 + 1_000)], H0);
    assert!(due(&engine, D3 - 47 * HOUR_MS).is_empty());
    let day = NativeBucketId::starting_at(NativeBucketKind::Day, D3).unwrap();
    let day_file = engine.storage().root().join(day.relative_path());
    assert!(day_file.exists());

    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0).unwrap();
    assert!(checkpoint
        .buckets
        .iter()
        .all(|bucket| bucket.bucket.kind() != NativeBucketKind::Day));
    assert_eq!(checkpoint.payload_segments.len(), 1);
    assert!(!day_file.exists());
    drop(engine);

    let engine = open(&root, config(), D3);
    assert_eq!(engine.counts().days, 0);
    let due = due(&engine, D3 + 1_000);
    assert_eq!(offsets(&due), vec![1]);
    assert_eq!(copy_of(&engine, &due[0]).frame, b"frame-1");
}

#[test]
fn a_crash_cuts_copies_back_with_the_buckets_and_the_replay_copies_again() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, D3 + 1_000)], H0);
    engine.checkpoint(COMMIT_LOG_END, H0).unwrap();
    ingest(&engine, &[timer(2, D3 + 2_000)], H0);
    drop(engine);

    let engine = open(&root, config(), H0);
    assert_eq!(engine.source_frontier(), 2);
    ingest(&engine, &[timer(2, D3 + 2_000)], H0);
    let due = due(&engine, D3 + 2_000);
    assert_eq!(offsets(&due), vec![1, 2]);
    assert_eq!(copy_of(&engine, &due[0]).frame, b"frame-1");
    assert_eq!(copy_of(&engine, &due[1]).frame, b"frame-2");
}

#[test]
fn a_copied_timer_of_a_moved_day_goes_straight_to_its_hour() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, D3 + HOUR_MS)], H0);
    let now = D3 - 47 * HOUR_MS;
    assert!(due(&engine, now).is_empty());
    // Still beyond the mid horizon, so copied, but its day is already moved into hours.
    ingest(&engine, &[timer(2, D3 + 2 * HOUR_MS)], now);
    let counts = engine.counts();
    assert_eq!((counts.days, counts.hours), (0, 2));
    let due = due(&engine, D3 + 2 * HOUR_MS);
    assert_eq!(offsets(&due), vec![1, 2]);
    assert_eq!(copy_of(&engine, &due[1]).frame, b"frame-2");
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
