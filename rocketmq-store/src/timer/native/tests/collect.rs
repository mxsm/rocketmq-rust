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

use super::*;

/// The file of the minute run starting at `start_ms`.
fn minute_file(engine: &NativeTimerEngine, start_ms: i64) -> std::path::PathBuf {
    let minute = NativeBucketId::starting_at(NativeBucketKind::Minute, start_ms).unwrap();
    engine.storage().root().join(minute.relative_path())
}

/// A queue of `TOPIC` whose timers go to another lane than queue 0's.
fn other_queue() -> i32 {
    (1..).find(|queue| native_lane(TOPIC, *queue, LANES) != lane()).unwrap()
}

#[test]
fn delivered_minute_runs_are_deleted_and_stop_holding_the_fence_back() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 1_000), timer(2, H0 + MINUTE_MS + 1_000)], H0);
    assert_eq!(deliver(&engine, H0 + MINUTE_MS + 1_000), vec![1, 2]);

    // The second minute has not passed for the Source stage yet, whatever the clock says.
    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0 + 3 * MINUTE_MS).unwrap();
    assert_eq!(checkpoint.commit_log_fence, 100);
    assert_eq!(engine.counts().minutes, 1);
    assert!(
        minute_file(&engine, H0).exists(),
        "deleted only once no checkpoint names it"
    );
    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0 + 3 * MINUTE_MS).unwrap();
    assert_eq!(checkpoint.commit_log_fence, 200);
    assert!(!minute_file(&engine, H0).exists());

    due(&engine, H0 + 2 * MINUTE_MS);
    engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();
    assert_eq!(engine.counts().minutes, 0);
    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();
    assert_eq!(checkpoint.commit_log_fence, COMMIT_LOG_END);
    assert!(!minute_file(&engine, H0 + MINUTE_MS).exists());
}

#[test]
fn a_run_with_a_timer_left_to_deliver_holds_back_the_runs_after_it() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let other = NativeTimerSource {
        real_queue_id: other_queue(),
        ..timer(2, H0 + 2_000)
    };
    ingest(
        &engine,
        &[timer(1, H0 + 1_000), other, timer(3, H0 + MINUTE_MS + 1_000)],
        H0,
    );
    // The other lane delivers nothing yet.
    assert_eq!(deliver(&engine, H0 + 2 * MINUTE_MS), vec![1, 3]);
    engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();
    assert_eq!(engine.counts().minutes, 2);

    let other_lane = native_lane(TOPIC, other_queue(), LANES);
    let taken = engine.poll_due(other_lane, H0 + 2 * MINUTE_MS, 10).unwrap();
    assert_eq!(offsets(&taken), vec![2]);
    engine.advance_delivered(other_lane, taken[0].entry.cursor());
    // The checkpoint that makes it durable collects both runs, and the next deletes them.
    engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();
    assert_eq!(engine.counts().minutes, 0);
    assert!(minute_file(&engine, H0 + MINUTE_MS).exists());
    engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();
    assert!(!minute_file(&engine, H0 + MINUTE_MS).exists());
}

#[test]
fn a_timer_due_in_a_collected_minute_goes_to_the_first_minute_not_collected() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 1_000)], H0);
    assert_eq!(deliver(&engine, H0 + 2 * MINUTE_MS), vec![1]);
    engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();

    // Late, and due in a minute whose run is about to be deleted.
    ingest(&engine, &[timer(2, H0 + 30_000)], H0 + 2 * MINUTE_MS);
    engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();
    assert!(!minute_file(&engine, H0).exists());
    assert!(minute_file(&engine, H0 + 2 * MINUTE_MS).exists());
    let due = due(&engine, H0 + 2 * MINUTE_MS);
    assert_eq!(offsets(&due), vec![2]);
    assert_eq!(due[0].entry.due_ms, H0 + 2 * MINUTE_MS);
}

#[test]
fn a_payload_day_is_deleted_once_its_timers_are_collected() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    let due_ms = D3 + 5 * MINUTE_MS;
    ingest(&engine, &[timer(1, due_ms)], H0);
    assert_eq!(engine.storage().payload_days().len(), 1);
    assert_eq!(deliver(&engine, due_ms), vec![1]);

    // Collection passes the end of the day.
    let next_day = D3 + DAY_MS;
    due(&engine, next_day);
    engine.checkpoint(COMMIT_LOG_END, next_day).unwrap();
    assert_eq!(engine.storage().payload_days().len(), 1);
    engine.checkpoint(COMMIT_LOG_END, next_day).unwrap();
    assert!(engine.storage().payload_days().is_empty());
}

#[test]
fn a_recall_is_dropped_once_the_timer_it_names_is_collected() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(
        &engine,
        &[
            timer(1, H0 + 1_000),
            recall(2, 1, H0 + 1_000),
            timer(3, H0 + 3 * MINUTE_MS),
            recall(4, 3, H0 + 3 * MINUTE_MS),
        ],
        H0,
    );
    assert_eq!(engine.counts().recalls, 2);
    assert_eq!(deliver(&engine, H0 + 2 * MINUTE_MS), vec![1]);
    engine.checkpoint(COMMIT_LOG_END, H0 + 2 * MINUTE_MS).unwrap();
    // The recall of timer 3, not yet due, still cancels it.
    assert_eq!(engine.counts().recalls, 1);
    let due = due(&engine, H0 + 3 * MINUTE_MS);
    assert_eq!(due[0].recalls.len(), 1);
}

#[test]
fn the_fence_of_the_checkpoint_opened_from_goes_once_its_buckets_are_gone() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 1_000)], H0);
    // Its hour comes near and is split, so a minute run is on disk at the restart.
    assert!(due(&engine, H0).is_empty());
    engine.checkpoint(COMMIT_LOG_END, H0).unwrap();
    engine.checkpoint(COMMIT_LOG_END, H0).unwrap();
    assert!(!hour_file(&engine, H0).exists());
    drop(engine);

    let engine = open(&root, config(), H0);
    assert_eq!(deliver(&engine, H0 + MINUTE_MS), vec![1]);
    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0 + MINUTE_MS).unwrap();
    assert_eq!(checkpoint.commit_log_fence, 100);
    let checkpoint = engine.checkpoint(COMMIT_LOG_END, H0 + MINUTE_MS).unwrap();
    assert_eq!(checkpoint.commit_log_fence, COMMIT_LOG_END);
}

#[test]
fn a_timer_added_to_a_loaded_run_keeps_the_run_until_it_is_delivered() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 30_000)], H0);
    assert!(due(&engine, H0).is_empty());
    // Indexed into the loaded run, for a lane that has nothing else in it.
    let other = NativeTimerSource {
        real_queue_id: other_queue(),
        ..timer(2, H0 + 40_000)
    };
    ingest(&engine, &[other], H0);
    assert_eq!(deliver(&engine, H0 + MINUTE_MS), vec![1]);
    engine.checkpoint(COMMIT_LOG_END, H0 + MINUTE_MS).unwrap();
    assert_eq!(engine.counts().minutes, 1);
}
