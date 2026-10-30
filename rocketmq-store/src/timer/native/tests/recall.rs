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

use super::super::engine::NativeRecallVerdict;
use super::*;

/// The key of a recall of timer `target`, as [`recall`] writes it.
fn key(target: i64) -> Vec<u8> {
    format!("{TOPIC}_key-{target}").into_bytes()
}

#[test]
fn a_recall_registered_before_the_deadline_cancels_the_timer_ahead_of_its_tombstone() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 5_000), timer(2, H0 + 5_000)], H0);
    assert_eq!(
        engine.register_recall(&key(1), 0, H0 + 5_000, H0),
        Ok(NativeRecallVerdict::Cancelled)
    );
    let due = due(&engine, H0 + 5_000);
    assert_eq!(offsets(&due), vec![1, 2]);
    assert_eq!(due[0].recalls.len(), 1);
    assert_eq!(due[0].recalls[0].key, key(1));
    assert!(due[1].recalls.is_empty());
}

#[test]
fn a_recall_of_a_timer_that_may_be_taken_is_too_late_and_registers_nothing() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 5_000), timer(2, H0 + 9_000)], H0);
    assert_eq!(
        engine.register_recall(&key(1), 0, H0 + 5_000, H0 + 5_000),
        Ok(NativeRecallVerdict::TooLate)
    );
    // A batch took timers up to H0 + 6s on a clock read after the recall's.
    assert_eq!(offsets(&due(&engine, H0 + 6_000)), vec![1]);
    assert_eq!(
        engine.register_recall(&key(1), 0, H0 + 5_000, H0 + 4_000),
        Ok(NativeRecallVerdict::TooLate)
    );
    assert_eq!(engine.counts().recalls, 0);
    assert_eq!(
        engine.register_recall(&key(2), 0, H0 + 9_000, H0 + 6_000),
        Ok(NativeRecallVerdict::Cancelled)
    );
}

#[test]
fn a_restart_keeps_recalls_of_delivered_timers_too_late() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 5_000)], H0);
    assert_eq!(deliver(&engine, H0 + 5_000), vec![1]);
    engine.checkpoint(COMMIT_LOG_END, H0 + 5_000).unwrap();
    drop(engine);

    // The clock went back across the restart.
    let engine = open(&root, config(), H0);
    assert_eq!(
        engine.register_recall(&key(1), 0, H0 + 5_000, H0),
        Ok(NativeRecallVerdict::TooLate)
    );
}

#[test]
fn a_withdrawn_recall_stops_cancelling_unless_its_tombstone_was_indexed() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    ingest(&engine, &[timer(1, H0 + 5_000), timer(2, H0 + 5_000)], H0);
    // The tombstone of the recall of timer 1 could not be written.
    engine.register_recall(&key(1), 0, H0 + 5_000, H0).unwrap();
    engine.withdraw_recall(&key(1), 0);
    // That of timer 2 was written and indexed before a retry of it failed.
    engine.register_recall(&key(2), 0, H0 + 5_000, H0).unwrap();
    ingest(&engine, &[recall(3, 2, H0 + 5_000)], H0);
    engine.register_recall(&key(2), 0, H0 + 5_000, H0).unwrap();
    engine.withdraw_recall(&key(2), 0);

    assert_eq!(engine.counts().recalls, 1);
    let due = due(&engine, H0 + 5_000);
    assert_eq!(offsets(&due), vec![1, 2]);
    assert!(due[0].recalls.is_empty());
    assert_eq!(due[1].recalls.len(), 1);
}

#[test]
fn a_recall_with_a_deadline_no_timer_can_have_is_rejected() {
    let root = tempdir().unwrap();
    let engine = open(&root, config(), H0);
    assert_eq!(
        engine.register_recall(&key(1), 0, -1, H0),
        Err(NativeRejection::InvalidDeadline)
    );
    assert_eq!(
        engine.register_recall(&key(1), 0, H0 + 401 * DAY_MS, H0),
        Err(NativeRejection::BeyondMaxDelay)
    );
    assert_eq!(engine.counts().recalls, 0);
}
