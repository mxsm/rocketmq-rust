// Copyright 2023 The RocketMQ Rust Authors
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

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;

use crate::timer::checkpoint::TimerCheckpointV2Record;
use crate::timer::checkpoint::VersionedTimerCheckpointStore;
use crate::timer::metrics::TimerStorageMetrics;
use crate::timer::paged_timer_wheel::PagedTimerWheel;
use crate::timer::segmented_timer_log::SegmentedTimerLog;
use crate::timer::segmented_timer_log::TimerLogV2Record;
use crate::timer::segmented_timer_log::TIMER_LOG_V2_PHYSICAL_RECORD_SIZE;
use crate::timer::slot::Slot;
use crate::timer::storage_format::TimerLogOffset;
use crate::timer::storage_format::TimerStorageFingerprint;
use tempfile::tempdir;

fn record(queue_offset: i64) -> TimerLogV2Record {
    TimerLogV2Record {
        previous_offset: if queue_offset == 0 { -1 } else { (queue_offset - 1) * 40 },
        source_physical_offset: queue_offset * 100,
        source_size: 64,
        timer_magic: 1,
        deliver_time_ms: 5_000,
        slot_time_ms: 5_000,
        generation: 3,
        source_queue_offset: queue_offset,
    }
}

#[test]
fn codec_rejects_crc_and_policy_changes() {
    let directory = tempdir().unwrap();
    let format_path = directory.path().join("FORMAT");
    let fingerprint = TimerStorageFingerprint {
        precision_ms: 1_000,
        wheel_slots: 32,
        segment_size: 81_920,
        page_size: 4_096,
        record_version: 2,
        delete_key_mode: 0,
    };
    fingerprint.load_or_create(&format_path).unwrap();
    assert!(TimerStorageFingerprint {
        precision_ms: 500,
        ..fingerprint
    }
    .load_or_create(&format_path)
    .is_err());

    let mut encoded = record(1).encode();
    encoded[24] ^= 1;
    assert!(TimerLogV2Record::decode(&encoded).is_err());
}

#[test]
fn segmented_log_crosses_segments_recovers_tail_and_gcs_only_sealed_segments() {
    let directory = tempdir().unwrap();
    let metrics = Arc::new(TimerStorageMetrics::default());
    let segment_size = TIMER_LOG_V2_PHYSICAL_RECORD_SIZE * 4;
    let log = SegmentedTimerLog::new(directory.path(), segment_size, 2, Arc::clone(&metrics)).unwrap();
    log.load().unwrap();
    log.append_batch(&(0..8).map(record).collect::<Vec<_>>()).unwrap();
    log.flush().unwrap();
    assert_eq!(log.segment_ids().len(), 3);
    assert_eq!(log.read(TimerLogOffset::new(200)).unwrap(), record(5));
    assert_eq!(
        log.gc(
            TimerLogOffset::new(240),
            TimerLogOffset::new(240),
            TimerLogOffset::new(240)
        )
        .unwrap(),
        2
    );
    assert_eq!(log.segment_ids().len(), 1);
}

#[test]
fn paged_wheel_flushes_only_dirty_pages_and_ignores_ahead_generation() {
    let directory = tempdir().unwrap();
    let metrics = Arc::new(TimerStorageMetrics::default());
    let wheel = PagedTimerWheel::new(directory.path(), 32, 288, Arc::clone(&metrics)).unwrap();
    wheel.load(0).unwrap();
    wheel.put_slot(9, Slot::new_with_num_magic(9_000, 0, 40, 2, 1)).unwrap();
    let generation = wheel.flush_dirty().unwrap();
    assert_eq!(metrics.snapshot().physical_write_bytes, 320);
    wheel.commit_generation(generation).unwrap();
    wheel.put_slot(9, Slot::new_with_num_magic(9_000, 0, 80, 3, 1)).unwrap();
    assert_eq!(wheel.flush_dirty().unwrap(), generation + 1);

    let before_commit = PagedTimerWheel::new(directory.path(), 32, 288, Arc::clone(&metrics)).unwrap();
    before_commit.load(generation).unwrap();
    assert_eq!(before_commit.get_slot(9).unwrap().num, 2);

    let committed = PagedTimerWheel::new(directory.path(), 32, 288, metrics).unwrap();
    committed.load(generation).unwrap();
    assert_eq!(committed.get_slot(9).unwrap().num, 2);
}

#[test]
fn checkpoint_falls_back_from_corrupt_or_unvalidated_newest_copy() {
    let directory = tempdir().unwrap();
    let base = directory.path().join("timercheck");
    let store = VersionedTimerCheckpointStore::new(&base);
    store.load_best(|_| Ok(())).unwrap();
    let first = store
        .commit(TimerCheckpointV2Record {
            timer_log_durable_length: 40,
            policy_hash: 9,
            ..TimerCheckpointV2Record::default()
        })
        .unwrap();
    store
        .commit(TimerCheckpointV2Record {
            timer_log_durable_length: 80,
            policy_hash: 9,
            ..TimerCheckpointV2Record::default()
        })
        .unwrap();
    OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(format!("{}.b", base.display()))
        .unwrap()
        .write_all(b"torn")
        .unwrap();

    let reloaded = VersionedTimerCheckpointStore::new(base);
    let (selected, report) = reloaded
        .load_best(|record| {
            (record.policy_hash == 9)
                .then_some(())
                .ok_or_else(|| "policy mismatch".into())
        })
        .unwrap();
    assert_eq!(selected, Some(first));
    assert_eq!(report.rejected.len(), 1);
}
