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

use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerSourceCqOffset;
use tempfile::tempdir;
use tempfile::TempDir;

use super::*;
use crate::timer::native_format::NativeDueCursor;
use crate::timer::native_format::NativeLocator;
use crate::timer::payload_record::TimerPayloadRecordV1;

const LANES: u16 = 4;
const NOW_MS: i64 = 1_700_000_000_000;

fn entry(cq_offset: i64) -> NativeIndexEntry {
    NativeIndexEntry {
        due_ms: NOW_MS + cq_offset,
        cq_offset,
        locator: NativeLocator::CommitLog {
            offset: cq_offset * 200,
        },
        generation: 0,
        key_hash: cq_offset as u64,
        size: 200,
        lane: (cq_offset % i64::from(LANES)) as u16,
    }
}

fn entries(range: std::ops::Range<i64>) -> Vec<NativeIndexEntry> {
    range.map(entry).collect()
}

fn hour() -> NativeBucketId {
    NativeBucketId::containing(NativeBucketKind::Hour, NOW_MS).unwrap()
}

fn day() -> NativeBucketId {
    NativeBucketId::containing(NativeBucketKind::Day, NOW_MS + 10 * 86_400_000).unwrap()
}

fn minute() -> NativeBucketId {
    NativeBucketId::containing(NativeBucketKind::Minute, NOW_MS).unwrap()
}

fn cancel(key: &str) -> NativeCancelRecord {
    NativeCancelRecord {
        due_ms: NOW_MS + 5_000,
        tombstone_cq_offset: 9,
        generation: 0,
        key: key.as_bytes().to_vec(),
    }
}

fn open(root: &TempDir) -> (NativeTimerStorage, Option<NativeCheckpoint>) {
    NativeTimerStorage::open(root.path(), NativeStorageConfig::new(LANES), NOW_MS).unwrap()
}

fn read(storage: &NativeTimerStorage, bucket: NativeBucketId) -> Vec<NativeIndexEntry> {
    let mut read = Vec::new();
    storage
        .visit_entries(bucket, |entries| {
            read.extend_from_slice(entries);
            Ok(())
        })
        .unwrap();
    read
}

/// A checkpoint of what `synced` made durable.
fn checkpoint(sequence: u64, synced: &NativeSyncedLengths) -> NativeCheckpoint {
    NativeCheckpoint {
        sequence,
        written_ms: NOW_MS,
        source_cq_offset: 100,
        commit_log_fence: 0,
        cancel_log_length: synced.cancel_log_length,
        lane_watermarks: vec![NativeDueCursor::START; usize::from(LANES)],
        buckets: synced.buckets.clone(),
        payload_segments: synced.payload_segments.clone(),
    }
}

#[test]
fn a_new_directory_gets_a_format_and_keeps_its_lane_count() {
    let root = tempdir().unwrap();
    let (storage, checkpoint) = open(&root);
    assert_eq!(checkpoint, None);
    assert_eq!(
        storage.format(),
        NativeFormat {
            lane_count: LANES,
            created_ms: NOW_MS
        }
    );
    assert_eq!(storage.last_checkpoint_sequence(), 0);
    drop(storage);

    // Reopening keeps the first FORMAT, and another lane count is refused.
    assert_eq!(open(&root).0.format().created_ms, NOW_MS);
    assert!(matches!(
        NativeTimerStorage::open(root.path(), NativeStorageConfig::new(LANES * 2), NOW_MS),
        Err(NativeStorageError::LaneCountMismatch {
            found: LANES,
            configured: 8
        })
    ));
}

#[test]
fn timer_files_without_a_format_are_refused() {
    let root = tempdir().unwrap();
    let dir = root.path().join(NATIVE_TIMER_DIR);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(NATIVE_CANCEL_LOG_NAME), b"").unwrap();
    assert!(matches!(
        NativeTimerStorage::open(root.path(), NativeStorageConfig::new(LANES), NOW_MS),
        Err(NativeStorageError::MissingFormat(_))
    ));
}

#[test]
fn appended_entries_read_back_per_bucket() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..3)).unwrap();
    storage.append_entries(day(), &entries(10..12)).unwrap();
    let length = storage.append_entries(hour(), &entries(3..5)).unwrap();

    assert_eq!(read(&storage, hour()), entries(0..5));
    assert_eq!(read(&storage, day()), entries(10..12));
    // The header, then one frame per append.
    assert_eq!(length, HEADER_LEN + 2 * 16 + 5 * 48);
    assert!(matches!(
        storage.visit_entries(minute(), |_| Ok(())),
        Err(NativeStorageError::UnknownBucket(_))
    ));

    // A minute run is written as sorted frames.
    let mut unsorted = entries(20..22);
    unsorted.reverse();
    assert!(matches!(
        storage.append_entries(minute(), &unsorted),
        Err(NativeStorageError::Format(NativeFormatError::InvalidField {
            field: "order",
            ..
        }))
    ));
    storage.append_entries(minute(), &entries(20..22)).unwrap();
    assert_eq!(read(&storage, minute()), entries(20..22));
}

#[test]
fn a_bucket_larger_than_one_read_is_visited_in_runs() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    // About 1.9 MiB of frames, so frames straddle the 1 MiB reads.
    let written = entries(0..40_000);
    for chunk in written.chunks(7_000) {
        storage.append_entries(hour(), chunk).unwrap();
    }
    let mut runs = 0;
    let mut read = Vec::new();
    storage
        .visit_entries(hour(), |entries| {
            runs += 1;
            read.extend_from_slice(entries);
            Ok(())
        })
        .unwrap();
    assert_eq!(read, written);
    assert!(runs >= 2);
}

#[test]
fn reopening_cuts_every_file_back_to_the_last_checkpoint() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..3)).unwrap();
    storage.append_cancels(&[cancel("TopicA_k1")]).unwrap();
    let synced = storage.sync().unwrap();
    let written = checkpoint(1, &synced);
    storage.write_checkpoint(&written).unwrap();

    // Written after the checkpoint: more entries, a new bucket and another recall.
    storage.append_entries(hour(), &entries(3..6)).unwrap();
    storage.append_entries(day(), &entries(10..12)).unwrap();
    storage.append_cancels(&[cancel("TopicA_k2")]).unwrap();
    storage.sync().unwrap();
    drop(storage);

    let (storage, recovered) = open(&root);
    assert_eq!(recovered, Some(written));
    assert_eq!(read(&storage, hour()), entries(0..3));
    assert!(!root.path().join(NATIVE_TIMER_DIR).join(day().relative_path()).exists());
    assert_eq!(storage.buckets().len(), 1);
    assert_eq!(storage.read_cancels().unwrap(), vec![cancel("TopicA_k1")]);
    // Appends go on from the cut.
    storage.append_entries(hour(), &entries(3..4)).unwrap();
    assert_eq!(read(&storage, hour()), entries(0..4));
}

#[test]
fn a_bucket_created_during_a_sync_starts_again_after_a_reopen() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..2)).unwrap();
    let mut synced = storage.sync().unwrap();
    // A bucket created after its files were synced has nothing durable; a checkpoint that
    // names it records nothing to keep.
    storage.append_entries(day(), &entries(10..11)).unwrap();
    assert!(storage
        .sync()
        .unwrap()
        .buckets
        .iter()
        .any(|bucket| bucket.bucket == day()));
    synced.buckets.push(NativeBucketLength {
        bucket: day(),
        durable_length: 0,
    });
    storage.write_checkpoint(&checkpoint(1, &synced)).unwrap();
    drop(storage);

    let (storage, _) = open(&root);
    assert_eq!(
        storage
            .buckets()
            .into_iter()
            .map(|bucket| bucket.bucket)
            .collect::<Vec<_>>(),
        vec![hour()]
    );
    storage.append_entries(day(), &entries(12..13)).unwrap();
    assert_eq!(read(&storage, day()), entries(12..13));
}

#[test]
fn without_a_checkpoint_nothing_written_survives_a_reopen() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..3)).unwrap();
    storage.append_cancels(&[cancel("TopicA_k1")]).unwrap();
    storage.sync().unwrap();
    drop(storage);

    let (storage, recovered) = open(&root);
    assert_eq!(recovered, None);
    assert!(storage.buckets().is_empty());
    assert!(storage.read_cancels().unwrap().is_empty());
}

#[test]
fn checkpoints_alternate_slots_and_a_torn_newer_one_falls_back() {
    let root = tempdir().unwrap();
    let dir = root.path().join(NATIVE_TIMER_DIR);
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..2)).unwrap();
    let first = checkpoint(1, &storage.sync().unwrap());
    storage.write_checkpoint(&first).unwrap();
    storage.append_entries(hour(), &entries(2..4)).unwrap();
    let second = checkpoint(2, &storage.sync().unwrap());
    storage.write_checkpoint(&second).unwrap();
    assert_eq!(storage.last_checkpoint_sequence(), 2);
    assert!(dir.join("checkpoint.0").exists() && dir.join("checkpoint.1").exists());
    drop(storage);

    // Tear the newer slot: recovery goes back to the first checkpoint and its lengths.
    let newer = dir.join("checkpoint.1");
    let bytes = fs::read(&newer).unwrap();
    fs::write(&newer, &bytes[..bytes.len() / 2]).unwrap();
    let (storage, recovered) = open(&root);
    assert_eq!(recovered, Some(first));
    assert_eq!(read(&storage, hour()), entries(0..2));

    // The next checkpoint overwrites the torn slot.
    let third = checkpoint(3, &storage.sync().unwrap());
    storage.write_checkpoint(&third).unwrap();
    drop(storage);
    assert_eq!(open(&root).1, Some(third));
}

#[test]
fn a_checkpoint_must_be_newer_and_covered_by_durable_bytes() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..2)).unwrap();
    let synced = storage.sync().unwrap();
    storage.write_checkpoint(&checkpoint(5, &synced)).unwrap();
    assert!(matches!(
        storage.write_checkpoint(&checkpoint(5, &synced)),
        Err(NativeStorageError::StaleCheckpoint { sequence: 5, last: 5 })
    ));

    // Written but not synced yet.
    storage.append_entries(hour(), &entries(2..4)).unwrap();
    let mut ahead = checkpoint(6, &synced);
    ahead.buckets = storage.buckets();
    assert!(matches!(
        storage.write_checkpoint(&ahead),
        Err(NativeStorageError::CheckpointAheadOfFiles { sequence: 6 })
    ));
    storage.sync().unwrap();
    storage.write_checkpoint(&ahead).unwrap();
}

#[test]
fn a_file_missing_bytes_its_checkpoint_made_durable_is_refused() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..3)).unwrap();
    storage
        .write_checkpoint(&checkpoint(1, &storage.sync().unwrap()))
        .unwrap();
    drop(storage);

    let path = root.path().join(NATIVE_TIMER_DIR).join(hour().relative_path());
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(HEADER_LEN + 10).unwrap();
    drop(file);
    assert!(matches!(
        NativeTimerStorage::open(root.path(), NativeStorageConfig::new(LANES), NOW_MS),
        Err(NativeStorageError::ShorterThanCheckpoint { .. })
    ));
}

#[test]
fn a_deleted_bucket_is_gone_from_disk_and_from_the_list() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_entries(hour(), &entries(0..2)).unwrap();
    storage.append_entries(day(), &entries(10..11)).unwrap();
    assert!(storage.delete_bucket(hour()).unwrap());
    assert!(!storage.delete_bucket(hour()).unwrap());
    assert_eq!(
        storage
            .buckets()
            .into_iter()
            .map(|bucket| bucket.bucket)
            .collect::<Vec<_>>(),
        vec![day()]
    );
    assert!(!root.path().join(NATIVE_TIMER_DIR).join(hour().relative_path()).exists());
    // Writing to it again starts a new file.
    storage.append_entries(hour(), &entries(5..6)).unwrap();
    assert_eq!(read(&storage, hour()), entries(5..6));
}

/// The encoded payload copy of timer `cq_offset`, due on `day`.
fn payload(cq_offset: i64, day: i32) -> Vec<u8> {
    TimerPayloadRecordV1 {
        due_time_ms: i64::from(day) * 86_400_000 + cq_offset,
        lane: 1,
        timer_id: TimerId::new(cq_offset as u128),
        generation: TimerGeneration::new(0),
        source_cq_offset: TimerSourceCqOffset::new(cq_offset),
        source_physical_offset: cq_offset * 100,
        real_queue_id: 0,
        real_topic: "TopicA".to_owned(),
        frame: vec![cq_offset as u8; 40],
    }
    .encode()
    .unwrap()
}

fn open_with_segment(root: &TempDir, payload_segment_bytes: u64) -> (NativeTimerStorage, Option<NativeCheckpoint>) {
    let config = NativeStorageConfig {
        payload_segment_bytes,
        ..NativeStorageConfig::new(LANES)
    };
    NativeTimerStorage::open(root.path(), config, NOW_MS).unwrap()
}

const DAY: i32 = 20_000;

#[test]
fn payload_copies_fill_segments_in_turn_and_read_back() {
    let root = tempdir().unwrap();
    let record_len = payload(0, DAY).len() as u64;
    let (storage, _) = open_with_segment(&root, 2 * record_len);
    let records: Vec<_> = (0..5).map(|cq| payload(cq, DAY)).collect();
    let locations = storage.append_payloads(DAY, &records).unwrap();
    assert_eq!(
        locations
            .iter()
            .map(|location| location.segment.segment_id)
            .collect::<Vec<_>>(),
        vec![0, 0, 1, 1, 2]
    );
    assert_eq!(locations[1].offset as u64, record_len);
    for (cq, location) in locations.iter().enumerate() {
        let record = storage.read_payload(*location).unwrap();
        assert_eq!(record.source_cq_offset, TimerSourceCqOffset::new(cq as i64));
    }
    // The next append goes on in the day's last segment.
    let next = storage.append_payloads(DAY, &[payload(5, DAY)]).unwrap();
    assert_eq!((next[0].segment.segment_id, next[0].offset as u64), (2, record_len));
    assert!(storage
        .root()
        .join(
            NativePayloadSegment {
                due_day_utc: DAY,
                segment_id: 2
            }
            .relative_path()
        )
        .exists());
}

#[test]
fn a_deleted_payload_day_is_gone_with_its_directory() {
    let root = tempdir().unwrap();
    let record_len = payload(0, DAY).len() as u64;
    let (storage, _) = open_with_segment(&root, 2 * record_len);
    let records: Vec<_> = (0..3).map(|cq| payload(cq, DAY)).collect();
    storage.append_payloads(DAY, &records).unwrap();
    let kept = storage.append_payloads(DAY + 1, &[payload(9, DAY + 1)]).unwrap();
    assert_eq!(storage.payload_days(), vec![DAY, DAY + 1]);

    storage.delete_payload_day(DAY).unwrap();
    assert_eq!(storage.payload_days(), vec![DAY + 1]);
    let day_dir = storage
        .root()
        .join(
            NativePayloadSegment {
                due_day_utc: DAY,
                segment_id: 0,
            }
            .relative_path(),
        )
        .parent()
        .unwrap()
        .to_path_buf();
    assert!(!day_dir.exists());
    assert!(storage
        .sync()
        .unwrap()
        .payload_segments
        .iter()
        .all(|segment| segment.due_day_utc == DAY + 1));
    assert!(storage.read_payload(kept[0]).is_ok());
}

#[test]
fn a_payload_larger_than_a_segment_or_read_past_its_end_is_refused() {
    let root = tempdir().unwrap();
    let (storage, _) = open_with_segment(&root, 64);
    assert!(matches!(
        storage.append_payloads(DAY, &[payload(1, DAY)]),
        Err(NativeStorageError::PayloadTooLarge { segment_bytes: 64, .. })
    ));
    let missing = NativePayloadLocation {
        segment: NativePayloadSegment {
            due_day_utc: DAY,
            segment_id: 0,
        },
        offset: 0,
        length: 10,
    };
    assert!(matches!(
        storage.read_payload(missing),
        Err(NativeStorageError::CorruptPayload { .. })
    ));
    assert!(matches!(
        NativeTimerStorage::open(
            root.path(),
            NativeStorageConfig {
                payload_segment_bytes: 0,
                ..NativeStorageConfig::new(LANES)
            },
            NOW_MS
        ),
        Err(NativeStorageError::InvalidSegmentSize(0))
    ));
}

#[test]
fn reopening_cuts_payload_segments_back_to_the_last_checkpoint() {
    let root = tempdir().unwrap();
    let record_len = payload(0, DAY).len() as u64;
    let (storage, _) = open_with_segment(&root, 2 * record_len);
    let kept = storage.append_payloads(DAY, &[payload(1, DAY)]).unwrap();
    let synced = storage.sync().unwrap();
    assert_eq!(
        synced.payload_segments,
        vec![NativePayloadSegmentLength {
            due_day_utc: DAY,
            segment_id: 0,
            durable_length: record_len
        }]
    );
    storage.write_checkpoint(&checkpoint(1, &synced)).unwrap();
    // After the checkpoint: the rest of segment 0, segment 1, and another day.
    let lost = storage
        .append_payloads(DAY, &[payload(2, DAY), payload(3, DAY)])
        .unwrap();
    storage.append_payloads(DAY + 1, &[payload(4, DAY + 1)]).unwrap();
    storage.sync().unwrap();
    drop(storage);

    let (storage, _) = open_with_segment(&root, 2 * record_len);
    assert_eq!(
        storage.read_payload(kept[0]).unwrap().source_cq_offset,
        TimerSourceCqOffset::new(1)
    );
    assert!(matches!(
        storage.read_payload(lost[0]),
        Err(NativeStorageError::CorruptPayload { .. })
    ));
    assert_eq!(storage.written_lengths().payload_segments.len(), 1);
    // Appends go on from the cut.
    let again = storage.append_payloads(DAY, &[payload(2, DAY)]).unwrap();
    assert_eq!(again[0], lost[0]);
}

#[test]
fn a_checkpoint_naming_unsynced_payload_bytes_is_refused() {
    let root = tempdir().unwrap();
    let (storage, _) = open(&root);
    storage.append_payloads(DAY, &[payload(1, DAY)]).unwrap();
    let mut ahead = checkpoint(1, &NativeSyncedLengths::default());
    ahead.payload_segments = storage.written_lengths().payload_segments;
    assert!(matches!(
        storage.write_checkpoint(&ahead),
        Err(NativeStorageError::CheckpointAheadOfFiles { sequence: 1 })
    ));
    storage.sync().unwrap();
    storage.write_checkpoint(&ahead).unwrap();
}
