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

use tempfile::tempdir;
use tempfile::TempDir;

use super::*;
use crate::timer::native_format::NativeDueCursor;
use crate::timer::native_format::NativeLocator;

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
        key: key.as_bytes().to_vec(),
    }
}

fn open(root: &TempDir) -> (NativeTimerStorage, Option<NativeCheckpoint>) {
    NativeTimerStorage::open(root.path(), LANES, NOW_MS).unwrap()
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
        payload_segments: Vec::new(),
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
        NativeTimerStorage::open(root.path(), LANES * 2, NOW_MS),
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
        NativeTimerStorage::open(root.path(), LANES, NOW_MS),
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
        NativeTimerStorage::open(root.path(), LANES, NOW_MS),
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
