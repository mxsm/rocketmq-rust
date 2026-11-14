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

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;

use crate::config::FlushDiskType;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::NativeMappedMemory;
use crate::mapped_file::ResidencyCheck;
use crate::mapped_file::SelectMappedBufferResult;

type NativeMappedFile = DefaultMappedFile<NativeMappedMemory>;

fn mapped_file(file_size: u64, drain_timeout: Duration) -> (tempfile::TempDir, Arc<NativeMappedFile>) {
    let directory = tempfile::tempdir().expect("temporary mapped-file directory");
    let path = directory.path().join(format!("{:020}", 0));
    let mapped_file = NativeMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), file_size)
        .expect("mapped file")
        .with_committed_read_drain_timeout(drain_timeout);
    (directory, Arc::new(mapped_file))
}

fn committed(mapped_file: &NativeMappedFile, pos: usize, size: usize) -> Option<SelectMappedBufferResult> {
    mapped_file
        .try_select_committed_range(pos as i32, size as i32, ResidencyCheck::AssumeResident)
        .expect("selection")
}

#[test]
fn committed_range_borrows_the_active_segment_and_ignores_later_appends() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_secs(30));
    assert!(mapped_file.append_message_bytes(b"first"));

    let first = committed(&mapped_file, 0, 5).expect("committed bytes");
    assert!(first.is_range_backed(), "an active segment is read without a copy");
    assert!(!first.has_byte_snapshot());
    assert!(mapped_file.append_message_bytes(b"second"));

    assert_eq!(first.get_buffer(), b"first");
    let second = committed(&mapped_file, 5, 6).expect("bytes appended after the first range");
    assert_eq!(second.get_buffer(), b"second");
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 2);
    drop(first);
    drop(second);
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
}

#[test]
fn mutable_bytes_of_a_committed_range_borrow_the_segment_until_dropped() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_millis(20));
    assert!(mapped_file.append_message_bytes(b"AAAAAAAABBBBBBBB"));
    let mut selected = committed(&mapped_file, 0, 16).expect("records");
    let bytes = selected.bytes_mut().expect("bytes").clone();
    drop(selected);

    assert_eq!(bytes.as_ref(), b"AAAAAAAABBBBBBBB");
    assert!(
        mapped_file.try_truncate_positions(8).is_err(),
        "the bytes still borrow the segment"
    );
    drop(bytes);
    mapped_file.try_truncate_positions(8).expect("no borrow remains");
}

#[test]
fn committed_range_stops_at_the_read_position() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_secs(30));
    assert!(mapped_file.append_message_bytes(b"0123456789"));

    assert!(
        committed(&mapped_file, 4, 7).is_none(),
        "one byte past the read position"
    );
    assert!(committed(&mapped_file, 10, 1).is_none());
    assert_eq!(
        committed(&mapped_file, 4, 6).expect("readable tail").get_buffer(),
        b"456789"
    );
}

#[test]
fn sealed_segment_selection_returns_its_read_only_range() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_secs(30));
    assert!(mapped_file.append_message_bytes(b"sealed"));
    let before_seal = committed(&mapped_file, 0, 6).expect("committed bytes");
    assert!(mapped_file.try_seal_readable().expect("seal"));

    let after_seal = committed(&mapped_file, 0, 6).expect("sealed bytes");
    assert!(after_seal.is_range_backed());
    assert_eq!(after_seal.get_buffer(), b"sealed");
    assert_eq!(before_seal.get_buffer(), b"sealed");
}

#[test]
fn truncation_changes_nothing_while_committed_ranges_stay_held() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_millis(20));
    assert!(mapped_file.append_message_bytes(b"AAAAAAAA"));
    assert!(mapped_file.append_message_bytes(b"BBBBBBBB"));
    let held = committed(&mapped_file, 8, 8).expect("second record");

    let error = mapped_file
        .try_truncate_positions(8)
        .expect_err("held range blocks truncation");
    assert_eq!(error.code(), rocketmq_error::STORAGE_UNAVAILABLE.code());
    assert_eq!(mapped_file.get_wrote_position(), 16);
    assert_eq!(mapped_file.get_committed_position(), 0);
    assert_eq!(held.get_buffer(), b"BBBBBBBB");

    // Lowering through the infallible setter fails closed the same way.
    mapped_file.set_wrote_position(8);
    assert_eq!(mapped_file.get_wrote_position(), 16);

    drop(held);
    mapped_file.try_truncate_positions(8).expect("no range remains");
    assert_eq!(mapped_file.get_wrote_position(), 8);
    assert_eq!(mapped_file.get_flushed_position(), 8);
    assert!(mapped_file.append_message_bytes(b"CCCCCCCC"));
    assert_eq!(
        committed(&mapped_file, 8, 8).expect("rewritten record").get_buffer(),
        b"CCCCCCCC"
    );
}

#[test]
fn readers_keep_selecting_copies_while_a_truncation_drains_their_ranges() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_secs(30));
    assert!(mapped_file.append_message_bytes(b"AAAAAAAA"));
    assert!(mapped_file.append_message_bytes(b"BBBBBBBB"));
    let held = committed(&mapped_file, 8, 8).expect("second record");

    let truncator_file = Arc::clone(&mapped_file);
    let truncator = std::thread::spawn(move || truncator_file.try_truncate_positions(8));

    // Once the truncation has closed the gate, this reader, which still holds a range, must get a
    // copy from the locked path instead of waiting for the truncation.
    let copy = loop {
        let selected = committed(&mapped_file, 0, 8).expect("first record");
        if !selected.is_range_backed() {
            break selected;
        }
        drop(selected);
        std::thread::yield_now();
    };
    assert_eq!(copy.get_buffer(), b"AAAAAAAA");
    assert!(!truncator.is_finished(), "the truncation waits for the held range");

    drop(held);
    truncator
        .join()
        .expect("truncator thread")
        .expect("truncation after the drain");
    assert_eq!(mapped_file.get_wrote_position(), 8);
    let reopened = committed(&mapped_file, 0, 8).expect("first record");
    assert!(reopened.is_range_backed(), "the gate reopens after the truncation");
}

#[test]
fn lowering_the_wrote_position_waits_for_a_range_another_thread_drops() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_secs(30));
    assert!(mapped_file.append_message_bytes(b"AAAAAAAA"));
    assert!(mapped_file.append_message_bytes(b"BBBBBBBB"));
    let held = committed(&mapped_file, 8, 8).expect("second record");
    let (release, released) = mpsc::channel::<()>();
    let reader = std::thread::spawn(move || {
        released.recv().expect("release signal");
        assert_eq!(held.get_buffer(), b"BBBBBBBB");
        drop(held);
    });

    let (done, finished) = mpsc::channel::<()>();
    let truncator_file = Arc::clone(&mapped_file);
    let truncator = std::thread::spawn(move || {
        truncator_file.set_wrote_position(8);
        done.send(()).expect("test alive");
    });
    assert!(finished.try_recv().is_err(), "the range is still held");
    release.send(()).expect("reader alive");
    finished.recv().expect("truncation done");
    reader.join().expect("reader thread");
    truncator.join().expect("truncator thread");
    assert_eq!(mapped_file.get_wrote_position(), 8);
}

#[test]
fn positioned_writes_below_the_read_position_wait_for_committed_ranges() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_millis(20));
    assert!(mapped_file.append_message_bytes(b"AAAAAAAA"));
    let held = committed(&mapped_file, 0, 8).expect("record");

    assert!(
        !mapped_file.put_slice(b"Z", 0),
        "a held range blocks a write below the read position"
    );
    assert!(
        mapped_file.put_slice(b"after", 16),
        "writes above the read position need no drain"
    );
    assert_eq!(held.get_buffer(), b"AAAAAAAA");

    drop(held);
    assert!(mapped_file.put_slice(b"Z", 0));
    assert_eq!(committed(&mapped_file, 0, 8).expect("record").get_buffer(), b"ZAAAAAAA");
}

#[test]
fn warm_up_is_refused_while_committed_ranges_are_held() {
    let (_directory, mapped_file) = mapped_file(4096, Duration::from_millis(20));
    assert!(mapped_file.append_message_bytes(b"AAAAAAAA"));
    let held = committed(&mapped_file, 0, 8).expect("record");

    assert!(mapped_file.try_warm_mapped_file(FlushDiskType::AsyncFlush, 1).is_err());
    drop(held);
    assert!(mapped_file
        .try_warm_mapped_file(FlushDiskType::AsyncFlush, 1)
        .expect("warm-up after the drain"));
    assert_eq!(committed(&mapped_file, 0, 8).expect("record").get_buffer(), b"AAAAAAAA");
}

const RECORD: usize = 128;

fn checksum(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0x811c_9dc5_u32, |hash, byte| {
        (hash ^ u32::from(*byte)).wrapping_mul(0x0100_0193)
    })
}

fn record(sequence: u64, epoch: u64) -> [u8; RECORD] {
    let mut bytes = [0_u8; RECORD];
    bytes[0..4].copy_from_slice(&(RECORD as u32).to_le_bytes());
    bytes[8..16].copy_from_slice(&sequence.to_le_bytes());
    bytes[16..24].copy_from_slice(&epoch.to_le_bytes());
    for (index, byte) in bytes[24..].iter_mut().enumerate() {
        *byte = (sequence.wrapping_mul(31) ^ epoch.wrapping_mul(131) ^ index as u64) as u8;
    }
    let sum = checksum(&bytes[8..]);
    bytes[4..8].copy_from_slice(&sum.to_le_bytes());
    bytes
}

fn assert_record(bytes: &[u8]) {
    assert_eq!(bytes.len(), RECORD);
    assert_eq!(
        u32::from_le_bytes(bytes[0..4].try_into().expect("length")) as usize,
        RECORD
    );
    let stored = u32::from_le_bytes(bytes[4..8].try_into().expect("checksum"));
    assert_eq!(stored, checksum(&bytes[8..]), "a selected record is torn or rewritten");
}

/// An appender writes checksummed records and repeatedly truncates the segment and rewrites the
/// truncated region with different bytes while readers select committed ranges and hold several at
/// a time. Every range must hold a whole record when selected and the same bytes when dropped.
#[test]
fn committed_ranges_stay_unchanged_while_truncation_and_appends_rewrite_the_segment() {
    const FILE_RECORDS: usize = 2048;
    const TRUNCATIONS: u64 = 40;
    const READERS: usize = 3;
    const HELD: usize = 4;

    let (_directory, mapped_file) = mapped_file((FILE_RECORDS * RECORD) as u64, Duration::from_secs(30));
    let stop = Arc::new(AtomicBool::new(false));
    let borrowed = Arc::new(AtomicU64::new(0));
    let copied = Arc::new(AtomicU64::new(0));

    let readers: Vec<_> = (0..READERS)
        .map(|reader| {
            let mapped_file = Arc::clone(&mapped_file);
            let stop = Arc::clone(&stop);
            let borrowed = Arc::clone(&borrowed);
            let copied = Arc::clone(&copied);
            std::thread::spawn(move || {
                let mut held: VecDeque<(SelectMappedBufferResult, Vec<u8>)> = VecDeque::new();
                let mut seed = 0x9e37_79b9_u64.wrapping_mul(reader as u64 + 1);
                while !stop.load(Ordering::Acquire) {
                    let records = usize::try_from(mapped_file.get_read_position()).unwrap_or(0) / RECORD;
                    if records == 0 {
                        std::thread::yield_now();
                        continue;
                    }
                    seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    let index = (seed >> 33) as usize % records;
                    let Some(selected) = committed(&mapped_file, index * RECORD, RECORD) else {
                        // A truncation moved the read position below this record meanwhile.
                        continue;
                    };
                    if selected.is_range_backed() {
                        borrowed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        copied.fetch_add(1, Ordering::Relaxed);
                    }
                    let snapshot = selected.get_buffer().to_vec();
                    assert_record(&snapshot);
                    held.push_back((selected, snapshot));
                    if held.len() > HELD {
                        let (oldest, snapshot) = held.pop_front().expect("held range");
                        assert_eq!(oldest.get_buffer(), snapshot.as_slice(), "held bytes changed");
                    }
                }
                for (range, snapshot) in held {
                    assert_eq!(range.get_buffer(), snapshot.as_slice(), "held bytes changed");
                }
            })
        })
        .collect();

    let mut sequence = 0_u64;
    let mut seed = 0x2545_f491_4f6c_dd1d_u64;
    for epoch in 0..TRUNCATIONS {
        while (mapped_file.get_wrote_position() as usize) < FILE_RECORDS * RECORD * 3 / 4 {
            assert!(mapped_file.append_message_bytes(&record(sequence, epoch)));
            sequence += 1;
        }
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let keep = FILE_RECORDS / 8 + (seed >> 33) as usize % (FILE_RECORDS / 2);
        mapped_file
            .try_truncate_positions((keep * RECORD) as i32)
            .expect("readers drop their ranges within the drain timeout");
    }
    stop.store(true, Ordering::Release);
    for reader in readers {
        reader.join().expect("reader thread");
    }
    assert!(borrowed.load(Ordering::Relaxed) > 0, "readers borrowed committed bytes");
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
}
