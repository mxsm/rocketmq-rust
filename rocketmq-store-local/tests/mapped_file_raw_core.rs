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

use rocketmq_store_local::mapped_file::MappedFileRawCore;
use std::cell::Cell;

#[test]
fn raw_core_starts_with_an_empty_segment() {
    let core = MappedFileRawCore::new(16);

    assert_eq!(core.file_size(), 16);
    assert_eq!(core.wrote_position(), 0);
    assert_eq!(core.committed_position(), 0);
    assert_eq!(core.flushed_position(), 0);
    assert!(!core.is_full());
    assert_eq!(core.store_timestamp(), 0);
    assert_eq!(core.start_timestamp(), -1);
    assert_eq!(core.stop_timestamp(), -1);
}

#[test]
fn copied_reads_preserve_file_and_readable_boundaries() {
    let core = MappedFileRawCore::new(8);
    let mapped = *b"abcdefgh";

    assert_eq!(core.copied_read_slice(|| &mapped, 6, 2), Some(&b"gh"[..]));
    assert_eq!(core.copied_read_slice(|| &mapped, 8, 0), Some(&b""[..]));
    assert_eq!(core.copied_read_slice(|| &mapped, 7, 2), None);

    core.set_wrote_position(6);
    assert_eq!(
        core.copied_readable_slice(|| &mapped, 2, 4, core.normal_read_position()),
        Some(&b"cdef"[..])
    );
    assert_eq!(
        core.copied_readable_slice(|| &mapped, 2, 5, core.normal_read_position()),
        None
    );

    core.set_committed_position(4);
    assert_eq!(core.normal_read_position(), 6);
    assert_eq!(core.transient_read_position(), 4);
    assert!(core.is_readable_byte_range(1, 3, core.transient_read_position()));
    assert!(!core.is_readable_byte_range(1, 4, core.transient_read_position()));
    assert!(core.is_readable_range(1, 3, core.transient_read_position()));
    assert_eq!(core.readable_tail_size(1, 4), Some(3));
    assert_eq!(core.readable_tail_size(-1, 4), None);
}

#[test]
fn append_operations_distinguish_progress_updates() {
    let core = MappedFileRawCore::new(8);
    let mut mapped = [0; 8];

    assert!(core.append_bytes_with_position_update(|| &mut mapped, b"abcdef", 1, 3));
    assert_eq!(&mapped[..3], b"bcd");
    assert_eq!(core.wrote_position(), 3);

    assert!(core.append_bytes_without_position_update(|| &mut mapped, b"XY", 0, 2));
    assert_eq!(&mapped[3..5], b"XY");
    assert_eq!(core.wrote_position(), 3);

    assert!(!core.append_bytes_with_position_update(|| &mut mapped, b"x", 2, 1));
    assert_eq!(core.wrote_position(), 3);
    assert!(core.append_bytes_with_position_update(|| &mut mapped, b"", 0, 0));
    assert_eq!(core.wrote_position(), 3);
}

#[test]
fn direct_write_requires_an_explicit_non_empty_commit() {
    let core = MappedFileRawCore::new(8);
    let mut mapped = [0; 8];
    core.set_wrote_position(6);

    let (range, start) = core.direct_write_range(|| &mut mapped, 2).unwrap();
    assert_eq!(start, 6);
    range.copy_from_slice(b"ok");
    assert_eq!(core.wrote_position(), 6);
    assert!(core.direct_write_range(|| &mut mapped, 0).is_some());
    assert!(core.direct_write_range(|| &mut mapped, 3).is_none());
    assert!(!core.commit_direct_write(0));
    assert!(!core.commit_direct_write(3));
    assert!(core.commit_direct_write(2));
    assert_eq!(core.wrote_position(), 8);
    assert!(core.is_full());
}

#[test]
fn indexed_writes_preserve_segment_and_exact_end_rules() {
    let core = MappedFileRawCore::new(8);
    let mut mapped = [0; 8];

    assert!(core.write_bytes_segment(|| &mut mapped, b"AB", 0, usize::MAX, 2));
    assert_eq!(&mapped[..2], b"AB");
    assert!(core.write_bytes_segment(|| &mut mapped, b"abcdef", 2, 3, 2));
    assert_eq!(&mapped[2..4], b"de");
    assert!(core.write_bytes_segment(|| &mut mapped, b"", 8, 0, 0));
    assert!(!core.write_bytes_segment(|| &mut mapped, b"x", 7, 1, 2));

    assert!(core.put_slice(|| &mut mapped, b"WXYZ", 4));
    assert_eq!(&mapped[4..], b"WXYZ");
    assert!(!core.put_slice(|| &mut mapped, b"", 8));
    assert!(!core.put_slice(|| &mut mapped, b"X", 8));
}

#[test]
fn raw_slices_keep_the_legacy_exact_end_rejection() {
    let core = MappedFileRawCore::new(8);
    let mapped = *b"abcdefgh";

    assert_eq!(core.raw_slice(|| &mapped, 6, 1), Some(&b"g"[..]));
    assert_eq!(core.raw_slice(|| &mapped, 6, 2), None);
    assert_eq!(core.raw_slice(|| &mapped, 0, 0), Some(&b""[..]));
    assert_eq!(core.raw_slice(|| &mapped, 8, 0), None);
    assert_eq!(core.readable_slice(|| &mapped, 2, 2, 4), Some(&b"cd"[..]));
    assert_eq!(core.readable_slice(|| &mapped, 2, 3, 4), None);
    assert!(core.is_file_range(6, 2));
    assert!(!core.is_file_range(7, 2));
}

#[test]
fn byte_providers_are_lazy_and_short_slices_fail_closed() {
    let core = MappedFileRawCore::new(8);
    let provider_calls = Cell::new(0);
    let mapped = [0; 8];

    assert_eq!(
        core.copied_read_slice(
            || {
                provider_calls.set(provider_calls.get() + 1);
                &mapped
            },
            7,
            2,
        ),
        None
    );
    assert_eq!(provider_calls.get(), 0);

    let mut mapped = [0; 8];
    assert!(!core.append_bytes_with_position_update(
        || {
            provider_calls.set(provider_calls.get() + 1);
            &mut mapped
        },
        b"x",
        0,
        9,
    ));
    assert_eq!(provider_calls.get(), 0);

    assert!(!core.append_bytes_with_position_update(
        || {
            provider_calls.set(provider_calls.get() + 1);
            &mut mapped
        },
        b"x",
        2,
        1,
    ));
    assert_eq!(provider_calls.get(), 1);

    let short = [0; 4];
    assert_eq!(core.copied_read_slice(|| &short, 2, 3), None);
    assert_eq!(core.raw_slice(|| &short, 2, 3), None);
    assert_eq!(core.readable_slice(|| &short, 2, 3, 8), None);

    let mut short = [0; 4];
    assert!(!core.append_bytes_with_position_update(|| &mut short, b"abcde", 0, 5));
    assert_eq!(core.wrote_position(), 0);
    assert!(core.direct_write_range(|| &mut short, 5).is_none());
    assert!(!core.write_bytes_segment(|| &mut short, b"abc", 2, 0, 3));
    assert!(!core.put_slice(|| &mut short, b"abc", 2));
}

#[test]
fn progress_thresholds_and_flush_ranges_are_owned_by_the_raw_core() {
    let core = MappedFileRawCore::new(16 * 1024);
    core.set_wrote_position(8 * 1024);

    assert!(!core.is_able_to_commit(3));
    assert!(core.is_able_to_commit(2));
    assert_eq!(core.commit(2), 8 * 1024);
    assert_eq!(core.committed_position(), 8 * 1024);

    core.set_flushed_position(0);
    assert!(core.is_able_to_flush(core.normal_read_position(), 2));
    assert!(!core.is_able_to_flush(core.normal_read_position(), 3));
    assert_eq!(core.prepare_flush_range(2, 6), Some((2, 4)));
    assert_eq!(core.committed_position(), 8 * 1024);
    core.record_transient_flush_range(10);
    assert_eq!(core.committed_position(), 10);
    assert_eq!(core.prepare_flush_range(4, 4), None);
    assert_eq!(core.prepare_flush_range(0, 16 * 1024 + 1), None);

    core.record_flush_success(10);
    assert_eq!(core.flushed_position(), 10);
    assert!(core.last_flush_time() > 0);
}

#[test]
fn append_and_lifecycle_timestamps_preserve_atomic_progress_semantics() {
    let core = MappedFileRawCore::new(u64::MAX);
    core.set_wrote_position(i32::MAX);
    core.record_append(1, 42);

    assert_eq!(core.wrote_position(), i32::MIN);
    assert_eq!(core.store_timestamp(), 42);
    core.set_store_timestamp(43);
    assert_eq!(core.store_timestamp(), 43);

    core.set_start_timestamp(100);
    core.set_stop_timestamp(200);
    assert_eq!(core.start_timestamp(), 100);
    assert_eq!(core.stop_timestamp(), 200);
}

#[test]
fn lock_and_cache_range_checks_delegate_to_the_progress_kernel() {
    let core = MappedFileRawCore::new(16);

    assert_eq!(core.lock_region_range(4, 20), Some((4, 12)));
    assert_eq!(core.lock_region_range(16, 1), None);
    assert_eq!(core.lock_region_range(0, 0), None);
    assert!(core.is_valid_cache_range(12, 4));
    assert!(!core.is_valid_cache_range(13, 4));
    assert!(!core.is_valid_cache_range(-1, 1));
    assert!(!core.is_valid_cache_range(0, 0));
}
