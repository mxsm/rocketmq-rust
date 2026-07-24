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

    assert_eq!(core.copied_read_slice(|| &mapped, 8, 1), None);
    assert_eq!(provider_calls.get(), 0);

    let short = [0; 4];
    assert_eq!(core.copied_read_slice(|| &short, 2, 3), None);
    assert_eq!(core.raw_slice(|| &short, 2, 3), None);
    assert_eq!(core.readable_slice(|| &short, 2, 3, 8), None);
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
