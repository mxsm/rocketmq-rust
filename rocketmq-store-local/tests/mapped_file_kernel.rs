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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use rocketmq_store_local::mapped_file::kernel::MappedFileProgress;
use rocketmq_store_local::mapped_file::kernel::ReferenceResource;
use rocketmq_store_local::mapped_file::kernel::ReferenceResourceBase;
use rocketmq_store_local::mapped_file::kernel::ReferenceResourceCounter;
use rocketmq_store_local::mapped_file::kernel::OS_PAGE_SIZE;

#[test]
fn progress_starts_with_legacy_defaults() {
    let progress = MappedFileProgress::new(4096);

    assert_eq!(progress.file_size(), 4096);
    assert_eq!(progress.wrote_position(), 0);
    assert_eq!(progress.committed_position(), 0);
    assert_eq!(progress.flushed_position(), 0);
    assert_eq!(progress.store_timestamp(), 0);
    assert_eq!(progress.last_flush_time(), 0);
    assert_eq!(progress.start_timestamp(), -1);
    assert_eq!(progress.stop_timestamp(), -1);
}

#[test]
fn progress_preserves_segment_full_boundary_and_readable_rules() {
    let progress = MappedFileProgress::new(32);
    progress.set_wrote_position(31);
    progress.set_committed_position(17);

    assert!(!progress.is_full());
    assert_eq!(progress.read_position(false), 31);
    assert_eq!(progress.read_position(true), 17);

    progress.advance_wrote_position(1);
    assert!(progress.is_full());
}

#[test]
fn lock_region_range_preserves_legacy_empty_boundary_and_clipping_policy() {
    let progress = MappedFileProgress::new(4096);

    assert_eq!(progress.lock_region_range(0, 0), None);
    assert_eq!(progress.lock_region_range(4096, 1), None);
    assert_eq!(progress.lock_region_range(u64::MAX, usize::MAX), None);
    assert_eq!(progress.lock_region_range(0, 1), Some((0, 1)));
    assert_eq!(progress.lock_region_range(3072, 4096), Some((3072, 1024)));
    assert_eq!(progress.lock_region_range(4095, usize::MAX), Some((4095, 1)));
}

#[cfg(target_pointer_width = "32")]
#[test]
fn lock_region_range_rejects_offsets_that_do_not_fit_usize_after_clipping() {
    let progress = MappedFileProgress::new(u64::MAX);
    let offset = usize::MAX as u64 + 1;

    assert_eq!(progress.lock_region_range(offset, 1), None);
}

#[test]
fn successful_flush_is_the_only_operation_that_advances_durable_progress() {
    let progress = MappedFileProgress::new(64);
    progress.set_wrote_position(24);
    progress.set_committed_position(20);

    assert_eq!(
        progress.flushed_position(),
        0,
        "crash-before-flush must remain non-durable"
    );
    assert_eq!(progress.last_flush_time(), 0);

    progress.record_flush_success(20);

    assert_eq!(progress.flushed_position(), 20);
    assert!(progress.last_flush_time() > 0);
}

#[test]
fn append_commit_and_timestamps_keep_legacy_setter_semantics() {
    let progress = MappedFileProgress::new(64);
    progress.record_append(7, 1_700_000_000_123);
    progress.commit_wrote_position();
    progress.set_start_timestamp(101);
    progress.set_stop_timestamp(202);

    assert_eq!(progress.wrote_position(), 7);
    assert_eq!(progress.committed_position(), 7);
    assert_eq!(progress.store_timestamp(), 1_700_000_000_123);
    assert_eq!(progress.start_timestamp(), 101);
    assert_eq!(progress.stop_timestamp(), 202);
}

#[test]
fn page_threshold_policy_preserves_exact_legacy_boundaries() {
    assert_eq!(OS_PAGE_SIZE, 1024 * 4);

    let progress = MappedFileProgress::new(OS_PAGE_SIZE * 4);
    progress.set_flushed_position(0);

    assert!(!progress.is_able_to_flush(4095, 1));
    assert!(progress.is_able_to_flush(4096, 1));
    assert!(!progress.is_able_to_flush(8191, 2));
    assert!(progress.is_able_to_flush(8192, 2));
    assert!(progress.is_able_to_flush(1, 0));
    assert!(progress.is_able_to_flush(1, -1));
    assert!(!progress.is_able_to_flush(0, 0));

    progress.set_flushed_position(4096);
    assert!(!progress.is_able_to_flush(4096, 0));
    assert!(!progress.is_able_to_flush(4095, 0));
    assert!(!progress.is_able_to_flush(4095, 1));

    progress.set_wrote_position(4095);
    progress.set_committed_position(0);
    assert!(!progress.is_able_to_commit(1));
    progress.set_wrote_position(4096);
    assert!(progress.is_able_to_commit(1));
    assert!(progress.is_able_to_commit(0));
    progress.set_committed_position(4096);
    assert!(!progress.is_able_to_commit(0));
    assert!(!progress.is_able_to_commit(-1));
}

#[test]
fn full_segment_short_circuits_flush_and_commit_thresholds() {
    let progress = MappedFileProgress::new(OS_PAGE_SIZE);
    progress.set_wrote_position(OS_PAGE_SIZE as i32);
    progress.set_committed_position(OS_PAGE_SIZE as i32);
    progress.set_flushed_position(OS_PAGE_SIZE as i32);

    assert!(progress.is_able_to_flush(0, i32::MAX));
    assert!(progress.is_able_to_commit(i32::MAX));
}

#[test]
fn graceful_and_forced_shutdown_keep_legacy_reference_semantics() {
    let graceful = ReferenceResourceCounter::new();
    assert!(graceful.hold());
    graceful.shutdown(0);
    assert_eq!(graceful.get_ref_count(), 1);
    graceful.release();
    assert!(graceful.is_cleanup_over());

    let forced = ReferenceResourceCounter::new();
    assert!(forced.hold());
    forced.shutdown(0);
    forced.shutdown(0);
    assert!(forced.get_ref_count() <= -1000);
    assert!(forced.is_cleanup_over());
}

struct CountingResource {
    base: ReferenceResourceBase,
    cleanup_count: AtomicUsize,
}

impl ReferenceResource for CountingResource {
    fn base(&self) -> &ReferenceResourceBase {
        &self.base
    }

    fn cleanup(&self, _current_ref: i64) -> bool {
        self.cleanup_count.fetch_add(1, Ordering::SeqCst);
        true
    }
}

#[test]
fn concurrent_final_releases_cleanup_exactly_once() {
    let resource = Arc::new(CountingResource {
        base: ReferenceResourceBase::new(),
        cleanup_count: AtomicUsize::new(0),
    });
    let holders = 16;
    for _ in 0..holders {
        assert!(resource.hold());
    }

    let handles = (0..holders)
        .map(|_| {
            let resource = Arc::clone(&resource);
            thread::spawn(move || resource.release())
        })
        .collect::<Vec<_>>();
    resource.release();
    for handle in handles {
        handle.join().expect("release worker must finish");
    }

    assert_eq!(resource.cleanup_count.load(Ordering::SeqCst), 1);
    assert!(resource.is_cleanup_over());
}
