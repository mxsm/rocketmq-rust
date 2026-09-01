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

use std::sync::Arc;
use std::sync::Barrier;

use crate::config::FlushDiskType;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::MappedFileAdmissionState;
use crate::mapped_file::MappedWriteLease;
use cheetah_string::CheetahString;
use tempfile::TempDir;

fn mapped_file(size: u64) -> (TempDir, DefaultMappedFile) {
    let directory = TempDir::new().expect("temporary mapped-file directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file = DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), size)
        .expect("mapped file");
    (directory, mapped_file)
}

fn lazy_mapped_file(size: u64) -> (TempDir, DefaultMappedFile) {
    let directory = TempDir::new().expect("temporary mapped-file directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file =
        DefaultMappedFile::try_new_lazy_read_only(CheetahString::from(path.to_string_lossy().into_owned()), size)
            .expect("lazy mapped file");
    (directory, mapped_file)
}

#[test]
fn closing_rejects_new_read_write_and_maintenance_operations() {
    let (_directory, mapped_file) = mapped_file(64);
    assert!(mapped_file.append_message_bytes(b"readable"));
    mapped_file.shutdown(u64::MAX);

    assert!(matches!(mapped_file.try_get_bytes(0, 1), Ok(None)));
    assert!(matches!(mapped_file.try_get_data(0, 1), Ok(None)));
    assert!(matches!(mapped_file.try_get_slice(0, 1), Ok(None)));
    assert!(matches!(mapped_file.try_select_mapped_buffer(0, 1), Ok(None)));
    assert!(matches!(
        mapped_file.try_select_mapped_buffer_with_position(0),
        Ok(None)
    ));
    assert!(matches!(mapped_file.try_select_mapped_buffer(-1, -1), Ok(None)));
    assert!(matches!(mapped_file.try_get_slice(usize::MAX, 2), Ok(None)));
    assert!(matches!(mapped_file.reserve_write(1), Ok(None)));
    assert_eq!(
        mapped_file.try_flush(0).expect("closed flush sentinel"),
        mapped_file.get_flushed_position()
    );
    assert_eq!(
        mapped_file.try_commit(0).expect("closed commit sentinel"),
        mapped_file.get_committed_position()
    );
    assert_eq!(
        mapped_file.try_flush_range(8, 1).expect("closed flush range sentinel"),
        0
    );
    assert!(!mapped_file
        .try_warm_mapped_file(FlushDiskType::AsyncFlush, 1)
        .expect("closed warm sentinel"));
    assert!(!mapped_file.try_mlock().expect("closed mlock sentinel"));
    assert!(!mapped_file.try_munlock().expect("closed munlock sentinel"));
}

#[test]
fn sealed_readable_admits_reads_and_maintenance_but_rejects_writes() {
    let (_directory, mapped_file) = mapped_file(64);
    assert!(mapped_file.append_message_bytes(b"sealed"));

    assert!(mapped_file.seal_readable());
    assert!(!mapped_file.seal_readable());
    assert_eq!(
        mapped_file.lifecycle_snapshot().state,
        MappedFileAdmissionState::SealedReadable
    );
    assert_eq!(
        mapped_file.try_get_bytes(0, 6).expect("read admission").as_deref(),
        Some(&b"sealed"[..])
    );
    mapped_file.try_flush(0).expect("maintenance admission");

    assert!(matches!(mapped_file.reserve_write(1), Ok(None)));
}

#[test]
fn admitted_write_remains_valid_until_its_guard_drops() {
    let (_directory, mapped_file) = mapped_file(16);
    let mut lease = mapped_file
        .reserve_write(4)
        .expect("pre-close lease")
        .expect("valid reservation");
    lease.buffer_mut().copy_from_slice(b"data");

    mapped_file.shutdown(u64::MAX);
    let pending = mapped_file.lifecycle_snapshot();
    assert_eq!(pending.state, MappedFileAdmissionState::Closing);
    assert_eq!(pending.active_leases, 1);
    assert!(pending.started_at.is_some());
    assert!(!pending.logical_cleanup_marked);

    assert_eq!(lease.commit(4, None).expect("admitted write finishes"), Some(4));
    let drained = mapped_file.lifecycle_snapshot();
    assert_eq!(drained.active_leases, 0);
    assert!(drained.logical_cleanup_marked);
}

#[test]
fn close_before_lazy_initialization_prevents_late_mapping_publication() {
    let (_directory, mapped_file) = lazy_mapped_file(64);
    assert!(!mapped_file.is_mapped());

    mapped_file.shutdown(u64::MAX);
    assert!(matches!(mapped_file.try_get_bytes(0, 1), Ok(None)));
    assert!(!mapped_file.is_mapped());
    assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 0);
}

#[test]
fn repeated_force_observation_never_invalidates_a_live_lease() {
    let (_directory, mapped_file) = mapped_file(16);
    let lease = mapped_file
        .reserve_write(4)
        .expect("pre-close lease")
        .expect("valid reservation");

    mapped_file.shutdown(0);
    mapped_file.shutdown(0);
    let snapshot = mapped_file.lifecycle_snapshot();
    assert_eq!(snapshot.active_leases, 1);
    assert!(snapshot.force_observed);
    assert!(!snapshot.logical_cleanup_marked);

    drop(lease);
    assert!(mapped_file.lifecycle_snapshot().logical_cleanup_marked);
}

#[test]
fn concurrent_close_publishes_one_timestamped_generation() {
    const CLOSERS: usize = 8;

    let (_directory, mapped_file) = mapped_file(16);
    let mapped_file = Arc::new(mapped_file);
    let start = Arc::new(Barrier::new(CLOSERS));
    let closers = (0..CLOSERS)
        .map(|_| {
            let mapped_file = Arc::clone(&mapped_file);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                mapped_file.shutdown(u64::MAX);
                let snapshot = mapped_file.lifecycle_snapshot();
                assert_eq!(snapshot.state, MappedFileAdmissionState::Closing);
                assert_eq!(snapshot.generation, 1);
                assert!(snapshot.started_at.is_some());
            })
        })
        .collect::<Vec<_>>();

    for closer in closers {
        closer.join().expect("close worker");
    }

    let snapshot = mapped_file.lifecycle_snapshot();
    assert_eq!(snapshot.state, MappedFileAdmissionState::Closing);
    assert_eq!(snapshot.active_leases, 0);
    assert_eq!(snapshot.generation, 1);
    assert!(snapshot.started_at.is_some());
    assert!(snapshot.logical_cleanup_marked);
}
