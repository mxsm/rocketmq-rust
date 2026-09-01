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
use crate::mapped_file::MappedFileError;
use crate::mapped_file::MappedFileOperation;
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

fn assert_unavailable(error: rocketmq_store_api::StoreError, operation: MappedFileOperation) {
    assert_unavailable_in_state(error, MappedFileAdmissionState::Closing, operation);
}

fn assert_unavailable_in_state(
    error: rocketmq_store_api::StoreError,
    state: MappedFileAdmissionState,
    operation: MappedFileOperation,
) {
    assert_eq!(error.code().as_str(), "storage.backend.unavailable");
    let source = std::error::Error::source(&error).and_then(|source| source.downcast_ref::<MappedFileError>());
    assert!(matches!(
        source,
        Some(MappedFileError::Unavailable {
            state: observed_state,
            operation: observed,
        }) if *observed_state == state && *observed == operation
    ));
}

fn assert_unavailable_typed(error: MappedFileError, operation: MappedFileOperation) {
    assert!(matches!(
        error,
        MappedFileError::Unavailable {
            state: MappedFileAdmissionState::Closing,
            operation: observed,
        } if observed == operation
    ));
}

#[test]
fn closing_rejects_new_read_write_and_maintenance_operations() {
    let (_directory, mapped_file) = mapped_file(64);
    assert!(mapped_file.append_message_bytes(b"readable"));
    mapped_file.shutdown(u64::MAX);

    assert_unavailable(
        mapped_file.try_get_bytes(0, 1).expect_err("read must be rejected"),
        MappedFileOperation::Read,
    );
    assert_unavailable(
        mapped_file
            .try_get_data(0, 1)
            .expect_err("readable data must be rejected"),
        MappedFileOperation::Read,
    );
    assert_unavailable(
        mapped_file.try_get_slice(0, 1).expect_err("slice must be rejected"),
        MappedFileOperation::Read,
    );
    let selection_error = match mapped_file.try_select_mapped_buffer(0, 1) {
        Err(error) => error,
        Ok(_) => panic!("selection must be rejected"),
    };
    assert_unavailable(selection_error, MappedFileOperation::Read);
    let tail_selection_error = match mapped_file.try_select_mapped_buffer_with_position(0) {
        Err(error) => error,
        Ok(_) => panic!("tail selection must be rejected"),
    };
    assert_unavailable(tail_selection_error, MappedFileOperation::Read);
    let invalid_selection_error = match mapped_file.try_select_mapped_buffer(-1, -1) {
        Err(error) => error,
        Ok(_) => panic!("invalid selection must still observe closing"),
    };
    assert_unavailable(invalid_selection_error, MappedFileOperation::Read);
    assert_unavailable(
        mapped_file
            .try_get_slice(usize::MAX, 2)
            .expect_err("invalid slice must still observe closing"),
        MappedFileOperation::Read,
    );
    let write_error = match mapped_file.reserve_write(1) {
        Ok(_) => panic!("write must be rejected"),
        Err(error) => error,
    };
    assert_unavailable(write_error, MappedFileOperation::Write);
    assert_unavailable(
        mapped_file.try_flush(0).expect_err("flush must be rejected"),
        MappedFileOperation::Maintenance,
    );
    assert_unavailable(
        mapped_file.try_commit(0).expect_err("commit must be rejected"),
        MappedFileOperation::Maintenance,
    );
    assert_unavailable_typed(
        mapped_file
            .try_flush_range(8, 1)
            .expect_err("invalid flush range must still observe closing"),
        MappedFileOperation::Maintenance,
    );
    assert_unavailable(
        mapped_file
            .try_warm_mapped_file(FlushDiskType::AsyncFlush, 1)
            .expect_err("warm-up must be rejected"),
        MappedFileOperation::Write,
    );
    assert_unavailable(
        mapped_file.try_mlock().expect_err("mlock must be rejected"),
        MappedFileOperation::Maintenance,
    );
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

    let error = match mapped_file.reserve_write(1) {
        Ok(_) => panic!("sealed segment must reject writes"),
        Err(error) => error,
    };
    assert_unavailable_in_state(
        error,
        MappedFileAdmissionState::SealedReadable,
        MappedFileOperation::Write,
    );
}

#[test]
fn admitted_write_remains_valid_until_its_guard_drops() {
    let (_directory, mapped_file) = mapped_file(16);
    let mut lease = mapped_file.reserve_write(4).expect("pre-close lease");
    lease.buffer_mut().copy_from_slice(b"data");

    mapped_file.shutdown(u64::MAX);
    let pending = mapped_file.lifecycle_snapshot();
    assert_eq!(pending.state, MappedFileAdmissionState::Closing);
    assert_eq!(pending.active_leases, 1);
    assert!(pending.started_at.is_some());
    assert!(!pending.logical_cleanup_marked);

    assert_eq!(lease.commit(4, None).expect("admitted write finishes"), 4);
    let drained = mapped_file.lifecycle_snapshot();
    assert_eq!(drained.active_leases, 0);
    assert!(drained.logical_cleanup_marked);
}

#[test]
fn close_before_lazy_initialization_prevents_late_mapping_publication() {
    let (_directory, mapped_file) = lazy_mapped_file(64);
    assert!(!mapped_file.is_mapped());

    mapped_file.shutdown(u64::MAX);
    assert_unavailable(
        mapped_file.try_get_bytes(0, 1).expect_err("late lazy read must fail"),
        MappedFileOperation::Read,
    );
    assert!(!mapped_file.is_mapped());
    assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 0);
}

#[test]
fn repeated_force_observation_never_invalidates_a_live_lease() {
    let (_directory, mapped_file) = mapped_file(16);
    let lease = mapped_file.reserve_write(4).expect("pre-close lease");

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
