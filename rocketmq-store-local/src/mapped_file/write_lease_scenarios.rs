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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Barrier;

use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::MappedWriteLease;
use cheetah_string::CheetahString;
use tempfile::TempDir;

fn mapped_file(size: u64) -> (TempDir, DefaultMappedFile) {
    let temp_dir = TempDir::new().expect("temporary mapped-file directory");
    let path = temp_dir.path().join("00000000000000000000");
    let file = DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), size)
        .expect("mapped file");
    (temp_dir, file)
}

#[test]
fn zero_sized_reservation_stays_on_the_contract_channel() {
    let (_temp_dir, file) = mapped_file(8);

    assert!(matches!(file.reserve_write(0), Ok(None)));
}

#[test]
fn dropped_lease_does_not_modify_or_publish_the_mapping() {
    let (_temp_dir, file) = mapped_file(16);

    {
        let mut lease = file.reserve_write(4).expect("reservation").expect("valid reservation");
        lease.buffer_mut().copy_from_slice(b"drop");
        assert_eq!(file.get_wrote_position(), 0);
        assert!(file.get_bytes_readable_checked(0, 4).is_none());
    }

    assert_eq!(file.get_wrote_position(), 0);
    assert_eq!(file.get_bytes(0, 4).as_deref(), Some(&[0; 4][..]));
}

#[test]
fn committed_lease_copies_then_publishes_position_and_timestamp() {
    let (_temp_dir, file) = mapped_file(16);
    let mut lease = file.reserve_write(4).expect("reservation").expect("valid reservation");

    assert_eq!(lease.start_position(), 0);
    assert_eq!(lease.capacity(), 4);
    lease.buffer_mut().copy_from_slice(b"data");
    assert_eq!(lease.commit(4, Some(42)).expect("commit"), Some(4));

    assert_eq!(file.get_wrote_position(), 4);
    assert_eq!(file.get_store_timestamp(), 42);
    assert_eq!(file.get_bytes_readable_checked(0, 4).as_deref(), Some(&b"data"[..]));
}

#[test]
fn admitted_write_lease_defers_destroy_until_commit_finishes() {
    let (temp_dir, file) = mapped_file(16);
    let path = temp_dir.path().join("00000000000000000000");
    let mut lease = file
        .reserve_write(4)
        .expect("reservation wins before shutdown")
        .expect("valid reservation");
    lease.buffer_mut().copy_from_slice(b"data");

    let pending = file.try_destroy(0);
    assert!(!pending.is_namespace_removed());
    assert!(path.exists());
    assert_eq!(lease.commit(4, None).expect("admitted write may finish"), Some(4));

    assert!(file.try_destroy(0).is_namespace_removed());
    assert!(!path.exists());
}

#[test]
fn forced_shutdown_never_bypasses_an_admitted_lease() {
    let (temp_dir, file) = mapped_file(16);
    let path = temp_dir.path().join("00000000000000000000");
    let mut lease = file
        .reserve_write(4)
        .expect("reservation wins before shutdown")
        .expect("valid reservation");
    lease.buffer_mut().copy_from_slice(b"data");

    assert!(!file.try_destroy(0).is_namespace_removed());
    assert!(!file.try_destroy(0).is_namespace_removed());
    assert!(path.exists());
    assert_eq!(
        lease
            .commit(4, None)
            .expect("admitted lease remains valid after force observation"),
        Some(4)
    );
    assert!(file.try_destroy(0).is_namespace_removed());
    assert!(!path.exists());
}

#[test]
fn oversized_commit_is_rejected_without_partial_publication() {
    let (_temp_dir, file) = mapped_file(8);
    assert!(file.append_message_bytes(b"123456"));
    let mut lease = file
        .reserve_write(4)
        .expect("remaining reservation")
        .expect("valid reservation");

    assert_eq!(lease.start_position(), 6);
    assert_eq!(lease.capacity(), 2);
    lease.buffer_mut().copy_from_slice(b"78");
    assert_eq!(lease.commit(3, None).expect("contract result"), None);
    assert_eq!(file.get_wrote_position(), 6);
    assert_eq!(file.get_bytes(6, 2).as_deref(), Some(&[0; 2][..]));
}

#[test]
fn panic_while_encoding_drops_staging_without_publishing() {
    let (_temp_dir, file) = mapped_file(8);

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut lease = file.reserve_write(4).expect("reservation").expect("valid reservation");
        lease.buffer_mut().copy_from_slice(b"boom");
        panic!("simulated encoder panic");
    }));

    assert!(result.is_err());
    assert_eq!(file.get_wrote_position(), 0);
    assert_eq!(file.get_bytes(0, 4).as_deref(), Some(&[0; 4][..]));
}

#[test]
fn concurrent_writers_receive_non_overlapping_reservations() {
    let temp_dir = TempDir::new().expect("temporary mapped-file directory");
    let path = temp_dir.path().join("00000000000000000000");
    let file: Arc<DefaultMappedFile> = Arc::new(
        DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 8).expect("mapped file"),
    );
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();

    for payload in [*b"AAAA", *b"BBBB"] {
        let file = Arc::clone(&file);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let mut lease = file.reserve_write(4).expect("reservation").expect("valid reservation");
            let start = lease.start_position();
            lease.buffer_mut().copy_from_slice(&payload);
            assert_eq!(lease.commit(4, None).expect("commit"), Some(start + 4));
            start
        }));
    }

    barrier.wait();
    let mut starts: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().expect("writer thread"))
        .collect();
    starts.sort_unstable();

    assert_eq!(starts, vec![0, 4]);
    assert_eq!(file.get_wrote_position(), 8);
    let bytes = file.get_bytes_readable_checked(0, 8).expect("published data");
    assert!(bytes.as_ref() == b"AAAABBBB" || bytes.as_ref() == b"BBBBAAAA");
}
