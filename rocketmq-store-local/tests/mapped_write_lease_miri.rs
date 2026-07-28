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
use std::sync::Mutex;
use std::sync::MutexGuard;

use rocketmq_store_local::mapped_file::MappedFileError;
use rocketmq_store_local::mapped_file::MappedFileResult;
use rocketmq_store_local::mapped_file::MappedWriteLease;

/// Safe heap-backed owner used to exercise the production write-lease interface
/// under Miri without relying on operating-system memory mapping support.
struct SafeLeaseOwner {
    bytes: Mutex<Vec<u8>>,
    wrote_position: AtomicUsize,
    writer: Mutex<()>,
}

impl SafeLeaseOwner {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: Mutex::new(vec![0; capacity]),
            wrote_position: AtomicUsize::new(0),
            writer: Mutex::new(()),
        }
    }

    fn reserve_write(&self, required_space: usize) -> MappedFileResult<SafeWriteLease<'_>> {
        let writer = self.writer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let start_position = self.wrote_position.load(Ordering::Acquire);
        let capacity = self.bytes.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).len();
        if required_space == 0 {
            return Err(MappedFileError::InvalidWriteCommit { reserved: 0, actual: 0 });
        }
        if start_position >= capacity {
            return Err(MappedFileError::file_full(start_position, capacity as u64));
        }

        let reserved = required_space.min(capacity - start_position);
        Ok(SafeWriteLease {
            owner: self,
            _writer: writer,
            staging: vec![0; reserved],
            start_position,
        })
    }

    fn snapshot(&self) -> Vec<u8> {
        self.bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

struct SafeWriteLease<'a> {
    owner: &'a SafeLeaseOwner,
    _writer: MutexGuard<'a, ()>,
    staging: Vec<u8>,
    start_position: usize,
}

impl MappedWriteLease for SafeWriteLease<'_> {
    fn start_position(&self) -> usize {
        self.start_position
    }

    fn capacity(&self) -> usize {
        self.staging.len()
    }

    fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.staging
    }

    fn commit(self, actual_bytes: usize, _store_timestamp: Option<u64>) -> MappedFileResult<usize> {
        if actual_bytes == 0 || actual_bytes > self.capacity() {
            return Err(MappedFileError::InvalidWriteCommit {
                reserved: self.capacity(),
                actual: actual_bytes,
            });
        }
        let end_position = self
            .start_position
            .checked_add(actual_bytes)
            .filter(|end| {
                *end <= self
                    .owner
                    .bytes
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len()
            })
            .ok_or_else(|| {
                let capacity = self
                    .owner
                    .bytes
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                MappedFileError::out_of_bounds(self.start_position, actual_bytes, capacity as u64)
            })?;

        let mut bytes = self.owner.bytes.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        bytes[self.start_position..end_position].copy_from_slice(&self.staging[..actual_bytes]);
        self.owner.wrote_position.store(end_position, Ordering::Release);
        Ok(end_position)
    }
}

#[test]
fn write_lease_publishes_only_committed_bytes() {
    let owner = SafeLeaseOwner::new(16);
    let mut lease = owner.reserve_write(8).expect("safe lease reservation");
    lease.buffer_mut()[..6].copy_from_slice(b"commit");

    assert_eq!(lease.commit(6, None).expect("safe lease commit"), 6);
    assert_eq!(&owner.snapshot()[..6], b"commit");
    assert_eq!(&owner.snapshot()[6..], &[0; 10]);
}

#[test]
fn dropped_write_lease_does_not_publish_staged_bytes() {
    let owner = SafeLeaseOwner::new(8);
    {
        let mut lease = owner.reserve_write(4).expect("safe lease reservation");
        lease.buffer_mut().copy_from_slice(b"drop");
    }

    assert_eq!(owner.wrote_position.load(Ordering::Acquire), 0);
    assert_eq!(owner.snapshot(), vec![0; 8]);
}

#[test]
fn invalid_commit_preserves_bytes_and_position() {
    let owner = SafeLeaseOwner::new(8);
    let lease = owner.reserve_write(4).expect("safe lease reservation");

    assert!(lease.commit(5, None).is_err());
    assert_eq!(owner.wrote_position.load(Ordering::Acquire), 0);
    assert_eq!(owner.snapshot(), vec![0; 8]);
}

#[test]
fn tail_reservation_is_bounded_by_remaining_capacity() {
    let owner = SafeLeaseOwner::new(8);
    let mut first = owner.reserve_write(6).expect("first safe lease reservation");
    first.buffer_mut().copy_from_slice(b"first!");
    assert_eq!(first.commit(6, None).expect("first safe lease commit"), 6);

    let tail = owner.reserve_write(8).expect("tail safe lease reservation");
    assert_eq!(tail.start_position(), 6);
    assert_eq!(tail.capacity(), 2);
}
