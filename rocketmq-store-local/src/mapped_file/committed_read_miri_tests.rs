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

//! Committed reads under Miri, without operating-system mappings.
//!
//! A heap allocation with raw provenance stands for the mapping: the writer appends through the raw
//! base pointer, as `MappedMemory::as_mut_ptr` does, and readers build slices of the committed prefix
//! from the same pointer, as `CommittedPrefix` does, while holding the production gate. Miri checks
//! that borrowed slices and tail writes never race and that the slices stay valid under the
//! aliasing model while the writer keeps writing the tail.

use std::ptr::NonNull;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::mapped_file::committed_read::CommittedReadAdmission;
use crate::mapped_file::committed_read::CommittedReadGate;
use crate::mapped_file::committed_read::CommittedReadGuard;

const RECORD: usize = 8;

struct HeapSegment {
    base: NonNull<u8>,
    len: usize,
    gate: Arc<CommittedReadGate>,
    writer: Mutex<()>,
    read_position: AtomicUsize,
}

// SAFETY: the allocation is owned by the segment; every write happens under `writer`, and reads
// below the read position are ordered with rewrites by the gate.
unsafe impl Send for HeapSegment {}
// SAFETY: see `Send`.
unsafe impl Sync for HeapSegment {}

impl Drop for HeapSegment {
    fn drop(&mut self) {
        // SAFETY: `base` and `len` come from `Box::into_raw` in `new`, and no borrow outlives the
        // segment.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.base.as_ptr(), self.len)) });
    }
}

struct Borrowed<'a> {
    bytes: &'a [u8],
    _guard: CommittedReadGuard,
}

impl HeapSegment {
    fn new(records: usize) -> Self {
        let allocation = Box::into_raw(vec![0_u8; records * RECORD].into_boxed_slice());
        Self {
            base: NonNull::new(allocation.cast::<u8>()).expect("allocation"),
            len: records * RECORD,
            gate: Arc::new(CommittedReadGate::new()),
            writer: Mutex::new(()),
            read_position: AtomicUsize::new(0),
        }
    }

    fn append(&self, value: u8) {
        let _writer = self.writer.lock();
        let position = self.read_position.load(Ordering::Acquire);
        assert!(position + RECORD <= self.len);
        // SAFETY: the destination is inside the allocation and at the read position, so no
        // borrowed slice covers it; `writer` serializes writers.
        unsafe { std::ptr::write_bytes(self.base.as_ptr().add(position), value, RECORD) };
        self.read_position.store(position + RECORD, Ordering::Release);
    }

    fn borrow(&self, record: usize) -> Option<Borrowed<'_>> {
        let guard = match self.gate.try_enter() {
            CommittedReadAdmission::Entered(guard) => guard,
            CommittedReadAdmission::Busy => return None,
            CommittedReadAdmission::Disabled => {
                {
                    let _writer = self.writer.lock();
                    self.gate.enable();
                }
                match self.gate.try_enter() {
                    CommittedReadAdmission::Entered(guard) => guard,
                    CommittedReadAdmission::Disabled | CommittedReadAdmission::Busy => return None,
                }
            }
        };
        let start = record * RECORD;
        if start + RECORD > self.read_position.load(Ordering::Acquire) {
            return None;
        }
        // SAFETY: the record is below the read position loaded after entering the gate, which
        // keeps it unchanged until `guard` is dropped with the borrow.
        let bytes = unsafe { std::slice::from_raw_parts(self.base.as_ptr().add(start), RECORD) };
        Some(Borrowed { bytes, _guard: guard })
    }

    fn truncate(&self, position: usize) {
        let writer = self.writer.lock();
        if !self.gate.is_enabled() || position >= self.read_position.load(Ordering::Acquire) {
            self.read_position.store(position, Ordering::Release);
            return;
        }
        drop(writer);
        let window = self
            .gate
            .begin_rewrite(Duration::from_secs(600))
            .expect("readers leave");
        let _writer = self.writer.lock();
        self.read_position.store(position, Ordering::Release);
        drop(window);
    }
}

#[test]
fn borrowed_prefix_slices_coexist_with_tail_appends() {
    let segment = Arc::new(HeapSegment::new(16));
    for value in 0..4 {
        segment.append(value);
    }
    let first = segment.borrow(0).expect("record 0");
    let second = segment.borrow(1).expect("record 1");

    let appender_segment = Arc::clone(&segment);
    let appender = std::thread::spawn(move || {
        for value in 4..12 {
            appender_segment.append(value);
        }
    });
    for _ in 0..4 {
        assert_eq!(first.bytes, [0; RECORD]);
        assert_eq!(second.bytes, [1; RECORD]);
    }
    appender.join().expect("appender");
    assert_eq!(segment.borrow(11).expect("record 11").bytes, [11; RECORD]);
}

#[test]
fn truncation_rewrites_a_record_only_after_its_borrower_leaves() {
    let segment = Arc::new(HeapSegment::new(8));
    for value in 0..4 {
        segment.append(value);
    }
    let (borrowed, ready) = mpsc::channel::<()>();
    let reader_segment = Arc::clone(&segment);
    let reader = std::thread::spawn(move || {
        let held = reader_segment.borrow(2).expect("record 2");
        borrowed.send(()).expect("test alive");
        for _ in 0..8 {
            assert_eq!(held.bytes, [2; RECORD], "a borrowed record changed");
            std::thread::yield_now();
        }
    });
    ready.recv().expect("reader borrowed");
    segment.truncate(2 * RECORD);
    segment.append(20);
    segment.append(30);
    reader.join().expect("reader");
    assert_eq!(segment.borrow(2).expect("rewritten record").bytes, [20; RECORD]);
    assert_eq!(segment.borrow(1).expect("kept record").bytes, [1; RECORD]);
}
