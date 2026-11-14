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

//! Loom models of the committed-read gate.
//!
//! One `UnsafeCell` byte stands for a committed byte of a writable mapping. Readers borrow it
//! through the gate without the writer lock, or copy it under the lock when the gate is closed. A
//! rewrite lowers the read position and rewrites the byte, following the production protocol:
//! check under the lock, release it, close, drain, change under the lock, reopen. Loom reports a
//! data race if a gate reader's access is not ordered before or after the rewrite.

use loom::cell::UnsafeCell;
use loom::sync::atomic::AtomicUsize;
use loom::sync::atomic::Ordering;
use loom::sync::Arc;
use loom::sync::Mutex;
use loom::thread;

#[allow(
    dead_code,
    reason = "the included production model has helpers these focused scenarios do not call"
)]
#[path = "../src/mapped_file/committed_read_model.rs"]
mod committed_read_model;

use committed_read_model::CommittedReadAtomic;
use committed_read_model::CommittedReadEntry;
use committed_read_model::CommittedReadState;

impl CommittedReadAtomic for AtomicUsize {
    fn new(value: usize) -> Self {
        Self::new(value)
    }

    fn load_acquire(&self) -> usize {
        self.load(Ordering::Acquire)
    }

    fn fetch_add_acquire(&self, value: usize) -> usize {
        self.fetch_add(value, Ordering::Acquire)
    }

    fn fetch_or_acq_rel(&self, bits: usize) -> usize {
        self.fetch_or(bits, Ordering::AcqRel)
    }

    fn fetch_and_release(&self, bits: usize) -> usize {
        self.fetch_and(bits, Ordering::Release)
    }

    fn fetch_sub_release(&self, value: usize) -> usize {
        self.fetch_sub(value, Ordering::Release)
    }
}

const ORIGINAL: u8 = 7;
const REWRITTEN: u8 = 9;

struct Segment {
    gate: CommittedReadState<AtomicUsize>,
    writer: Mutex<()>,
    read_position: AtomicUsize,
    byte: UnsafeCell<u8>,
}

impl Segment {
    fn new(enabled: bool) -> Self {
        let segment = Self {
            gate: CommittedReadState::new(),
            writer: Mutex::new(()),
            read_position: AtomicUsize::new(1),
            byte: UnsafeCell::new(ORIGINAL),
        };
        if enabled {
            segment.gate.enable();
        }
        segment
    }

    /// One selection: a borrowed read through the gate, or a copy under the writer lock.
    fn select(&self) -> Option<u8> {
        let entry = match self.gate.try_enter() {
            CommittedReadEntry::Disabled => {
                {
                    let _writer = self.writer.lock().expect("writer lock");
                    self.gate.enable();
                }
                self.gate.try_enter()
            }
            entry => entry,
        };
        if entry != CommittedReadEntry::Entered {
            let _writer = self.writer.lock().expect("writer lock");
            if self.read_position.load(Ordering::Acquire) == 0 {
                return None;
            }
            return Some(self.byte.with(|byte| unsafe { *byte }));
        }
        let selected = if self.read_position.load(Ordering::Acquire) == 0 {
            None
        } else {
            // SAFETY: the byte is below the read position loaded after entering the gate.
            let first = self.byte.with(|byte| unsafe { *byte });
            thread::yield_now();
            let second = self.byte.with(|byte| unsafe { *byte });
            assert_eq!(first, second, "a borrowed committed byte changed");
            Some(first)
        };
        self.gate.exit();
        selected
    }

    /// Truncates the byte away and appends a different one in its place.
    fn rewrite(&self) {
        let writer = self.writer.lock().expect("writer lock");
        let change = || {
            self.read_position.store(0, Ordering::Release);
            // SAFETY: the gate is disabled under the writer lock, or every reader has left it.
            self.byte.with_mut(|byte| unsafe { *byte = REWRITTEN });
            self.read_position.store(1, Ordering::Release);
        };
        if !self.gate.is_enabled() {
            change();
            return;
        }
        drop(writer);
        self.gate.close();
        while self.gate.readers() != 0 {
            thread::yield_now();
        }
        {
            let _writer = self.writer.lock().expect("writer lock");
            change();
        }
        self.gate.reopen();
    }
}

fn assert_valid(selected: Option<u8>) {
    if let Some(byte) = selected {
        assert!(byte == ORIGINAL || byte == REWRITTEN, "unexpected byte {byte}");
    }
}

#[test]
fn borrowed_reads_never_overlap_a_rewrite() {
    loom::model(|| {
        let segment = Arc::new(Segment::new(true));
        let reader_segment = Arc::clone(&segment);
        let reader = thread::spawn(move || assert_valid(reader_segment.select()));
        segment.rewrite();
        reader.join().expect("reader");
        assert_eq!(segment.select(), Some(REWRITTEN));
    });
}

#[test]
fn enabling_races_with_a_rewrite_without_exposing_rewritten_bytes() {
    loom::model(|| {
        let segment = Arc::new(Segment::new(false));
        let reader_segment = Arc::clone(&segment);
        let reader = thread::spawn(move || assert_valid(reader_segment.select()));
        segment.rewrite();
        reader.join().expect("reader");
        assert_eq!(segment.select(), Some(REWRITTEN));
    });
}

#[test]
fn two_readers_and_a_rewrite_keep_borrowed_bytes_stable() {
    loom::model(|| {
        let segment = Arc::new(Segment::new(true));
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let segment = Arc::clone(&segment);
                thread::spawn(move || assert_valid(segment.select()))
            })
            .collect();
        segment.rewrite();
        for reader in readers {
            reader.join().expect("reader");
        }
    });
}
