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

use loom::sync::atomic::AtomicUsize;
use loom::sync::atomic::Ordering;
use loom::sync::Arc;
use loom::sync::Mutex;
use loom::thread;

#[test]
fn position_publication_never_exposes_partially_copied_bytes() {
    loom::model(|| {
        let bytes = Arc::new(Mutex::new([0_u8; 2]));
        let published = Arc::new(AtomicUsize::new(0));

        let writer_bytes = Arc::clone(&bytes);
        let writer_published = Arc::clone(&published);
        let writer = thread::spawn(move || {
            let mut bytes = writer_bytes.lock().expect("writer lock");
            bytes.copy_from_slice(b"ok");
            writer_published.store(2, Ordering::Release);
        });

        let reader_bytes = Arc::clone(&bytes);
        let reader_published = Arc::clone(&published);
        let reader = thread::spawn(move || {
            if reader_published.load(Ordering::Acquire) == 2 {
                assert_eq!(*reader_bytes.lock().expect("reader lock"), *b"ok");
            }
        });

        writer.join().expect("writer");
        reader.join().expect("reader");
    });
}

#[derive(Debug)]
struct WriteState {
    bytes: [u8; 4],
    wrote_position: usize,
}

#[test]
fn concurrent_write_leases_publish_contiguous_non_overlapping_ranges() {
    loom::model(|| {
        let state = Arc::new(Mutex::new(WriteState {
            bytes: [0; 4],
            wrote_position: 0,
        }));
        let starts = Arc::new(Mutex::new(Vec::with_capacity(2)));
        let mut writers = Vec::with_capacity(2);

        for payload in [*b"AA", *b"BB"] {
            let state = Arc::clone(&state);
            let starts = Arc::clone(&starts);
            writers.push(thread::spawn(move || {
                let mut state = state.lock().expect("write sequencer");
                let start = state.wrote_position;
                let end = start + payload.len();
                state.bytes[start..end].copy_from_slice(&payload);
                state.wrote_position = end;
                starts.lock().expect("start positions").push(start);
            }));
        }

        for writer in writers {
            writer.join().expect("writer");
        }

        let state = state.lock().expect("final state");
        let mut starts = starts.lock().expect("final starts").clone();
        starts.sort_unstable();
        assert_eq!(starts, vec![0, 2]);
        assert_eq!(state.wrote_position, 4);
        assert!(state.bytes == *b"AABB" || state.bytes == *b"BBAA");
    });
}

struct ReadLease {
    holds: Arc<AtomicUsize>,
}

impl ReadLease {
    fn acquire(holds: Arc<AtomicUsize>) -> Self {
        holds.fetch_add(1, Ordering::AcqRel);
        Self { holds }
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        let previous = self.holds.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "a read lease must release exactly one hold");
    }
}

#[test]
fn destroy_observes_every_live_read_lease_before_recycle() {
    loom::model(|| {
        let holds = Arc::new(AtomicUsize::new(0));
        let recycling = Arc::new(AtomicUsize::new(0));
        let lease = ReadLease::acquire(Arc::clone(&holds));

        let destroy_holds = Arc::clone(&holds);
        let destroy_recycling = Arc::clone(&recycling);
        let destroy = thread::spawn(move || {
            if destroy_holds.load(Ordering::Acquire) == 0 {
                destroy_recycling.store(1, Ordering::Release);
            }
        });

        assert_eq!(recycling.load(Ordering::Acquire), 0);
        drop(lease);
        destroy.join().expect("destroy");
        if recycling.load(Ordering::Acquire) == 0 {
            assert_eq!(holds.load(Ordering::Acquire), 0);
            recycling.store(1, Ordering::Release);
        }
        assert_eq!(recycling.load(Ordering::Acquire), 1);
        assert_eq!(holds.load(Ordering::Acquire), 0);
    });
}
