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

// The production model also contains projection helpers that these focused Loom scenarios do not
// need. Including the source keeps every lifecycle decision on the production transition code.
#[allow(
    dead_code,
    reason = "the included production model exposes transitions outside these focused Loom scenarios"
)]
#[path = "../src/mapped_file/lifecycle_model.rs"]
mod lifecycle_model;

use lifecycle_model::AcquireTransitionError;
use lifecycle_model::BeginCloseTransition;
use lifecycle_model::LifecycleAtomicUsize;
use lifecycle_model::LifecycleTransitionState;
use lifecycle_model::MappedFileAdmissionState;
use lifecycle_model::MappedFileOperation;
use lifecycle_model::ReleaseTransition;

impl LifecycleAtomicUsize for AtomicUsize {
    fn new(value: usize) -> Self {
        Self::new(value)
    }

    fn load_acquire(&self) -> usize {
        self.load(Ordering::Acquire)
    }

    fn compare_exchange_weak_acquire(&self, current: usize, new: usize) -> Result<usize, usize> {
        self.compare_exchange_weak(current, new, Ordering::Acquire, Ordering::Acquire)
    }

    fn compare_exchange_weak_acq_rel(&self, current: usize, new: usize) -> Result<usize, usize> {
        self.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
    }

    fn compare_exchange_weak_acq_rel_relaxed(&self, current: usize, new: usize) -> Result<usize, usize> {
        self.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Relaxed)
    }
}

type LoomLifecycle = LifecycleTransitionState<AtomicUsize>;

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

struct ModelLease {
    lifecycle: Arc<LoomLifecycle>,
    drain_transitions: Arc<AtomicUsize>,
}

impl ModelLease {
    fn try_acquire(
        lifecycle: Arc<LoomLifecycle>,
        operation: MappedFileOperation,
        drain_transitions: Arc<AtomicUsize>,
    ) -> Result<Self, AcquireTransitionError> {
        lifecycle.try_acquire(operation)?;
        Ok(Self {
            lifecycle,
            drain_transitions,
        })
    }
}

impl Drop for ModelLease {
    fn drop(&mut self) {
        let transition = self.lifecycle.release();
        assert_ne!(transition, ReleaseTransition::Underflow);
        if transition == ReleaseTransition::Drained {
            self.drain_transitions.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn acquire_and_close_have_one_serialized_outcome() {
    loom::model(|| {
        let lifecycle = Arc::new(LoomLifecycle::new());
        let admitted = Arc::new(Mutex::new(None));
        let drain_transitions = Arc::new(AtomicUsize::new(0));

        let acquire_lifecycle = Arc::clone(&lifecycle);
        let acquire_admitted = Arc::clone(&admitted);
        let acquire_drains = Arc::clone(&drain_transitions);
        let acquire = thread::spawn(move || {
            match ModelLease::try_acquire(acquire_lifecycle, MappedFileOperation::Write, acquire_drains) {
                Ok(lease) => *acquire_admitted.lock().expect("admitted slot") = Some(lease),
                Err(AcquireTransitionError::Unavailable(MappedFileAdmissionState::Closing)) => {}
                Err(error) => panic!("unexpected acquire error: {error:?}"),
            }
        });

        let close_lifecycle = Arc::clone(&lifecycle);
        let close = thread::spawn(move || {
            assert_eq!(close_lifecycle.begin_close(), BeginCloseTransition::Started);
        });

        acquire.join().expect("acquire");
        close.join().expect("close");

        let lease = admitted.lock().expect("admitted slot").take();
        let expected_active = usize::from(lease.is_some());
        assert_eq!(lifecycle.state(), MappedFileAdmissionState::Closing);
        assert_eq!(lifecycle.active_leases(), expected_active);
        assert_eq!(lifecycle.logical_cleanup_marked(), expected_active == 0);

        drop(lease);
        assert_eq!(lifecycle.active_leases(), 0);
        assert!(lifecycle.logical_cleanup_marked());
        assert_eq!(drain_transitions.load(Ordering::SeqCst), expected_active);
    });
}

#[test]
fn seal_and_write_admission_have_one_serialized_outcome() {
    loom::model(|| {
        let lifecycle = Arc::new(LoomLifecycle::new());
        let admitted_write = Arc::new(Mutex::new(None));
        let drain_transitions = Arc::new(AtomicUsize::new(0));

        let write_lifecycle = Arc::clone(&lifecycle);
        let write_slot = Arc::clone(&admitted_write);
        let write_drains = Arc::clone(&drain_transitions);
        let write = thread::spawn(move || {
            match ModelLease::try_acquire(write_lifecycle, MappedFileOperation::Write, write_drains) {
                Ok(lease) => *write_slot.lock().expect("write slot") = Some(lease),
                Err(AcquireTransitionError::Unavailable(MappedFileAdmissionState::SealedReadable)) => {}
                Err(error) => panic!("unexpected write error: {error:?}"),
            }
        });

        let seal_lifecycle = Arc::clone(&lifecycle);
        let seal = thread::spawn(move || {
            assert!(seal_lifecycle.seal_readable());
        });

        write.join().expect("write");
        seal.join().expect("seal");

        let write_lease = admitted_write.lock().expect("write slot").take();
        let expected_write = usize::from(write_lease.is_some());
        assert_eq!(lifecycle.state(), MappedFileAdmissionState::SealedReadable);
        assert_eq!(lifecycle.active_leases(), expected_write);

        let read_lease = ModelLease::try_acquire(
            Arc::clone(&lifecycle),
            MappedFileOperation::Read,
            Arc::clone(&drain_transitions),
        )
        .expect("sealed segments remain readable");
        let maintenance_lease = ModelLease::try_acquire(
            Arc::clone(&lifecycle),
            MappedFileOperation::Maintenance,
            Arc::clone(&drain_transitions),
        )
        .expect("sealed segments permit maintenance");
        assert_eq!(
            lifecycle.try_acquire(MappedFileOperation::Write),
            Err(AcquireTransitionError::Unavailable(
                MappedFileAdmissionState::SealedReadable
            ))
        );

        drop(read_lease);
        drop(maintenance_lease);
        drop(write_lease);
        assert_eq!(lifecycle.active_leases(), 0);
        assert_eq!(drain_transitions.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn last_drop_and_close_converge_on_a_drained_closing_state() {
    loom::model(|| {
        let lifecycle = Arc::new(LoomLifecycle::new());
        let drain_transitions = Arc::new(AtomicUsize::new(0));
        let lease = ModelLease::try_acquire(
            Arc::clone(&lifecycle),
            MappedFileOperation::Read,
            Arc::clone(&drain_transitions),
        )
        .expect("initial read lease");

        let release = thread::spawn(move || drop(lease));
        let close_lifecycle = Arc::clone(&lifecycle);
        let close = thread::spawn(move || {
            assert_eq!(close_lifecycle.begin_close(), BeginCloseTransition::Started);
        });

        release.join().expect("release");
        close.join().expect("close");

        assert_eq!(lifecycle.state(), MappedFileAdmissionState::Closing);
        assert_eq!(lifecycle.active_leases(), 0);
        assert!(lifecycle.logical_cleanup_marked());
        assert!(drain_transitions.load(Ordering::SeqCst) <= 1);
    });
}

#[test]
fn concurrent_close_starts_one_generation() {
    loom::model(|| {
        let lifecycle = Arc::new(LoomLifecycle::new());
        let started = Arc::new(AtomicUsize::new(0));
        let mut closers = Vec::with_capacity(2);

        for _ in 0..2 {
            let lifecycle = Arc::clone(&lifecycle);
            let started = Arc::clone(&started);
            closers.push(thread::spawn(move || {
                let transition = lifecycle.begin_close();
                if transition == BeginCloseTransition::Started {
                    started.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for closer in closers {
            closer.join().expect("close");
        }

        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(lifecycle.state(), MappedFileAdmissionState::Closing);
        assert_eq!(lifecycle.active_leases(), 0);
        assert!(lifecycle.logical_cleanup_marked());
    });
}

#[test]
fn repeated_close_does_not_change_active_leases() {
    loom::model(|| {
        let lifecycle = Arc::new(LoomLifecycle::new());
        let drain_transitions = Arc::new(AtomicUsize::new(0));
        let lease = ModelLease::try_acquire(
            Arc::clone(&lifecycle),
            MappedFileOperation::Read,
            Arc::clone(&drain_transitions),
        )
        .expect("initial read lease");
        assert_eq!(lifecycle.begin_close(), BeginCloseTransition::Started);

        let repeat_lifecycle = Arc::clone(&lifecycle);
        let repeat = thread::spawn(move || {
            let active_before = repeat_lifecycle.active_leases();
            assert_eq!(repeat_lifecycle.begin_close(), BeginCloseTransition::AlreadyClosing);
            assert_eq!(repeat_lifecycle.active_leases(), active_before);
        });
        repeat.join().expect("repeat close");

        assert_eq!(lifecycle.active_leases(), 1);
        assert!(!lifecycle.logical_cleanup_marked());

        drop(lease);
        assert_eq!(lifecycle.active_leases(), 0);
        assert!(lifecycle.logical_cleanup_marked());
        assert_eq!(drain_transitions.load(Ordering::SeqCst), 1);
    });
}
