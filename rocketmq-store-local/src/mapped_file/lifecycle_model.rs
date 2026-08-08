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

use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Admission state for one mapped-file segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MappedFileAdmissionState {
    /// Reads, writes, and maintenance operations may be admitted.
    ActiveWritable = 0,
    /// Reads and maintenance operations may be admitted, but writes are sealed.
    SealedReadable = 1,
    /// No new operation may be admitted.
    Closing = 2,
}

impl MappedFileAdmissionState {
    #[inline]
    pub fn allows(self, operation: MappedFileOperation) -> bool {
        match (self, operation) {
            (Self::ActiveWritable, _) => true,
            (Self::SealedReadable, MappedFileOperation::Read | MappedFileOperation::Maintenance) => true,
            (Self::SealedReadable, MappedFileOperation::Write) | (Self::Closing, _) => false,
        }
    }
}

impl fmt::Display for MappedFileAdmissionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ActiveWritable => "active-writable",
            Self::SealedReadable => "sealed-readable",
            Self::Closing => "closing",
        })
    }
}

/// Kind of operation admitted against the mapped-file lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MappedFileOperation {
    /// Immutable mapped-file access.
    Read,
    /// Mapped-file mutation or write reservation.
    Write,
    /// Flush, commit, warm-up, residency, or memory-lock maintenance.
    Maintenance,
}

impl fmt::Display for MappedFileOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Maintenance => "maintenance",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcquireTransitionError {
    Unavailable(MappedFileAdmissionState),
    LeaseCountOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BeginCloseTransition {
    Started,
    AlreadyClosing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReleaseTransition {
    Remaining(usize),
    Drained,
    Underflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LifecyclePackedSnapshot {
    pub(crate) state: MappedFileAdmissionState,
    pub(crate) active_leases: usize,
}

/// Minimal atomic facade shared by the production implementation and Loom.
pub(crate) trait LifecycleAtomicUsize: Send + Sync {
    fn new(value: usize) -> Self;

    fn load_acquire(&self) -> usize;

    fn compare_exchange_weak_acquire(&self, current: usize, new: usize) -> Result<usize, usize>;

    fn compare_exchange_weak_acq_rel(&self, current: usize, new: usize) -> Result<usize, usize>;

    fn compare_exchange_weak_acq_rel_relaxed(&self, current: usize, new: usize) -> Result<usize, usize>;
}

impl LifecycleAtomicUsize for AtomicUsize {
    #[inline]
    fn new(value: usize) -> Self {
        AtomicUsize::new(value)
    }

    #[inline]
    fn load_acquire(&self) -> usize {
        self.load(Ordering::Acquire)
    }

    #[inline]
    fn compare_exchange_weak_acquire(&self, current: usize, new: usize) -> Result<usize, usize> {
        self.compare_exchange_weak(current, new, Ordering::Acquire, Ordering::Acquire)
    }

    #[inline]
    fn compare_exchange_weak_acq_rel(&self, current: usize, new: usize) -> Result<usize, usize> {
        self.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
    }

    #[inline]
    fn compare_exchange_weak_acq_rel_relaxed(&self, current: usize, new: usize) -> Result<usize, usize> {
        self.compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Relaxed)
    }
}

const STATE_BITS: usize = 2;
const STATE_SHIFT: u32 = usize::BITS - STATE_BITS as u32;
const ACTIVE_LEASE_MASK: usize = usize::MAX >> STATE_BITS;
const STATE_MASK: usize = !ACTIVE_LEASE_MASK;

#[inline]
const fn encode_state(state: MappedFileAdmissionState) -> usize {
    (state as usize) << STATE_SHIFT
}

#[inline]
const fn decode_state(word: usize) -> MappedFileAdmissionState {
    match (word & STATE_MASK) >> STATE_SHIFT {
        0 => MappedFileAdmissionState::ActiveWritable,
        1 => MappedFileAdmissionState::SealedReadable,
        _ => MappedFileAdmissionState::Closing,
    }
}

/// Lock-free lifecycle transitions shared by production and Loom tests.
///
/// The high two bits encode the admission state and the remaining bits encode the active lease
/// count. Every admission-state transition and lease-count change is linearized by one CAS on the
/// packed word, so a close cannot race with a successful post-close admission.
#[derive(Debug)]
pub(crate) struct LifecycleTransitionState<A: LifecycleAtomicUsize = AtomicUsize> {
    word: A,
}

impl<A: LifecycleAtomicUsize> LifecycleTransitionState<A> {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            word: A::new(encode_state(MappedFileAdmissionState::ActiveWritable)),
        }
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> LifecyclePackedSnapshot {
        let word = self.word.load_acquire();
        LifecyclePackedSnapshot {
            state: decode_state(word),
            active_leases: word & ACTIVE_LEASE_MASK,
        }
    }

    #[inline]
    pub(crate) fn state(&self) -> MappedFileAdmissionState {
        self.snapshot().state
    }

    #[inline]
    pub(crate) fn active_leases(&self) -> usize {
        self.snapshot().active_leases
    }

    #[inline]
    pub(crate) fn try_acquire(&self, operation: MappedFileOperation) -> Result<(), AcquireTransitionError> {
        let mut current = self.word.load_acquire();
        loop {
            let state = decode_state(current);
            if !state.allows(operation) {
                return Err(AcquireTransitionError::Unavailable(state));
            }

            let active_leases = current & ACTIVE_LEASE_MASK;
            if active_leases == ACTIVE_LEASE_MASK {
                return Err(AcquireTransitionError::LeaseCountOverflow);
            }

            let next = current + 1;
            match self.word.compare_exchange_weak_acquire(current, next) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    pub(crate) fn release(&self) -> ReleaseTransition {
        let mut current = self.word.load_acquire();
        loop {
            let active_leases = current & ACTIVE_LEASE_MASK;
            if active_leases == 0 {
                return ReleaseTransition::Underflow;
            }

            let next = current - 1;
            match self.word.compare_exchange_weak_acq_rel_relaxed(current, next) {
                Ok(_) if active_leases == 1 && decode_state(current) == MappedFileAdmissionState::Closing => {
                    return ReleaseTransition::Drained;
                }
                Ok(_) => return ReleaseTransition::Remaining(active_leases - 1),
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    pub(crate) fn begin_close(&self) -> BeginCloseTransition {
        let mut current = self.word.load_acquire();
        loop {
            if decode_state(current) == MappedFileAdmissionState::Closing {
                return BeginCloseTransition::AlreadyClosing;
            }

            let next = (current & ACTIVE_LEASE_MASK) | encode_state(MappedFileAdmissionState::Closing);
            match self.word.compare_exchange_weak_acq_rel(current, next) {
                Ok(_) => return BeginCloseTransition::Started,
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    pub(crate) fn seal_readable(&self) -> bool {
        let mut current = self.word.load_acquire();
        loop {
            if decode_state(current) != MappedFileAdmissionState::ActiveWritable {
                return false;
            }

            let next = (current & ACTIVE_LEASE_MASK) | encode_state(MappedFileAdmissionState::SealedReadable);
            match self.word.compare_exchange_weak_acq_rel(current, next) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    pub(crate) fn logical_cleanup_marked(&self) -> bool {
        let snapshot = self.snapshot();
        snapshot.state == MappedFileAdmissionState::Closing && snapshot.active_leases == 0
    }
}

impl<A: LifecycleAtomicUsize> Default for LifecycleTransitionState<A> {
    fn default() -> Self {
        Self::new()
    }
}
