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
use std::time::Duration;
use std::time::Instant;

use parking_lot::Condvar;
use parking_lot::Mutex;

use super::lifecycle_model::AcquireTransitionError;
use super::lifecycle_model::BeginCloseTransition;
use super::lifecycle_model::LifecyclePackedSnapshot;
use super::lifecycle_model::LifecycleTransitionState;
pub use super::lifecycle_model::MappedFileAdmissionState;
pub use super::lifecycle_model::MappedFileOperation;
use super::lifecycle_model::ReleaseTransition;

#[derive(Debug, Clone, Copy)]
pub struct MappedFileLifecycleSnapshot {
    pub state: MappedFileAdmissionState,
    pub active_leases: usize,
    pub generation: u64,
    pub started_at: Option<Instant>,
    pub force_observed: bool,
    pub logical_cleanup_marked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleAcquireError {
    Unavailable {
        state: MappedFileAdmissionState,
        operation: MappedFileOperation,
    },
    LeaseCountOverflow,
}

struct LifecycleControl {
    started_at: Option<Instant>,
    generation: u64,
    force_observed: bool,
}

impl LifecycleControl {
    fn snapshot(&self, packed: LifecyclePackedSnapshot) -> MappedFileLifecycleSnapshot {
        MappedFileLifecycleSnapshot {
            state: packed.state,
            active_leases: packed.active_leases,
            generation: self.generation,
            started_at: self.started_at,
            force_observed: self.force_observed,
            logical_cleanup_marked: packed.state == MappedFileAdmissionState::Closing && packed.active_leases == 0,
        }
    }
}

pub(crate) struct SegmentLifecycle {
    transitions: LifecycleTransitionState,
    close_control: Mutex<LifecycleControl>,
    drained: Condvar,
}

impl SegmentLifecycle {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self {
            transitions: LifecycleTransitionState::new(),
            close_control: Mutex::new(LifecycleControl {
                started_at: None,
                generation: 0,
                force_observed: false,
            }),
            drained: Condvar::new(),
        })
    }

    #[inline]
    pub(crate) fn state(&self) -> MappedFileAdmissionState {
        self.transitions.state()
    }

    #[inline]
    pub(crate) fn is_available(&self) -> bool {
        self.state() != MappedFileAdmissionState::Closing
    }

    pub(crate) fn try_acquire(
        self: &Arc<Self>,
        operation: MappedFileOperation,
    ) -> Result<MappedFileLease, LifecycleAcquireError> {
        self.try_admit(operation)?;
        Ok(MappedFileLease {
            lifecycle: Arc::clone(self),
            operation,
            armed: true,
        })
    }

    #[inline]
    pub(crate) fn try_acquire_borrowed(
        &self,
        operation: MappedFileOperation,
    ) -> Result<BorrowedMappedFileLease<'_>, LifecycleAcquireError> {
        self.try_admit(operation)?;
        Ok(BorrowedMappedFileLease {
            lifecycle: self,
            operation,
            armed: true,
        })
    }

    #[inline]
    fn try_admit(&self, operation: MappedFileOperation) -> Result<(), LifecycleAcquireError> {
        self.transitions.try_acquire(operation).map_err(|error| match error {
            AcquireTransitionError::Unavailable(state) => LifecycleAcquireError::Unavailable { state, operation },
            AcquireTransitionError::LeaseCountOverflow => LifecycleAcquireError::LeaseCountOverflow,
        })
    }

    pub(crate) fn begin_close(&self, interval_forcibly: u64) -> MappedFileLifecycleSnapshot {
        let mut control = self.close_control.lock();
        let transition = if self.transitions.state() == MappedFileAdmissionState::Closing {
            BeginCloseTransition::AlreadyClosing
        } else {
            // The mutex protects this metadata and makes it visible before Closing is published by
            // the release half of the packed-word CAS in begin_close.
            control.started_at = Some(Instant::now());
            control.generation = control.generation.saturating_add(1).max(1);
            let transition = self.transitions.begin_close();
            debug_assert_eq!(transition, BeginCloseTransition::Started);
            transition
        };

        if transition == BeginCloseTransition::AlreadyClosing {
            let elapsed = control
                .started_at
                .map(|started_at| started_at.elapsed())
                .unwrap_or_default();
            if elapsed >= Duration::from_millis(interval_forcibly) {
                control.force_observed = true;
            }
        }

        let packed = self.transitions.snapshot();
        if packed.active_leases == 0 {
            self.drained.notify_all();
        }
        control.snapshot(packed)
    }

    #[inline]
    pub(crate) fn seal_readable(&self) -> bool {
        self.transitions.seal_readable()
    }

    pub(crate) fn snapshot(&self) -> MappedFileLifecycleSnapshot {
        let control = self.close_control.lock();
        // Load the packed word after taking the metadata lock so a snapshot cannot combine a stale
        // admission state with metadata from a newer close generation.
        let packed = self.transitions.snapshot();
        debug_assert!(
            packed.state != MappedFileAdmissionState::Closing || control.started_at.is_some(),
            "Closing must be published after its timestamp"
        );
        control.snapshot(packed)
    }

    pub(crate) fn wait_for_drain(&self, timeout: Duration) -> bool {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        let mut control = self.close_control.lock();
        while self.transitions.active_leases() != 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            if self.drained.wait_for(&mut control, deadline - now).timed_out() && self.transitions.active_leases() != 0
            {
                return false;
            }
        }
        true
    }

    #[inline]
    pub(crate) fn compatibility_ref_count(&self) -> i64 {
        let packed = self.transitions.snapshot();
        let active = i64::try_from(packed.active_leases).unwrap_or(i64::MAX);
        if packed.state != MappedFileAdmissionState::Closing {
            active.saturating_add(1)
        } else {
            active
        }
    }

    #[inline]
    pub(crate) fn logical_cleanup_marked(&self) -> bool {
        self.transitions.logical_cleanup_marked()
    }

    #[inline]
    fn release_one(&self) -> ReleaseTransition {
        let transition = self.transitions.release();
        if matches!(transition, ReleaseTransition::Drained | ReleaseTransition::Remaining(0)) {
            // Pair the final release with wait_for_drain's predicate check under the same mutex so
            // the condvar notification cannot be lost between checking and sleeping.
            let _control = self.close_control.lock();
            self.drained.notify_all();
        }
        transition
    }
}

mod lease_proof_private {
    pub trait Sealed {}
}

/// Unforgeable proof that an operation was admitted by one mapped-file lifecycle.
///
/// The trait is crate-private and sealed; safe callers cannot manufacture an implementation or a
/// lease without incrementing the canonical packed counter.
pub(crate) trait MappedFileLeaseProof: lease_proof_private::Sealed {
    fn lifecycle(&self) -> &SegmentLifecycle;

    fn operation(&self) -> MappedFileOperation;
}

/// Owned admission token for one mapped-file operation.
///
/// The token is intentionally non-cloneable. Dropping it releases exactly one active admission.
#[doc(hidden)]
#[must_use = "dropping the lease immediately releases mapped-file admission"]
pub struct MappedFileLease {
    lifecycle: Arc<SegmentLifecycle>,
    operation: MappedFileOperation,
    armed: bool,
}

impl lease_proof_private::Sealed for MappedFileLease {}

impl MappedFileLeaseProof for MappedFileLease {
    #[inline]
    fn lifecycle(&self) -> &SegmentLifecycle {
        &self.lifecycle
    }

    #[inline]
    fn operation(&self) -> MappedFileOperation {
        self.operation
    }
}

impl Drop for MappedFileLease {
    fn drop(&mut self) {
        release_armed(&self.lifecycle, &mut self.armed);
    }
}

/// Borrowed admission token for one synchronous mapped-file operation.
///
/// Unlike [`MappedFileLease`], this token borrows the lifecycle and therefore does not clone its
/// [`Arc`]. It remains non-cloneable and releases exactly one admission on drop.
#[must_use = "dropping the lease immediately releases mapped-file admission"]
pub(crate) struct BorrowedMappedFileLease<'a> {
    lifecycle: &'a SegmentLifecycle,
    operation: MappedFileOperation,
    armed: bool,
}

impl lease_proof_private::Sealed for BorrowedMappedFileLease<'_> {}

impl MappedFileLeaseProof for BorrowedMappedFileLease<'_> {
    #[inline]
    fn lifecycle(&self) -> &SegmentLifecycle {
        self.lifecycle
    }

    #[inline]
    fn operation(&self) -> MappedFileOperation {
        self.operation
    }
}

impl Drop for BorrowedMappedFileLease<'_> {
    fn drop(&mut self) {
        release_armed(self.lifecycle, &mut self.armed);
    }
}

#[inline]
fn release_armed(lifecycle: &SegmentLifecycle, armed: &mut bool) {
    if !*armed {
        return;
    }
    let transition = lifecycle.release_one();
    debug_assert!(!matches!(transition, ReleaseTransition::Underflow));
    *armed = false;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_and_owned_leases_share_one_active_counter() {
        let lifecycle = SegmentLifecycle::shared();
        let owned = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect("owned read lease");
        let borrowed = lifecycle
            .try_acquire_borrowed(MappedFileOperation::Maintenance)
            .expect("borrowed maintenance lease");

        assert_eq!(lifecycle.snapshot().active_leases, 2);
        drop(borrowed);
        assert_eq!(lifecycle.snapshot().active_leases, 1);
        drop(owned);
        assert_eq!(lifecycle.snapshot().active_leases, 0);
    }

    #[test]
    fn borrowed_lease_does_not_clone_lifecycle_owner() {
        let lifecycle = SegmentLifecycle::shared();
        assert_eq!(Arc::strong_count(&lifecycle), 1);

        let borrowed = lifecycle
            .try_acquire_borrowed(MappedFileOperation::Read)
            .expect("borrowed read lease");
        assert_eq!(Arc::strong_count(&lifecycle), 1);
        drop(borrowed);

        let owned = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect("owned read lease");
        assert_eq!(Arc::strong_count(&lifecycle), 2);
        drop(owned);
        assert_eq!(Arc::strong_count(&lifecycle), 1);
    }

    #[test]
    fn borrowed_last_drop_drains_close_exactly_once() {
        let lifecycle = SegmentLifecycle::shared();
        let borrowed = lifecycle
            .try_acquire_borrowed(MappedFileOperation::Write)
            .expect("borrowed write lease");

        let closing = lifecycle.begin_close(u64::MAX);
        assert_eq!(closing.state, MappedFileAdmissionState::Closing);
        assert_eq!(closing.active_leases, 1);
        assert!(!closing.logical_cleanup_marked);
        assert!(matches!(
            lifecycle.try_acquire_borrowed(MappedFileOperation::Read),
            Err(LifecycleAcquireError::Unavailable {
                state: MappedFileAdmissionState::Closing,
                operation: MappedFileOperation::Read,
            })
        ));

        drop(borrowed);
        let drained = lifecycle.snapshot();
        assert_eq!(drained.active_leases, 0);
        assert!(drained.logical_cleanup_marked);
        assert!(lifecycle.wait_for_drain(Duration::ZERO));
    }

    #[test]
    fn borrowed_lease_releases_once_during_unwind() {
        let lifecycle = SegmentLifecycle::shared();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _borrowed = lifecycle
                .try_acquire_borrowed(MappedFileOperation::Read)
                .expect("borrowed read lease");
            panic!("exercise borrowed lease unwind");
        }));

        assert!(result.is_err());
        assert_eq!(lifecycle.snapshot().active_leases, 0);
    }
}
