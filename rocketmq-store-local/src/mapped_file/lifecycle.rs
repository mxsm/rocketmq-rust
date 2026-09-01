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
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Condvar;
use parking_lot::Mutex;

use super::lifecycle_model::AcquireTransitionOutcome;
use super::lifecycle_model::AcquireTransitionRejection;
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

/// Caller-owned acquisition outcome carrying the admitted value or rejection data.
#[derive(Debug)]
#[must_use]
pub(crate) enum LifecycleAcquireOutcome<T> {
    Acquired(T),
    Rejected(LifecycleAcquireRejection),
}

impl<T> LifecycleAcquireOutcome<T> {
    /// Returns the acquired value, discarding rejection data.
    pub(crate) fn acquired(self) -> Option<T> {
        match self {
            Self::Acquired(value) => Some(value),
            Self::Rejected(_) => None,
        }
    }

    #[cfg(test)]
    #[track_caller]
    pub(crate) fn expect_acquired(self, context: &str) -> T {
        match self {
            Self::Acquired(value) => value,
            Self::Rejected(rejection) => panic!("expected acquisition ({context}), got {rejection:?}"),
        }
    }
}

/// Source-free semantic rejection data for a refused lifecycle acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LifecycleAcquireRejection {
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
    physical_detach: PhysicalDetachState,
}

struct DrainWaiterPresence<'a> {
    waiters: &'a AtomicUsize,
}

impl<'a> DrainWaiterPresence<'a> {
    fn register(waiters: &'a AtomicUsize) -> Self {
        waiters.fetch_add(1, Ordering::Release);
        Self { waiters }
    }
}

impl Drop for DrainWaiterPresence<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::Release);
    }
}

/// Owner-only callback invoked after Closing drains every admitted operation.
///
/// Implementations must not retain or call back into the lifecycle. Keeping this boundary
/// acyclic lets a final lease drop detach physical slots without extending the mapped-file
/// object's lifetime.
pub(crate) trait PhysicalDetachHook: Send + Sync {
    fn detach_owner_slots(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhysicalDetachState {
    Attached,
    Detaching,
    Detached,
}

/// Result of attempting to claim the one physical-owner detach transition.
pub(crate) enum PhysicalDetachClaimResult<'a> {
    /// This caller owns the detach transition until the claim is completed or dropped.
    Claimed(PhysicalDetachClaim<'a>),
    /// Another caller currently owns the detach transition.
    InProgress,
    /// The mapping and file-owner slots were already detached.
    AlreadyDetached,
    /// Closing has not drained every admitted operation yet.
    Pending {
        state: MappedFileAdmissionState,
        active_leases: usize,
    },
}

/// Unwind-safe exactly-once claim for detaching physical owner slots.
pub(crate) struct PhysicalDetachClaim<'a> {
    lifecycle: &'a SegmentLifecycle,
    armed: bool,
}

impl PhysicalDetachClaim<'_> {
    /// Commits the detach transition after both physical owner slots have been taken.
    pub(crate) fn complete(mut self) {
        let mut control = self.lifecycle.close_control.lock();
        debug_assert_eq!(control.physical_detach, PhysicalDetachState::Detaching);
        control.physical_detach = PhysicalDetachState::Detached;
        self.armed = false;
    }
}

impl Drop for PhysicalDetachClaim<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut control = self.lifecycle.close_control.lock();
        if control.physical_detach == PhysicalDetachState::Detaching {
            control.physical_detach = PhysicalDetachState::Attached;
        }
    }
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
    drain_waiters: AtomicUsize,
    drained: Condvar,
    seal_wait_control: Mutex<()>,
    seal_waiters: AtomicUsize,
    writers_drained: Condvar,
    physical_detach_hook: OnceLock<Arc<dyn PhysicalDetachHook>>,
}

impl SegmentLifecycle {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self {
            transitions: LifecycleTransitionState::new(),
            close_control: Mutex::new(LifecycleControl {
                started_at: None,
                generation: 0,
                force_observed: false,
                physical_detach: PhysicalDetachState::Attached,
            }),
            drain_waiters: AtomicUsize::new(0),
            drained: Condvar::new(),
            seal_wait_control: Mutex::new(()),
            seal_waiters: AtomicUsize::new(0),
            writers_drained: Condvar::new(),
            physical_detach_hook: OnceLock::new(),
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
    ) -> LifecycleAcquireOutcome<MappedFileLease> {
        match self.try_admit(operation) {
            Ok(()) => LifecycleAcquireOutcome::Acquired(MappedFileLease {
                lifecycle: Arc::clone(self),
                operation,
                armed: true,
            }),
            Err(rejection) => LifecycleAcquireOutcome::Rejected(rejection),
        }
    }

    #[inline]
    pub(crate) fn try_acquire_borrowed(
        &self,
        operation: MappedFileOperation,
    ) -> LifecycleAcquireOutcome<BorrowedMappedFileLease<'_>> {
        match self.try_admit(operation) {
            Ok(()) => LifecycleAcquireOutcome::Acquired(BorrowedMappedFileLease {
                lifecycle: self,
                operation,
                armed: true,
            }),
            Err(rejection) => LifecycleAcquireOutcome::Rejected(rejection),
        }
    }

    #[inline]
    fn try_admit(&self, operation: MappedFileOperation) -> Result<(), LifecycleAcquireRejection> {
        match self.transitions.try_acquire(operation) {
            AcquireTransitionOutcome::Acquired => Ok(()),
            AcquireTransitionOutcome::Rejected(AcquireTransitionRejection::Unavailable(state)) => {
                Err(LifecycleAcquireRejection::Unavailable { state, operation })
            }
            AcquireTransitionOutcome::Rejected(AcquireTransitionRejection::LeaseCountOverflow) => {
                Err(LifecycleAcquireRejection::LeaseCountOverflow)
            }
        }
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
        let snapshot = control.snapshot(packed);
        drop(control);
        if packed.active_leases == 0 {
            self.try_trigger_physical_detach();
        }
        snapshot
    }

    /// Rejects new writers and waits until every writer admitted before the seal has completed.
    ///
    /// The seal CAS changes the state while preserving both packed counters. A writer CAS is
    /// therefore ordered wholly before the seal and counted, or wholly after it and rejected.
    pub(crate) fn seal_readable_and_wait_for_writers(&self) -> LifecycleAcquireOutcome<bool> {
        // Publish waiter presence before the seal CAS. A final writer that observes the sealed
        // packed word acquires this publication through that CAS, so it cannot miss the waiter.
        self.seal_waiters.fetch_add(1, Ordering::Release);
        let started = loop {
            match self.transitions.state() {
                MappedFileAdmissionState::ActiveWritable if self.transitions.seal_readable() => break true,
                MappedFileAdmissionState::ActiveWritable => continue,
                MappedFileAdmissionState::SealedReadable => break false,
                MappedFileAdmissionState::Closing => {
                    self.seal_waiters.fetch_sub(1, Ordering::Release);
                    return LifecycleAcquireOutcome::Rejected(LifecycleAcquireRejection::Unavailable {
                        state: MappedFileAdmissionState::Closing,
                        operation: MappedFileOperation::Maintenance,
                    });
                }
            }
        };
        if self.transitions.active_writers() == 0 {
            self.seal_waiters.fetch_sub(1, Ordering::Release);
            return LifecycleAcquireOutcome::Acquired(started);
        }

        let mut control = self.seal_wait_control.lock();
        if self.transitions.active_writers() == 0 {
            self.seal_waiters.fetch_sub(1, Ordering::Release);
            return LifecycleAcquireOutcome::Acquired(started);
        }
        while self.transitions.active_writers() != 0 {
            self.writers_drained.wait(&mut control);
        }
        self.seal_waiters.fetch_sub(1, Ordering::Release);
        LifecycleAcquireOutcome::Acquired(started)
    }

    /// Installs the acyclic physical-owner detach callback before the mapped file is published.
    pub(crate) fn install_physical_detach_hook(&self, hook: Arc<dyn PhysicalDetachHook>) -> bool {
        if self.physical_detach_hook.set(hook).is_err() {
            return false;
        }
        if self.logical_cleanup_marked() {
            self.try_trigger_physical_detach();
        }
        true
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
        // Register before entering the packed-word modification order. If the identity RMW wins,
        // the final release acquires this presence publication before deciding whether to notify;
        // if the final release wins, the identity RMW observes zero and this caller never sleeps.
        let _presence = DrainWaiterPresence::register(&self.drain_waiters);
        if self.transitions.active_leases_after_drain_waiter_registration() == 0 {
            return true;
        }
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

    /// Publishes one already-prepared owner candidate while holding the same cold lock as Closing.
    ///
    /// Candidate construction and filesystem I/O must happen before this call. The closure should
    /// only publish a fully built owner into its slot, keeping the critical section short. A lease
    /// admitted before close still protects any owner it captured before close; a lazy candidate
    /// that was not yet published loses the race when Closing is already visible and is dropped.
    pub(crate) fn try_publish_before_close<L, R>(
        &self,
        lease: &L,
        operation: MappedFileOperation,
        publish: impl FnOnce() -> R,
    ) -> LifecycleAcquireOutcome<R>
    where
        L: MappedFileLeaseProof + ?Sized,
    {
        let _control = self.close_control.lock();
        let state = self.transitions.state();
        if !std::ptr::eq(lease.lifecycle(), self) || lease.operation() != operation || !state.allows(operation) {
            return LifecycleAcquireOutcome::Rejected(LifecycleAcquireRejection::Unavailable { state, operation });
        }
        LifecycleAcquireOutcome::Acquired(publish())
    }

    /// Claims the unique physical-owner detach transition after Closing has drained.
    ///
    /// Dropping a claim before [`PhysicalDetachClaim::complete`] restores the attached state so a
    /// later attempt can finish any owner slot that was not taken before an unwind or early return.
    pub(crate) fn try_claim_physical_detach(&self) -> PhysicalDetachClaimResult<'_> {
        let mut control = self.close_control.lock();
        let packed = self.transitions.snapshot();
        if packed.state != MappedFileAdmissionState::Closing || packed.active_leases != 0 {
            return PhysicalDetachClaimResult::Pending {
                state: packed.state,
                active_leases: packed.active_leases,
            };
        }

        match control.physical_detach {
            PhysicalDetachState::Attached => {
                control.physical_detach = PhysicalDetachState::Detaching;
                PhysicalDetachClaimResult::Claimed(PhysicalDetachClaim {
                    lifecycle: self,
                    armed: true,
                })
            }
            PhysicalDetachState::Detaching => PhysicalDetachClaimResult::InProgress,
            PhysicalDetachState::Detached => PhysicalDetachClaimResult::AlreadyDetached,
        }
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
    fn release_one(&self, operation: MappedFileOperation) -> ReleaseTransition {
        let outcome = self.transitions.release(operation);
        if outcome.writers_drained_after_rejection() && self.seal_waiters.load(Ordering::Acquire) != 0 {
            let _control = self.seal_wait_control.lock();
            self.writers_drained.notify_all();
        }
        let transition = outcome.transition();
        self.finish_release_transition(transition);
        transition
    }

    fn finish_release_transition(&self, transition: ReleaseTransition) {
        let final_total = matches!(transition, ReleaseTransition::Drained | ReleaseTransition::Remaining(0));
        let notify_drain_waiters = final_total && self.drain_waiters.load(Ordering::Acquire) != 0;
        if notify_drain_waiters {
            // Pair the final release with wait_for_drain's predicate check under the same mutex so
            // the condvar notification cannot be lost between checking and sleeping.
            let _control = self.close_control.lock();
            self.drained.notify_all();
        }
        if transition == ReleaseTransition::Drained {
            self.try_trigger_physical_detach();
        }
    }

    fn try_trigger_physical_detach(&self) {
        let Some(hook) = self.physical_detach_hook.get().cloned() else {
            return;
        };
        let PhysicalDetachClaimResult::Claimed(claim) = self.try_claim_physical_detach() else {
            return;
        };

        let detached = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook.detach_owner_slots()));
        match detached {
            Ok(()) => claim.complete(),
            Err(_) => {
                tracing::error!("mapped-file physical detach hook panicked; detach remains retryable");
            }
        }
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
        release_armed(&self.lifecycle, self.operation, &mut self.armed);
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
        release_armed(self.lifecycle, self.operation, &mut self.armed);
    }
}

#[inline]
fn release_armed(lifecycle: &SegmentLifecycle, operation: MappedFileOperation, armed: &mut bool) {
    if !*armed {
        return;
    }
    let transition = lifecycle.release_one(operation);
    debug_assert!(!matches!(transition, ReleaseTransition::Underflow));
    *armed = false;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    struct CountingDetachHook {
        calls: AtomicUsize,
        panic_once: AtomicBool,
    }

    impl CountingDetachHook {
        fn new(panic_once: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                panic_once: AtomicBool::new(panic_once),
            }
        }
    }

    impl PhysicalDetachHook for CountingDetachHook {
        fn detach_owner_slots(&self) {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.panic_once.swap(false, Ordering::AcqRel) {
                panic!("injected detach failure");
            }
        }
    }

    #[test]
    fn borrowed_and_owned_leases_share_one_active_counter() {
        let lifecycle = SegmentLifecycle::shared();
        let owned = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect_acquired("owned read lease");
        let borrowed = lifecycle
            .try_acquire_borrowed(MappedFileOperation::Maintenance)
            .expect_acquired("borrowed maintenance lease");

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
            .expect_acquired("borrowed read lease");
        assert_eq!(Arc::strong_count(&lifecycle), 1);
        drop(borrowed);

        let owned = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect_acquired("owned read lease");
        assert_eq!(Arc::strong_count(&lifecycle), 2);
        drop(owned);
        assert_eq!(Arc::strong_count(&lifecycle), 1);
    }

    #[test]
    fn borrowed_last_drop_drains_close_exactly_once() {
        let lifecycle = SegmentLifecycle::shared();
        let borrowed = lifecycle
            .try_acquire_borrowed(MappedFileOperation::Write)
            .expect_acquired("borrowed write lease");

        let closing = lifecycle.begin_close(u64::MAX);
        assert_eq!(closing.state, MappedFileAdmissionState::Closing);
        assert_eq!(closing.active_leases, 1);
        assert!(!closing.logical_cleanup_marked);
        assert!(matches!(
            lifecycle.try_acquire_borrowed(MappedFileOperation::Read),
            LifecycleAcquireOutcome::Rejected(LifecycleAcquireRejection::Unavailable {
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
                .expect_acquired("borrowed read lease");
            panic!("exercise borrowed lease unwind");
        }));

        assert!(result.is_err());
        assert_eq!(lifecycle.snapshot().active_leases, 0);
    }

    #[test]
    fn seal_waits_for_packed_writer_and_rejects_late_writer() {
        let lifecycle = SegmentLifecycle::shared();
        let writer = lifecycle
            .try_acquire(MappedFileOperation::Write)
            .expect_acquired("registered writer");
        let sealer = {
            let lifecycle = Arc::clone(&lifecycle);
            std::thread::spawn(move || lifecycle.seal_readable_and_wait_for_writers())
        };

        let deadline = Instant::now() + Duration::from_secs(1);
        while lifecycle.state() == MappedFileAdmissionState::ActiveWritable {
            assert!(Instant::now() < deadline, "sealer must publish write rejection");
            std::thread::yield_now();
        }
        assert!(matches!(
            lifecycle.try_acquire(MappedFileOperation::Write),
            LifecycleAcquireOutcome::Rejected(LifecycleAcquireRejection::Unavailable {
                state: MappedFileAdmissionState::SealedReadable,
                operation: MappedFileOperation::Write,
            })
        ));
        drop(writer);

        assert!(matches!(
            sealer.join().expect("sealer does not panic"),
            LifecycleAcquireOutcome::Acquired(true)
        ));
        assert_eq!(lifecycle.snapshot().active_leases, 0);
    }

    #[test]
    fn final_release_wakes_all_registered_non_closing_drain_waiters() {
        for seal_before_wait in [false, true] {
            let lifecycle = SegmentLifecycle::shared();
            if seal_before_wait {
                assert!(matches!(
                    lifecycle.seal_readable_and_wait_for_writers(),
                    LifecycleAcquireOutcome::Acquired(true)
                ));
            }
            let lease = lifecycle
                .try_acquire(MappedFileOperation::Read)
                .expect_acquired("read lease");
            let waiters = (0..2)
                .map(|_| {
                    let lifecycle = Arc::clone(&lifecycle);
                    std::thread::spawn(move || lifecycle.wait_for_drain(Duration::from_secs(5)))
                })
                .collect::<Vec<_>>();

            let deadline = Instant::now() + Duration::from_secs(1);
            while lifecycle.drain_waiters.load(Ordering::Acquire) != 2 {
                assert!(Instant::now() < deadline, "drain waiters must publish their presence");
                std::thread::yield_now();
            }
            drop(lease);

            for waiter in waiters {
                assert!(waiter.join().expect("drain waiter does not panic"));
            }
            assert_eq!(lifecycle.drain_waiters.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    fn timed_out_drain_wait_unregisters_presence() {
        let lifecycle = SegmentLifecycle::shared();
        let lease = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect_acquired("read lease");

        assert!(!lifecycle.wait_for_drain(Duration::ZERO));
        assert_eq!(lifecycle.drain_waiters.load(Ordering::Acquire), 0);
        drop(lease);
    }

    #[test]
    fn final_typed_lease_drop_triggers_physical_detach_once() {
        let lifecycle = SegmentLifecycle::shared();
        let hook = Arc::new(CountingDetachHook::new(false));
        assert!(lifecycle.install_physical_detach_hook(hook.clone()));
        let lease = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect_acquired("read lease");

        lifecycle.begin_close(u64::MAX);
        assert_eq!(hook.calls.load(Ordering::Acquire), 0);
        drop(lease);

        assert_eq!(hook.calls.load(Ordering::Acquire), 1);
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::AlreadyDetached
        ));
        lifecycle.begin_close(0);
        assert_eq!(hook.calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn panicking_physical_detach_hook_is_caught_and_retryable() {
        let lifecycle = SegmentLifecycle::shared();
        let hook = Arc::new(CountingDetachHook::new(true));
        assert!(lifecycle.install_physical_detach_hook(hook.clone()));

        lifecycle.begin_close(u64::MAX);
        assert_eq!(hook.calls.load(Ordering::Acquire), 1);
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::Claimed(_)
        ));

        lifecycle.begin_close(0);
        assert_eq!(hook.calls.load(Ordering::Acquire), 2);
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::AlreadyDetached
        ));
    }

    #[test]
    fn physical_detach_waits_for_closing_and_every_active_lease() {
        let lifecycle = SegmentLifecycle::shared();
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::Pending {
                state: MappedFileAdmissionState::ActiveWritable,
                active_leases: 0,
            }
        ));

        let lease = lifecycle
            .try_acquire(MappedFileOperation::Read)
            .expect_acquired("read lease");
        lifecycle.begin_close(u64::MAX);
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::Pending {
                state: MappedFileAdmissionState::Closing,
                active_leases: 1,
            }
        ));

        drop(lease);
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::Claimed(_)
        ));
    }

    #[test]
    fn physical_detach_claim_is_retryable_on_drop_and_exactly_once_after_commit() {
        let lifecycle = SegmentLifecycle::shared();
        lifecycle.begin_close(u64::MAX);

        let first = match lifecycle.try_claim_physical_detach() {
            PhysicalDetachClaimResult::Claimed(claim) => claim,
            _ => panic!("first drained close must claim detach"),
        };
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::InProgress
        ));
        drop(first);

        let retry = match lifecycle.try_claim_physical_detach() {
            PhysicalDetachClaimResult::Claimed(claim) => claim,
            _ => panic!("dropped detach claim must be retryable"),
        };
        retry.complete();
        assert!(matches!(
            lifecycle.try_claim_physical_detach(),
            PhysicalDetachClaimResult::AlreadyDetached
        ));
    }

    #[test]
    fn publication_and_close_share_one_linearization_lock() {
        let lifecycle = SegmentLifecycle::shared();
        let first = lifecycle
            .try_acquire_borrowed(MappedFileOperation::Read)
            .expect_acquired("publication lease");
        assert_eq!(
            lifecycle
                .try_publish_before_close(&first, MappedFileOperation::Read, || 7)
                .expect_acquired("publication before close"),
            7
        );
        drop(first);

        let losing_candidate = lifecycle
            .try_acquire_borrowed(MappedFileOperation::Read)
            .expect_acquired("pre-close lazy candidate lease");
        lifecycle.begin_close(u64::MAX);
        assert!(matches!(
            lifecycle.try_publish_before_close(&losing_candidate, MappedFileOperation::Read, || 9),
            LifecycleAcquireOutcome::Rejected(LifecycleAcquireRejection::Unavailable {
                state: MappedFileAdmissionState::Closing,
                operation: MappedFileOperation::Read,
            })
        ));
    }
}
