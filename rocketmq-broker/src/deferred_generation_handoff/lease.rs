// Copyright 2026 The RocketMQ Rust Authors
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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

use super::state::DeferredGenerationHandoffState;
use super::state::DeferredGenerationTargetState;
use super::state::LegacyEnrollmentCheckError;
use super::state::LegacyWakeBeginFailure;
use super::DeferredGeneration;
use super::DeferredGenerationHandoff;
use super::DeferredGenerationTarget;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredGenerationSeal {
    Sealed,
    AlreadySealed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredGenerationRouteError {
    ShutdownSealed,
}

pub(crate) struct DeferredGenerationArrivalAdapter<'a> {
    pub(super) handoff: &'a DeferredGenerationHandoff,
}

impl DeferredGenerationArrivalAdapter<'_> {
    /// Installs the affine wait lease into the exact V1 table node while the
    /// handoff write gate is held. An installer that rejects the lease must
    /// return it so accounting can be rolled back without running `Drop`
    /// recursively under the write gate.
    pub(crate) fn install_legacy_wait<T, E>(
        &self,
        target: DeferredGenerationTarget,
        install: impl FnOnce(LegacyWaitLease) -> Result<T, (E, LegacyWaitLease)>,
        rollback_table: impl FnOnce(),
    ) -> Result<T, DeferredGenerationLegacyEnrollmentError<E>> {
        let outcome = self.handoff.with_legacy_table_transaction(|state, write_gate| {
            if let Err(error) = state.check_legacy_enrollment(&target) {
                return LegacyEnrollmentTransactionOutcome::Error(DeferredGenerationLegacyEnrollmentError::from_check(
                    error,
                ));
            }
            state.record_legacy_wait(&target);
            let armed = Arc::new(AtomicBool::new(false));
            let lease = LegacyWaitLease {
                write_gate: Arc::clone(write_gate),
                target: target.clone(),
                armed: Arc::clone(&armed),
            };
            let mut rollback = LegacyWaitEnrollmentRollback {
                state,
                target: &target,
                armed,
                committed: false,
            };
            match std::panic::catch_unwind(AssertUnwindSafe(|| install(lease))) {
                Ok(Ok(value)) => {
                    rollback.commit();
                    LegacyEnrollmentTransactionOutcome::Value(value)
                }
                Ok(Err((error, lease))) => {
                    drop(rollback);
                    drop(lease);
                    LegacyEnrollmentTransactionOutcome::Error(DeferredGenerationLegacyEnrollmentError::Enrollment(
                        error,
                    ))
                }
                Err(payload) => {
                    drop(rollback);
                    let _ = std::panic::catch_unwind(AssertUnwindSafe(rollback_table));
                    LegacyEnrollmentTransactionOutcome::Panicked(payload)
                }
            }
        });
        match outcome {
            LegacyEnrollmentTransactionOutcome::Value(value) => Ok(value),
            LegacyEnrollmentTransactionOutcome::Error(error) => Err(error),
            LegacyEnrollmentTransactionOutcome::Panicked(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Performs V1 table insertion while the handoff write gate is held, then
    /// starts exact legacy-wait accounting only on success.
    pub(crate) fn enroll_legacy_wait<T, E>(
        &self,
        target: DeferredGenerationTarget,
        enroll: impl FnOnce() -> Result<T, E>,
    ) -> Result<(T, LegacyWaitLease), DeferredGenerationLegacyEnrollmentError<E>> {
        self.install_legacy_wait(
            target,
            |lease| match enroll() {
                Ok(value) => Ok((value, lease)),
                Err(error) => Err((error, lease)),
            },
            || {},
        )
    }

    pub(crate) fn acquire_route(
        &self,
        target: DeferredGenerationTarget,
    ) -> Result<RoutePermit, DeferredGenerationRouteError> {
        self.handoff.acquire_route(target)
    }

    /// Removes legacy table entries and creates one affine route permit per
    /// removed entry under the coordinator -> table lock order.
    ///
    /// `claim` is not called after shutdown or once the target is New, so a V1
    /// table cannot consume an event owned by the new generation. Returned
    /// permits become visible only after the coordinator gate has been
    /// released.
    pub(crate) fn claim_legacy_table<T>(
        &self,
        target: DeferredGenerationTarget,
        claim: impl FnOnce(&mut Vec<T>),
        rollback_table: impl FnOnce(Vec<T>),
    ) -> Result<Vec<(T, RoutePermit)>, DeferredGenerationRouteError> {
        let outcome = self.handoff.with_legacy_table_transaction(|state, write_gate| {
            if state.shutdown_sealed {
                return Err(DeferredGenerationRouteError::ShutdownSealed);
            }
            if state.generation_for(&target) != DeferredGeneration::Legacy {
                return Ok(LegacyClaimTransactionOutcome::Value(Vec::new()));
            }

            let mut entries = Vec::new();
            if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| claim(&mut entries))) {
                rollback_table(entries);
                return Ok(LegacyClaimTransactionOutcome::Panicked(payload));
            }
            let mut claimed = Vec::with_capacity(entries.len());
            for entry in entries {
                let generation = state.generation_for(&target);
                debug_assert_eq!(generation, DeferredGeneration::Legacy);
                let route = RoutePermit::provisional(Arc::clone(write_gate), target.clone(), generation);
                claimed.push((entry, route));
            }
            let candidate_count = claimed.len();
            let target_state = state
                .targets
                .entry(target.clone())
                .or_insert_with(|| DeferredGenerationTargetState::new(DeferredGeneration::Legacy));
            target_state.candidates = target_state
                .candidates
                .checked_add(candidate_count)
                .expect("affine route candidate count must not overflow");
            for (_, route) in &claimed {
                route.arm();
            }
            Ok(LegacyClaimTransactionOutcome::Value(claimed))
        })?;
        match outcome {
            LegacyClaimTransactionOutcome::Value(claimed) => Ok(claimed),
            LegacyClaimTransactionOutcome::Panicked(payload) => std::panic::resume_unwind(payload),
        }
    }

    /// Executes the fixed Pull → Pop → Notification → Lite producer order.
    pub(crate) fn route_arrival<P, O, N, L>(&self, pull: P, pop: O, notification: N, pop_lite: L)
    where
        P: FnOnce(&DeferredGenerationHandoff),
        O: FnOnce(&DeferredGenerationHandoff),
        N: FnOnce(&DeferredGenerationHandoff),
        L: FnOnce(&DeferredGenerationHandoff),
    {
        pull(self.handoff);
        pop(self.handoff);
        notification(self.handoff);
        pop_lite(self.handoff);
    }
}

enum LegacyEnrollmentTransactionOutcome<T, E> {
    Value(T),
    Error(DeferredGenerationLegacyEnrollmentError<E>),
    Panicked(Box<dyn std::any::Any + Send>),
}

enum LegacyClaimTransactionOutcome<T> {
    Value(Vec<(T, RoutePermit)>),
    Panicked(Box<dyn std::any::Any + Send>),
}

struct LegacyWaitEnrollmentRollback<'a> {
    state: &'a mut DeferredGenerationHandoffState,
    target: &'a DeferredGenerationTarget,
    armed: Arc<AtomicBool>,
    committed: bool,
}

impl LegacyWaitEnrollmentRollback<'_> {
    fn commit(&mut self) {
        self.armed.store(true, Ordering::Release);
        self.committed = true;
    }
}

impl Drop for LegacyWaitEnrollmentRollback<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.armed.store(false, Ordering::Release);
            self.state.release_legacy_wait(self.target);
        }
    }
}

#[derive(Debug)]
pub(crate) enum DeferredGenerationLegacyEnrollmentError<E> {
    ShutdownSealed,
    LegacyAcceptanceSealed,
    TargetAlreadyNew,
    Enrollment(E),
}

impl<E> DeferredGenerationLegacyEnrollmentError<E> {
    fn from_check(error: LegacyEnrollmentCheckError) -> Self {
        match error {
            LegacyEnrollmentCheckError::ShutdownSealed => Self::ShutdownSealed,
            LegacyEnrollmentCheckError::LegacyAcceptanceSealed => Self::LegacyAcceptanceSealed,
            LegacyEnrollmentCheckError::TargetAlreadyNew => Self::TargetAlreadyNew,
        }
    }
}

/// A route permit is affine candidate accounting, not legacy waiter occupancy.
#[derive(Debug)]
#[must_use]
pub(crate) struct RoutePermit {
    pub(super) write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
    pub(super) target: DeferredGenerationTarget,
    pub(super) generation: DeferredGeneration,
    pub(super) armed: Arc<AtomicBool>,
}

impl RoutePermit {
    pub(super) fn committed(
        write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
        target: DeferredGenerationTarget,
        generation: DeferredGeneration,
    ) -> Self {
        Self {
            write_gate,
            target,
            generation,
            armed: Arc::new(AtomicBool::new(true)),
        }
    }

    fn provisional(
        write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
        target: DeferredGenerationTarget,
        generation: DeferredGeneration,
    ) -> Self {
        Self {
            write_gate,
            target,
            generation,
            armed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    #[must_use]
    pub(crate) const fn generation(&self) -> DeferredGeneration {
        self.generation
    }

    #[must_use]
    pub(crate) fn target(&self) -> &DeferredGenerationTarget {
        &self.target
    }
}

impl Drop for RoutePermit {
    fn drop(&mut self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.write_gate.lock().release_candidate(&self.target);
        }
    }
}

/// Held by the exact V1 table node until removal or conversion to a wake.
#[derive(Debug)]
#[must_use]
pub(crate) struct LegacyWaitLease {
    write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
    target: DeferredGenerationTarget,
    armed: Arc<AtomicBool>,
}

impl LegacyWaitLease {
    #[must_use]
    pub(crate) fn target(&self) -> &DeferredGenerationTarget {
        &self.target
    }

    /// Performs waiter--, candidate--, and active-wake++ atomically.
    pub(crate) fn begin_wake(self, route: RoutePermit) -> Result<LegacyWakeLease, LegacyWakeBeginError> {
        if !Arc::ptr_eq(&self.write_gate, &route.write_gate) {
            return Err(LegacyWakeBeginError::DifferentHandoff { wait: self, route });
        }
        if self.target != route.target {
            return Err(LegacyWakeBeginError::TargetMismatch { wait: self, route });
        }
        if route.generation != DeferredGeneration::Legacy {
            return Err(LegacyWakeBeginError::NotLegacy { wait: self, route });
        }
        let result = self.write_gate.lock().begin_legacy_wake(&self.target);
        if let Err(reason) = result {
            return Err(match reason {
                LegacyWakeBeginFailure::NotReady => LegacyWakeBeginError::NotReady { wait: self, route },
                LegacyWakeBeginFailure::PopLiteSingleFlight => {
                    LegacyWakeBeginError::PopLiteSingleFlight { wait: self, route }
                }
            });
        }
        self.armed.store(false, Ordering::Release);
        route.armed.store(false, Ordering::Release);
        Ok(LegacyWakeLease {
            write_gate: Arc::clone(&self.write_gate),
            target: self.target.clone(),
            armed: true,
        })
    }
}

/// The lease slot embedded in one exact legacy request node.
///
/// Pull request clones share this cell, so clearing the old vector and
/// requeueing a clone cannot duplicate occupancy or lose the affine lease.
#[derive(Debug)]
pub(crate) struct LegacyWaitHandoff {
    identity: u64,
    lease: Mutex<Option<LegacyWaitLease>>,
    session_cleanup: Mutex<Option<rocketmq_transport::api::v1::LegacySessionExecutionEnrollment>>,
    session_closed: AtomicBool,
}

impl Default for LegacyWaitHandoff {
    fn default() -> Self {
        static NEXT_IDENTITY: AtomicU64 = AtomicU64::new(1);

        Self {
            identity: NEXT_IDENTITY.fetch_add(1, Ordering::Relaxed),
            lease: Mutex::new(None),
            session_cleanup: Mutex::new(None),
            session_closed: AtomicBool::new(false),
        }
    }
}

impl LegacyWaitHandoff {
    pub(crate) fn identity(&self) -> u64 {
        self.identity
    }

    pub(crate) fn install(
        &self,
        expected_target: &DeferredGenerationTarget,
        lease: LegacyWaitLease,
    ) -> Result<(), LegacyWaitLease> {
        if lease.target() != expected_target {
            return Err(lease);
        }
        let mut installed = self.lease.lock();
        if installed.is_some() {
            return Err(lease);
        }
        *installed = Some(lease);
        Ok(())
    }

    #[must_use]
    pub(crate) fn target(&self) -> Option<DeferredGenerationTarget> {
        self.lease.lock().as_ref().map(|lease| lease.target().clone())
    }

    pub(crate) fn take(&self) -> Option<LegacyWaitLease> {
        self.lease.lock().take()
    }

    pub(crate) fn restore(&self, lease: LegacyWaitLease) -> Result<(), LegacyWaitLease> {
        let mut installed = self.lease.lock();
        if installed.is_some() {
            return Err(lease);
        }
        *installed = Some(lease);
        Ok(())
    }

    pub(crate) fn install_session_cleanup(
        &self,
        cleanup: rocketmq_transport::api::v1::LegacySessionExecutionEnrollment,
    ) -> Result<(), rocketmq_transport::api::v1::LegacySessionExecutionEnrollment> {
        let mut installed = self.session_cleanup.lock();
        if installed.is_some() {
            return Err(cleanup);
        }
        *installed = Some(cleanup);
        Ok(())
    }

    pub(crate) fn release_session_cleanup(&self) {
        drop(self.session_cleanup.lock().take());
    }

    pub(crate) fn take_session_execution(
        &self,
    ) -> Option<rocketmq_transport::api::v1::LegacySessionExecutionEnrollment> {
        self.session_cleanup.lock().take()
    }

    pub(crate) fn mark_session_closed(&self) {
        self.session_closed.store(true, Ordering::Release);
    }

    pub(crate) fn session_closed(&self) -> bool {
        self.session_closed.load(Ordering::Acquire)
    }

    pub(crate) fn release(&self) {
        let cleanup = self.session_cleanup.lock().take();
        let lease = self.lease.lock().take();
        drop(cleanup);
        drop(lease);
    }
}

impl Drop for LegacyWaitLease {
    fn drop(&mut self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.write_gate.lock().release_legacy_wait(&self.target);
        }
    }
}

#[derive(Debug)]
pub(crate) enum LegacyWakeBeginError {
    DifferentHandoff { wait: LegacyWaitLease, route: RoutePermit },
    TargetMismatch { wait: LegacyWaitLease, route: RoutePermit },
    NotLegacy { wait: LegacyWaitLease, route: RoutePermit },
    NotReady { wait: LegacyWaitLease, route: RoutePermit },
    PopLiteSingleFlight { wait: LegacyWaitLease, route: RoutePermit },
}

impl LegacyWakeBeginError {
    pub(crate) fn into_wait_and_route(self) -> (LegacyWaitLease, RoutePermit) {
        match self {
            Self::DifferentHandoff { wait, route }
            | Self::TargetMismatch { wait, route }
            | Self::NotLegacy { wait, route }
            | Self::NotReady { wait, route }
            | Self::PopLiteSingleFlight { wait, route } => (wait, route),
        }
    }
}

/// Held through handler admission, then transferred to the canonical continuation.
#[derive(Debug)]
#[must_use]
pub(crate) struct LegacyWakeLease {
    write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
    target: DeferredGenerationTarget,
    armed: bool,
}

impl LegacyWakeLease {
    pub(crate) fn into_continuation(mut self) -> LegacyContinuation {
        self.write_gate.lock().wake_into_continuation(&self.target);
        self.armed = false;
        LegacyContinuation {
            write_gate: Arc::clone(&self.write_gate),
            target: self.target.clone(),
            armed: true,
        }
    }
}

impl Drop for LegacyWakeLease {
    fn drop(&mut self) {
        if self.armed {
            self.write_gate.lock().release_wake_gate(&self.target);
        }
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) struct LegacyContinuation {
    write_gate: Arc<Mutex<DeferredGenerationHandoffState>>,
    target: DeferredGenerationTarget,
    armed: bool,
}

impl Drop for LegacyContinuation {
    fn drop(&mut self) {
        if self.armed {
            self.write_gate.lock().release_continuation(&self.target);
        }
    }
}
