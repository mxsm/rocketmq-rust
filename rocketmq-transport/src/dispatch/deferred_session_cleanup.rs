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

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use parking_lot::Mutex;

use super::DeferredRegistryErrorKind;
use crate::session_view::SessionId;

mod sealed {
    pub trait Sealed {}
}

pub(crate) trait DeferredSessionCleanupTarget: sealed::Sealed + Send + Sync + 'static {
    fn key(&self) -> usize;

    fn remove_session(&self, session_id: SessionId) -> usize;
}

#[derive(Clone)]
pub(crate) struct DeferredSessionCleanupRegistration {
    coordinator: Arc<CleanupCoordinator>,
    session_id: SessionId,
}

/// Opaque affine ownership of one session-close callback.
///
/// Dropping this value deregisters the callback. The transport deliberately
/// exposes neither the session cleanup registry nor its concrete registration
/// type through this capability.
#[must_use = "dropping the enrollment deregisters the session-close callback"]
#[cfg(test)]
pub(crate) struct SessionCleanupEnrollment {
    inner: Option<CleanupEnrollment>,
}

#[cfg(test)]
impl std::fmt::Debug for SessionCleanupEnrollment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionCleanupEnrollment")
            .field("active", &self.inner.is_some())
            .finish()
    }
}

/// Failure to atomically install a session-close callback.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[cfg(test)]
pub(crate) enum SessionCleanupInstallError<E> {
    /// The canonical network session was already closing or closed.
    #[error("session cleanup rejected a closed session")]
    SessionClosed,
    /// The caller rejected installation and recovered its affine enrollment.
    #[error("caller rejected session cleanup installation")]
    Install(E),
    /// An internal affine cleanup invariant was not satisfied.
    #[error("session cleanup invariant was not satisfied")]
    Invariant,
}

#[derive(Clone)]
#[cfg(test)]
pub(crate) struct SessionCleanupCapability {
    registration: DeferredSessionCleanupRegistration,
}

#[cfg(test)]
impl SessionCleanupCapability {
    pub(crate) fn new(registration: DeferredSessionCleanupRegistration) -> Self {
        Self { registration }
    }

    pub(crate) fn install<T, E>(
        &self,
        cleanup: impl Fn() + Send + Sync + 'static,
        install: impl FnOnce(SessionCleanupEnrollment) -> Result<T, (E, SessionCleanupEnrollment)>,
    ) -> Result<T, SessionCleanupInstallError<E>> {
        static NEXT_KEY: AtomicUsize = AtomicUsize::new(1);

        let key = NEXT_KEY
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current.checked_add(1))
            .map_err(|_| SessionCleanupInstallError::Invariant)?;
        let target = Arc::new(CallbackCleanupTarget {
            key,
            cleanup: Box::new(cleanup),
            completed: AtomicBool::new(false),
        });
        let mut install_error = None;
        let result = self.registration.clone().enroll(
            key,
            move || target as Arc<dyn DeferredSessionCleanupTarget>,
            |slot| {
                let Some(enrollment) = slot.take() else {
                    return Err(DeferredRegistryErrorKind::RegistryInvariant);
                };
                match install(SessionCleanupEnrollment {
                    inner: Some(enrollment),
                }) {
                    Ok(value) => Ok(value),
                    Err((error, mut returned)) => {
                        let Some(inner) = returned.inner.take() else {
                            return Err(DeferredRegistryErrorKind::RegistryInvariant);
                        };
                        *slot = Some(inner);
                        install_error = Some(error);
                        Err(DeferredRegistryErrorKind::Builder)
                    }
                }
            },
        );
        match result {
            Ok(value) => Ok(value),
            Err(DeferredRegistryErrorKind::SessionClosed) => Err(SessionCleanupInstallError::SessionClosed),
            Err(DeferredRegistryErrorKind::Builder) => match install_error {
                Some(error) => Err(SessionCleanupInstallError::Install(error)),
                None => Err(SessionCleanupInstallError::Invariant),
            },
            Err(_) => Err(SessionCleanupInstallError::Invariant),
        }
    }
}

#[cfg(test)]
struct CallbackCleanupTarget {
    key: usize,
    cleanup: Box<dyn Fn() + Send + Sync + 'static>,
    completed: AtomicBool,
}

#[cfg(test)]
impl sealed::Sealed for CallbackCleanupTarget {}

#[cfg(test)]
impl DeferredSessionCleanupTarget for CallbackCleanupTarget {
    fn key(&self) -> usize {
        self.key
    }

    fn remove_session(&self, _session_id: SessionId) -> usize {
        if !self.completed.swap(true, Ordering::AcqRel) {
            (self.cleanup)();
            1
        } else {
            0
        }
    }
}

#[derive(Clone)]
pub(crate) struct DeferredSessionCleanupOwner {
    registration: DeferredSessionCleanupRegistration,
}

impl DeferredSessionCleanupOwner {
    pub(crate) fn new(session_id: SessionId) -> Self {
        Self {
            registration: DeferredSessionCleanupRegistration {
                coordinator: Arc::new(CleanupCoordinator::new()),
                session_id,
            },
        }
    }

    pub(crate) fn registration(&self) -> DeferredSessionCleanupRegistration {
        self.registration.clone()
    }

    pub(crate) fn close(&self) -> DeferredSessionCleanupReport {
        self.registration.coordinator.close(self.registration.session_id)
    }

    pub(crate) fn remaining_wait_permits(&self) -> usize {
        self.registration.coordinator.live_enrollment_count()
    }

    #[cfg(test)]
    pub(crate) fn target_counts(&self) -> (usize, usize) {
        let state = self.registration.coordinator.state.lock();
        (
            state.targets.len(),
            state.targets.values().map(|record| record.live_enrollments).sum(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredSessionCleanupCloseOutcome {
    Completed,
    InProgress,
    AlreadyClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeferredSessionCleanupReport {
    pub(crate) outcome: DeferredSessionCleanupCloseOutcome,
    pub(crate) registered_waiters: usize,
    pub(crate) removed_waiters: usize,
    pub(crate) remaining_wait_permits: usize,
    pub(crate) panicked_targets: usize,
}

impl PartialEq<DeferredSessionCleanupCloseOutcome> for DeferredSessionCleanupReport {
    fn eq(&self, other: &DeferredSessionCleanupCloseOutcome) -> bool {
        self.outcome == *other
    }
}

impl PartialEq<DeferredSessionCleanupReport> for DeferredSessionCleanupCloseOutcome {
    fn eq(&self, other: &DeferredSessionCleanupReport) -> bool {
        *self == other.outcome
    }
}

impl DeferredSessionCleanupReport {
    pub(crate) const fn empty_completed() -> Self {
        Self {
            outcome: DeferredSessionCleanupCloseOutcome::Completed,
            registered_waiters: 0,
            removed_waiters: 0,
            remaining_wait_permits: 0,
            panicked_targets: 0,
        }
    }

    pub(crate) const fn is_healthy(self) -> bool {
        matches!(self.outcome, DeferredSessionCleanupCloseOutcome::Completed) && self.panicked_targets == 0
    }
}

pub(crate) struct CleanupEnrollment {
    coordinator: Weak<CleanupCoordinator>,
    target: Arc<dyn DeferredSessionCleanupTarget>,
    key: usize,
    active: Arc<AtomicBool>,
}

struct CleanupEnrollmentRollback<'a> {
    state: &'a mut CleanupState,
    key: usize,
    active: Arc<AtomicBool>,
    committed: bool,
}

impl CleanupEnrollmentRollback<'_> {
    fn commit(&mut self) {
        self.active.store(true, Ordering::Release);
        self.committed = true;
    }
}

impl Drop for CleanupEnrollmentRollback<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.active.store(false, Ordering::Release);
            release_enrollment(self.state, self.key);
        }
    }
}

impl DeferredSessionCleanupRegistration {
    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn enroll<T>(
        self,
        key: usize,
        make_target: impl FnOnce() -> Arc<dyn DeferredSessionCleanupTarget>,
        insert: impl FnOnce(&mut Option<CleanupEnrollment>) -> Result<T, DeferredRegistryErrorKind>,
    ) -> Result<T, DeferredRegistryErrorKind> {
        let mut state = self.coordinator.state.lock();
        if state.lifecycle != CleanupLifecycle::Open {
            return Err(DeferredRegistryErrorKind::SessionClosed);
        }

        let target = match state.targets.get_mut(&key) {
            Some(record) => {
                let Some(existing) = record.target.upgrade() else {
                    return Err(DeferredRegistryErrorKind::RegistryInvariant);
                };
                record.live_enrollments = record
                    .live_enrollments
                    .checked_add(1)
                    .ok_or(DeferredRegistryErrorKind::RegistryInvariant)?;
                existing
            }
            None => {
                let target = make_target();
                debug_assert_eq!(target.key(), key, "cleanup target factory must preserve its lookup key");
                state.targets.insert(
                    key,
                    TargetRecord {
                        target: Arc::downgrade(&target),
                        live_enrollments: 1,
                    },
                );
                target
            }
        };
        let active = Arc::new(AtomicBool::new(false));
        let mut enrollment = Some(CleanupEnrollment {
            coordinator: Arc::downgrade(&self.coordinator),
            target,
            key,
            active: Arc::clone(&active),
        });
        let mut rollback = CleanupEnrollmentRollback {
            state: &mut state,
            key,
            active,
            committed: false,
        };
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| insert(&mut enrollment)));
        match result {
            Ok(Ok(value)) if enrollment.is_none() => {
                rollback.commit();
                Ok(value)
            }
            Ok(Ok(_)) => {
                drop(rollback);
                drop(enrollment);
                Err(DeferredRegistryErrorKind::RegistryInvariant)
            }
            Ok(Err(error)) => {
                drop(rollback);
                drop(enrollment);
                Err(error)
            }
            Err(payload) => {
                drop(rollback);
                drop(enrollment);
                drop(state);
                std::panic::resume_unwind(payload)
            }
        }
    }
}

impl Drop for CleanupEnrollment {
    fn drop(&mut self) {
        if !self.active.swap(false, Ordering::AcqRel) {
            return;
        }
        let _keep_target_alive = &self.target;
        if let Some(coordinator) = self.coordinator.upgrade() {
            release_enrollment(&mut coordinator.state.lock(), self.key);
        }
    }
}

struct CleanupCoordinator {
    state: Mutex<CleanupState>,
}

impl CleanupCoordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(CleanupState::default()),
        }
    }

    fn close(&self, session_id: SessionId) -> DeferredSessionCleanupReport {
        let (mut targets, registered_waiters) = {
            let mut state = self.state.lock();
            match state.lifecycle {
                CleanupLifecycle::Open => state.lifecycle = CleanupLifecycle::Closing,
                CleanupLifecycle::Closing => {
                    return cleanup_report(DeferredSessionCleanupCloseOutcome::InProgress, &state, 0);
                }
                CleanupLifecycle::Closed => {
                    return cleanup_report(DeferredSessionCleanupCloseOutcome::AlreadyClosed, &state, 0);
                }
            }
            (
                state
                    .targets
                    .values()
                    .filter_map(|record| record.target.upgrade())
                    .collect::<Vec<_>>(),
                live_enrollment_count(&state),
            )
        };
        targets.sort_unstable_by_key(|target| target.key());
        let completion = CleanupCloseCompletion::new(self);
        let mut removed_waiters = 0usize;
        let mut panicked_targets = 0usize;

        for target in targets {
            match std::panic::catch_unwind(AssertUnwindSafe(|| target.remove_session(session_id))) {
                Ok(removed) => removed_waiters = removed_waiters.saturating_add(removed),
                Err(_) => panicked_targets = panicked_targets.saturating_add(1),
            }
        }

        completion.complete();
        let remaining_wait_permits = self.live_enrollment_count();
        DeferredSessionCleanupReport {
            outcome: DeferredSessionCleanupCloseOutcome::Completed,
            registered_waiters,
            removed_waiters,
            remaining_wait_permits,
            panicked_targets,
        }
    }

    fn live_enrollment_count(&self) -> usize {
        live_enrollment_count(&self.state.lock())
    }
}

struct CleanupCloseCompletion<'a> {
    coordinator: &'a CleanupCoordinator,
    armed: bool,
}

impl<'a> CleanupCloseCompletion<'a> {
    fn new(coordinator: &'a CleanupCoordinator) -> Self {
        Self {
            coordinator,
            armed: true,
        }
    }

    fn complete(mut self) {
        self.coordinator.state.lock().lifecycle = CleanupLifecycle::Closed;
        self.armed = false;
    }
}

impl Drop for CleanupCloseCompletion<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator.state.lock().lifecycle = CleanupLifecycle::Closed;
            self.armed = false;
        }
    }
}

struct CleanupState {
    lifecycle: CleanupLifecycle,
    targets: HashMap<usize, TargetRecord>,
}

impl Default for CleanupState {
    fn default() -> Self {
        Self {
            lifecycle: CleanupLifecycle::Open,
            targets: HashMap::new(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CleanupLifecycle {
    Open,
    Closing,
    Closed,
}

pub(in crate::dispatch) struct TargetRecord {
    target: Weak<dyn DeferredSessionCleanupTarget>,
    live_enrollments: usize,
}

fn release_enrollment(state: &mut CleanupState, key: usize) {
    let remove = state.targets.get_mut(&key).is_some_and(|record| {
        let decremented = record.live_enrollments.checked_sub(1);
        debug_assert!(
            decremented.is_some(),
            "a cleanup enrollment owns one checked live count"
        );
        record.live_enrollments = decremented.unwrap_or(0);
        record.live_enrollments == 0
    });
    if remove {
        state.targets.remove(&key);
    }
}

fn live_enrollment_count(state: &CleanupState) -> usize {
    state
        .targets
        .values()
        .fold(0usize, |total, record| total.saturating_add(record.live_enrollments))
}

fn cleanup_report(
    outcome: DeferredSessionCleanupCloseOutcome,
    state: &CleanupState,
    removed_waiters: usize,
) -> DeferredSessionCleanupReport {
    let remaining_wait_permits = live_enrollment_count(state);
    DeferredSessionCleanupReport {
        outcome,
        registered_waiters: remaining_wait_permits,
        removed_waiters,
        remaining_wait_permits,
        panicked_targets: 0,
    }
}

pub(crate) struct RegistryCleanupTarget<R>
where
    R: Send + 'static,
{
    registry: Weak<super::deferred_registry::RegistryInner<R>>,
    key: usize,
}

impl<R> RegistryCleanupTarget<R>
where
    R: Send + 'static,
{
    pub(crate) fn key_for(registry: &Arc<super::deferred_registry::RegistryInner<R>>) -> usize {
        Arc::as_ptr(registry) as usize
    }

    pub(crate) fn new(registry: &Arc<super::deferred_registry::RegistryInner<R>>) -> Arc<Self> {
        Self {
            registry: Arc::downgrade(registry),
            key: Self::key_for(registry),
        }
        .into()
    }
}

impl<R> sealed::Sealed for RegistryCleanupTarget<R> where R: Send + 'static {}

impl<R> DeferredSessionCleanupTarget for RegistryCleanupTarget<R>
where
    R: Send + 'static,
{
    fn key(&self) -> usize {
        self.key
    }

    fn remove_session(&self, session_id: SessionId) -> usize {
        if let Some(registry) = self.registry.upgrade() {
            registry.remove_session(session_id)
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

    use super::*;

    struct CountingTarget {
        key: usize,
        calls: Arc<AtomicUsize>,
    }

    impl sealed::Sealed for CountingTarget {}

    impl DeferredSessionCleanupTarget for CountingTarget {
        fn key(&self) -> usize {
            self.key
        }

        fn remove_session(&self, _session_id: SessionId) -> usize {
            self.calls.fetch_add(1, Ordering::SeqCst);
            0
        }
    }

    fn enroll_counting_target(
        registration: DeferredSessionCleanupRegistration,
        key: usize,
        target: Arc<CountingTarget>,
        factory_calls: Arc<AtomicUsize>,
    ) -> CleanupEnrollment {
        let mut held = None;
        registration
            .enroll(
                key,
                move || {
                    factory_calls.fetch_add(1, Ordering::SeqCst);
                    target as Arc<dyn DeferredSessionCleanupTarget>
                },
                |enrollment| {
                    held = enrollment.take();
                    Ok(())
                },
            )
            .expect("cleanup enrollment");
        held.expect("insert owns cleanup enrollment")
    }

    #[test]
    fn same_key_is_constructed_and_called_once_while_live_counts_remain_affine() {
        let owner = DeferredSessionCleanupOwner::new(SessionId::from_session_owner(41));
        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(CountingTarget {
            key: 7,
            calls: Arc::clone(&calls),
        });
        let first = enroll_counting_target(owner.registration(), 7, Arc::clone(&target), Arc::clone(&factory_calls));
        let second = enroll_counting_target(owner.registration(), 7, target, Arc::clone(&factory_calls));

        assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
        assert_eq!(owner.target_counts(), (1, 2));
        let report = owner.close();
        assert_eq!(report.outcome, DeferredSessionCleanupCloseOutcome::Completed);
        assert_eq!(report.registered_waiters, 2);
        assert_eq!(report.removed_waiters, 0);
        assert_eq!(report.remaining_wait_permits, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(first);
        assert_eq!(owner.target_counts(), (1, 1));
        drop(second);
        assert_eq!(owner.target_counts(), (0, 0));
        assert_eq!(owner.remaining_wait_permits(), 0);
        assert_eq!(owner.close(), DeferredSessionCleanupCloseOutcome::AlreadyClosed);
    }

    #[test]
    fn different_keys_are_called_once_and_cleanup_owners_are_isolated() {
        let first_owner = DeferredSessionCleanupOwner::new(SessionId::from_session_owner(42));
        let second_owner = DeferredSessionCleanupOwner::new(SessionId::from_session_owner(43));
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let third_calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let first = enroll_counting_target(
            first_owner.registration(),
            8,
            Arc::new(CountingTarget {
                key: 8,
                calls: Arc::clone(&first_calls),
            }),
            Arc::clone(&factory_calls),
        );
        let second = enroll_counting_target(
            first_owner.registration(),
            9,
            Arc::new(CountingTarget {
                key: 9,
                calls: Arc::clone(&second_calls),
            }),
            Arc::clone(&factory_calls),
        );
        let third = enroll_counting_target(
            second_owner.registration(),
            8,
            Arc::new(CountingTarget {
                key: 8,
                calls: Arc::clone(&third_calls),
            }),
            Arc::clone(&factory_calls),
        );

        assert_eq!(first_owner.close(), DeferredSessionCleanupCloseOutcome::Completed);
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        assert_eq!(third_calls.load(Ordering::SeqCst), 0);
        assert_eq!(second_owner.target_counts(), (1, 1));
        assert_eq!(second_owner.close(), DeferredSessionCleanupCloseOutcome::Completed);
        assert_eq!(third_calls.load(Ordering::SeqCst), 1);
        drop((first, second, third));
        assert_eq!(first_owner.target_counts(), (0, 0));
        assert_eq!(second_owner.target_counts(), (0, 0));
    }

    #[test]
    fn panicking_legacy_install_rolls_back_provisional_enrollment_before_unwind() {
        let owner = Arc::new(DeferredSessionCleanupOwner::new(SessionId::from_session_owner(9_002)));
        let capability = SessionCleanupCapability::new(owner.registration());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _ = capability.install(
                    || {},
                    |_provisional| -> Result<(), ((), SessionCleanupEnrollment)> {
                        worker_barrier.wait();
                        panic!("legacy cleanup installer panic");
                    },
                );
            }));
            done_tx.send(panic.is_err()).expect("report panic completion");
        });

        barrier.wait();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("provisional enrollment must not self-deadlock"),
            "panic must resume after registry rollback"
        );
        worker.join().expect("panic is contained by worker");
        assert_eq!(owner.target_counts(), (0, 0));
        assert_eq!(owner.close(), DeferredSessionCleanupCloseOutcome::Completed);
    }

    struct PanickingTarget {
        key: usize,
        registration: DeferredSessionCleanupRegistration,
        observed: Arc<AtomicUsize>,
    }

    impl sealed::Sealed for PanickingTarget {}

    impl DeferredSessionCleanupTarget for PanickingTarget {
        fn key(&self) -> usize {
            self.key
        }

        fn remove_session(&self, session_id: SessionId) -> usize {
            let observed = match self.registration.coordinator.close(session_id).outcome {
                DeferredSessionCleanupCloseOutcome::InProgress => 1,
                DeferredSessionCleanupCloseOutcome::Completed | DeferredSessionCleanupCloseOutcome::AlreadyClosed => 2,
            };
            self.observed.store(observed, Ordering::SeqCst);
            panic!("cleanup target panic");
        }
    }

    #[test]
    fn panicking_first_target_does_not_skip_later_registry_cleanup() {
        let runtime = rocketmq_runtime::RuntimeOwner::new(rocketmq_runtime::RuntimeConfig::server_default(
            "deferred-cleanup-panicking-target",
        ))
        .expect("cleanup panic test runtime");
        let parent = runtime
            .root_context()
            .component("deferred-cleanup-panicking-target")
            .task_group()
            .clone();
        let session = crate::session_view::EmbeddedSessionRecord::new(44);
        let owner = DeferredSessionCleanupOwner::new(session.view().id());
        let observed = Arc::new(AtomicUsize::new(0));
        let target = Arc::new(PanickingTarget {
            key: 0,
            registration: owner.registration(),
            observed: Arc::clone(&observed),
        });
        let mut held = None;
        owner
            .registration()
            .enroll(
                0,
                move || target as Arc<dyn DeferredSessionCleanupTarget>,
                |enrollment| {
                    held = enrollment.take();
                    Ok(())
                },
            )
            .expect("panicking target enrollment");

        let admission_controller =
            crate::admission::AdmissionController::new(crate::admission::AdmissionLimits::default());
        let admission = crate::dispatch::DeferredAdmission::try_configure(
            &admission_controller,
            crate::dispatch::DeferredWaitLimits::new(4, 1024 * 1024),
        )
        .expect("cleanup panic deferred admission");
        let registry = crate::dispatch::DeferredRegistry::<usize>::new();
        let retained = crate::dispatch::DeferredRegistry::<usize>::try_retained_size(
            crate::dispatch::DeferredRetainedSizeParts::new(0),
        )
        .expect("cleanup panic retained size");
        let sequence = AtomicU64::new(1);
        let original = crate::dispatch::OriginalRequestIdentity::capture(
            44,
            &sequence,
            &RemotingCommand::create_remoting_command(39).set_opaque(44),
        )
        .expect("cleanup panic original identity");
        let control = crate::dispatch::RequestControlView::from_meta(
            &crate::dispatch::RequestMeta::new(std::time::Instant::now(), None),
            session.view().state().clone(),
            &parent,
        );
        let (sink, _receiver) = crate::dispatch::ResponseSink::local(control.clone());
        let responder = sink
            .deferred_seed_for_test(
                crate::telemetry::TransportTelemetry::noop(),
                session.view().id(),
                control,
            )
            .with_session_cleanup(owner.registration())
            .into_responder(original);
        let response_state = Arc::clone(responder.response_state());
        let permit = admission.try_reserve(retained).expect("cleanup panic wait permit");
        let registration = registry
            .register(crate::dispatch::DeferredRequest::new(
                44,
                crate::dispatch::DeferredParts::new(responder, permit),
            ))
            .expect("cleanup panic registry enrollment");
        registration.commit().expect("cleanup panic registry commit");
        assert_eq!(registry.test_index_counts(), (1, 1, 1));
        assert_eq!(admission.snapshot().waiting_count(), 1);

        let report = owner.close();
        assert_eq!(report.outcome, DeferredSessionCleanupCloseOutcome::Completed);
        assert_eq!(report.panicked_targets, 1);
        assert!(!report.is_healthy());
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert_eq!(registry.test_index_counts(), (0, 0, 0));
        assert_eq!(
            response_state.terminal_state(),
            Some(crate::dispatch::ResponseTerminalState::Closed)
        );
        assert_eq!(admission.snapshot().waiting_count(), 0);
        assert_eq!(owner.close(), DeferredSessionCleanupCloseOutcome::AlreadyClosed);
        drop(held);
        assert_eq!(owner.target_counts(), (0, 0));
    }
}
