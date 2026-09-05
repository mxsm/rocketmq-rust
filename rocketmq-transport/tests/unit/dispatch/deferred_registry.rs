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

use std::alloc::Layout;
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;
use std::time::Instant;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

use super::internal::checked_claim_runtime_sum;
use super::internal::checked_registry_component_sum;
use super::internal::checked_registry_layout_bytes;
use super::internal::checked_registry_with_expiry_bytes;
use super::internal::reserve_deferred_id;
use super::internal::Entry;
use super::internal::EntryPhaseTag;
use super::internal::ExpiryKey;
use super::internal::RegistryLayoutSizes;
use super::*;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::dispatch::deferred_session_cleanup::RegistryCleanupTarget;
use crate::dispatch::deferred_session_cleanup::TargetRecord;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredExpiryKind;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseSink;
use crate::dispatch::ResponseTerminalState;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

#[path = "deferred_registry/expiry.rs"]
mod expiry;

struct Harness {
    _runtime: RuntimeOwner,
    parent: rocketmq_runtime::TaskGroup,
    session: EmbeddedSessionRecord,
    admission: DeferredAdmission,
    owner: u64,
    sequence: AtomicU64,
}

impl Harness {
    fn new(name: &'static str, owner: u64) -> Self {
        let runtime = RuntimeOwner::plan(RuntimeConfig::server_default(name))
            .expect("test runtime configuration is valid")
            .build()
            .expect("registry test runtime owner");
        let parent = runtime.root_context().component(name).task_group().clone();
        let session = EmbeddedSessionRecord::new(owner);
        let controller = AdmissionController::new(AdmissionLimits::default());
        let admission = DeferredAdmission::try_configure(&controller, DeferredWaitLimits::new(64, 16 * 1024 * 1024))
            .expect("registry test deferred admission");
        Self {
            _runtime: runtime,
            parent,
            session,
            admission,
            owner,
            sequence: AtomicU64::new(1),
        }
    }

    fn identity(&self, opaque: i32) -> OriginalRequestIdentity {
        OriginalRequestIdentity::capture(
            self.owner,
            &self.sequence,
            &RemotingCommand::create_remoting_command(39).set_opaque(opaque),
        )
        .expect("registry test identity")
    }

    fn parts<R>(&self, original: OriginalRequestIdentity) -> DeferredParts
    where
        R: Send + 'static,
    {
        self.parts_with_declared::<R>(original, DeferredRetainedSizeParts::new(0))
    }

    fn parts_with_declared<R>(
        &self,
        original: OriginalRequestIdentity,
        declared: DeferredRetainedSizeParts,
    ) -> DeferredParts
    where
        R: Send + 'static,
    {
        let retained = DeferredRegistry::<R>::try_retained_size(declared).expect("registry retained size");
        self.parts_with_retained(original, retained)
    }

    fn parts_with_retained(&self, original: OriginalRequestIdentity, retained: DeferredRetainedSize) -> DeferredParts {
        self.parts_for_session(original, retained, &self.session)
    }

    fn parts_with_cleanup<R>(
        &self,
        original: OriginalRequestIdentity,
        cleanup: &crate::dispatch::DeferredSessionCleanupOwner,
    ) -> DeferredParts
    where
        R: Send + 'static,
    {
        let retained = DeferredRegistry::<R>::try_retained_size(DeferredRetainedSizeParts::new(0))
            .expect("registry retained size");
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), None),
            self.session.view().state().clone(),
            &self.parent,
        );
        let (sink, _receiver) = ResponseSink::local(control.clone());
        let seed = sink
            .deferred_seed_for_test(TransportTelemetry::noop(), self.session.view().id(), control)
            .with_session_cleanup(cleanup.registration());
        let responder = seed.into_responder(original);
        let permit = self.admission.try_reserve(retained).expect("registry wait permit");
        DeferredParts::new(responder, permit)
    }

    fn parts_for_session(
        &self,
        original: OriginalRequestIdentity,
        retained: DeferredRetainedSize,
        session: &EmbeddedSessionRecord,
    ) -> DeferredParts {
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), None),
            session.view().state().clone(),
            &self.parent,
        );
        let (sink, _receiver) = ResponseSink::local(control.clone());
        let seed = sink.deferred_seed_for_test(TransportTelemetry::noop(), session.view().id(), control);
        let responder = seed.into_responder(original);
        let permit = self.admission.try_reserve(retained).expect("registry wait permit");
        DeferredParts::new(responder, permit)
    }
}

fn assert_registry_released<R>(registry: &DeferredRegistry<R>, admission: &DeferredAdmission)
where
    R: Send + 'static,
{
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    let snapshot = admission.snapshot();
    assert_eq!(snapshot.waiting_count(), 0);
    assert_eq!(snapshot.retained_bytes(), 0);
}

fn expiring_parts<R>(
    harness: &Harness,
    original: OriginalRequestIdentity,
    deadline: Option<crate::deadline::RequestDeadline>,
    cleanup: Option<&crate::dispatch::DeferredSessionCleanupOwner>,
) -> DeferredParts
where
    R: Send + 'static,
{
    let retained = DeferredRegistry::<R>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("expiry registry retained size");
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), deadline),
        harness.session.view().state().clone(),
        &harness.parent,
    );
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let seed = sink.deferred_seed_for_test(TransportTelemetry::noop(), harness.session.view().id(), control);
    let seed = match cleanup {
        Some(cleanup) => seed.with_session_cleanup(cleanup.registration()),
        None => seed,
    };
    let responder = seed.into_responder(original);
    let permit = harness.admission.try_reserve(retained).expect("expiry wait permit");
    DeferredParts::new(responder, permit)
}

#[repr(align(128))]
struct AlignedResume([u8; 3]);

#[test]
fn retained_size_counts_fixed_and_registry_storage_once_and_preserves_caller_parts() {
    fn arc_allocation<T>() -> usize {
        let header = Layout::array::<AtomicUsize>(2).expect("Arc header layout");
        let (allocation, _) = header.extend(Layout::new::<T>()).expect("Arc data layout");
        allocation.pad_to_align().size()
    }
    let caller = DeferredRetainedSizeParts::new(11)
        .with_filter_bytes(13)
        .with_secondary_index_bytes(17)
        .with_metadata_bytes(19);
    let base = DeferredRetainedSize::try_from_parts(caller)
        .expect("base retained size")
        .bytes();
    let registry = DeferredRegistry::<AlignedResume>::try_retained_size(caller)
        .expect("registry retained size")
        .bytes();
    let inline_resume = Layout::new::<AlignedResume>().size();
    let responder = Layout::new::<DeferredResponder>().size();
    let permit = Layout::new::<DeferredWaitPermit>().size();
    let primary_entry = Layout::new::<(DeferredId, Entry<AlignedResume>)>().size();
    let primary_net = primary_entry - inline_resume - responder - permit;
    let request_index = Layout::new::<(RequestId, DeferredId)>().size();
    let session_owner = Layout::new::<(SessionId, HashSet<DeferredId>)>().size();
    let session_member = Layout::new::<DeferredId>().size();
    let cleanup_target = arc_allocation::<RegistryCleanupTarget<AlignedResume>>();
    let cleanup_target_record = Layout::new::<(usize, TargetRecord)>().size();
    let ticket = arc_allocation::<ClaimTicket>();
    let marker = arc_allocation::<ClaimMarker<AlignedResume>>();
    let claim_slot = Layout::new::<(DeferredId, std::sync::Weak<ClaimMarker<AlignedResume>>)>().size();
    let completion = arc_allocation::<crate::dispatch::deferred_resume::ResumeCompletion>();
    let job_cell = arc_allocation::<crate::dispatch::deferred_resume::ResumeJobCell>();
    let claim_runtime = ticket + marker + claim_slot + completion + job_cell;
    let expiry_index = Layout::new::<ExpiryKey>().size();
    let independent_registry_charge = inline_resume
        + primary_net
        + request_index
        + session_owner
        + session_member
        + cleanup_target
        + cleanup_target_record
        + claim_runtime
        + expiry_index;
    assert_eq!(registry - base, independent_registry_charge);

    let empty = DeferredRegistry::<AlignedResume>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("empty caller parts")
        .bytes();
    assert_eq!(registry - empty, 11 + 13 + 17 + 19);
    assert!(DeferredRegistry::<()>::try_retained_size(DeferredRetainedSizeParts::new(0)).is_ok());
    assert!(DeferredRegistry::<AlignedResume>::try_retained_size(
        DeferredRetainedSizeParts::new(usize::MAX).with_metadata_bytes(1)
    )
    .is_err());
    let _ = AlignedResume([0; 3]).0;
}

#[test]
fn retained_layout_checked_arithmetic_rejects_every_overflow_boundary() {
    let valid = RegistryLayoutSizes {
        inline_resume: 1,
        primary_entry: 4,
        responder: 1,
        permit: 1,
        request_index: 1,
        session_owner: 1,
        session_member: 1,
        cleanup_target: 1,
        cleanup_target_record: 1,
        claim_runtime: 1,
    };
    assert_eq!(checked_registry_layout_bytes(valid), Some(8));
    assert_eq!(checked_registry_with_expiry_bytes(8, 3), Some(11));
    assert_eq!(checked_registry_with_expiry_bytes(usize::MAX, 1), None);

    assert!(checked_registry_layout_bytes(RegistryLayoutSizes {
        inline_resume: usize::MAX,
        ..valid
    })
    .is_none());
    assert!(checked_registry_layout_bytes(RegistryLayoutSizes {
        responder: usize::MAX - 1,
        ..valid
    })
    .is_none());
    assert!(checked_registry_layout_bytes(RegistryLayoutSizes {
        primary_entry: 2,
        ..valid
    })
    .is_none());
    assert!(checked_registry_layout_bytes(RegistryLayoutSizes {
        session_owner: usize::MAX,
        ..valid
    })
    .is_none());
    assert!(checked_registry_layout_bytes(RegistryLayoutSizes {
        cleanup_target: usize::MAX,
        ..valid
    })
    .is_none());

    assert!(checked_registry_component_sum(usize::MAX, 1, 0, 0, 0, 0).is_none());
    assert!(checked_registry_component_sum(usize::MAX - 1, 1, 1, 0, 0, 0).is_none());
    assert!(checked_registry_component_sum(usize::MAX - 2, 1, 1, 1, 0, 0).is_none());
    assert!(checked_registry_component_sum(usize::MAX - 3, 1, 1, 1, 1, 0).is_none());
    assert!(checked_registry_component_sum(usize::MAX - 4, 1, 1, 1, 1, 1).is_none());
    assert_eq!(checked_claim_runtime_sum(1, 2, 3, 4), Some(10));
    assert!(checked_claim_runtime_sum(usize::MAX, 1, 0, 0).is_none());
    assert!(checked_claim_runtime_sum(usize::MAX - 1, 1, 1, 0).is_none());
    assert!(checked_claim_runtime_sum(usize::MAX - 2, 1, 1, 1).is_none());
}

#[test]
fn worst_case_session_bucket_charge_is_identical_for_one_or_many_sessions() {
    let harness = Harness::new("deferred-registry-session-charge", 8111);
    let retained =
        DeferredRegistry::<u64>::try_retained_size(DeferredRetainedSizeParts::new(0)).expect("registry retained size");
    let same_session = DeferredRegistry::<u64>::new();
    let same_first = same_session
        .register(DeferredRequest::new(
            1,
            harness.parts_with_retained(harness.identity(11), retained),
        ))
        .expect("first same-session registration");
    let same_second = same_session
        .register(DeferredRequest::new(
            2,
            harness.parts_with_retained(harness.identity(12), retained),
        ))
        .expect("second same-session registration");
    assert_eq!(same_session.inner.index_counts(), (2, 2, 1));

    let foreign_session = EmbeddedSessionRecord::new(8112);
    let foreign_sequence = AtomicU64::new(1);
    let foreign_identity = OriginalRequestIdentity::capture(
        8112,
        &foreign_sequence,
        &RemotingCommand::create_remoting_command(39).set_opaque(14),
    )
    .expect("foreign session identity");
    let many_sessions = DeferredRegistry::<u64>::new();
    let many_first = many_sessions
        .register(DeferredRequest::new(
            3,
            harness.parts_with_retained(harness.identity(13), retained),
        ))
        .expect("first many-session registration");
    let many_second = many_sessions
        .register(DeferredRequest::new(
            4,
            harness.parts_for_session(foreign_identity, retained, &foreign_session),
        ))
        .expect("second many-session registration");
    assert_eq!(many_sessions.inner.index_counts(), (2, 2, 2));
    assert_eq!(harness.admission.snapshot().waiting_count(), 4);
    assert_eq!(harness.admission.snapshot().retained_bytes(), retained.bytes() * 4);

    drop((same_first, same_second, many_first, many_second));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
    assert_eq!(harness.admission.snapshot().retained_bytes(), 0);
}

#[test]
fn deferred_id_allocator_returns_max_minus_one_once_and_never_wraps() {
    let sequence = AtomicU64::new(u64::MAX - 1);
    assert_eq!(reserve_deferred_id(&sequence), Some(DeferredId::for_test(u64::MAX - 1)));
    assert_eq!(sequence.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(reserve_deferred_id(&sequence), None);
    assert_eq!(sequence.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(reserve_deferred_id(&AtomicU64::new(0)), None);
}

#[test]
fn underreported_permit_is_rejected_before_id_index_and_builder_with_exact_parts() {
    let harness = Harness::new("deferred-registry-underfloor", 8101);
    let original = harness.identity(1);
    let fixed_only =
        DeferredRetainedSize::try_from_parts(DeferredRetainedSizeParts::new(0)).expect("fixed-only retained size");
    let parts = harness.parts_with_retained(original, fixed_only);
    let retained_bytes = parts.retained_bytes();
    let sequence = Arc::new(AtomicU64::new(700));
    let registry = DeferredRegistry::<u64>::with_test_sequence(Arc::clone(&sequence));
    let called = Arc::new(AtomicBool::new(false));
    let called_by_builder = Arc::clone(&called);
    let error = registry
        .register_with(parts, move |_| {
            called_by_builder.store(true, Ordering::SeqCst);
            Ok::<_, std::io::Error>(7)
        })
        .expect_err("underreported permit must fail");
    assert_eq!(error.kind(), DeferredRegistryErrorKind::RetainedSizeUnderreported);
    assert_eq!(error.request_id(), original.request_id());
    assert!(!called.load(Ordering::SeqCst));
    let recovered = error.into_parts().expect("preflight returns exact parts");
    assert_eq!(recovered.request_id(), original.request_id());
    assert_eq!(recovered.retained_bytes(), retained_bytes);
    drop(recovered);

    let direct_original = harness.identity(101);
    let direct = registry
        .register(DeferredRequest::new(
            8,
            harness.parts_with_retained(direct_original, fixed_only),
        ))
        .expect_err("direct underreported request must fail");
    assert_eq!(direct.kind(), DeferredRegistryErrorKind::RetainedSizeUnderreported);
    let direct_request = direct.into_request().expect("direct preflight returns exact request");
    assert_eq!(direct_request.request_id(), direct_original.request_id());
    assert_eq!(direct_request.resume(), &8);
    drop(direct_request);
    assert_eq!(
        sequence.load(Ordering::SeqCst),
        700,
        "retained-size preflight must run before deferred-id allocation"
    );
    assert_registry_released(&registry, &harness.admission);
}

#[test]
fn guard_drop_atomically_cleans_all_indexes_and_empty_session_bucket() {
    let harness = Harness::new("deferred-registry-rollback", 8102);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(9, harness.parts::<u64>(harness.identity(2))))
        .expect("prepared registration");
    assert_eq!(registry.inner.index_counts(), (1, 1, 1));
    assert_eq!(
        registry.inner.phase(registration.deferred_id()),
        Some(EntryPhaseTag::Prepared)
    );
    drop(registration);
    assert_registry_released(&registry, &harness.admission);
}

#[test]
fn duplicate_request_has_one_deterministic_owner_and_recovers_the_loser() {
    let harness = Harness::new("deferred-registry-duplicate", 8103);
    let original = harness.identity(3);
    let registry = DeferredRegistry::<u64>::new();
    let winner = registry
        .register(DeferredRequest::new(1, harness.parts::<u64>(original)))
        .expect("first request wins");
    let loser = registry
        .register(DeferredRequest::new(2, harness.parts::<u64>(original)))
        .expect_err("same request cannot register twice");
    assert_eq!(loser.kind(), DeferredRegistryErrorKind::DuplicateRequest);
    assert_eq!(harness.admission.snapshot().waiting_count(), 2);
    let loser = loser.into_request().expect("loser request");
    assert_eq!(loser.resume(), &2);
    drop(loser);
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);
    assert_eq!(registry.inner.index_counts(), (1, 1, 1));
    drop(winner);
    assert_registry_released(&registry, &harness.admission);
}

#[derive(Debug)]
struct BuilderFailure(&'static str);

impl fmt::Display for BuilderFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for BuilderFailure {}

#[test]
fn typed_builder_failure_preserves_source_and_parts_while_outer_formatting_is_redacted() {
    let harness = Harness::new("deferred-registry-builder-error", 8104);
    let registry = DeferredRegistry::<u64>::new();
    let original = harness.identity(4);
    let error = registry
        .register_with(harness.parts::<u64>(original), |_| {
            Err(BuilderFailure("secret business key"))
        })
        .expect_err("builder failure");
    assert_eq!(error.kind(), DeferredRegistryErrorKind::Builder);
    assert_eq!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<BuilderFailure>())
            .unwrap()
            .0,
        "secret business key"
    );
    assert!(!format!("{error}").contains("secret business key"));
    assert!(!format!("{error:?}").contains("secret business key"));
    let (source, parts) = error.into_builder_failure().expect("builder error and exact parts");
    assert_eq!(source.0, "secret business key");
    assert_eq!(parts.request_id(), original.request_id());
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);
    drop(parts);
    assert_registry_released(&registry, &harness.admission);
}

struct ReentrantLease {
    registry: DeferredRegistry<ReentrantLease>,
    drops: Arc<AtomicUsize>,
}

impl Drop for ReentrantLease {
    fn drop(&mut self) {
        assert_eq!(self.registry.inner.index_counts().1, 1);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn panicking_builder_rolls_back_after_reentrant_lease_drop_without_holding_the_lock() {
    let harness = Harness::new("deferred-registry-builder-panic", 8105);
    let registry = DeferredRegistry::<ReentrantLease>::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let builder_registry = registry.clone();
    let builder_drops = Arc::clone(&drops);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ =
            registry.register_with::<BuilderFailure, _>(harness.parts::<ReentrantLease>(harness.identity(5)), |_| {
                let _lease = ReentrantLease {
                    registry: builder_registry,
                    drops: builder_drops,
                };
                panic!("builder panic");
            });
    }));
    assert!(result.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_registry_released(&registry, &harness.admission);
}

#[tokio::test]
async fn provisional_claim_replays_the_first_reason_once_after_commit() {
    let harness = Harness::new("deferred-registry-wake", 8106);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register_with(harness.parts::<u64>(harness.identity(6)), move |_| {
            Ok::<_, BuilderFailure>(11)
        })
        .expect("prepared registration");
    let id = registration.deferred_id();
    assert_eq!(registry.inner.phase(id), Some(EntryPhaseTag::Prepared));
    let claim = registry.claim(id, DeferredWakeReason::MessageArrived);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("provisional claim completed before commit: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    registration.commit().expect("commit registration");
    let claimed = claim.await.expect("published claim");
    assert_eq!(claimed.reason(), DeferredWakeReason::MessageArrived);
    assert_eq!(*claimed.resume_data(), 11);
}

async fn assert_provisional_claim_from_phase(name: &'static str, owner: u64, building: bool) {
    let harness = Harness::new(name, owner);
    let registry = DeferredRegistry::<u64>::new();
    let parts = harness.parts::<u64>(harness.identity(owner as i32));
    let request_id = parts.request_id();
    let mut enrollment = None;
    let id = registry
        .inner
        .insert_shell(
            request_id,
            parts.session_id(),
            parts.control().clone(),
            parts.response_state(),
            None,
            &mut enrollment,
        )
        .expect("insert shell");
    if building {
        assert!(registry.inner.transition_to_building(id));
        assert_eq!(registry.inner.phase(id), Some(EntryPhaseTag::Building));
    } else {
        assert_eq!(registry.inner.phase(id), Some(EntryPhaseTag::Shell));
    }

    let claim = registry.claim(id, DeferredWakeReason::MessageArrived);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("provisional claim completed before publication: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    let request = DeferredRequest::new(37, parts);
    if building {
        registry
            .inner
            .store_prepared_from_building(id, request)
            .expect("store building request");
    } else {
        registry
            .inner
            .store_prepared_from_shell(id, request)
            .expect("store shell request");
    }
    let request = registry.inner.begin_activation(id).expect("begin activation");
    request.register_response().expect("register response");
    registry.inner.publish_active(id, request).expect("publish active");
    let claimed = claim.await.expect("provisional claim publishes");
    assert_eq!(claimed.request_id(), request_id);
    assert_eq!(*claimed.resume_data(), 37);
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);
    drop(claimed);
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn shell_and_building_claims_wait_for_one_durable_publication() {
    assert_provisional_claim_from_phase("deferred-registry-shell-claim", 8117, false).await;
    assert_provisional_claim_from_phase("deferred-registry-building-claim", 8118, true).await;
}

#[tokio::test]
async fn cancelled_waiter_replaces_its_ticket_and_retains_the_first_reason() {
    let harness = Harness::new("deferred-registry-ticket-replacement", 8119);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(41, harness.parts::<u64>(harness.identity(19))))
        .expect("prepared registration");
    let id = registration.deferred_id();

    let mut first = Box::pin(registry.claim(id, DeferredWakeReason::MessageArrived));
    tokio::select! {
        biased;
        result = &mut first => panic!("first waiter completed before publication: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert_eq!(registry.inner.ticket_epoch(id), Some(1));
    drop(first);

    let mut replacement = Box::pin(registry.claim(id, DeferredWakeReason::Timeout));
    tokio::select! {
        biased;
        result = &mut replacement => panic!("replacement completed before publication: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert_eq!(registry.inner.ticket_epoch(id), Some(2));
    registration.commit().expect("publish registration");
    let claimed = replacement.await.expect("replacement wins publication");
    assert_eq!(claimed.reason(), DeferredWakeReason::MessageArrived);
}

#[tokio::test]
async fn ticket_epoch_and_waiter_overflow_retire_every_index() {
    let epoch_harness = Harness::new("deferred-registry-ticket-epoch-overflow", 8120);
    let epoch_registry = DeferredRegistry::<u64>::new();
    let epoch_registration = epoch_registry
        .register(DeferredRequest::new(
            43,
            epoch_harness.parts::<u64>(epoch_harness.identity(20)),
        ))
        .expect("prepared epoch registration");
    let epoch_id = epoch_registration.deferred_id();
    epoch_registry.inner.set_ticket_epoch(epoch_id, u64::MAX);
    let epoch_error = epoch_registry
        .claim(epoch_id, DeferredWakeReason::Timeout)
        .await
        .expect_err("epoch overflow retires the entry");
    assert_eq!(epoch_error.kind(), DeferredClaimErrorKind::RegistryInvariant);
    assert_eq!(epoch_registry.inner.index_counts(), (0, 0, 0));
    drop(epoch_registration);
    assert_eq!(epoch_harness.admission.snapshot().waiting_count(), 0);

    let waiter_harness = Harness::new("deferred-registry-ticket-waiter-overflow", 8121);
    let waiter_registry = DeferredRegistry::<u64>::new();
    let waiter_registration = waiter_registry
        .register(DeferredRequest::new(
            47,
            waiter_harness.parts::<u64>(waiter_harness.identity(21)),
        ))
        .expect("prepared waiter registration");
    let waiter_id = waiter_registration.deferred_id();
    let ticket = waiter_registry.inner.install_claim_ticket(waiter_id, 1, usize::MAX);
    let waiter_error = waiter_registry
        .claim(waiter_id, DeferredWakeReason::ForcedRefresh)
        .await
        .expect_err("waiter overflow retires the entry");
    assert_eq!(waiter_error.kind(), DeferredClaimErrorKind::RegistryInvariant);
    assert_eq!(ticket.resolution(), TicketResolution::RemovedInvariant);
    assert_eq!(waiter_registry.inner.index_counts(), (0, 0, 0));
    drop(waiter_registration);
    assert_eq!(waiter_harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn terminal_and_invalid_claim_cas_retire_all_registry_ownership() {
    let terminal_harness = Harness::new("deferred-registry-terminal-claim", 8122);
    let terminal_registry = DeferredRegistry::<u64>::new();
    let terminal_request = DeferredRequest::new(53, terminal_harness.parts::<u64>(terminal_harness.identity(22)));
    let terminal_state = Arc::clone(terminal_request.parts.responder.response_state());
    let terminal_registration = terminal_registry
        .register(terminal_request)
        .expect("prepared terminal registration");
    let terminal_id = terminal_registration.deferred_id();
    terminal_registration.commit().expect("publish terminal registration");
    terminal_state.cancel().expect("external lifecycle terminal wins");
    let terminal_error = terminal_registry
        .claim(terminal_id, DeferredWakeReason::Timeout)
        .await
        .expect_err("terminal claim fails");
    assert_eq!(terminal_error.kind(), DeferredClaimErrorKind::AlreadyCompleted);
    assert_eq!(
        terminal_error.prior_terminal_state(),
        Some(ResponseTerminalState::Cancelled)
    );
    assert_eq!(terminal_registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(terminal_harness.admission.snapshot().waiting_count(), 0);

    let invalid_harness = Harness::new("deferred-registry-invalid-claim", 8123);
    let invalid_registry = DeferredRegistry::<u64>::new();
    let invalid_parts = invalid_harness.parts::<u64>(invalid_harness.identity(23));
    let mut enrollment = None;
    let invalid_id = invalid_registry
        .inner
        .insert_shell(
            invalid_parts.request_id(),
            invalid_parts.session_id(),
            invalid_parts.control().clone(),
            invalid_parts.response_state(),
            None,
            &mut enrollment,
        )
        .expect("insert invalid shell");
    invalid_registry
        .inner
        .store_prepared_from_shell(invalid_id, DeferredRequest::new(59, invalid_parts))
        .expect("store invalid request");
    let invalid_request = invalid_registry
        .inner
        .begin_activation(invalid_id)
        .expect("begin invalid activation");
    invalid_registry
        .inner
        .publish_active(invalid_id, invalid_request)
        .expect("publish intentionally unregistered response");
    let invalid_error = invalid_registry
        .claim(invalid_id, DeferredWakeReason::MessageArrived)
        .await
        .expect_err("open response state is an invalid claim transition");
    assert_eq!(invalid_error.kind(), DeferredClaimErrorKind::RegistryInvariant);
    assert_eq!(invalid_registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(invalid_harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn activating_claim_is_published_without_a_second_ready_state_machine() {
    let harness = Harness::new("deferred-registry-activating-wake", 8107);
    let registry = DeferredRegistry::<u64>::new();
    let mut registration = registry
        .register(DeferredRequest::new(15, harness.parts::<u64>(harness.identity(7))))
        .expect("prepared registration");
    let id = registration.deferred_id();
    let request = registry.inner.begin_activation(id).expect("begin activation");
    let claim = registry.claim(id, DeferredWakeReason::ForcedRefresh);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("activating claim completed before publish: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    request.register_response().expect("register response state");
    registry.inner.publish_active(id, request).expect("publish active");
    drop(registration.owner.take());
    let claimed = claim.await.expect("published activating claim");
    assert_eq!(claimed.reason(), DeferredWakeReason::ForcedRefresh);
}

async fn assert_first_claim_reason_wins(
    name: &'static str,
    owner: u64,
    first_reason: DeferredWakeReason,
    second_reason: DeferredWakeReason,
) {
    let harness = Harness::new(name, owner);
    let registry = DeferredRegistry::<u64>::new();
    let parts = harness.parts::<u64>(harness.identity(owner as i32));
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(27, parts))
        .expect("prepared registration");
    let id = registration.deferred_id();
    let first = registry.claim(id, first_reason);
    let second = registry.claim(id, second_reason);
    tokio::pin!(first);
    tokio::pin!(second);
    tokio::select! {
        biased;
        result = &mut first => panic!("first claim completed before publication: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    tokio::select! {
        biased;
        result = &mut second => panic!("second claim completed before publication: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    registration.commit().expect("publish active registration");
    let claimed = first.await.expect("first waiter wins");
    assert_eq!(claimed.reason(), first_reason);
    let error = second.await.expect_err("second waiter observes the live marker");
    assert_eq!(error.kind(), DeferredClaimErrorKind::AlreadyClaimed);
    assert_eq!(error.request_id(), Some(claimed.request_id()));
    assert_eq!(error.prior_terminal_reason(), None);
    assert!(!registry.test_contains(id));
    assert_eq!(registry.test_session_member_count(harness.session.view().id()), 1);
    assert_eq!(registry.test_claim_marker_count(), 1);
    drop(claimed);
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ClaimDropped)
    );
    assert_eq!(
        registry
            .claim(id, DeferredWakeReason::ForcedRefresh)
            .await
            .expect_err("marker disappears after the affine claim drops")
            .kind(),
        DeferredClaimErrorKind::NotFound
    );
    assert_registry_released(&registry, &harness.admission);
}

#[tokio::test]
async fn message_and_timeout_claims_freeze_the_first_reason_in_both_linearizations() {
    assert_first_claim_reason_wins(
        "deferred-registry-message-before-timeout",
        8113,
        DeferredWakeReason::MessageArrived,
        DeferredWakeReason::Timeout,
    )
    .await;
    assert_first_claim_reason_wins(
        "deferred-registry-timeout-before-message",
        8114,
        DeferredWakeReason::Timeout,
        DeferredWakeReason::MessageArrived,
    )
    .await;
}

#[tokio::test]
async fn duplicate_claim_drops_its_upgraded_marker_after_releasing_the_registry_lock() {
    let harness = Harness::new("deferred-registry-marker-drop", 8116);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(31, harness.parts::<u64>(harness.identity(17))))
        .expect("prepared registration");
    let id = registration.deferred_id();
    registration.commit().expect("publish registration");
    let claimed = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("first claim owns the marker");

    let upgraded = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let checkpoint_upgraded = Arc::clone(&upgraded);
    let checkpoint_release = Arc::clone(&release);
    registry.inner.set_claim_marker_checkpoint(Box::new(move || {
        checkpoint_upgraded.wait();
        checkpoint_release.wait();
    }));

    let duplicate_registry = registry.clone();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    harness
        .parent
        .spawn_service("deferred-registry.duplicate-claim", async move {
            let result = duplicate_registry.claim(id, DeferredWakeReason::Timeout).await;
            let _ = result_tx.send(result);
        })
        .expect("lifecycle-owned duplicate claim task");

    upgraded.wait();
    drop(claimed);
    release.wait();
    let error = result_rx
        .await
        .expect("duplicate task publishes its result")
        .expect_err("the upgraded marker remains a duplicate claim");
    assert_eq!(error.kind(), DeferredClaimErrorKind::AlreadyCompleted);
    assert_eq!(error.prior_terminal_state(), Some(ResponseTerminalState::Cancelled));
    assert_eq!(registry.inner.claim_marker_count(), 0);
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn provisional_waiter_observes_parent_removal_even_when_it_awaits_after_rollback() {
    let harness = Harness::new("deferred-registry-removal-before-await", 8114);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(29, harness.parts::<u64>(harness.identity(14))))
        .expect("prepared registration");
    let id = registration.deferred_id();
    let claim = registry.claim(id, DeferredWakeReason::Timeout);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("claim completed before rollback: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    harness.parent.cancel();
    drop(registration);
    let error = claim.await.expect_err("durable removal wakes a registered waiter");
    assert_eq!(error.kind(), DeferredClaimErrorKind::ParentCancelled);
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
}

#[test]
fn lifecycle_stop_after_builder_takes_priority_and_consumes_source_and_parts() {
    let harness = Harness::new("deferred-registry-lifecycle-builder", 8108);
    let registry = DeferredRegistry::<u64>::new();
    let parent = harness.parent.clone();
    let error = registry
        .register_with(harness.parts::<u64>(harness.identity(8)), move |_| {
            parent.cancel();
            Err(BuilderFailure("must be consumed"))
        })
        .expect_err("parent cancellation wins over builder error");
    assert_eq!(error.kind(), DeferredRegistryErrorKind::ParentCancelled);
    assert!(error.source().is_none());
    assert!(error.into_builder_failure().is_none());
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[test]
fn simultaneous_lifecycle_stops_report_parent_before_session_and_deadline() {
    let harness = Harness::new("deferred-registry-lifecycle-priority", 8115);
    let registry = DeferredRegistry::<u64>::new();
    let original = harness.identity(16);
    let retained =
        DeferredRegistry::<u64>::try_retained_size(DeferredRetainedSizeParts::new(0)).expect("registry retained size");
    let control = RequestControlView::from_meta(
        &RequestMeta::new(
            Instant::now(),
            Some(crate::deadline::RequestDeadline::after(std::time::Duration::ZERO)),
        ),
        harness.session.view().state().clone(),
        &harness.parent,
    );
    let (sink, _receiver) = ResponseSink::local(control.clone());
    let responder = sink
        .deferred_seed_for_test(TransportTelemetry::noop(), harness.session.view().id(), control)
        .into_responder(original);
    let permit = harness.admission.try_reserve(retained).expect("registry wait permit");
    harness.session.close();
    harness.parent.cancel();

    let error = registry
        .register_with(DeferredParts::new(responder, permit), |_| Ok::<_, BuilderFailure>(23))
        .expect_err("parent cancellation has highest stop priority");
    assert_eq!(error.kind(), DeferredRegistryErrorKind::ParentCancelled);
    assert!(error.source().is_none());
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[test]
fn lifecycle_stop_at_commit_rolls_back_response_and_wait_ownership() {
    let harness = Harness::new("deferred-registry-lifecycle-commit", 8109);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(17, harness.parts::<u64>(harness.identity(9))))
        .expect("prepared registration");
    harness.session.close();
    let error = registration.commit().expect_err("closed session rejects commit");
    assert_eq!(error.category(), "session_closed");
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[test]
fn lifecycle_stop_after_response_registration_rolls_back_before_active_publish() {
    let harness = Harness::new("deferred-registry-lifecycle-second-commit-check", 8113);
    let registry = DeferredRegistry::<u64>::new();
    let retained =
        DeferredRegistry::<u64>::try_retained_size(DeferredRetainedSizeParts::new(0)).expect("registry retained size");
    let commit_session = EmbeddedSessionRecord::new(8114);
    let sequence = AtomicU64::new(1);
    let original = OriginalRequestIdentity::capture(
        8114,
        &sequence,
        &RemotingCommand::create_remoting_command(39).set_opaque(15),
    )
    .expect("commit checkpoint identity");
    let mut registration = registry
        .register(DeferredRequest::new(
            21,
            harness.parts_for_session(original, retained, &commit_session),
        ))
        .expect("prepared registration");
    registration.set_commit_checkpoint(move || commit_session.close());

    let error = registration
        .commit()
        .expect_err("session close after response registration rejects Active publish");
    assert_eq!(error.category(), "session_closed");
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
    assert_eq!(harness.admission.snapshot().retained_bytes(), 0);
}

#[test]
fn active_registry_owns_resources_once_until_the_registry_is_dropped() {
    let harness = Harness::new("deferred-registry-active-release", 8110);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(19, harness.parts::<u64>(harness.identity(10))))
        .expect("prepared registration");
    registration.commit().expect("active registration");
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);
    drop(registry);
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
    assert_eq!(harness.admission.snapshot().retained_bytes(), 0);
}

#[test]
fn session_cleanup_detaches_multiple_registries_once_and_preserves_other_sessions() {
    let harness = Harness::new("deferred-registry-session-cleanup", 8120);
    let cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    let first = DeferredRegistry::<u64>::new();
    let second = DeferredRegistry::<String>::new();
    let first_registration = first
        .register(DeferredRequest::new(
            1,
            harness.parts_with_cleanup::<u64>(harness.identity(201), &cleanup),
        ))
        .expect("first cleanup registration");
    first_registration.commit().expect("first cleanup commit");
    let first_sibling = first
        .register(DeferredRequest::new(
            2,
            harness.parts_with_cleanup::<u64>(harness.identity(214), &cleanup),
        ))
        .expect("same-registry cleanup registration");
    first_sibling.commit().expect("same-registry cleanup commit");
    let second_registration = second
        .register(DeferredRequest::new(
            "second".to_owned(),
            harness.parts_with_cleanup::<String>(harness.identity(202), &cleanup),
        ))
        .expect("second cleanup registration");
    second_registration.commit().expect("second cleanup commit");

    let other = Harness::new("deferred-registry-other-session", 8121);
    let other_registration = first
        .register(DeferredRequest::new(3, other.parts::<u64>(other.identity(203))))
        .expect("other session registration");
    other_registration.commit().expect("other session commit");

    harness.session.close();
    let cleanup_report = cleanup.close();
    assert_eq!(
        cleanup_report.outcome,
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    assert_eq!(cleanup_report.registered_waiters, 3);
    assert_eq!(cleanup_report.removed_waiters, 3);
    assert_eq!(cleanup_report.remaining_wait_permits, 0);
    assert_eq!(first.inner.session_member_count(harness.session.view().id()), 0);
    assert_eq!(second.inner.session_member_count(harness.session.view().id()), 0);
    assert_eq!(first.inner.session_cleanup_call_count(), 1);
    assert_eq!(second.inner.session_cleanup_call_count(), 1);
    assert_eq!(first.inner.index_counts(), (1, 1, 1));
    assert_eq!(second.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
    assert_eq!(other.admission.snapshot().waiting_count(), 1);
    assert_eq!(
        cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::AlreadyClosed
    );
    drop(first);
    assert_eq!(other.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn claimed_session_cleanup_closes_marker_and_fresh_claim_is_not_found() {
    let harness = Harness::new("deferred-registry-claimed-cleanup", 8122);
    let cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    let registry = DeferredRegistry::<u64>::new();
    let parts = harness.parts_with_cleanup::<u64>(harness.identity(204), &cleanup);
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(9, parts))
        .expect("claimed cleanup registration");
    let id = registration.deferred_id();
    registration.commit().expect("claimed cleanup commit");
    let claimed = registry
        .claim(id, DeferredWakeReason::ForcedRefresh)
        .await
        .expect("claim wins before close");
    assert_eq!(registry.inner.session_member_count(harness.session.view().id()), 1);

    harness.session.close();
    assert_eq!(
        cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    assert_eq!(registry.inner.session_member_count(harness.session.view().id()), 0);
    assert_eq!(registry.inner.claim_marker_count(), 0);
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::SessionClosed)
    );
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);
    let error = registry
        .claim(id, DeferredWakeReason::Timeout)
        .await
        .expect_err("fresh post-cleanup claim has no tombstone");
    assert_eq!(error.kind(), DeferredClaimErrorKind::NotFound);
    drop(claimed);
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::SessionClosed)
    );
    assert_registry_released(&registry, &harness.admission);
}

#[test]
fn registry_shutdown_is_typed_idempotent_and_rejects_new_ownership() {
    let harness = Harness::new("deferred-registry-shutdown", 8123);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(11, harness.parts::<u64>(harness.identity(205))))
        .expect("shutdown registration");
    registration.commit().expect("shutdown commit");
    let outcome = registry.shutdown();
    let DeferredRegistryShutdownOutcome::Completed(stats) = outcome else {
        panic!("first shutdown must complete: {outcome:?}");
    };
    assert_eq!(stats.detached_entries(), 1);
    assert_eq!(stats.terminalized_responses(), 1);
    assert_eq!(stats.in_progress_responses(), 0);
    assert_eq!(stats.invariant_failures(), 0);
    assert_eq!(registry.shutdown(), DeferredRegistryShutdownOutcome::AlreadyClosed);
    assert_registry_released(&registry, &harness.admission);

    let called = Arc::new(AtomicBool::new(false));
    let builder_called = Arc::clone(&called);
    let error = registry
        .register_with(harness.parts::<u64>(harness.identity(206)), move |_| {
            builder_called.store(true, Ordering::SeqCst);
            Ok::<_, BuilderFailure>(12)
        })
        .expect_err("closed registry rejects registration");
    assert_eq!(error.kind(), DeferredRegistryErrorKind::ParentCancelled);
    assert!(!called.load(Ordering::SeqCst));
    assert!(
        error.into_parts().is_none(),
        "lifecycle stop consumes and releases response ownership"
    );
    assert_registry_released(&registry, &harness.admission);
}

#[test]
fn closed_cleanup_owner_rejects_before_id_allocation_and_builder_execution() {
    let harness = Harness::new("deferred-registry-close-before-insert", 8124);
    let cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    assert_eq!(
        cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    let sequence = Arc::new(AtomicU64::new(900));
    let registry = DeferredRegistry::<u64>::with_test_sequence(Arc::clone(&sequence));
    let called = Arc::new(AtomicBool::new(false));
    let builder_called = Arc::clone(&called);
    let error = registry
        .register_with(
            harness.parts_with_cleanup::<u64>(harness.identity(207), &cleanup),
            move |_| {
                builder_called.store(true, Ordering::SeqCst);
                Ok::<_, BuilderFailure>(13)
            },
        )
        .expect_err("closed cleanup owner rejects registration");
    assert_eq!(error.kind(), DeferredRegistryErrorKind::SessionClosed);
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(sequence.load(Ordering::SeqCst), 900);
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[test]
fn cleanup_removes_shell_and_prepared_entries_independently() {
    let harness = Harness::new("deferred-registry-shell-prepared-cleanup", 8131);

    let shell_cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    let shell_registry = DeferredRegistry::<u64>::new();
    let mut shell_parts = harness.parts_with_cleanup::<u64>(harness.identity(215), &shell_cleanup);
    let shell_state = shell_parts.response_state();
    let shell_id = shell_registry
        .insert_shell(
            shell_parts.request_id(),
            shell_parts.session_id(),
            shell_parts.control().clone(),
            Arc::clone(&shell_state),
            shell_parts.session_cleanup(),
            None,
        )
        .expect("shell enrollment");
    shell_parts.clear_session_cleanup();
    assert_eq!(shell_registry.inner.phase(shell_id), Some(EntryPhaseTag::Shell));
    assert_eq!(
        shell_cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    assert_eq!(shell_registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(shell_state.terminal_state(), Some(ResponseTerminalState::Closed));
    drop(shell_parts);

    let prepared_cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    let prepared_registry = DeferredRegistry::<u64>::new();
    let prepared_parts = harness.parts_with_cleanup::<u64>(harness.identity(216), &prepared_cleanup);
    let prepared_state = prepared_parts.response_state();
    let prepared = prepared_registry
        .register(DeferredRequest::new(21, prepared_parts))
        .expect("prepared enrollment");
    assert_eq!(
        prepared_registry.inner.phase(prepared.deferred_id()),
        Some(EntryPhaseTag::Prepared)
    );
    assert_eq!(
        prepared_cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    assert_eq!(prepared_registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(prepared_state.terminal_state(), Some(ResponseTerminalState::Closed));
    assert_eq!(
        prepared
            .commit()
            .expect_err("prepared commit observes cleanup")
            .category(),
        "session_closed"
    );
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

struct BuildingDropLease {
    drops: Arc<AtomicUsize>,
}

impl Drop for BuildingDropLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cleanup_detaches_building_entry_and_notifies_ticket_before_builder_returns() {
    let harness = Harness::new("deferred-registry-building-cleanup", 8125);
    let cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    let registry = DeferredRegistry::<BuildingDropLease>::new();
    let builder_registry = registry.clone();
    let parts = harness.parts_with_cleanup::<BuildingDropLease>(harness.identity(208), &cleanup);
    let drops = Arc::new(AtomicUsize::new(0));
    let builder_drops = Arc::clone(&drops);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let builder_entered = Arc::clone(&entered);
    let builder_release = Arc::clone(&release);
    let (id_tx, id_rx) = std::sync::mpsc::sync_channel(1);
    let builder = std::thread::spawn(move || {
        builder_registry.register_with(parts, move |id| {
            id_tx.send(id).expect("publish building id");
            builder_entered.wait();
            builder_release.wait();
            Ok::<_, BuilderFailure>(BuildingDropLease { drops: builder_drops })
        })
    });
    let id = id_rx.recv().expect("builder publishes id");
    entered.wait();
    let claim = registry.claim(id, DeferredWakeReason::MessageArrived);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("building claim completed before cleanup: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    assert_eq!(
        cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    let error = claim.await.expect_err("cleanup wakes provisional ticket");
    assert_eq!(error.kind(), DeferredClaimErrorKind::SessionClosed);
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    release.wait();
    let error = builder
        .join()
        .expect("building registration thread")
        .expect_err("closed building registration cannot publish");
    assert_eq!(error.kind(), DeferredRegistryErrorKind::SessionClosed);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn session_cleanup_does_not_overwrite_sending_response() {
    let harness = Harness::new("deferred-registry-sending-cleanup", 8126);
    let cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(
            15,
            harness.parts_with_cleanup::<u64>(harness.identity(209), &cleanup),
        ))
        .expect("sending cleanup registration");
    let id = registration.deferred_id();
    registration.commit().expect("sending cleanup commit");
    let claimed = registry
        .claim(id, DeferredWakeReason::ForcedRefresh)
        .await
        .expect("sending cleanup claim");
    let state = claimed.response_state_for_test();
    let send = state.begin_sending().expect("sending owner");
    harness.session.close();
    assert_eq!(
        cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    assert_eq!(state.snapshot(), crate::dispatch::ResponseStateSnapshot::Sending);
    send.fail(crate::dispatch::WriteProgress::NotStarted)
        .expect("sending owner keeps terminal authority");
    drop(claimed);
    assert_eq!(
        state.terminal_state(),
        Some(ResponseTerminalState::Failed {
            progress: crate::dispatch::WriteProgress::NotStarted,
        })
    );
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

struct BlockingShutdownLease {
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl Drop for BlockingShutdownLease {
    fn drop(&mut self) {
        self.entered.wait();
        self.release.wait();
    }
}

#[test]
fn concurrent_registry_shutdown_reports_in_progress_until_registry_batch_drops() {
    let harness = Harness::new("deferred-registry-concurrent-shutdown", 8127);
    let registry = DeferredRegistry::<BlockingShutdownLease>::new();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let registration = registry
        .register(DeferredRequest::new(
            BlockingShutdownLease {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            },
            harness.parts::<BlockingShutdownLease>(harness.identity(210)),
        ))
        .expect("concurrent shutdown registration");
    registration.commit().expect("concurrent shutdown commit");
    let winner_registry = registry.clone();
    let winner = std::thread::spawn(move || winner_registry.shutdown());
    entered.wait();
    assert_eq!(registry.shutdown(), DeferredRegistryShutdownOutcome::InProgress);
    release.wait();
    assert!(matches!(
        winner.join().expect("shutdown winner thread"),
        DeferredRegistryShutdownOutcome::Completed(_)
    ));
    assert_eq!(registry.shutdown(), DeferredRegistryShutdownOutcome::AlreadyClosed);
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

struct PanickingShutdownLease {
    registry: DeferredRegistry<PanickingShutdownLease>,
    observed: Arc<AtomicUsize>,
    panicked: Arc<AtomicBool>,
}

impl Drop for PanickingShutdownLease {
    fn drop(&mut self) {
        let observed = match self.registry.shutdown() {
            DeferredRegistryShutdownOutcome::InProgress => 1,
            DeferredRegistryShutdownOutcome::Completed(_) | DeferredRegistryShutdownOutcome::AlreadyClosed => 2,
        };
        self.observed.store(observed, Ordering::SeqCst);
        if !self.panicked.swap(true, Ordering::SeqCst) {
            panic!("registry-owned resume drop panic");
        }
    }
}

#[test]
fn panicking_registry_owned_drop_still_seals_shutdown_without_holding_the_lock() {
    let harness = Harness::new("deferred-registry-panicking-shutdown", 8133);
    let registry = DeferredRegistry::<PanickingShutdownLease>::new();
    let observed = Arc::new(AtomicUsize::new(0));
    let panicked = Arc::new(AtomicBool::new(false));
    let registration = registry
        .register(DeferredRequest::new(
            PanickingShutdownLease {
                registry: registry.clone(),
                observed: Arc::clone(&observed),
                panicked: Arc::clone(&panicked),
            },
            harness.parts::<PanickingShutdownLease>(harness.identity(218)),
        ))
        .expect("panicking shutdown registration");
    registration.commit().expect("panicking shutdown commit");

    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| registry.shutdown()));
    assert!(panic.is_err());
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    assert!(panicked.load(Ordering::SeqCst));
    assert_eq!(registry.shutdown(), DeferredRegistryShutdownOutcome::AlreadyClosed);
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn simultaneous_parent_and_session_cleanup_cancel_entry_and_ticket_consistently() {
    let harness = Harness::new("deferred-registry-parent-session-priority", 8134);
    let cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(harness.session.view().id());
    let registry = DeferredRegistry::<u64>::new();
    let parts = harness.parts_with_cleanup::<u64>(harness.identity(219), &cleanup);
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(23, parts))
        .expect("parent/session priority registration");
    let id = registration.deferred_id();
    let claim = registry.claim(id, DeferredWakeReason::MessageArrived);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("prepared claim completed before cleanup: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    harness.parent.cancel();
    harness.session.close();
    assert_eq!(
        cleanup.close(),
        crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
    );
    let error = claim.await.expect_err("parent cancellation resolves cleanup ticket");
    assert_eq!(error.kind(), DeferredClaimErrorKind::ParentCancelled);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ParentCancelled)
    );
    assert_eq!(state.terminal_state(), Some(ResponseTerminalState::Cancelled));
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ParentCancelled)
    );
    assert_eq!(
        registration
            .commit()
            .expect_err("removed registration uses parent priority")
            .category(),
        "parent_cancelled"
    );
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn registry_shutdown_wakes_provisional_ticket_and_claims_stay_parent_cancelled() {
    let harness = Harness::new("deferred-registry-shutdown-ticket", 8128);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(16, harness.parts::<u64>(harness.identity(211))))
        .expect("shutdown ticket registration");
    let id = registration.deferred_id();
    let claim = registry.claim(id, DeferredWakeReason::Timeout);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("provisional claim completed before shutdown: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    let DeferredRegistryShutdownOutcome::Completed(stats) = registry.shutdown() else {
        panic!("shutdown winner completes");
    };
    assert_eq!(stats.detached_entries(), 1);
    assert_eq!(stats.notified_tickets(), 1);
    let error = claim.await.expect_err("shutdown wakes provisional ticket");
    assert_eq!(error.kind(), DeferredClaimErrorKind::ParentCancelled);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ParentCancelled)
    );
    let fresh = registry
        .claim(id, DeferredWakeReason::Timeout)
        .await
        .expect_err("closed registry classifies every claim through its parent");
    assert_eq!(fresh.kind(), DeferredClaimErrorKind::ParentCancelled);
    drop(registration);
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

struct ReentrantShutdownLease {
    registry: DeferredRegistry<ReentrantShutdownLease>,
    observed: Arc<AtomicUsize>,
}

impl Drop for ReentrantShutdownLease {
    fn drop(&mut self) {
        let observed = match self.registry.shutdown() {
            DeferredRegistryShutdownOutcome::InProgress => 1,
            DeferredRegistryShutdownOutcome::Completed(_) | DeferredRegistryShutdownOutcome::AlreadyClosed => 2,
        };
        self.observed.store(observed, Ordering::SeqCst);
    }
}

#[test]
fn registry_shutdown_is_reentrant_without_holding_the_registry_lock() {
    let harness = Harness::new("deferred-registry-reentrant-shutdown", 8129);
    let registry = DeferredRegistry::<ReentrantShutdownLease>::new();
    let observed = Arc::new(AtomicUsize::new(0));
    let registration = registry
        .register(DeferredRequest::new(
            ReentrantShutdownLease {
                registry: registry.clone(),
                observed: Arc::clone(&observed),
            },
            harness.parts::<ReentrantShutdownLease>(harness.identity(212)),
        ))
        .expect("reentrant shutdown registration");
    registration.commit().expect("reentrant shutdown commit");
    assert!(matches!(
        registry.shutdown(),
        DeferredRegistryShutdownOutcome::Completed(_)
    ));
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    assert_eq!(registry.shutdown(), DeferredRegistryShutdownOutcome::AlreadyClosed);
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[test]
fn session_cleanup_wins_from_activating_and_commit_reports_session_closed() {
    let harness = Harness::new("deferred-registry-activating-cleanup", 8130);
    let cleanup = Arc::new(crate::dispatch::DeferredSessionCleanupOwner::new(
        harness.session.view().id(),
    ));
    let registry = DeferredRegistry::<u64>::new();
    let mut registration = registry
        .register(DeferredRequest::new(
            17,
            harness.parts_with_cleanup::<u64>(harness.identity(213), &cleanup),
        ))
        .expect("activating cleanup registration");
    let close = Arc::clone(&cleanup);
    registration.set_commit_checkpoint(move || {
        assert_eq!(
            close.close(),
            crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
        );
    });
    let error = registration
        .commit()
        .expect_err("cleanup between response registration and final publish wins");
    assert_eq!(error.category(), "session_closed");
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test]
async fn resume_without_an_owned_processor_terminalizes_as_processor_unavailable() {
    let harness = Harness::new("deferred-resume-processor-unavailable", 98194);
    let registry = DeferredRegistry::<u64>::new();
    let parts = harness.parts::<u64>(harness.identity(306));
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(46, parts))
        .expect("register processor-unavailable request");
    let id = registration.deferred_id();
    registration.commit().expect("publish processor-unavailable request");
    let claim = registry
        .claim(id, DeferredWakeReason::ForcedRefresh)
        .await
        .expect("claim processor-unavailable request");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&handler_calls);
    let error = claim
        .resume(DeferredResumeRetainedSize::new(0), move |_, _| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(
                crate::dispatch::RemotingResponse::command(RemotingCommand::create_response_command_with_code(0))
                    .expect("unused remoting response"),
            )
        })
        .await
        .expect_err("missing session executor rejects before handler execution");

    assert_eq!(error.kind(), DeferredResumeErrorKind::ExecutorClosing);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ProcessorUnavailable)
    );
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ProcessorUnavailable)
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn claimed_owner_cutoff_cancels_without_reentering_the_handler() {
    let harness = Harness::new("deferred-resume-claimed-owner-cutoff", 98195);
    let registry = DeferredRegistry::<u64>::new();
    let now = tokio::time::Instant::now();
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(30));
    let parts = expiring_parts::<u64>(&harness, harness.identity(307), Some(deadline), None);
    let state = parts.response_state();
    let parts = parts
        .try_with_expiry(
            now + Duration::from_secs(25),
            DeferredExpiryMargins::new(Duration::from_secs(5), Duration::from_secs(5)),
        )
        .expect("attach claimed owner cutoff");
    let registration = registry
        .register(DeferredRequest::new(47, parts))
        .expect("register claimed owner cutoff");
    let id = registration.deferred_id();
    registration.commit().expect("publish claimed owner cutoff");
    let claim = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("claim before owner cutoff");
    tokio::time::advance(Duration::from_secs(20)).await;
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&handler_calls);
    let error = claim
        .resume(DeferredResumeRetainedSize::new(0), move |_, _| async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(
                crate::dispatch::RemotingResponse::command(RemotingCommand::create_response_command_with_code(0))
                    .expect("unused remoting response"),
            )
        })
        .await
        .expect_err("owner cutoff cancels before handler execution");

    assert_eq!(error.kind(), DeferredResumeErrorKind::Cancelled);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::OwnerDeadline)
    );
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::OwnerDeadline)
    );
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
}
