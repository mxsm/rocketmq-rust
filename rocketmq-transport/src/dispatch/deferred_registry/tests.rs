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
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Instant;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

use super::internal::checked_claim_runtime_sum;
use super::internal::checked_registry_component_sum;
use super::internal::checked_registry_layout_bytes;
use super::internal::reserve_deferred_id;
use super::internal::Entry;
use super::internal::EntryPhaseTag;
use super::internal::RegistryLayoutSizes;
use super::*;
use crate::admission::AdmissionController;
use crate::admission::AdmissionLimits;
use crate::dispatch::DeferredAdmission;
use crate::dispatch::DeferredWaitLimits;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponseSink;
use crate::dispatch::ResponseTerminalState;
use crate::session_view::EmbeddedSessionRecord;
use crate::telemetry::TransportTelemetry;

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
        let runtime = RuntimeOwner::new(RuntimeConfig::server_default(name)).expect("registry test runtime owner");
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
        let (sink, _receiver) = ResponseSink::local();
        let seed = sink.deferred_seed_for_test(TransportTelemetry::noop(), session.view().id(), control);
        let responder = seed.into_responder(original);
        let permit = self.admission.try_reserve(retained).expect("registry wait permit");
        DeferredParts::new(responder, permit)
    }
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
    let ticket = arc_allocation::<ClaimTicket>();
    let marker = arc_allocation::<ClaimMarker<AlignedResume>>();
    let claim_slot = Layout::new::<(DeferredId, std::sync::Weak<ClaimMarker<AlignedResume>>)>().size();
    let completion = arc_allocation::<crate::dispatch::deferred_resume::ResumeCompletion>();
    let job_cell = arc_allocation::<crate::dispatch::deferred_resume::ResumeJobCell>();
    let claim_runtime = ticket + marker + claim_slot + completion + job_cell;
    let independent_registry_charge =
        inline_resume + primary_net + request_index + session_owner + session_member + claim_runtime;
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
        claim_runtime: 1,
    };
    assert_eq!(checked_registry_layout_bytes(valid), Some(6));

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

    assert!(checked_registry_component_sum(usize::MAX, 1, 0, 0, 0).is_none());
    assert!(checked_registry_component_sum(usize::MAX - 1, 1, 1, 0, 0).is_none());
    assert!(checked_registry_component_sum(usize::MAX - 2, 1, 1, 1, 0).is_none());
    assert!(checked_registry_component_sum(usize::MAX - 3, 1, 1, 1, 1).is_none());
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
    assert_eq!(
        sequence.load(Ordering::SeqCst),
        700,
        "retained-size preflight must run before deferred-id allocation"
    );
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
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
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
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
    assert_eq!(loser.into_request().expect("loser request").resume(), &2);
    assert_eq!(registry.inner.index_counts(), (1, 1, 1));
    drop(winner);
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
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
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
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
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
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
    let id = registry
        .inner
        .insert_shell(request_id, parts.session_id(), parts.control().clone())
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
    let invalid_id = invalid_registry
        .inner
        .insert_shell(
            invalid_parts.request_id(),
            invalid_parts.session_id(),
            invalid_parts.control().clone(),
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

#[tokio::test]
async fn concurrent_claims_have_one_winner_and_transient_marker_diagnostics() {
    let harness = Harness::new("deferred-registry-one-claim", 8113);
    let registry = DeferredRegistry::<u64>::new();
    let registration = registry
        .register(DeferredRequest::new(27, harness.parts::<u64>(harness.identity(13))))
        .expect("prepared registration");
    let id = registration.deferred_id();
    let first = registry.claim(id, DeferredWakeReason::MessageArrived);
    let second = registry.claim(id, DeferredWakeReason::Timeout);
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
    assert_eq!(claimed.reason(), DeferredWakeReason::MessageArrived);
    let error = second.await.expect_err("second waiter observes the live marker");
    assert_eq!(error.kind(), DeferredClaimErrorKind::AlreadyClaimed);
    assert_eq!(error.request_id(), Some(claimed.request_id()));
    assert_eq!(registry.inner.claim_marker_count(), 1);
    drop(claimed);
    assert_eq!(registry.inner.claim_marker_count(), 0);
    assert_eq!(
        registry
            .claim(id, DeferredWakeReason::ForcedRefresh)
            .await
            .expect_err("marker disappears after the affine claim drops")
            .kind(),
        DeferredClaimErrorKind::NotFound
    );
    assert_eq!(registry.inner.index_counts(), (0, 0, 0));
    assert_eq!(harness.admission.snapshot().waiting_count(), 0);
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
    let (sink, _receiver) = ResponseSink::local();
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
