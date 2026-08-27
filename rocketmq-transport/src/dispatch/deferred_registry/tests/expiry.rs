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

use super::*;

fn assert_expiry_registry_released<R>(registry: &DeferredRegistry<R>, harness: &Harness)
where
    R: Send + 'static,
{
    assert_registry_released(registry, &harness.admission);
    let future = tokio::time::Instant::now() + Duration::from_secs(24 * 60 * 60);
    let batch = registry.sweep_expired_at_for_test(future, NonZeroUsize::new(usize::MAX).expect("non-zero limit"));
    assert_eq!(
        batch.stats().examined(),
        0,
        "expiry index must not retain a stale entry"
    );
}

fn parts_with_telemetry<R>(
    harness: &Harness,
    original: OriginalRequestIdentity,
    telemetry: TransportTelemetry,
) -> DeferredParts
where
    R: Send + 'static,
{
    let retained = DeferredRegistry::<R>::try_retained_size(DeferredRetainedSizeParts::new(0))
        .expect("telemetry registry retained size");
    let control = RequestControlView::from_meta(
        &RequestMeta::new(Instant::now(), None),
        harness.session.view().state().clone(),
        &harness.parent,
    );
    let (sink, _receiver) = ResponseSink::local();
    let responder = sink
        .deferred_seed_for_test(telemetry, harness.session.view().id(), control)
        .into_responder(original);
    let permit = harness.admission.try_reserve(retained).expect("telemetry wait permit");
    DeferredParts::new(responder, permit)
}

#[derive(Debug)]
struct PanicOnDrop(&'static str);

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("{}", self.0);
    }
}

#[derive(Debug)]
struct PanickingBuilderError;

impl std::fmt::Display for PanickingBuilderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("panicking builder error")
    }
}

impl std::error::Error for PanickingBuilderError {}

impl Drop for PanickingBuilderError {
    fn drop(&mut self) {
        panic!("builder error drop panic");
    }
}

#[test]
fn parent_cancellation_is_frozen_before_successful_resume_drop_panics() {
    let harness = Harness::new("deferred-expiry-success-drop-panic", 98204);
    let registry = DeferredRegistry::<PanicOnDrop>::new();
    let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
    let parts = parts_with_telemetry::<PanicOnDrop>(&harness, harness.identity(310), telemetry);
    let state = parts.response_state();
    let parent = harness.parent.clone();

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = registry.register_with::<BuilderFailure, _>(parts, |_| {
            parent.cancel();
            Ok(PanicOnDrop("resume drop panic"))
        });
    }));

    assert!(result.is_err());
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ParentCancelled)
    );
    let terminals = terminals.lock();
    assert_eq!(terminals.len(), 1, "only the system terminal winner records a metric");
    assert_eq!(terminals[0].1, "parent_cancelled");
    drop(terminals);
    assert_expiry_registry_released(&registry, &harness);
}

#[test]
fn session_close_is_frozen_before_builder_error_drop_panics() {
    let harness = Harness::new("deferred-expiry-error-drop-panic", 98205);
    let registry = DeferredRegistry::<u64>::new();
    let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
    let parts = parts_with_telemetry::<u64>(&harness, harness.identity(311), telemetry);
    let state = parts.response_state();

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = registry.register_with(parts, |_| {
            harness.session.close();
            Err(PanickingBuilderError)
        });
    }));

    assert!(result.is_err());
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::SessionClosed)
    );
    let terminals = terminals.lock();
    assert_eq!(terminals.len(), 1, "only the system terminal winner records a metric");
    assert_eq!(terminals[0].1, "session_closed");
    drop(terminals);
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn deferred_expiry_long_poll_uses_the_unified_timeout_claim() {
    let harness = Harness::new("deferred-expiry-protocol-claim", 98190);
    let registry = DeferredRegistry::<u64>::new();
    let protocol_at = tokio::time::Instant::now() + Duration::from_secs(10);
    let parts = expiring_parts::<u64>(&harness, harness.identity(301), None, None)
        .try_with_expiry(
            protocol_at,
            DeferredExpiryMargins::new(Duration::from_secs(1), Duration::from_secs(1)),
        )
        .expect("attach protocol expiry");
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(41, parts))
        .expect("register protocol expiry");
    registration.commit().expect("publish protocol expiry");

    tokio::time::advance(Duration::from_secs(10)).await;
    let batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().examined(), 1);
    assert_eq!(batch.stats().long_poll_claims(), 1);
    let mut claims = batch.into_claims();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].reason(), DeferredWakeReason::Timeout);
    drop(claims.pop());
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ClaimDropped)
    );
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn message_claim_before_expiry_sweep_keeps_message_as_the_immutable_winner() {
    let harness = Harness::new("deferred-expiry-message-before-sweep", 98206);
    let registry = DeferredRegistry::<u64>::new();
    let protocol_at = tokio::time::Instant::now() + Duration::from_secs(10);
    let parts = expiring_parts::<u64>(&harness, harness.identity(312), None, None)
        .try_with_expiry(
            protocol_at,
            DeferredExpiryMargins::new(Duration::from_secs(1), Duration::from_secs(1)),
        )
        .expect("attach message-first expiry");
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(51, parts))
        .expect("register message-first expiry");
    let id = registration.deferred_id();
    registration.commit().expect("publish message-first expiry");

    let claimed = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("message claim wins before sweep");
    assert_eq!(claimed.reason(), DeferredWakeReason::MessageArrived);
    let batch = registry.sweep_expired_at_for_test(protocol_at, NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().examined(), 0);
    assert!(batch.into_claims().is_empty());
    let loser = registry
        .claim(id, DeferredWakeReason::Timeout)
        .await
        .expect_err("timeout observes the message claim marker");
    assert_eq!(loser.kind(), DeferredClaimErrorKind::AlreadyClaimed);
    assert_eq!(loser.request_id(), Some(claimed.request_id()));
    assert_eq!(loser.prior_terminal_reason(), None);
    assert_eq!(registry.test_claim_marker_count(), 1);
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);

    drop(claimed);
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ClaimDropped)
    );
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn expiry_sweep_before_message_claim_keeps_timeout_as_the_immutable_winner() {
    let harness = Harness::new("deferred-expiry-sweep-before-message", 98207);
    let registry = DeferredRegistry::<u64>::new();
    let protocol_at = tokio::time::Instant::now() + Duration::from_secs(10);
    let parts = expiring_parts::<u64>(&harness, harness.identity(313), None, None)
        .try_with_expiry(
            protocol_at,
            DeferredExpiryMargins::new(Duration::from_secs(1), Duration::from_secs(1)),
        )
        .expect("attach sweep-first expiry");
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(52, parts))
        .expect("register sweep-first expiry");
    let id = registration.deferred_id();
    registration.commit().expect("publish sweep-first expiry");

    let batch = registry.sweep_expired_at_for_test(protocol_at, NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().examined(), 1);
    assert_eq!(batch.stats().long_poll_claims(), 1);
    let mut claims = batch.into_claims();
    assert_eq!(claims.len(), 1);
    let claimed = claims.pop().expect("sweep returns timeout owner");
    assert_eq!(claimed.reason(), DeferredWakeReason::Timeout);
    let loser = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect_err("message observes the timeout claim marker");
    assert_eq!(loser.kind(), DeferredClaimErrorKind::AlreadyClaimed);
    assert_eq!(loser.request_id(), Some(claimed.request_id()));
    assert_eq!(loser.prior_terminal_reason(), None);
    assert_eq!(registry.test_claim_marker_count(), 1);
    assert_eq!(harness.admission.snapshot().waiting_count(), 1);

    drop(claimed);
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ClaimDropped)
    );
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn deferred_expiry_owner_cutoff_wins_without_a_timeout_claim() {
    let harness = Harness::new("deferred-expiry-owner-cutoff", 98191);
    let registry = DeferredRegistry::<u64>::new();
    let now = tokio::time::Instant::now();
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(30));
    let parts = expiring_parts::<u64>(&harness, harness.identity(302), Some(deadline), None);
    let state = parts.response_state();
    let parts = parts
        .try_with_expiry(
            now + Duration::from_secs(25),
            DeferredExpiryMargins::new(Duration::from_secs(5), Duration::from_secs(5)),
        )
        .expect("attach owner-capped expiry");
    assert_eq!(
        parts.expiry().expect("expiry").kind(),
        DeferredExpiryKind::OwnerDeadline
    );
    let registration = registry
        .register(DeferredRequest::new(42, parts))
        .expect("register owner-capped expiry");
    registration.commit().expect("publish owner-capped expiry");

    tokio::time::advance(Duration::from_secs(20)).await;
    let batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().owner_expired(), 1);
    assert_eq!(batch.stats().long_poll_claims(), 0);
    assert!(batch.into_claims().is_empty());
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::OwnerDeadline)
    );
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn deferred_expiry_cursor_does_not_let_provisional_entry_starve_active_entry() {
    let harness = Harness::new("deferred-expiry-cursor", 98192);
    let registry = DeferredRegistry::<u64>::new();
    let protocol_at = tokio::time::Instant::now() + Duration::from_secs(5);
    let margins = DeferredExpiryMargins::new(Duration::from_secs(1), Duration::from_secs(1));
    let first = expiring_parts::<u64>(&harness, harness.identity(303), None, None)
        .try_with_expiry(protocol_at, margins)
        .expect("attach first expiry");
    let first_registration = registry
        .register(DeferredRequest::new(43, first))
        .expect("register provisional expiry");
    let second = expiring_parts::<u64>(&harness, harness.identity(304), None, None)
        .try_with_expiry(protocol_at, margins)
        .expect("attach second expiry");
    let second_registration = registry
        .register(DeferredRequest::new(44, second))
        .expect("register active expiry");
    second_registration.commit().expect("publish active expiry");

    tokio::time::advance(Duration::from_secs(5)).await;
    let first_batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(first_batch.stats().pending_long_poll(), 1);
    assert!(first_batch.into_claims().is_empty());
    let second_batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(second_batch.stats().long_poll_claims(), 1);
    drop(second_batch);

    first_registration.commit().expect("publish persisted timeout");
    let wrapped = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(wrapped.stats().long_poll_claims(), 1);
    drop(wrapped);
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn deferred_expiry_session_close_between_scan_and_claim_wins_deterministically() {
    let harness = Harness::new("deferred-expiry-session-race", 98193);
    let cleanup = Arc::new(crate::dispatch::DeferredSessionCleanupOwner::new(
        harness.session.view().id(),
    ));
    let registry = DeferredRegistry::<u64>::new();
    let protocol_at = tokio::time::Instant::now() + Duration::from_secs(5);
    let parts = expiring_parts::<u64>(&harness, harness.identity(305), None, Some(&cleanup));
    let state = parts.response_state();
    let parts = parts
        .try_with_expiry(
            protocol_at,
            DeferredExpiryMargins::new(Duration::from_secs(1), Duration::from_secs(1)),
        )
        .expect("attach raced expiry");
    let registration = registry
        .register(DeferredRequest::new(45, parts))
        .expect("register raced expiry");
    registration.commit().expect("publish raced expiry");
    let close = Arc::clone(&cleanup);
    registry.inner.set_sweep_claim_checkpoint(Box::new(move || {
        assert_eq!(
            close.close(),
            crate::dispatch::deferred_session_cleanup::DeferredSessionCleanupCloseOutcome::Completed
        );
    }));

    tokio::time::advance(Duration::from_secs(5)).await;
    let batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().examined(), 1);
    assert_eq!(batch.stats().long_poll_claims(), 0);
    assert!(batch.into_claims().is_empty());
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::SessionClosed)
    );
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test]
async fn shutdown_freezes_parent_over_session_for_ticket_state_and_one_metric() {
    let harness = Harness::new("deferred-expiry-shutdown-priority", 98196);
    let registry = DeferredRegistry::<u64>::new();
    let (telemetry, terminals) = TransportTelemetry::with_deferred_terminal_capture();
    let parts = parts_with_telemetry::<u64>(&harness, harness.identity(306), telemetry);
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(46, parts))
        .expect("register shutdown priority request");
    let id = registration.deferred_id();
    let claim = registry.claim(id, DeferredWakeReason::MessageArrived);
    tokio::pin!(claim);
    tokio::select! {
        biased;
        result = &mut claim => panic!("provisional claim completed before shutdown: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    harness.session.close();
    harness.parent.cancel();
    assert!(matches!(
        registry.shutdown(),
        DeferredRegistryShutdownOutcome::Completed(_)
    ));
    let error = claim.await.expect_err("shutdown resolves the provisional ticket");
    assert_eq!(error.kind(), DeferredClaimErrorKind::ParentCancelled);
    assert_eq!(
        error.prior_terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ParentCancelled)
    );
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::ParentCancelled)
    );
    let terminals = terminals.lock();
    assert_eq!(terminals.len(), 1, "only the terminal CAS winner records a metric");
    assert_eq!(terminals[0].1, "parent_cancelled");
    drop(terminals);
    drop(registration);
    assert_expiry_registry_released(&registry, &harness);
}

async fn assert_owner_sweep_priority(parent: bool, expected: crate::dispatch::DeferredTerminalReason) {
    let name = if parent {
        "deferred-expiry-parent-owner-priority"
    } else {
        "deferred-expiry-session-owner-priority"
    };
    let harness = Harness::new(name, if parent { 98197 } else { 98198 });
    let registry = DeferredRegistry::<u64>::new();
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(10));
    let parts = expiring_parts::<u64>(&harness, harness.identity(307), Some(deadline), None);
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(47, parts))
        .expect("register priority request");
    registration.commit().expect("publish priority request");
    if parent {
        harness.parent.cancel();
    } else {
        harness.session.close();
    }

    tokio::time::advance(Duration::from_secs(10)).await;
    let batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().examined(), 1);
    assert_eq!(batch.stats().owner_expired(), 0);
    assert!(batch.into_claims().is_empty());
    assert_eq!(state.terminal_reason(), Some(expected));
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn sweep_prioritizes_parent_then_session_over_owner_deadline() {
    assert_owner_sweep_priority(true, crate::dispatch::DeferredTerminalReason::ParentCancelled).await;
    assert_owner_sweep_priority(false, crate::dispatch::DeferredTerminalReason::SessionClosed).await;
}

#[derive(Clone, Copy)]
enum ProvisionalPhase {
    Building,
    Prepared,
    Activating,
}

async fn assert_owner_sweep_from_phase(phase: ProvisionalPhase, owner: u64) {
    let harness = Harness::new("deferred-expiry-provisional-phase", owner);
    let registry = DeferredRegistry::<u64>::new();
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(10));
    let mut parts = Some(expiring_parts::<u64>(
        &harness,
        harness.identity(owner as i32),
        Some(deadline),
        None,
    ));
    let state = parts.as_ref().expect("phase parts").response_state();
    let mut enrollment = None;
    let id = registry
        .inner
        .insert_shell(
            parts.as_ref().expect("phase parts").request_id(),
            parts.as_ref().expect("phase parts").session_id(),
            parts.as_ref().expect("phase parts").control().clone(),
            Arc::clone(&state),
            parts.as_ref().expect("phase parts").expiry(),
            &mut enrollment,
        )
        .expect("insert phase shell");
    let mut activating = None;
    match phase {
        ProvisionalPhase::Building => assert!(registry.inner.transition_to_building(id)),
        ProvisionalPhase::Prepared | ProvisionalPhase::Activating => {
            registry
                .inner
                .store_prepared_from_shell(
                    id,
                    DeferredRequest::new(48, parts.take().expect("prepared phase parts")),
                )
                .expect("store prepared phase");
            if matches!(phase, ProvisionalPhase::Activating) {
                activating = Some(registry.inner.begin_activation(id).expect("begin activating phase"));
            }
        }
    }

    tokio::time::advance(Duration::from_secs(10)).await;
    let batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().owner_expired(), 1);
    assert!(batch.into_claims().is_empty());
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::OwnerDeadline)
    );
    assert_eq!(registry.test_index_counts(), (0, 0, 0));
    assert_eq!(registry.test_claim_marker_count(), 0);
    let externally_owned = matches!(phase, ProvisionalPhase::Building | ProvisionalPhase::Activating);
    assert_eq!(
        harness.admission.snapshot().waiting_count(),
        usize::from(externally_owned)
    );
    drop(activating);
    drop(parts);
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn owner_sweep_terminalizes_building_prepared_and_activating_phases() {
    assert_owner_sweep_from_phase(ProvisionalPhase::Building, 98199).await;
    assert_owner_sweep_from_phase(ProvisionalPhase::Prepared, 98200).await;
    assert_owner_sweep_from_phase(ProvisionalPhase::Activating, 98201).await;
}

#[tokio::test(start_paused = true)]
async fn delayed_sweep_crossing_long_poll_and_owner_cutoff_chooses_owner() {
    let harness = Harness::new("deferred-expiry-delayed-owner", 98202);
    let registry = DeferredRegistry::<u64>::new();
    let now = tokio::time::Instant::now();
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(20));
    let parts = expiring_parts::<u64>(&harness, harness.identity(308), Some(deadline), None);
    let state = parts.response_state();
    let parts = parts
        .try_with_expiry(
            now + Duration::from_secs(5),
            DeferredExpiryMargins::new(Duration::from_secs(3), Duration::from_secs(2)),
        )
        .expect("attach delayed expiry");
    assert_eq!(
        parts.expiry().expect("expiry policy").kind(),
        DeferredExpiryKind::LongPollTimeout
    );
    let registration = registry
        .register(DeferredRequest::new(49, parts))
        .expect("register delayed expiry");
    registration.commit().expect("publish delayed expiry");

    tokio::time::advance(Duration::from_secs(15)).await;
    let batch = registry.sweep_expired(NonZeroUsize::new(1).expect("non-zero limit"));
    assert_eq!(batch.stats().examined(), 1);
    assert_eq!(batch.stats().owner_expired(), 1);
    assert_eq!(batch.stats().long_poll_claims(), 0);
    assert!(batch.into_claims().is_empty());
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::OwnerDeadline)
    );
    assert_expiry_registry_released(&registry, &harness);
}

#[tokio::test(start_paused = true)]
async fn owner_only_claim_marker_uses_canonical_deadline_before_claim_drop() {
    let harness = Harness::new("deferred-expiry-owner-only-marker", 98203);
    let registry = DeferredRegistry::<u64>::new();
    let deadline = crate::deadline::RequestDeadline::after(Duration::from_secs(10));
    let parts = expiring_parts::<u64>(&harness, harness.identity(309), Some(deadline), None);
    let state = parts.response_state();
    let registration = registry
        .register(DeferredRequest::new(50, parts))
        .expect("register owner-only marker");
    let id = registration.deferred_id();
    registration.commit().expect("publish owner-only marker");
    let claimed = registry
        .claim(id, DeferredWakeReason::MessageArrived)
        .await
        .expect("claim before owner deadline");

    tokio::time::advance(Duration::from_secs(10)).await;
    drop(claimed);
    assert_eq!(
        state.terminal_reason(),
        Some(crate::dispatch::DeferredTerminalReason::OwnerDeadline)
    );
    assert_expiry_registry_released(&registry, &harness);
}
