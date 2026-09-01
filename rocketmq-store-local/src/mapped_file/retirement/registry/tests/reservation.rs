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
use crate::mapped_file::retirement::writer::model_io::ModelFaultAction;
use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
use crate::mapped_file::retirement::writer::ManagedLedgerWriter;

#[test]
fn reserved_intent_commits_only_from_a_real_writer_protocol_receipt() {
    let (registry, owner, queue) = registered_registry(0);
    let operation = operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 3, NONCE);
    let reservation = registry
        .prepare_retirement(operation, &owner, &queue)
        .expect("intent reservation succeeds");
    let binding = reservation.binding().clone();
    let mut ledger =
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x31; 16], 4, 91, 92, 0, true, 5)
            .expect("replay cursor is valid");

    let token = ledger
        .append_retirement_intent(reservation.begin_append())
        .expect("the exact writer receipt commits the reservation");

    assert_eq!(token.binding(), &binding);
    assert_eq!(token.durable_sequence(), 91);
    assert_eq!(token.ledger_generation(), 4);
    assert_eq!(ledger.io_for_test().events().len(), 9);
    assert_eq!(registry.ticket_high_water(), 1);
    assert!(!registry.needs_recovery());
}

#[test]
fn ambiguous_writer_failure_fences_the_registry_and_retains_the_owner() {
    let (registry, owner, queue) = registered_registry(0);
    let weak_owner = Arc::downgrade(&owner);
    let reservation = registry
        .prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 3, NONCE),
            &owner,
            &queue,
        )
        .expect("intent reservation succeeds");
    let mut ledger = ManagedLedgerWriter::for_test(
        ModelLedgerIo::empty().with_fault(1, ModelFaultAction::ErrorBefore),
        store_uuid(),
        [0x31; 16],
        4,
        91,
        92,
        0,
        true,
        5,
    )
    .expect("replay cursor is valid");
    drop(owner);

    ledger
        .append_retirement_intent(reservation.begin_append())
        .expect_err("frame sync failure is ambiguous");

    assert!(registry.needs_recovery());
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(weak_owner.upgrade().is_some());
}

#[test]
fn queue_identity_can_only_be_reproduced_by_cloning_the_original_seal() {
    let identity = QueueIdentity::allocate();
    let clone = identity.clone();
    let unrelated = QueueIdentity::allocate();

    assert!(identity.same_as(&clone));
    assert!(!identity.same_as(&unrelated));
}

#[test]
fn raw_registry_authority_surfaces_are_module_private() {
    let registry_source = include_str!("../../registry.rs").replace("\r\n", "\n");
    let guards_source = include_str!("../guards.rs");
    let registry_production = registry_source
        .split_once("\n#[cfg(test)]\nmod tests;")
        .expect("registry tests follow production code")
        .0;
    let registry_impl = registry_production
        .split_once("impl<O> RetirementRegistry<O> {")
        .expect("RetirementRegistry implementation exists")
        .1;
    let raw_constructor = registry_impl
        .lines()
        .find(|line| line.contains("fn new("))
        .expect("raw registry constructor exists");
    let raw_registration = registry_impl
        .lines()
        .find(|line| line.contains("fn register_published("))
        .expect("raw published-file registration exists");
    let raw_handoff_commit = guards_source
        .split_once("/// Finalizes token consumption only after the caller completed the exact queue handoff.")
        .expect("prepared handoff commit documentation exists")
        .1
        .lines()
        .find(|line| line.contains("fn commit("))
        .expect("prepared handoff commit exists");

    for (authority, declaration) in [
        ("raw replay high-water", raw_constructor),
        ("raw published-file registration", raw_registration),
    ] {
        assert!(
            declaration.trim_start().starts_with("fn "),
            "{authority} leaked outside the registry module through `{declaration}`"
        );
    }

    let types_source = include_str!("../types.rs");
    let queue_identity_impl = types_source
        .split_once("impl QueueIdentity {")
        .expect("QueueIdentity implementation exists")
        .1;
    let raw_queue_identity = queue_identity_impl
        .lines()
        .find(|line| line.contains("fn allocate("))
        .expect("raw queue identity allocator exists");
    let registration_impl = types_source
        .split_once("impl<O> PublishedFileRegistration<O> {")
        .expect("PublishedFileRegistration implementation exists")
        .1;
    let raw_registration_builder = registration_impl
        .lines()
        .find(|line| line.contains("fn new("))
        .expect("raw registration builder exists");

    for (authority, declaration) in [
        ("raw queue identity", raw_queue_identity),
        ("raw registration builder", raw_registration_builder),
    ] {
        assert!(
            declaration.trim_start().starts_with("pub(super) fn "),
            "{authority} leaked outside the registry module through `{declaration}`"
        );
    }
    assert!(
        raw_handoff_commit.trim_start().starts_with("pub(super) fn "),
        "queue handoff commit escaped the registry trust boundary through `{raw_handoff_commit}`"
    );
}

#[test]
fn registration_reserves_incarnation_path_and_physical_key() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("first identity is registered");

    assert_eq!(registry.retained_identity_count(), 1);
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));

    assert_eq!(
        registry.register_published(registration(1, 12, FILE_LENGTH, owner.clone(), queue.clone())),
        Err(RegistryViolation::DuplicateIncarnation {
            incarnation: incarnation(1),
        })
    );
    assert_eq!(
        registry.register_published(registration(2, 12, 0, owner.clone(), queue.clone())),
        Err(RegistryViolation::CanonicalPathReserved {
            path: canonical_path(0),
            incumbent: incarnation(1),
        })
    );
    assert_eq!(
        registry.register_published(registration(2, 11, FILE_LENGTH, owner.clone(), queue.clone())),
        Err(RegistryViolation::PhysicalKeyReserved {
            physical_key: physical_key(11),
            incumbent: incarnation(1),
        })
    );
    assert_eq!(
        registry.register_published(registration(3, 13, FILE_LENGTH * 2, owner, queue)),
        Err(RegistryViolation::OwnerAlreadyRegistered {
            incumbent: incarnation(1),
        })
    );
}

#[test]
fn preparation_requires_the_exact_registered_identity_and_owner() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");

    assert_eq!(
        registry
            .prepare_retirement(
                operation(1, 12, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                &owner,
                &queue,
            )
            .err()
            .expect("wrong key is rejected"),
        RegistryViolation::PhysicalKeyMismatch {
            incarnation: incarnation(1),
        }
    );
    assert_eq!(
        registry
            .prepare_retirement(
                operation_with_path(
                    1,
                    11,
                    0,
                    FILE_LENGTH,
                    "consumequeue/topic/00000000000000000000",
                    RetirementReason::TtlExpired,
                    1,
                    NONCE,
                ),
                &owner,
                &queue,
            )
            .err()
            .expect("wrong path is rejected"),
        RegistryViolation::CanonicalPathMismatch {
            incarnation: incarnation(1),
        }
    );
    assert_eq!(
        registry
            .prepare_retirement(
                operation(1, 11, FILE_LENGTH, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                &owner,
                &queue,
            )
            .err()
            .expect("wrong offset is rejected"),
        RegistryViolation::SegmentOffsetMismatch {
            incarnation: incarnation(1),
        }
    );
    assert_eq!(
        registry
            .prepare_retirement(
                operation(1, 11, 0, FILE_LENGTH * 2, RetirementReason::TtlExpired, 1, NONCE),
                &owner,
                &queue,
            )
            .err()
            .expect("wrong length is rejected"),
        RegistryViolation::ExpectedLengthMismatch {
            incarnation: incarnation(1),
        }
    );

    let other_owner = Arc::new(TestOwner(2));
    assert_eq!(
        registry
            .prepare_retirement(
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                &other_owner,
                &queue,
            )
            .err()
            .expect("wrong owner is rejected"),
        RegistryViolation::OwnerIdentityMismatch {
            incarnation: incarnation(1),
        }
    );
    let other_queue = QueueIdentity::allocate();
    assert_eq!(
        registry
            .prepare_retirement(
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                &owner,
                &other_queue,
            )
            .err()
            .expect("wrong queue is rejected"),
        RegistryViolation::QueueIdentityMismatch {
            incarnation: incarnation(1),
        }
    );

    assert_eq!(
        RetirementOperation::new(
            incarnation(1),
            RetirementReason::TtlExpired,
            0,
            0,
            FILE_LENGTH,
            NONCE,
            physical_key(11),
            canonical_path(0),
        ),
        Err(RegistryViolation::ZeroMappingGeneration)
    );
    assert_eq!(
        RetirementOperation::new(
            incarnation(1),
            RetirementReason::TtlExpired,
            1,
            0,
            FILE_LENGTH,
            [0; 16],
            physical_key(11),
            canonical_path(0),
        ),
        Err(RegistryViolation::ZeroRetirementNonce)
    );
}

#[test]
fn durable_proof_mints_a_token_bound_to_every_intent_field() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner = Arc::new(TestOwner(7));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");

    let reservation = registry
        .prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::OffsetTruncate, 23, NONCE),
            &owner,
            &queue,
        )
        .expect("intent is prepared without holding the registry lock");
    let binding = reservation.binding().clone();
    assert_eq!(registry.retained_identity_count(), 1);
    let append = reservation.begin_append();
    let evidence = DurableIntentEvidence::writer_verified_for_test(append.intent_record(), 4, 91, 92, 1_000)
        .expect("test writer receipt is structurally valid");
    let token = append.commit(evidence).expect("durability evidence mints the token");

    assert_eq!(registry.ticket_high_water(), 1);
    assert_eq!(token.binding(), &binding);
    assert_eq!(token.binding().ticket_id().get(), 1);
    assert_eq!(token.binding().incarnation(), incarnation(1));
    assert_eq!(token.binding().reason(), RetirementReason::OffsetTruncate);
    assert_eq!(token.binding().mapping_generation(), 23);
    assert_eq!(token.binding().segment_offset(), 0);
    assert_eq!(token.binding().expected_length(), FILE_LENGTH);
    assert_eq!(token.binding().retirement_nonce(), NONCE);
    assert_eq!(token.binding().target_key(), physical_key(11));
    assert_eq!(token.binding().canonical_path(), &canonical_path(0));
    assert_eq!(token.durable_sequence(), 91);
    assert_eq!(token.ledger_generation(), 4);
}

#[test]
fn mismatched_durable_proof_fences_the_registry_and_retains_strong_identity() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    let weak_owner = Arc::downgrade(&owner);
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");
    let reservation = registry
        .prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 7, NONCE),
            &owner,
            &queue,
        )
        .expect("intent is prepared");
    let expected = reservation.binding().clone();
    let append = reservation.begin_append();
    let wrong_record = LedgerRecord::RetirementIntent {
        ticket_id: expected.ticket_id(),
        incarnation: expected.incarnation(),
        reason: RetirementReason::Reset,
        mapping_generation: expected.mapping_generation(),
        segment_offset: expected.segment_offset(),
        expected_length: expected.expected_length(),
        retirement_nonce: expected.retirement_nonce(),
        target_key: expected.target_key(),
        canonical_path: expected.canonical_path().clone(),
    };
    let wrong_evidence = DurableIntentEvidence::writer_verified_for_test(wrong_record, 4, 91, 92, 1_000)
        .expect("the wrong record is independently well formed");
    drop(owner);

    assert!(matches!(
        append.commit(wrong_evidence),
        Err(RegistryViolation::DurableEvidenceMismatch { .. })
    ));
    assert!(registry.needs_recovery());
    assert_eq!(registry.retained_identity_count(), 1);
    assert!(registry.is_path_reserved(&canonical_path(0)));
    assert!(weak_owner.upgrade().is_some());
    assert!(matches!(
        registry.prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 7, NONCE),
            &weak_owner.upgrade().expect("registry retains owner"),
            &queue,
        ),
        Err(RegistryViolation::NeedsRecovery)
    ));
}

#[test]
fn reservation_rollback_and_unwind_release_only_the_unproven_intent() {
    let registry = RetirementRegistry::new(store_uuid(), 8);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");

    let reservation = registry
        .prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
            &owner,
            &queue,
        )
        .expect("first reservation succeeds");
    assert_eq!(reservation.binding().ticket_id().get(), 9);
    reservation.rollback();
    assert_eq!(registry.ticket_high_water(), 8);
    assert!(registry.contains_incarnation(incarnation(1)));

    let panic_result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let reservation = registry
            .prepare_retirement(
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                &owner,
                &queue,
            )
            .expect("reservation succeeds before unwind");
        assert_eq!(reservation.binding().ticket_id().get(), 9);
        panic!("exercise reservation Drop during unwind");
    }));
    assert!(panic_result.is_err());

    let retry = registry
        .prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
            &owner,
            &queue,
        )
        .expect("unproven reservation was rolled back");
    assert_eq!(retry.binding().ticket_id().get(), 9);
}

#[test]
fn inflight_append_unwind_requires_replay_and_retains_the_reserved_identity() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner = Arc::new(TestOwner(1));
    let weak_owner = Arc::downgrade(&owner);
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");

    let unwind = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let append = registry
            .prepare_retirement(
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                &owner,
                &queue,
            )
            .expect("intent is reserved")
            .begin_append();
        assert_eq!(append.binding().ticket_id().get(), 1);
        panic!("writer outcome is now ambiguous");
    }));
    assert!(unwind.is_err());
    drop(owner);

    assert!(registry.needs_recovery());
    assert_eq!(registry.ticket_high_water(), 0);
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
    assert!(weak_owner.upgrade().is_some());
}

#[test]
fn replay_evidence_cannot_commit_an_inflight_writer_reservation() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");
    let append = registry
        .prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
            &owner,
            &queue,
        )
        .expect("intent is reserved")
        .begin_append();
    let evidence = DurableIntentEvidence::replay_verified_for_test(append.intent_record(), 1, 9, 10, 200)
        .expect("test replay proof is valid");

    assert_eq!(
        append.commit(evidence).expect_err("replay evidence is rejected"),
        RegistryViolation::WriterEvidenceRequired
    );
    assert!(registry.needs_recovery());
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
}

#[test]
fn concurrent_preparation_has_one_winner_without_holding_a_lock_across_work() {
    const CONTENDERS: usize = 8;
    let registry = Arc::new(RetirementRegistry::new(store_uuid(), 0));
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");
    let rendezvous = Arc::new(Barrier::new(CONTENDERS));

    let threads: Vec<_> = (0..CONTENDERS)
        .map(|_| {
            let registry = Arc::clone(&registry);
            let owner = Arc::clone(&owner);
            let queue = queue.clone();
            let rendezvous = Arc::clone(&rendezvous);
            std::thread::spawn(move || {
                let reservation = registry.prepare_retirement(
                    operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                    &owner,
                    &queue,
                );
                rendezvous.wait();
                reservation.is_ok()
            })
        })
        .collect();

    let winners = threads
        .into_iter()
        .map(|thread| thread.join().expect("contender does not panic"))
        .filter(|won| *won)
        .count();
    assert_eq!(winners, 1);
    assert_eq!(registry.ticket_high_water(), 0);
    assert!(registry.contains_incarnation(incarnation(1)));
}
