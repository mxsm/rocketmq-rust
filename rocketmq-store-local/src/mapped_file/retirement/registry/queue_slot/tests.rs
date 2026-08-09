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

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

// These tests exercise the registry-owned queue-slot trust boundary.
use super::*;
use crate::mapped_file::retirement::codec::DecodeOutcome;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::codec::COMMIT_SEAL_LENGTH;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::platform::authorize_namespace_transition;
use crate::mapped_file::retirement::platform::authorize_tombstone_removal;
use crate::mapped_file::retirement::platform::NamespaceAbsenceProof;
use crate::mapped_file::retirement::platform::NamespaceTombstoneProof;
use crate::mapped_file::retirement::platform::NamespaceTransition;
use crate::mapped_file::retirement::registry::DurableIntentEvidence;
use crate::mapped_file::retirement::registry::DurableRetirementToken;
use crate::mapped_file::retirement::registry::PublishedFileRegistration;
use crate::mapped_file::retirement::registry::RegistryError;
use crate::mapped_file::retirement::registry::RetirementIntentBinding;
use crate::mapped_file::retirement::registry::RetirementOperation;
use crate::mapped_file::retirement::registry::RetirementRegistry;
use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
use crate::mapped_file::retirement::writer::ManagedLedgerWriter;

mod creation;
mod interleaving;
mod reconciliation;

const FILE_LENGTH: u64 = 1_024;

#[derive(Debug)]
struct TestOwner;

fn store_uuid() -> StoreUuid {
    StoreUuid::new([0x52; 16]).expect("test Store UUID is nonzero")
}

fn incarnation(create_sequence: u64) -> FileIncarnationId {
    FileIncarnationId::new(store_uuid(), create_sequence).expect("test incarnation is nonzero")
}

fn physical_key(inode: u64) -> PhysicalFileKey {
    PhysicalFileKey::unix(7, inode)
}

fn canonical_path(offset: u64) -> StoreRelativePath {
    StoreRelativePath::new(&format!("commitlog/{offset:020}")).expect("test path is canonical")
}

fn register_managed_owner(
    registry: &RetirementRegistry<TestOwner>,
    slot: &ManagedMappedFileQueueGeneration<TestOwner>,
    owner: Arc<TestOwner>,
    create_sequence: u64,
    inode: u64,
    offset: u64,
    mapping_generation: u64,
) {
    slot.install_managed_member_for_test(
        Arc::clone(&owner),
        incarnation(create_sequence),
        physical_key(inode),
        canonical_path(offset),
        offset,
        FILE_LENGTH,
        mapping_generation,
    )
    .expect("managed member is installed once");
    registry
        .register_published(
            PublishedFileRegistration::new(
                incarnation(create_sequence),
                physical_key(inode),
                canonical_path(offset),
                offset,
                FILE_LENGTH,
                owner,
                slot.queue_identity(),
            )
            .expect("published registration is valid"),
        )
        .expect("published identity is registered");
}

fn operation(create_sequence: u64, inode: u64, offset: u64, mapping_generation: u64) -> RetirementOperation {
    RetirementOperation::new(
        incarnation(create_sequence),
        RetirementReason::TtlExpired,
        mapping_generation,
        offset,
        FILE_LENGTH,
        [create_sequence as u8; 16],
        physical_key(inode),
        canonical_path(offset),
    )
    .expect("retirement operation is valid")
}

fn commit_token(
    registry: &RetirementRegistry<TestOwner>,
    owner: &Arc<TestOwner>,
    slot: &ManagedMappedFileQueueGeneration<TestOwner>,
    operation: RetirementOperation,
) -> (DurableRetirementToken<TestOwner>, RetirementIntentBinding) {
    let reservation = registry
        .prepare_retirement(operation, owner, &slot.queue_identity())
        .expect("intent is prepared");
    let binding = reservation.binding().clone();
    let append = reservation.begin_append();
    let evidence = DurableIntentEvidence::writer_verified_for_test(
        append.intent_record(),
        4,
        binding.ticket_id().get() + 30,
        binding.ticket_id().get() + 31,
        1_000 + binding.ticket_id().get() * 200,
    )
    .expect("test writer evidence is valid");
    let token = append.commit(evidence).expect("intent is committed");
    (token, binding)
}

fn intent_record(ticket: u64, operation: RetirementOperation) -> LedgerRecord {
    LedgerRecord::RetirementIntent {
        ticket_id: crate::mapped_file::retirement::identity::TicketId::new(ticket).expect("test ticket is nonzero"),
        incarnation: operation.incarnation(),
        reason: operation.reason(),
        mapping_generation: operation.mapping_generation(),
        segment_offset: operation.segment_offset(),
        expected_length: operation.expected_length(),
        retirement_nonce: operation.retirement_nonce(),
        target_key: operation.target_key(),
        canonical_path: operation.canonical_path().clone(),
    }
}

#[test]
fn read_snapshot_retains_its_generation_and_legacy_recovery_install_is_explicit() {
    let first = Arc::new(TestOwner);
    let second = Arc::new(TestOwner);
    let generation = MappedFileQueueGeneration::from_recovery_files(vec![Arc::clone(&first)]);
    let old_snapshot = generation.snapshot();

    generation.install_recovery_generation(vec![Arc::clone(&second)]);

    assert_eq!(old_snapshot.len(), 1);
    assert!(Arc::ptr_eq(&old_snapshot[0], &first));
    let current = generation.snapshot();
    assert_eq!(current.len(), 1);
    assert!(Arc::ptr_eq(&current[0], &second));
}

#[test]
fn wrong_queue_binding_owner_and_mapping_generation_return_the_original_token() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let wrong_slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner_a = Arc::new(TestOwner);
    let owner_b = Arc::new(TestOwner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner_a), 1, 11, 0, 3);
    register_managed_owner(&registry, &slot, Arc::clone(&owner_b), 2, 12, FILE_LENGTH, 4);

    let (token_a, binding_a) = commit_token(&registry, &owner_a, &slot, operation(1, 11, 0, 3));
    let (_token_b, binding_b) = commit_token(&registry, &owner_b, &slot, operation(2, 12, FILE_LENGTH, 4));

    let failure = wrong_slot
        .handoff_retirement(&registry, token_a, &binding_a)
        .expect_err("a foreign queue slot cannot consume the token");
    let (token_a, reason) = failure
        .into_retryable_parts()
        .expect("queue mismatch preserves the token");
    assert!(matches!(
        reason,
        QueueHandoffFailureReason::Registry(RegistryError::TokenQueueIdentityMismatch { .. })
    ));

    let failure = slot
        .handoff_retirement(&registry, token_a, &binding_b)
        .expect_err("a different durable binding cannot consume the token");
    let (token_a, reason) = failure
        .into_retryable_parts()
        .expect("binding mismatch preserves the token");
    assert!(matches!(
        reason,
        QueueHandoffFailureReason::Registry(RegistryError::TokenBindingMismatch { .. })
    ));

    slot.remove_member_for_conflict_test(&owner_a);
    let failure = slot
        .handoff_retirement(&registry, token_a, &binding_a)
        .expect_err("a missing exact owner cannot be removed");
    let (token_a, reason) = failure
        .into_retryable_parts()
        .expect("owner mismatch preserves the token");
    assert_eq!(reason, QueueHandoffFailureReason::CandidateMissing);

    slot.install_member_for_conflict_test(Arc::clone(&owner_a));
    slot.set_managed_generation_for_test(&owner_a, 9);
    let failure = slot
        .handoff_retirement(&registry, token_a, &binding_a)
        .expect_err("a stale mapping generation cannot be removed");
    let (token_a, reason) = failure
        .into_retryable_parts()
        .expect("generation mismatch preserves the token");
    assert_eq!(
        reason,
        QueueHandoffFailureReason::MappingGenerationMismatch { expected: 3, actual: 9 }
    );

    drop(token_a);
    assert!(registry.needs_recovery());
    assert!(registry.contains_incarnation(incarnation(1)));
}

#[test]
fn one_cas_conflict_returns_the_token_and_an_explicit_retry_succeeds() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    let concurrent = Arc::new(TestOwner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner), 1, 11, 0, 3);
    let (token, binding) = commit_token(&registry, &owner, &slot, operation(1, 11, 0, 3));

    let failure = slot
        .handoff_retirement_with_before_cas_for_test(&registry, token, &binding, || {
            slot.install_member_for_conflict_test(Arc::clone(&concurrent));
        })
        .expect_err("the exact single CAS observes a conflict");
    let (token, reason) = failure
        .into_retryable_parts()
        .expect("CAS conflict preserves the token");
    assert_eq!(reason, QueueHandoffFailureReason::CompareExchangeConflict);
    assert!(slot.snapshot().iter().any(|candidate| Arc::ptr_eq(candidate, &owner)));

    let capability = slot
        .handoff_retirement(&registry, token, &binding)
        .expect("the caller may explicitly retry with the same token");
    assert_eq!(capability.binding(), &binding);
    assert!(!slot.snapshot().iter().any(|candidate| Arc::ptr_eq(candidate, &owner)));
    assert!(slot
        .snapshot()
        .iter()
        .any(|candidate| Arc::ptr_eq(candidate, &concurrent)));
}

#[test]
fn concurrent_handoffs_have_one_cas_winner_and_the_loser_keeps_its_token() {
    let registry = Arc::new(RetirementRegistry::new(store_uuid(), 0));
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner_a = Arc::new(TestOwner);
    let owner_b = Arc::new(TestOwner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner_a), 1, 11, 0, 3);
    register_managed_owner(&registry, &slot, Arc::clone(&owner_b), 2, 12, FILE_LENGTH, 4);
    let (token_a, binding_a) = commit_token(&registry, &owner_a, &slot, operation(1, 11, 0, 3));
    let (token_b, binding_b) = commit_token(&registry, &owner_b, &slot, operation(2, 12, FILE_LENGTH, 4));
    let barrier = Arc::new(Barrier::new(2));

    let run = |token, binding: RetirementIntentBinding| {
        let registry = Arc::clone(&registry);
        let slot = slot.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let outcome = slot.handoff_retirement_with_before_cas_for_test(&registry, token, &binding, || {
                barrier.wait();
            });
            match outcome {
                Ok(capability) => Ok((capability, binding)),
                Err(failure) => Err(Box::new((failure, binding))),
            }
        })
    };

    let first = run(token_a, binding_a);
    let second = run(token_b, binding_b);
    let outcomes = [
        first.join().expect("first handoff thread"),
        second.join().expect("second handoff thread"),
    ];
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_err()).count(), 1);

    let mut winning_capability = None;
    let mut losing_attempt = None;
    for outcome in outcomes {
        match outcome {
            Ok((capability, _)) => winning_capability = Some(capability),
            Err(failure) => losing_attempt = Some(*failure),
        }
    }
    let winning_capability = winning_capability.expect("one CAS wins");
    let (failure, binding) = losing_attempt.expect("one CAS loses");
    let (token, reason) = failure
        .into_retryable_parts()
        .expect("the losing CAS preserves its token");
    assert_eq!(reason, QueueHandoffFailureReason::CompareExchangeConflict);
    let retried_capability = slot
        .handoff_retirement(&registry, token, &binding)
        .expect("the losing token succeeds after an explicit retry");
    assert!(slot.snapshot().is_empty());
    assert!(!registry.needs_recovery());

    drop(winning_capability);
    drop(retried_capability);
    assert!(registry.needs_recovery());
}

#[test]
fn dropping_an_issued_token_recovery_fences_without_releasing_registry_identity() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    let weak_owner = Arc::downgrade(&owner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner), 1, 11, 0, 3);
    let (token, _) = commit_token(&registry, &owner, &slot, operation(1, 11, 0, 3));
    drop(owner);

    drop(token);

    assert!(registry.needs_recovery());
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
    assert!(weak_owner.upgrade().is_some());
}

#[test]
fn replay_evidence_cannot_bypass_the_queue_slot_identity() {
    let registry = RetirementRegistry::new(store_uuid(), 1);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let foreign_slot = ManagedMappedFileQueueGeneration::<TestOwner>::new_write_disabled();
    let owner = Arc::new(TestOwner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner), 1, 11, 0, 3);
    let operation = operation(1, 11, 0, 3);
    let evidence = DurableIntentEvidence::replay_verified_for_test(intent_record(1, operation), 4, 31, 32, 1_200)
        .expect("test replay evidence is valid");

    assert_eq!(
        registry
            .restore_replayed_intent(evidence, &owner, &foreign_slot.queue_identity())
            .expect_err("the replay token is bound to the owning queue slot"),
        RegistryError::QueueIdentityMismatch {
            incarnation: incarnation(1),
        }
    );
    assert!(registry.needs_recovery());
}

#[test]
fn dropping_a_successful_queue_handoff_requires_replay_and_retains_the_owner() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    let weak_owner = Arc::downgrade(&owner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner), 1, 11, 0, 3);
    let (token, binding) = commit_token(&registry, &owner, &slot, operation(1, 11, 0, 3));
    let capability = slot
        .handoff_retirement(&registry, token, &binding)
        .expect("the exact queue handoff succeeds");
    drop(owner);

    drop(capability);

    assert!(registry.needs_recovery());
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
    assert!(weak_owner.upgrade().is_some());
}

#[test]
fn real_writer_and_registry_preserve_the_full_direct_unlink_stage_chain() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    let weak_owner = Arc::downgrade(&owner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner), 1, 11, 0, 3);
    let reservation = registry
        .prepare_retirement(operation(1, 11, 0, 3), &owner, &slot.queue_identity())
        .expect("intent is prepared");
    let binding = reservation.binding().clone();
    let mut ledger =
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x31; 16], 4, 91, 92, 0, true, 5)
            .expect("replay cursor is valid");
    let token = ledger
        .append_retirement_intent(reservation.begin_append())
        .expect("intent is durably written");
    let capability = slot
        .handoff_retirement(&registry, token, &binding)
        .expect("the exact queue handoff succeeds");

    let logical_removed = ledger
        .append_logical_removed(capability)
        .expect("LogicalRemoved is durably written and committed");

    assert_eq!(logical_removed.binding(), &binding);
    assert_eq!(logical_removed.durable_sequence(), 92);
    assert_eq!(
        registry.logical_removed_sequence_for_test(binding.ticket_id()),
        Some(92)
    );
    assert!(!registry.needs_recovery());
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
    assert!(weak_owner.upgrade().is_some());
    assert!(slot.snapshot().is_empty());

    let namespace = authorize_namespace_transition(logical_removed, NamespaceTransition::DirectUnlink)
        .expect("LogicalRemoved authorizes its exact namespace request");
    assert_eq!(namespace.request().ticket().ticket_id(), binding.ticket_id());
    assert_eq!(namespace.request().ticket().incarnation(), binding.incarnation());
    assert_eq!(namespace.request().ticket().reason(), binding.reason());
    assert_eq!(
        namespace.request().ticket().mapping_generation(),
        binding.mapping_generation()
    );
    assert_eq!(namespace.request().ticket().segment_offset(), binding.segment_offset());
    assert_eq!(
        namespace.request().ticket().expected_length(),
        binding.expected_length()
    );
    assert_eq!(
        namespace.request().ticket().retirement_nonce(),
        &binding.retirement_nonce()
    );
    assert_eq!(namespace.request().physical_key(), binding.target_key());
    assert_eq!(namespace.request().canonical_path(), binding.canonical_path());
    assert_eq!(
        namespace.request().tombstone_path().as_str(),
        "commitlog/.delete.t0000000000000001.i0000000000000001.s00000000000000000000.m0000000000000003.n01010101010101010101010101010101"
    );

    let (logical_removed, request) = namespace.into_parts_for_test();
    let absence = NamespaceAbsenceProof::verified_for_test(&request, None);
    drop(owner);
    let namespace_absent = ledger
        .append_namespace_absent(logical_removed, absence, 123)
        .expect("NamespaceAbsent is durably written and committed");
    assert_eq!(namespace_absent.durable_sequence(), 93);
    assert!(weak_owner.upgrade().is_some());
    let completed = ledger
        .append_completed(namespace_absent, 124)
        .expect("Completed is durably written and committed");

    assert_eq!(completed.binding(), &binding);
    assert_eq!(completed.durable_sequence(), 94);
    assert!(!registry.needs_recovery());
    assert_eq!(registry.retained_identity_count(), 0);
    assert!(!registry.contains_incarnation(incarnation(1)));
    assert!(!registry.is_path_reserved(&canonical_path(0)));
    assert!(weak_owner.upgrade().is_none());

    let log = ledger.io_for_test().log();
    let first =
        match crate::mapped_file::retirement::codec::decode_next_frame(log, 91, 4).expect("intent frame is valid") {
            DecodeOutcome::Frame(frame) => frame,
            other => panic!("expected intent frame, got {other:?}"),
        };
    let second_offset = first.encoded_len() + COMMIT_SEAL_LENGTH;
    let second = match crate::mapped_file::retirement::codec::decode_next_frame(&log[second_offset..], 92, 4)
        .expect("LogicalRemoved frame is valid")
    {
        DecodeOutcome::Frame(frame) => frame,
        other => panic!("expected LogicalRemoved frame, got {other:?}"),
    };
    assert_eq!(
        second.decode_record().expect("typed record decodes"),
        Some(LedgerRecord::LogicalRemoved {
            ticket_id: binding.ticket_id(),
            incarnation: binding.incarnation(),
            target_key: binding.target_key(),
            canonical_path: binding.canonical_path().clone(),
        })
    );
    let third_offset = second_offset + second.encoded_len() + COMMIT_SEAL_LENGTH;
    let third = match crate::mapped_file::retirement::codec::decode_next_frame(&log[third_offset..], 93, 4)
        .expect("NamespaceAbsent frame is valid")
    {
        DecodeOutcome::Frame(frame) => frame,
        other => panic!("expected NamespaceAbsent frame, got {other:?}"),
    };
    assert_eq!(
        third.decode_record().expect("typed record decodes"),
        Some(LedgerRecord::NamespaceAbsent {
            ticket_id: binding.ticket_id(),
            incarnation: binding.incarnation(),
            replacement_observed: false,
            observation_time_ns: 123,
            target_key: binding.target_key(),
            canonical_path: binding.canonical_path().clone(),
            tombstone_path: None,
        })
    );
    let fourth_offset = third_offset + third.encoded_len() + COMMIT_SEAL_LENGTH;
    let fourth = match crate::mapped_file::retirement::codec::decode_next_frame(&log[fourth_offset..], 94, 4)
        .expect("Completed frame is valid")
    {
        DecodeOutcome::Frame(frame) => frame,
        other => panic!("expected Completed frame, got {other:?}"),
    };
    assert_eq!(
        fourth.decode_record().expect("typed record decodes"),
        Some(LedgerRecord::Completed {
            ticket_id: binding.ticket_id(),
            incarnation: binding.incarnation(),
            completion_time_ns: 124,
            namespace_absent_sequence: 93,
        })
    );
}

#[test]
fn real_writer_and_registry_preserve_the_full_tombstone_stage_chain() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    let weak_owner = Arc::downgrade(&owner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner), 1, 17, 0, 4);
    let reservation = registry
        .prepare_retirement(operation(1, 17, 0, 4), &owner, &slot.queue_identity())
        .expect("intent is prepared");
    let binding = reservation.binding().clone();
    let mut ledger = ManagedLedgerWriter::for_test(
        ModelLedgerIo::empty(),
        store_uuid(),
        [0x41; 16],
        5,
        201,
        301,
        0,
        true,
        6,
    )
    .expect("replay cursor is valid");
    let token = ledger
        .append_retirement_intent(reservation.begin_append())
        .expect("intent is durably written");
    let handoff = slot
        .handoff_retirement(&registry, token, &binding)
        .expect("the exact queue handoff succeeds");
    let logical_removed = ledger
        .append_logical_removed(handoff)
        .expect("LogicalRemoved is durable");

    let move_authorization = authorize_namespace_transition(logical_removed, NamespaceTransition::MoveToTombstone)
        .expect("LogicalRemoved authorizes the exact rename");
    let (logical_removed, request) = move_authorization.into_parts_for_test();
    let tombstone_proof = NamespaceTombstoneProof::verified_for_test(&request, Some(physical_key(99)));
    let tombstoned = ledger
        .append_tombstoned(logical_removed, tombstone_proof)
        .expect("Tombstoned is durable");
    assert_eq!(tombstoned.durable_sequence(), 203);
    assert!(weak_owner.upgrade().is_some());

    let remove_authorization =
        authorize_tombstone_removal(tombstoned).expect("Tombstoned authorizes only its exact tombstone removal");
    assert_eq!(remove_authorization.transition(), NamespaceTransition::RemoveTombstone);
    let (tombstoned, request) = remove_authorization.into_parts_for_test();
    let absence_proof = NamespaceAbsenceProof::verified_for_test(&request, None);
    drop(owner);
    let namespace_absent = ledger
        .append_namespace_absent_after_tombstone(tombstoned, absence_proof, 223)
        .expect("NamespaceAbsent is durable");
    assert_eq!(namespace_absent.durable_sequence(), 204);
    assert!(weak_owner.upgrade().is_some());
    let completed = ledger
        .append_completed(namespace_absent, 224)
        .expect("Completed is durable");

    assert_eq!(completed.durable_sequence(), 205);
    assert_eq!(registry.retained_identity_count(), 0);
    assert!(weak_owner.upgrade().is_none());

    assert_eq!(
        decode_records(ledger.io_for_test().log(), 201, 5, 5),
        vec![
            LedgerRecord::RetirementIntent {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                reason: binding.reason(),
                mapping_generation: binding.mapping_generation(),
                segment_offset: binding.segment_offset(),
                expected_length: binding.expected_length(),
                retirement_nonce: binding.retirement_nonce(),
                target_key: binding.target_key(),
                canonical_path: binding.canonical_path().clone(),
            },
            LedgerRecord::LogicalRemoved {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                target_key: binding.target_key(),
                canonical_path: binding.canonical_path().clone(),
            },
            LedgerRecord::Tombstoned {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                target_key: binding.target_key(),
                retirement_nonce: binding.retirement_nonce(),
                canonical_path: binding.canonical_path().clone(),
                tombstone_path: request.tombstone_path().clone(),
            },
            LedgerRecord::NamespaceAbsent {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                replacement_observed: true,
                observation_time_ns: 223,
                target_key: binding.target_key(),
                canonical_path: binding.canonical_path().clone(),
                tombstone_path: Some(request.tombstone_path().clone()),
            },
            LedgerRecord::Completed {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                completion_time_ns: 224,
                namespace_absent_sequence: 204,
            },
        ]
    );
}

#[test]
fn superseded_path_is_sticky_without_replacing_the_durable_retirement_stage() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let slot = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    let weak_owner = Arc::downgrade(&owner);
    register_managed_owner(&registry, &slot, Arc::clone(&owner), 1, 23, 0, 5);
    let reservation = registry
        .prepare_retirement(operation(1, 23, 0, 5), &owner, &slot.queue_identity())
        .expect("intent is prepared");
    let binding = reservation.binding().clone();
    let mut ledger = ManagedLedgerWriter::for_test(
        ModelLedgerIo::empty(),
        store_uuid(),
        [0x51; 16],
        6,
        401,
        501,
        0,
        true,
        7,
    )
    .expect("replay cursor is valid");
    let token = ledger
        .append_retirement_intent(reservation.begin_append())
        .expect("intent is durable");
    let handoff = slot
        .handoff_retirement(&registry, token, &binding)
        .expect("queue handoff succeeds");
    let logical_removed = ledger
        .append_logical_removed(handoff)
        .expect("LogicalRemoved is durable");
    let replacement = physical_key(99);
    let logical_removed = ledger
        .append_superseded_path_after_logical(logical_removed, replacement)
        .expect("SupersededPath is durable without advancing the main stage");

    assert_eq!(logical_removed.durable_sequence(), 402);
    let authorization = authorize_namespace_transition(logical_removed, NamespaceTransition::DirectUnlink)
        .expect("the original LogicalRemoved stage still authorizes convergence");
    let (logical_removed, request) = authorization.into_parts_for_test();
    let absence = NamespaceAbsenceProof::verified_for_test(&request, Some(replacement));
    drop(owner);
    let namespace_absent = ledger
        .append_namespace_absent(logical_removed, absence, 423)
        .expect("the next stage follows the annotation append cursor");
    assert_eq!(namespace_absent.durable_sequence(), 404);
    let completed = ledger
        .append_completed(namespace_absent, 424)
        .expect("Completed is durable");

    assert_eq!(completed.durable_sequence(), 405);
    assert_eq!(registry.retained_identity_count(), 0);
    assert!(weak_owner.upgrade().is_none());
    assert_eq!(
        decode_records(ledger.io_for_test().log(), 401, 6, 5),
        vec![
            LedgerRecord::RetirementIntent {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                reason: binding.reason(),
                mapping_generation: binding.mapping_generation(),
                segment_offset: binding.segment_offset(),
                expected_length: binding.expected_length(),
                retirement_nonce: binding.retirement_nonce(),
                target_key: binding.target_key(),
                canonical_path: binding.canonical_path().clone(),
            },
            LedgerRecord::LogicalRemoved {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                target_key: binding.target_key(),
                canonical_path: binding.canonical_path().clone(),
            },
            LedgerRecord::SupersededPath {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                expected_target_key: binding.target_key(),
                observed_replacement_key: replacement,
                canonical_path: binding.canonical_path().clone(),
            },
            LedgerRecord::NamespaceAbsent {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                replacement_observed: true,
                observation_time_ns: 423,
                target_key: binding.target_key(),
                canonical_path: binding.canonical_path().clone(),
                tombstone_path: None,
            },
            LedgerRecord::Completed {
                ticket_id: binding.ticket_id(),
                incarnation: binding.incarnation(),
                completion_time_ns: 424,
                namespace_absent_sequence: 404,
            },
        ]
    );
}

fn decode_records(log: &[u8], first_sequence: u64, generation: u64, count: usize) -> Vec<LedgerRecord> {
    let mut offset = 0;
    let mut records = Vec::with_capacity(count);
    for index in 0..count {
        let sequence = first_sequence + index as u64;
        let frame = match crate::mapped_file::retirement::codec::decode_next_frame(&log[offset..], sequence, generation)
            .expect("frame is valid")
        {
            DecodeOutcome::Frame(frame) => frame,
            other => panic!("expected frame {sequence}, got {other:?}"),
        };
        offset += frame.encoded_len() + COMMIT_SEAL_LENGTH;
        records.push(
            frame
                .decode_record()
                .expect("typed record decodes")
                .expect("known record is present"),
        );
    }
    records
}
