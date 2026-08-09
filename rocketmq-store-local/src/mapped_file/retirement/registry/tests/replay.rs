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

#[test]
fn writer_proof_rejected_by_replay_restore_recovery_fences_the_current_store() {
    let registry = RetirementRegistry::new(store_uuid(), 7);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");
    let record = intent_record(
        7,
        operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
    );
    let writer_evidence = DurableIntentEvidence::writer_verified_for_test(record.clone(), 3, 70, 71, 900)
        .expect("writer evidence is valid");
    assert!(matches!(
        registry.restore_replayed_intent(writer_evidence, &owner, &queue),
        Err(RegistryError::ReplayEvidenceRequired)
    ));
    assert!(registry.needs_recovery());
}

#[test]
fn replay_proof_restores_a_durable_intent_and_duplicate_proof_recovery_fences() {
    let registry = RetirementRegistry::new(store_uuid(), 7);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");
    let record = intent_record(
        7,
        operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
    );

    let replay_evidence = DurableIntentEvidence::replay_verified_for_test(record.clone(), 3, 70, 71, 900)
        .expect("replay evidence is valid");
    let token = registry
        .restore_replayed_intent(replay_evidence, &owner, &queue)
        .expect("opaque replay proof restores one exact durable intent");
    assert_eq!(token.binding().ticket_id().get(), 7);
    assert_eq!(token.binding().reason(), RetirementReason::StoreDestroy);
    assert_eq!(registry.ticket_high_water(), 7);

    let duplicate_evidence = DurableIntentEvidence::replay_verified_for_test(record, 3, 70, 71, 900)
        .expect("duplicate proof is independently well formed");
    assert!(matches!(
        registry.restore_replayed_intent(duplicate_evidence, &owner, &queue),
        Err(RegistryError::DuplicateTicket { ticket_id }) if ticket_id.get() == 7
    ));
    assert!(registry.needs_recovery());
}

#[test]
fn every_current_store_replay_rejection_recovery_fences() {
    {
        let (registry, owner, queue) = registered_registry(1);
        let _reservation = registry
            .prepare_retirement(
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
                &owner,
                &queue,
            )
            .expect("live intent is reserved");
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
            ),
            &owner,
            &queue,
            RegistryError::IntentReservationBusy,
        );
    }
    {
        let (registry, owner, queue) = registered_registry(1);
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                2,
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
            ),
            &owner,
            &queue,
            RegistryError::ReplayedTicketAboveHighWater {
                ticket_id: crate::mapped_file::retirement::identity::TicketId::new(2).expect("test ticket is nonzero"),
                high_water: 1,
            },
        );
    }
    {
        let registry = RetirementRegistry::new(store_uuid(), 1);
        let owner = Arc::new(TestOwner(1));
        let queue = QueueIdentity::allocate();
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation(
                    2,
                    12,
                    FILE_LENGTH,
                    FILE_LENGTH,
                    RetirementReason::StoreDestroy,
                    8,
                    NONCE,
                ),
            ),
            &owner,
            &queue,
            RegistryError::UnknownIncarnation {
                incarnation: incarnation(2),
            },
        );
    }
    {
        let (registry, owner, queue) = registered_registry(1);
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation(1, 12, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
            ),
            &owner,
            &queue,
            RegistryError::PhysicalKeyMismatch {
                incarnation: incarnation(1),
            },
        );
    }
    {
        let (registry, owner, queue) = registered_registry(1);
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation(
                    1,
                    11,
                    FILE_LENGTH,
                    FILE_LENGTH,
                    RetirementReason::StoreDestroy,
                    8,
                    NONCE,
                ),
            ),
            &owner,
            &queue,
            RegistryError::SegmentOffsetMismatch {
                incarnation: incarnation(1),
            },
        );
    }
    {
        let (registry, owner, queue) = registered_registry(1);
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation_with_path(
                    1,
                    11,
                    0,
                    FILE_LENGTH,
                    "consumequeue/topic/00000000000000000000",
                    RetirementReason::StoreDestroy,
                    8,
                    NONCE,
                ),
            ),
            &owner,
            &queue,
            RegistryError::CanonicalPathMismatch {
                incarnation: incarnation(1),
            },
        );
    }
    {
        let (registry, owner, queue) = registered_registry(1);
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation(1, 11, 0, FILE_LENGTH * 2, RetirementReason::StoreDestroy, 8, NONCE),
            ),
            &owner,
            &queue,
            RegistryError::ExpectedLengthMismatch {
                incarnation: incarnation(1),
            },
        );
    }
    {
        let (registry, _owner, queue) = registered_registry(1);
        let other_owner = Arc::new(TestOwner(2));
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
            ),
            &other_owner,
            &queue,
            RegistryError::OwnerIdentityMismatch {
                incarnation: incarnation(1),
            },
        );
    }
    {
        let (registry, owner, _queue) = registered_registry(1);
        let other_queue = QueueIdentity::allocate();
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                1,
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
            ),
            &owner,
            &other_queue,
            RegistryError::QueueIdentityMismatch {
                incarnation: incarnation(1),
            },
        );
    }
    {
        let (registry, owner, queue) = registered_registry(2);
        let first = replay_test_evidence(
            1,
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, NONCE),
        );
        let _token = registry
            .restore_replayed_intent(first, &owner, &queue)
            .expect("first replay proof restores the durable intent");
        assert_replay_rejection_fences(
            &registry,
            replay_test_evidence(
                2,
                operation(1, 11, 0, FILE_LENGTH, RetirementReason::StoreDestroy, 8, OTHER_NONCE),
            ),
            &owner,
            &queue,
            RegistryError::IncarnationNotActive {
                incarnation: incarnation(1),
            },
        );
    }
}

#[test]
fn foreign_store_replay_evidence_is_rejected_without_fencing() {
    let (registry, owner, queue) = registered_registry(1);
    let foreign_store = StoreUuid::new([0x24; 16]).expect("foreign Store UUID is nonzero");
    let foreign_operation = RetirementOperation::new(
        FileIncarnationId::new(foreign_store, 1).expect("foreign incarnation is nonzero"),
        RetirementReason::StoreDestroy,
        8,
        0,
        FILE_LENGTH,
        NONCE,
        physical_key(11),
        canonical_path(0),
    )
    .expect("foreign operation is structurally valid");
    let foreign_record = intent_record(1, foreign_operation);
    let replay_evidence = DurableIntentEvidence::replay_verified_for_test(foreign_record.clone(), 3, 71, 72, 1_100)
        .expect("foreign replay evidence is valid");

    assert_eq!(
        registry
            .restore_replayed_intent(replay_evidence, &owner, &queue)
            .expect_err("foreign evidence is rejected"),
        RegistryError::StoreUuidMismatch
    );
    assert!(!registry.needs_recovery());
    let writer_evidence = DurableIntentEvidence::writer_verified_for_test(foreign_record, 3, 71, 72, 1_100)
        .expect("foreign writer evidence is valid");
    assert_eq!(
        registry
            .restore_replayed_intent(writer_evidence, &owner, &queue)
            .expect_err("foreign writer evidence is rejected before source validation"),
        RegistryError::StoreUuidMismatch
    );
    assert!(!registry.needs_recovery());
    registry
        .prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
            &owner,
            &queue,
        )
        .expect("foreign evidence does not disable the current store")
        .rollback();
}

#[test]
fn ticket_high_water_overflow_is_typed_and_retains_the_active_identity() {
    let registry = RetirementRegistry::new(store_uuid(), u64::MAX);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");

    assert!(matches!(
        registry.prepare_retirement(
            operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 1, NONCE),
            &owner,
            &queue,
        ),
        Err(RegistryError::TicketHighWaterExhausted)
    ));
    assert_eq!(registry.ticket_high_water(), u64::MAX);
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
}
