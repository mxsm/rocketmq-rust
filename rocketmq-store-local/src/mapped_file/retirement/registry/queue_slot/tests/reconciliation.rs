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
fn reconciled_queue_registers_complete_runtime_bindings_before_retirement() {
    let owner = Arc::new(TestOwner);
    let generation = ManagedMappedFileQueueGeneration::from_reconciled_members(vec![ManagedQueueMember::new(
        Arc::clone(&owner),
        incarnation(1),
        physical_key(31),
        canonical_path(0),
        0,
        FILE_LENGTH,
        7,
    )
    .expect("reconciled member identity is valid")])
    .expect("reconciled queue is valid");
    let registry = RetirementRegistry::new(store_uuid(), 0);

    generation
        .register_reconciled_members(&registry)
        .expect("the complete queue generation is registered atomically");

    let reservation = registry
        .prepare_retirement(operation(1, 31, 0, 7), &owner, &generation.queue_identity())
        .expect("registered owner and exact queue binding authorize retirement preparation");
    reservation.rollback();
    assert_eq!(registry.retained_identity_count(), 1);
}

#[test]
fn reconciled_queue_registration_failure_does_not_publish_a_partial_batch() {
    let incumbent_owner = Arc::new(TestOwner);
    let incumbent = ManagedMappedFileQueueGeneration::from_reconciled_members(vec![ManagedQueueMember::new(
        Arc::clone(&incumbent_owner),
        incarnation(1),
        physical_key(41),
        canonical_path(0),
        0,
        FILE_LENGTH,
        3,
    )
    .expect("incumbent identity is valid")])
    .expect("incumbent generation is valid");
    let registry = RetirementRegistry::new(store_uuid(), 0);
    incumbent
        .register_reconciled_members(&registry)
        .expect("incumbent is registered");

    let unique_owner = Arc::new(TestOwner);
    let conflicting_owner = Arc::new(TestOwner);
    let candidate = ManagedMappedFileQueueGeneration::from_reconciled_members(vec![
        ManagedQueueMember::new(
            Arc::clone(&unique_owner),
            incarnation(2),
            physical_key(42),
            canonical_path(FILE_LENGTH),
            FILE_LENGTH,
            FILE_LENGTH,
            4,
        )
        .expect("unique member is valid"),
        ManagedQueueMember::new(
            conflicting_owner,
            incarnation(3),
            physical_key(43),
            canonical_path(0),
            0,
            FILE_LENGTH,
            5,
        )
        .expect("conflicting member is structurally valid"),
    ])
    .expect("candidate generation is internally valid");

    assert!(matches!(
        candidate.register_reconciled_members(&registry),
        Err(RegistryViolation::CanonicalPathReserved { .. })
    ));
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(!registry.contains_incarnation(incarnation(2)));
    assert!(!registry.contains_incarnation(incarnation(3)));
    assert_eq!(registry.retained_identity_count(), 1);
}
