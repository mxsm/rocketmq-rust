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
use crate::mapped_file::retirement::writer::{
    IncarnationAllocationPlan, ManagedLedgerWriter, PublishedIncarnationReceipt,
};

#[test]
fn only_a_durable_publish_receipt_can_make_a_managed_owner_visible() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let queue = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);

    queue
        .publish_created_member(
            &registry,
            published_receipt(1, 0, physical_key(11)),
            Arc::clone(&owner),
            7,
        )
        .expect("durably published owner enters the queue");

    let snapshot = queue.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert!(Arc::ptr_eq(&snapshot[0], &owner));
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
}

#[test]
fn publication_failure_returns_the_exact_receipt_and_owner_for_recovery() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let queue = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    let receipt = published_receipt(1, 0, physical_key(11));

    let failure = queue
        .publish_created_member(&registry, receipt, Arc::clone(&owner), 0)
        .expect_err("zero mapping generation is rejected before publication");
    let (receipt, returned_owner, error) = failure.into_parts();
    assert!(Arc::ptr_eq(&returned_owner, &owner));
    assert_eq!(error, RegistryError::ZeroMappingGeneration);
    assert!(queue.snapshot().is_empty());
    assert!(!registry.contains_incarnation(incarnation(1)));

    queue
        .publish_created_member(&registry, receipt, returned_owner, 7)
        .expect("the exact returned capability can be retried");
    assert_eq!(queue.snapshot().len(), 1);
}

#[test]
fn a_reserved_canonical_path_rejects_a_second_durable_incarnation_without_partial_publication() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let queue = ManagedMappedFileQueueGeneration::new_write_disabled();
    queue
        .publish_created_member(
            &registry,
            published_receipt(1, 0, physical_key(11)),
            Arc::new(TestOwner),
            7,
        )
        .expect("first incarnation publishes");

    let second_owner = Arc::new(TestOwner);
    let failure = queue
        .publish_created_member(
            &registry,
            published_receipt(2, 0, physical_key(12)),
            Arc::clone(&second_owner),
            8,
        )
        .expect_err("canonical path is already reserved");
    let (_receipt, returned_owner, error) = failure.into_parts();
    assert!(Arc::ptr_eq(&returned_owner, &second_owner));
    assert!(matches!(error, RegistryError::CanonicalPathReserved { .. }));
    assert_eq!(queue.snapshot().len(), 1);
    assert!(!registry.contains_incarnation(incarnation(2)));
}

fn published_receipt(create_sequence: u64, offset: u64, key: PhysicalFileKey) -> PublishedIncarnationReceipt {
    let mut writer =
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x33; 16], 2, 100, 77, 0, true, 5)
            .expect("managed writer");
    let nonce = [create_sequence as u8; 16];
    let canonical = canonical_path(offset);
    let create = StoreRelativePath::new(&format!(
        "commitlog/.create.i{create_sequence:016x}.s{offset:020}.n{}",
        hex_nonce(nonce)
    ))
    .expect("create path");
    let plan = IncarnationAllocationPlan::new(
        incarnation(create_sequence),
        offset,
        FILE_LENGTH,
        nonce,
        canonical,
        create,
    )
    .expect("allocation plan");
    let allocated = writer.append_allocate_incarnation(plan).expect("Allocate is durable");
    let bound = writer.append_bind_incarnation(allocated, key).expect("Bind is durable");
    writer.append_publish_incarnation(bound).expect("Publish is durable")
}

fn hex_nonce(nonce: [u8; 16]) -> String {
    nonce.iter().map(|byte| format!("{byte:02x}")).collect()
}
