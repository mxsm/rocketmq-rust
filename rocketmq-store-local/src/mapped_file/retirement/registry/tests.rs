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
use std::sync::Arc;
use std::sync::Barrier;

use super::*;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;

macro_rules! assert_not_clone {
    ($type:ty) => {
        const _: fn() = || {
            trait AmbiguousIfClone<A> {
                fn marker() {}
            }
            impl<T: ?Sized> AmbiguousIfClone<()> for T {}
            impl<T: ?Sized + Clone> AmbiguousIfClone<u8> for T {}
            let _ = <$type as AmbiguousIfClone<_>>::marker;
        };
    };
}

#[derive(Debug)]
struct TestOwner(u64);

assert_not_clone!(DurableIntentEvidence);
assert_not_clone!(DurableRetirementToken<TestOwner>);
assert_not_clone!(RetirementIntentAppend<'static, TestOwner>);
assert_not_clone!(PreparedQueueHandoff<'static, TestOwner>);
assert_not_clone!(RetirementHandoffCapability<TestOwner>);
assert_not_clone!(LogicalRemovedCapability<TestOwner>);

mod handoff;
mod recovered;
mod replay;
mod reservation;

const FILE_LENGTH: u64 = 1_024;
const NONCE: [u8; 16] = [0x11; 16];
const OTHER_NONCE: [u8; 16] = [0x22; 16];

fn store_uuid() -> StoreUuid {
    StoreUuid::new([0x42; 16]).expect("test Store UUID is nonzero")
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

fn registration(
    create_sequence: u64,
    inode: u64,
    offset: u64,
    owner: Arc<TestOwner>,
    queue: QueueIdentity,
) -> PublishedFileRegistration<TestOwner> {
    PublishedFileRegistration::new(
        incarnation(create_sequence),
        physical_key(inode),
        canonical_path(offset),
        offset,
        FILE_LENGTH,
        owner,
        queue,
    )
    .expect("test registration is valid")
}

fn registered_registry(ticket_high_water: u64) -> (RetirementRegistry<TestOwner>, Arc<TestOwner>, QueueIdentity) {
    let registry = RetirementRegistry::new(store_uuid(), ticket_high_water);
    let owner = Arc::new(TestOwner(1));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");
    (registry, owner, queue)
}

#[allow(
    clippy::too_many_arguments,
    reason = "test helper mirrors the persisted retirement identity"
)]
fn operation(
    create_sequence: u64,
    inode: u64,
    offset: u64,
    expected_length: u64,
    reason: RetirementReason,
    mapping_generation: u64,
    nonce: [u8; 16],
) -> RetirementOperation {
    operation_with_path(
        create_sequence,
        inode,
        offset,
        expected_length,
        canonical_path(offset).as_str(),
        reason,
        mapping_generation,
        nonce,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "test helper mirrors the persisted retirement identity"
)]
fn operation_with_path(
    create_sequence: u64,
    inode: u64,
    offset: u64,
    expected_length: u64,
    path: &str,
    reason: RetirementReason,
    mapping_generation: u64,
    nonce: [u8; 16],
) -> RetirementOperation {
    RetirementOperation::new(
        incarnation(create_sequence),
        reason,
        mapping_generation,
        offset,
        expected_length,
        nonce,
        physical_key(inode),
        StoreRelativePath::new(path).expect("test path is canonical"),
    )
    .expect("test operation is valid")
}

fn intent_record(ticket_id: u64, operation: RetirementOperation) -> LedgerRecord {
    LedgerRecord::RetirementIntent {
        ticket_id: crate::mapped_file::retirement::identity::TicketId::new(ticket_id).expect("test ticket is nonzero"),
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

fn replay_test_evidence(ticket_id: u64, operation: RetirementOperation) -> DurableIntentEvidence {
    DurableIntentEvidence::replay_verified_for_test(
        intent_record(ticket_id, operation),
        3,
        ticket_id + 70,
        ticket_id + 71,
        900 + ticket_id * 200,
    )
    .expect("test replay evidence is valid")
}

fn assert_replay_rejection_fences(
    registry: &RetirementRegistry<TestOwner>,
    evidence: DurableIntentEvidence,
    owner: &Arc<TestOwner>,
    queue: &QueueIdentity,
    expected: RegistryViolation,
) {
    assert_eq!(
        registry
            .restore_replayed_intent(evidence, owner, queue)
            .expect_err("current-store replay evidence is rejected"),
        expected
    );
    assert!(registry.needs_recovery());
}

fn commit_test_intent(
    registry: &RetirementRegistry<TestOwner>,
    operation: RetirementOperation,
    owner: &Arc<TestOwner>,
    queue: &QueueIdentity,
) -> (DurableRetirementToken<TestOwner>, RetirementIntentBinding) {
    let reservation = registry
        .prepare_retirement(operation, owner, queue)
        .expect("intent is prepared");
    let binding = reservation.binding().clone();
    let append = reservation.begin_append();
    let evidence = DurableIntentEvidence::writer_verified_for_test(
        append.intent_record(),
        4,
        binding.ticket_id().get() + 90,
        binding.ticket_id().get() + 91,
        1_000 + binding.ticket_id().get() * 200,
    )
    .expect("test writer evidence is valid");
    let token = append.commit(evidence).expect("intent is committed");
    (token, binding)
}
