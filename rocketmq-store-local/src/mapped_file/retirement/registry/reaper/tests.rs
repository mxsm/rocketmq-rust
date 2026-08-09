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

use super::super::queue_slot::ManagedMappedFileQueueGeneration;
use super::super::LogicalRemovedCapability;
use super::super::PublishedFileRegistration;
use super::super::RetirementOperation;
use super::super::RetirementRegistry;
use super::*;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::platform::authorize_namespace_transition;
use crate::mapped_file::retirement::platform::authorize_tombstone_removal;
use crate::mapped_file::retirement::platform::AuthorizedNamespaceTransitionResult;
use crate::mapped_file::retirement::platform::NamespaceAbsenceProof;
use crate::mapped_file::retirement::platform::NamespaceTombstoneProof;
use crate::mapped_file::retirement::platform::NamespaceTransition;
use crate::mapped_file::retirement::platform::NamespaceTransitionOutcome;
use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
use crate::mapped_file::retirement::writer::ManagedLedgerWriter;

const FILE_LENGTH: u64 = 1_024;
const BOOTSTRAP_ID: [u8; 16] = [0x71; 16];

#[derive(Debug)]
struct TestOwner;

fn store_uuid() -> StoreUuid {
    StoreUuid::new([0x72; 16]).expect("test Store UUID is nonzero")
}

fn incarnation() -> FileIncarnationId {
    FileIncarnationId::new(store_uuid(), 1).expect("test incarnation is nonzero")
}

fn physical_key(inode: u64) -> PhysicalFileKey {
    PhysicalFileKey::unix(7, inode)
}

fn canonical_path() -> StoreRelativePath {
    StoreRelativePath::new("commitlog/00000000000000000000").expect("test path is canonical")
}

fn logical_removed_fixture() -> (
    RetirementRegistry<TestOwner>,
    ManagedMappedFileQueueGeneration<TestOwner>,
    ManagedLedgerWriter<ModelLedgerIo>,
    LogicalRemovedCapability<TestOwner>,
) {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let queue = ManagedMappedFileQueueGeneration::new_write_disabled();
    let owner = Arc::new(TestOwner);
    queue
        .install_managed_member_for_test(
            Arc::clone(&owner),
            incarnation(),
            physical_key(11),
            canonical_path(),
            0,
            FILE_LENGTH,
            1,
        )
        .expect("managed member is installed once");
    registry
        .register_published(
            PublishedFileRegistration::new(
                incarnation(),
                physical_key(11),
                canonical_path(),
                0,
                FILE_LENGTH,
                Arc::clone(&owner),
                queue.queue_identity(),
            )
            .expect("published registration is valid"),
        )
        .expect("published identity is registered");

    let operation = RetirementOperation::new(
        incarnation(),
        RetirementReason::TtlExpired,
        1,
        0,
        FILE_LENGTH,
        [0x73; 16],
        physical_key(11),
        canonical_path(),
    )
    .expect("retirement operation is valid");
    let reservation = registry
        .prepare_retirement(operation, &owner, &queue.queue_identity())
        .expect("retirement intent is reserved");
    let binding = reservation.binding().clone();
    let mut writer =
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), BOOTSTRAP_ID, 4, 1, 1, 0, true, 1)
            .expect("managed writer cursor is valid");
    let token = writer
        .append_retirement_intent(reservation.begin_append())
        .expect("RetirementIntent is durable");
    let handoff = queue
        .handoff_retirement(&registry, token, &binding)
        .expect("queue handoff succeeds exactly once");
    let logical_removed = writer
        .append_logical_removed(handoff)
        .expect("LogicalRemoved is durable");
    (registry, queue, writer, logical_removed)
}

fn absence_result(
    capability: LogicalRemovedCapability<TestOwner>,
    replacement: Option<PhysicalFileKey>,
) -> AuthorizedNamespaceTransitionResult<LogicalRemovedCapability<TestOwner>> {
    let authorization = authorize_namespace_transition(capability, NamespaceTransition::DirectUnlink)
        .expect("logical removal authorizes direct unlink");
    let (capability, request) = authorization.into_parts_for_test();
    let proof = NamespaceAbsenceProof::verified_for_test(&request, replacement);
    AuthorizedNamespaceTransitionResult::for_test(
        capability,
        NamespaceTransitionOutcome::NamespaceAbsentVerified(proof),
    )
}

#[test]
fn direct_absence_outcome_commits_the_exact_following_durable_stage() {
    let (registry, _queue, mut writer, logical_removed) = logical_removed_fixture();

    let progress = commit_logical_namespace_outcome(&mut writer, absence_result(logical_removed, None), 123)
        .expect("verified absence is durably committed");
    let LogicalNamespaceProgress::NamespaceAbsent(namespace_absent) = progress else {
        panic!("direct unlink must advance to NamespaceAbsent");
    };

    assert_eq!(namespace_absent.durable_sequence(), 3);
    assert_eq!(registry.retained_identity_count(), 1);
}

#[test]
fn tombstone_outcomes_require_both_durable_stages() {
    let (registry, _queue, mut writer, logical_removed) = logical_removed_fixture();
    let authorization = authorize_namespace_transition(logical_removed, NamespaceTransition::MoveToTombstone)
        .expect("logical removal authorizes a tombstone move");
    let (logical_removed, request) = authorization.into_parts_for_test();
    let tombstone = NamespaceTombstoneProof::verified_for_test(&request, None);
    let progress = commit_logical_namespace_outcome(
        &mut writer,
        AuthorizedNamespaceTransitionResult::for_test(
            logical_removed,
            NamespaceTransitionOutcome::Tombstoned(tombstone),
        ),
        223,
    )
    .expect("tombstone observation is durably committed");
    let LogicalNamespaceProgress::Tombstoned(tombstoned) = progress else {
        panic!("rename must advance to Tombstoned");
    };
    assert_eq!(tombstoned.durable_sequence(), 3);

    let authorization = authorize_tombstone_removal(tombstoned).expect("durable tombstone authorizes removal");
    let (tombstoned, request) = authorization.into_parts_for_test();
    let absence = NamespaceAbsenceProof::verified_for_test(&request, None);
    let progress = commit_tombstone_namespace_outcome(
        &mut writer,
        AuthorizedNamespaceTransitionResult::for_test(
            tombstoned,
            NamespaceTransitionOutcome::NamespaceAbsentVerified(absence),
        ),
        224,
    )
    .expect("tombstone absence is durably committed");
    let TombstoneNamespaceProgress::NamespaceAbsent(namespace_absent) = progress else {
        panic!("tombstone removal must advance to NamespaceAbsent");
    };
    assert_eq!(namespace_absent.durable_sequence(), 4);
    assert_eq!(registry.retained_identity_count(), 1);
}

#[test]
fn superseded_and_unsupported_outcomes_preserve_retry_authority() {
    let (_registry, _queue, mut writer, logical_removed) = logical_removed_fixture();
    let replacement = physical_key(99);
    let progress = commit_logical_namespace_outcome(
        &mut writer,
        AuthorizedNamespaceTransitionResult::for_test(
            logical_removed,
            NamespaceTransitionOutcome::Superseded {
                expected_key: physical_key(11),
                observed_key: replacement,
            },
        ),
        323,
    )
    .expect("SupersededPath is appended as a sticky annotation");
    let LogicalNamespaceProgress::Pending { capability, status } = progress else {
        panic!("SupersededPath remains at LogicalRemoved");
    };
    assert_eq!(capability.durable_sequence(), 2);
    assert!(matches!(status, NamespacePending::Superseded { observed_key, .. } if observed_key == replacement));

    let progress = commit_logical_namespace_outcome(
        &mut writer,
        AuthorizedNamespaceTransitionResult::for_test(
            capability,
            NamespaceTransitionOutcome::Unsupported {
                platform: "windows",
                reason: "writer matrix is not qualified",
            },
        ),
        324,
    )
    .expect("unsupported platform returns the durable capability unchanged");
    let LogicalNamespaceProgress::Pending { capability, status } = progress else {
        panic!("unsupported namespace work remains pending");
    };
    assert_eq!(capability.durable_sequence(), 2);
    assert!(matches!(
        status,
        NamespacePending::Unsupported {
            platform: "windows",
            ..
        }
    ));

    let progress = commit_logical_namespace_outcome(&mut writer, absence_result(capability, Some(replacement)), 325)
        .expect("retry uses the append cursor after SupersededPath");
    let LogicalNamespaceProgress::NamespaceAbsent(namespace_absent) = progress else {
        panic!("verified replacement plus old-target absence advances the stage");
    };
    assert_eq!(namespace_absent.durable_sequence(), 4);
}
