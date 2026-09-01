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

use super::engine::advance;
use super::engine::BackendFailure;
use super::engine::EntryObservation;
use super::engine::NamespaceIo;
use super::engine::NamespaceSnapshot;
use super::types::NamespaceAbsenceProof;
use super::types::NamespaceEntry;
use super::types::NamespaceFailureClass;
use super::types::NamespaceMutationAuthorization;
use super::types::NamespaceOperation;
use super::types::NamespacePolicyViolation;
use super::types::NamespaceRequestViolation;
use super::types::NamespaceRetirementRequest;
use super::types::NamespaceTicketBinding;
use super::types::NamespaceTombstoneProof;
use super::types::NamespaceTransition;
use super::types::NamespaceTransitionOutcome;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;

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

assert_not_clone!(NamespaceMutationAuthorization);
assert_not_clone!(NamespaceTombstoneProof);
assert_not_clone!(NamespaceAbsenceProof);
assert_not_clone!(NamespaceTransitionOutcome);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultPoint {
    RenameBefore,
    RenameAfter,
    UnlinkBefore,
    UnlinkAfter,
    Sync,
}

#[derive(Debug)]
struct ModelNamespace {
    canonical: EntryObservation,
    tombstone: EntryObservation,
    canonical_length: u64,
    tombstone_length: u64,
    fault: Option<FaultPoint>,
    rename_calls: usize,
    unlink_calls: usize,
    sync_calls: usize,
    live_owner: bool,
    sharing_blocks_mutation: bool,
    replacement_on_reverify: Option<PhysicalFileKey>,
    unknown_entries: Vec<&'static str>,
}

impl ModelNamespace {
    fn canonical_target() -> Self {
        Self {
            canonical: EntryObservation::ExpectedFile,
            tombstone: EntryObservation::Missing,
            canonical_length: 1024,
            tombstone_length: 1024,
            fault: None,
            rename_calls: 0,
            unlink_calls: 0,
            sync_calls: 0,
            live_owner: false,
            sharing_blocks_mutation: false,
            replacement_on_reverify: None,
            unknown_entries: Vec::new(),
        }
    }

    fn retryable(operation: NamespaceOperation) -> BackendFailure {
        BackendFailure::retryable(operation, NamespaceFailureClass::SharingViolation, Some(32))
    }

    fn take_fault(&mut self, point: FaultPoint) -> bool {
        if self.fault == Some(point) {
            self.fault = None;
            true
        } else {
            false
        }
    }
}

impl NamespaceIo for ModelNamespace {
    fn snapshot(
        &mut self,
        _expected_key: PhysicalFileKey,
        expected_length: u64,
    ) -> Result<NamespaceSnapshot, BackendFailure> {
        let observe_length = |entry, actual| match entry {
            EntryObservation::ExpectedFile if actual != expected_length => {
                EntryObservation::ExpectedFileWrongLength(actual)
            }
            observation => observation,
        };
        Ok(NamespaceSnapshot {
            canonical: observe_length(self.canonical, self.canonical_length),
            tombstone: observe_length(self.tombstone, self.tombstone_length),
        })
    }

    fn rename_to_tombstone(&mut self) -> Result<(), BackendFailure> {
        self.rename_calls += 1;
        if self.sharing_blocks_mutation || self.take_fault(FaultPoint::RenameBefore) {
            return Err(Self::retryable(NamespaceOperation::Rename));
        }
        self.canonical = EntryObservation::Missing;
        self.tombstone = EntryObservation::ExpectedFile;
        if self.take_fault(FaultPoint::RenameAfter) {
            return Err(Self::retryable(NamespaceOperation::Rename));
        }
        Ok(())
    }

    fn unlink(&mut self, entry: NamespaceEntry) -> Result<(), BackendFailure> {
        self.unlink_calls += 1;
        if self.sharing_blocks_mutation || self.take_fault(FaultPoint::UnlinkBefore) {
            return Err(Self::retryable(NamespaceOperation::Unlink));
        }
        match entry {
            NamespaceEntry::Canonical => self.canonical = EntryObservation::Missing,
            NamespaceEntry::Tombstone => self.tombstone = EntryObservation::Missing,
        }
        if self.take_fault(FaultPoint::UnlinkAfter) {
            return Err(Self::retryable(NamespaceOperation::Unlink));
        }
        Ok(())
    }

    fn sync_after_namespace(&mut self, _transition: NamespaceTransition) -> Result<(), BackendFailure> {
        self.sync_calls += 1;
        if self.take_fault(FaultPoint::Sync) {
            return Err(BackendFailure::retryable(
                NamespaceOperation::SyncParentOrHandle,
                NamespaceFailureClass::PermissionDenied,
                Some(5),
            ));
        }
        Ok(())
    }

    fn release_for_reverification(&mut self) {
        if let Some(replacement) = self.replacement_on_reverify.take() {
            self.canonical = EntryObservation::OtherFile(replacement);
        }
    }
}

fn request() -> NamespaceRetirementRequest {
    let store_uuid = StoreUuid::new([1; 16]).expect("test Store UUID is valid");
    let incarnation = FileIncarnationId::new(store_uuid, 7).expect("test incarnation is valid");
    let ticket = TicketId::new(42).expect("test ticket is valid");
    let binding = NamespaceTicketBinding::new(
        ticket,
        incarnation,
        RetirementReason::TtlExpired,
        0,
        3,
        1024,
        [0x44; 16],
    )
    .expect("test ticket binding is valid");
    let canonical = StoreRelativePath::new("commitlog/00000000000000000000").expect("canonical test path is valid");
    let tombstone = StoreRelativePath::new(
        "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n44444444444444444444444444444444",
    )
    .expect("tombstone test path is valid");
    NamespaceRetirementRequest::new(binding, PhysicalFileKey::unix(9, 11), canonical, tombstone)
        .expect("test reservation is valid")
}

fn run(model: &mut ModelNamespace, transition: NamespaceTransition) -> NamespaceTransitionOutcome {
    let request = request();
    let authorization = NamespaceMutationAuthorization::for_test(&request, transition);
    advance(&request, transition, model, authorization)
}

fn run_after_superseded(
    model: &mut ModelNamespace,
    transition: NamespaceTransition,
    replacement: PhysicalFileKey,
) -> NamespaceTransitionOutcome {
    let request = request().with_recorded_replacement_key(Some(replacement));
    let authorization = NamespaceMutationAuthorization::for_test(&request, transition);
    advance(&request, transition, model, authorization)
}

#[test]
fn zero_retirement_nonce_is_rejected_before_a_request_exists() {
    let store_uuid = StoreUuid::new([1; 16]).expect("test Store UUID is valid");
    let incarnation = FileIncarnationId::new(store_uuid, 7).expect("test incarnation is valid");
    let ticket = TicketId::new(42).expect("test ticket is valid");

    assert_eq!(
        NamespaceTicketBinding::new(ticket, incarnation, RetirementReason::TtlExpired, 0, 3, 1024, [0; 16],),
        Err(NamespaceRequestViolation::ZeroRetirementNonce)
    );
}

#[test]
fn zero_expected_length_is_rejected_before_a_request_exists() {
    let store_uuid = StoreUuid::new([1; 16]).expect("test Store UUID is valid");
    let incarnation = FileIncarnationId::new(store_uuid, 7).expect("test incarnation is valid");
    let ticket = TicketId::new(42).expect("test ticket is valid");

    assert_eq!(
        NamespaceTicketBinding::new(ticket, incarnation, RetirementReason::TtlExpired, 0, 3, 0, [0x44; 16],),
        Err(NamespaceRequestViolation::ZeroExpectedLength)
    );
}

#[test]
fn authorization_for_another_request_causes_zero_namespace_calls() {
    let authorized = request();
    let attempted = NamespaceRetirementRequest::new(
        authorized.ticket().clone(),
        PhysicalFileKey::unix(9, 12),
        authorized.canonical_path().clone(),
        authorized.tombstone_path().clone(),
    )
    .expect("second exact request is valid");
    let authorization = NamespaceMutationAuthorization::for_test(&authorized, NamespaceTransition::MoveToTombstone);
    let mut model = ModelNamespace::canonical_target();

    let outcome = advance(
        &attempted,
        NamespaceTransition::MoveToTombstone,
        &mut model,
        authorization,
    );

    assert_eq!(
        outcome,
        NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::AuthorizationMismatch)
    );
    assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
}

#[test]
fn authorization_for_another_transition_causes_zero_namespace_calls() {
    let request = request();
    let authorization = NamespaceMutationAuthorization::for_test(&request, NamespaceTransition::MoveToTombstone);
    let mut model = ModelNamespace::canonical_target();

    let outcome = advance(&request, NamespaceTransition::DirectUnlink, &mut model, authorization);

    assert_eq!(
        outcome,
        NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::AuthorizationMismatch)
    );
    assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
}

#[test]
fn authorization_binds_reason_and_expected_length_without_namespace_calls() {
    for (reason, expected_length) in [
        (RetirementReason::StoreDestroy, 1024),
        (RetirementReason::TtlExpired, 2048),
    ] {
        let authorized = request();
        let binding = NamespaceTicketBinding::new(
            authorized.ticket().ticket_id(),
            authorized.ticket().incarnation(),
            reason,
            authorized.ticket().segment_offset(),
            authorized.ticket().mapping_generation(),
            expected_length,
            *authorized.ticket().retirement_nonce(),
        )
        .expect("alternate durable binding is valid");
        let attempted = NamespaceRetirementRequest::new(
            binding,
            authorized.physical_key(),
            authorized.canonical_path().clone(),
            authorized.tombstone_path().clone(),
        )
        .expect("alternate exact request is valid");
        let authorization = NamespaceMutationAuthorization::for_test(&authorized, NamespaceTransition::MoveToTombstone);
        let mut model = ModelNamespace::canonical_target();

        let outcome = advance(
            &attempted,
            NamespaceTransition::MoveToTombstone,
            &mut model,
            authorization,
        );

        assert_eq!(
            outcome,
            NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::AuthorizationMismatch)
        );
        assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
    }
}

#[test]
fn an_expected_file_with_the_wrong_length_is_rejected_without_namespace_mutation() {
    let mut model = ModelNamespace::canonical_target();
    model.canonical_length = 512;

    let outcome = run(&mut model, NamespaceTransition::MoveToTombstone);

    assert_eq!(
        outcome,
        NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ExpectedLengthMismatch {
            entry: NamespaceEntry::Canonical,
            expected: 1024,
            actual: 512,
        })
    );
    assert_eq!(model.rename_calls, 0);
    assert_eq!(model.unlink_calls, 0);
    assert_eq!(model.sync_calls, 0);
}

#[test]
fn an_expected_tombstone_with_the_wrong_length_is_rejected_without_unlink() {
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::Missing;
    model.tombstone = EntryObservation::ExpectedFile;
    model.tombstone_length = 2048;

    let outcome = run(&mut model, NamespaceTransition::RemoveTombstone);

    assert_eq!(
        outcome,
        NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::ExpectedLengthMismatch {
            entry: NamespaceEntry::Tombstone,
            expected: 1024,
            actual: 2048,
        })
    );
    assert_eq!(model.unlink_calls, 0);
    assert_eq!(model.sync_calls, 0);
}

#[test]
fn same_path_replacement_is_superseded_without_namespace_mutation() {
    let replacement = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::OtherFile(replacement);

    let outcome = run(&mut model, NamespaceTransition::MoveToTombstone);

    assert!(matches!(
        outcome,
        NamespaceTransitionOutcome::Superseded {
            expected_key,
            observed_key,
        } if expected_key == PhysicalFileKey::unix(9, 11) && observed_key == replacement
    ));
    assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
}

#[test]
fn durable_superseded_observation_with_exact_old_tombstone_advances_without_renaming_replacement() {
    let replacement = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::OtherFile(replacement);
    model.tombstone = EntryObservation::ExpectedFile;

    let outcome = run_after_superseded(&mut model, NamespaceTransition::MoveToTombstone, replacement);

    let NamespaceTransitionOutcome::Tombstoned(proof) = outcome else {
        panic!("the durable replacement observation must resume at the exact old tombstone");
    };
    assert_eq!(proof.replacement_key(), Some(replacement));
    assert_eq!((model.rename_calls, model.unlink_calls), (0, 0));
}

#[test]
fn durable_superseded_observation_with_both_old_names_absent_advances_without_mutation() {
    let replacement = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::OtherFile(replacement);
    model.tombstone = EntryObservation::Missing;

    let outcome = run_after_superseded(&mut model, NamespaceTransition::MoveToTombstone, replacement);

    let NamespaceTransitionOutcome::NamespaceAbsentVerified(proof) = outcome else {
        panic!("the durable replacement observation must resume at verified old-name absence");
    };
    assert_eq!(proof.replacement_key(), Some(replacement));
    assert_eq!((model.rename_calls, model.unlink_calls), (0, 0));
}

#[test]
fn a_different_replacement_cannot_reuse_the_durable_superseded_observation() {
    let recorded = PhysicalFileKey::unix(9, 98);
    let observed = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::OtherFile(observed);

    let outcome = run_after_superseded(&mut model, NamespaceTransition::MoveToTombstone, recorded);

    assert!(matches!(
        outcome,
        NamespaceTransitionOutcome::Superseded { observed_key, .. } if observed_key == observed
    ));
    assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
}

#[test]
fn direct_unlink_also_rejects_a_preexisting_replacement_without_mutation() {
    let replacement = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::OtherFile(replacement);

    let outcome = run(&mut model, NamespaceTransition::DirectUnlink);

    assert!(matches!(
        outcome,
        NamespaceTransitionOutcome::Superseded { observed_key, .. } if observed_key == replacement
    ));
    assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
}

#[test]
fn tombstone_collision_is_rejected_without_namespace_mutation() {
    let mut model = ModelNamespace::canonical_target();
    model.tombstone = EntryObservation::OtherFile(PhysicalFileKey::unix(9, 12));

    let outcome = run(&mut model, NamespaceTransition::MoveToTombstone);

    assert!(matches!(
        outcome,
        NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::TombstoneCollision { .. })
    ));
    assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
}

#[test]
fn an_unknown_directory_is_never_removed_recursively() {
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::Directory;
    model.unknown_entries = vec!["unrelated", "nested-directory"];

    let outcome = run(&mut model, NamespaceTransition::DirectUnlink);

    assert!(matches!(
        outcome,
        NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::UnexpectedEntryType {
            entry: NamespaceEntry::Canonical,
        })
    ));
    assert_eq!(model.unknown_entries, ["unrelated", "nested-directory"]);
    assert_eq!((model.rename_calls, model.unlink_calls, model.sync_calls), (0, 0, 0));
}

#[test]
fn rename_failures_before_and_after_the_boundary_retry_idempotently() {
    for fault in [FaultPoint::RenameBefore, FaultPoint::RenameAfter] {
        let mut model = ModelNamespace::canonical_target();
        model.fault = Some(fault);

        let first = run(&mut model, NamespaceTransition::MoveToTombstone);
        assert!(matches!(
            first,
            NamespaceTransitionOutcome::Retryable(ref failure)
                if failure.operation() == NamespaceOperation::Rename
        ));

        let second = run(&mut model, NamespaceTransition::MoveToTombstone);
        assert!(matches!(second, NamespaceTransitionOutcome::Tombstoned(_)));
    }
}

#[test]
fn rename_sync_failure_reconciles_to_tombstoned_on_retry() {
    let mut model = ModelNamespace::canonical_target();
    model.fault = Some(FaultPoint::Sync);

    let first = run(&mut model, NamespaceTransition::MoveToTombstone);
    assert!(matches!(
        first,
        NamespaceTransitionOutcome::Retryable(ref failure)
            if failure.operation() == NamespaceOperation::SyncParentOrHandle
    ));
    assert_eq!(model.canonical, EntryObservation::Missing);
    assert_eq!(model.tombstone, EntryObservation::ExpectedFile);

    let second = run(&mut model, NamespaceTransition::MoveToTombstone);
    assert!(matches!(second, NamespaceTransitionOutcome::Tombstoned(_)));
}

#[test]
fn unlink_failures_before_and_after_the_boundary_retry_idempotently() {
    for fault in [FaultPoint::UnlinkBefore, FaultPoint::UnlinkAfter] {
        let mut model = ModelNamespace::canonical_target();
        model.fault = Some(fault);

        let first = run(&mut model, NamespaceTransition::DirectUnlink);
        assert!(matches!(
            first,
            NamespaceTransitionOutcome::Retryable(ref failure)
                if failure.operation() == NamespaceOperation::Unlink
        ));

        let second = run(&mut model, NamespaceTransition::DirectUnlink);
        assert!(matches!(second, NamespaceTransitionOutcome::NamespaceAbsentVerified(_)));
    }
}

#[test]
fn unlink_sync_failure_reconciles_to_verified_absence_on_retry() {
    let mut model = ModelNamespace::canonical_target();
    model.fault = Some(FaultPoint::Sync);

    let first = run(&mut model, NamespaceTransition::DirectUnlink);
    assert!(matches!(
        first,
        NamespaceTransitionOutcome::Retryable(ref failure)
            if failure.operation() == NamespaceOperation::SyncParentOrHandle
    ));
    assert_eq!(model.canonical, EntryObservation::Missing);

    let second = run(&mut model, NamespaceTransition::DirectUnlink);
    assert!(matches!(second, NamespaceTransitionOutcome::NamespaceAbsentVerified(_)));
}

#[test]
fn a_live_unix_style_owner_survives_namespace_absence() {
    let mut model = ModelNamespace::canonical_target();
    model.live_owner = true;

    let outcome = run(&mut model, NamespaceTransition::DirectUnlink);

    assert!(matches!(
        outcome,
        NamespaceTransitionOutcome::NamespaceAbsentVerified(_)
    ));
    assert!(model.live_owner);
}

#[test]
fn a_live_windows_style_owner_withholds_delete_sharing_and_is_retryable() {
    let mut model = ModelNamespace::canonical_target();
    model.live_owner = true;
    model.sharing_blocks_mutation = true;

    let first = run(&mut model, NamespaceTransition::MoveToTombstone);
    assert!(matches!(
        first,
        NamespaceTransitionOutcome::Retryable(ref failure)
            if failure.class() == NamespaceFailureClass::SharingViolation
    ));
    assert_eq!(model.canonical, EntryObservation::ExpectedFile);

    model.live_owner = false;
    model.sharing_blocks_mutation = false;
    let second = run(&mut model, NamespaceTransition::MoveToTombstone);
    assert!(matches!(second, NamespaceTransitionOutcome::Tombstoned(_)));
}

#[test]
fn tombstone_removal_is_idempotent_and_does_not_enumerate_unknown_entries() {
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::Missing;
    model.tombstone = EntryObservation::ExpectedFile;
    model.unknown_entries = vec!["unknown-sidecar", "foreign-directory"];

    let first = run(&mut model, NamespaceTransition::RemoveTombstone);
    assert!(matches!(first, NamespaceTransitionOutcome::NamespaceAbsentVerified(_)));
    let second = run(&mut model, NamespaceTransition::RemoveTombstone);
    assert!(matches!(second, NamespaceTransitionOutcome::NamespaceAbsentVerified(_)));
    assert_eq!(model.unlink_calls, 1);
    assert_eq!(model.unknown_entries, ["unknown-sidecar", "foreign-directory"]);
}

#[test]
fn replacement_plus_exact_old_tombstone_removes_only_the_tombstone() {
    let replacement = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::OtherFile(replacement);
    model.tombstone = EntryObservation::ExpectedFile;

    let outcome = run(&mut model, NamespaceTransition::RemoveTombstone);

    let NamespaceTransitionOutcome::NamespaceAbsentVerified(proof) = outcome else {
        panic!("old tombstone cleanup must produce a positive absence proof");
    };
    assert_eq!(proof.replacement_key(), Some(replacement));
    assert_eq!(model.canonical, EntryObservation::OtherFile(replacement));
    assert_eq!(model.tombstone, EntryObservation::Missing);
    assert_eq!((model.rename_calls, model.unlink_calls), (0, 1));
}

#[test]
fn replacement_plus_missing_old_tombstone_is_verified_without_mutation() {
    let replacement = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.canonical = EntryObservation::OtherFile(replacement);
    model.tombstone = EntryObservation::Missing;

    let outcome = run(&mut model, NamespaceTransition::RemoveTombstone);

    let NamespaceTransitionOutcome::NamespaceAbsentVerified(proof) = outcome else {
        panic!("two-name reconciliation must prove replacement plus tombstone absence");
    };
    assert_eq!(proof.replacement_key(), Some(replacement));
    assert_eq!((model.rename_calls, model.unlink_calls), (0, 0));
}

#[test]
fn replacement_appearing_between_unlink_and_reverification_is_in_the_absence_proof() {
    let replacement = PhysicalFileKey::unix(9, 99);
    let mut model = ModelNamespace::canonical_target();
    model.replacement_on_reverify = Some(replacement);

    let outcome = run(&mut model, NamespaceTransition::DirectUnlink);

    let NamespaceTransitionOutcome::NamespaceAbsentVerified(proof) = outcome else {
        panic!("post-unlink replacement must be preserved in the positive proof");
    };
    assert_eq!(proof.replacement_key(), Some(replacement));
    assert_eq!(model.canonical, EntryObservation::OtherFile(replacement));
    assert_eq!(model.unlink_calls, 1);
}
