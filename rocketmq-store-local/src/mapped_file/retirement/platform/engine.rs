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

use super::types::NamespaceAbsenceProof;
use super::types::NamespaceEntry;
use super::types::NamespaceFailure;
use super::types::NamespaceFailureClass;
use super::types::NamespaceMutationAuthorization;
use super::types::NamespaceOperation;
use super::types::NamespaceOperationResult;
use super::types::NamespacePolicyViolation;
use super::types::NamespaceRetirementRequest;
use super::types::NamespaceTombstoneProof;
use super::types::NamespaceTransition;
use crate::mapped_file::retirement::identity::PhysicalFileKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EntryObservation {
    Missing,
    ExpectedFile,
    ExpectedFileWrongLength(u64),
    OtherFile(PhysicalFileKey),
    Directory,
    ReparsePoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NamespaceSnapshot {
    pub(super) canonical: EntryObservation,
    pub(super) tombstone: EntryObservation,
}

#[derive(Debug)]
pub(super) struct BackendFailure {
    failure: NamespaceFailure,
    retryable: bool,
}

impl BackendFailure {
    pub(super) const fn retryable(
        operation: NamespaceOperation,
        class: NamespaceFailureClass,
        raw_code: Option<i32>,
    ) -> Self {
        Self {
            failure: NamespaceFailure::new(operation, class, raw_code),
            retryable: true,
        }
    }

    pub(super) const fn failed(
        operation: NamespaceOperation,
        class: NamespaceFailureClass,
        raw_code: Option<i32>,
    ) -> Self {
        Self {
            failure: NamespaceFailure::new(operation, class, raw_code),
            retryable: false,
        }
    }

    pub(super) fn into_result(self) -> NamespaceOperationResult {
        if self.retryable {
            NamespaceOperationResult::Retryable(self.failure)
        } else {
            NamespaceOperationResult::Failed(self.failure)
        }
    }

    pub(super) fn into_verification_error(self) -> NamespaceOperationResult {
        if self.retryable {
            NamespaceOperationResult::Retryable(self.failure)
        } else {
            NamespaceOperationResult::Failed(self.failure)
        }
    }
}

/// Sealed-by-module namespace seam. Production implementations hold only verified handles.
pub(super) trait NamespaceIo {
    fn snapshot(
        &mut self,
        expected_key: PhysicalFileKey,
        expected_length: u64,
    ) -> Result<NamespaceSnapshot, BackendFailure>;

    fn rename_to_tombstone(&mut self) -> Result<(), BackendFailure>;

    fn unlink(&mut self, entry: NamespaceEntry) -> Result<(), BackendFailure>;

    fn sync_after_namespace(&mut self, transition: NamespaceTransition) -> Result<(), BackendFailure>;

    fn release_for_reverification(&mut self);
}

enum Decision {
    MutateRename,
    MutateUnlink(NamespaceEntry),
    Tombstoned,
    Absent,
    Return(Box<NamespaceOperationResult>),
}

impl Decision {
    fn return_result(result: NamespaceOperationResult) -> Self {
        Self::Return(Box::new(result))
    }
}

/// Advances exactly one ledger-ordered namespace transition.
///
/// The non-Clone authorization is intentionally consumed even for an idempotent observation. M3
/// has no production constructor for it, so this write path cannot be reached before Wave B.
pub(super) fn advance<I: NamespaceIo>(
    request: &NamespaceRetirementRequest,
    transition: NamespaceTransition,
    io: &mut I,
    authorization: NamespaceMutationAuthorization,
) -> NamespaceOperationResult {
    if !authorization.authorizes(request, transition) {
        return NamespaceOperationResult::Rejected(NamespacePolicyViolation::AuthorizationMismatch);
    }
    let initial = match io.snapshot(request.physical_key(), request.ticket().expected_length()) {
        Ok(snapshot) => snapshot,
        Err(failure) => return failure.into_result(),
    };
    match decide(request, transition, initial) {
        Decision::Return(result) => *result,
        Decision::Tombstoned => settle_without_mutation(request, transition, io, Decision::Tombstoned),
        Decision::Absent => settle_without_mutation(request, transition, io, Decision::Absent),
        Decision::MutateRename => {
            if let Err(failure) = io.rename_to_tombstone() {
                return failure.into_result();
            }
            finish_mutation(request, transition, io, Decision::Tombstoned)
        }
        Decision::MutateUnlink(entry) => {
            if let Err(failure) = io.unlink(entry) {
                return failure.into_result();
            }
            finish_mutation(request, transition, io, Decision::Absent)
        }
    }
}

fn settle_without_mutation<I: NamespaceIo>(
    request: &NamespaceRetirementRequest,
    transition: NamespaceTransition,
    io: &mut I,
    expected: Decision,
) -> NamespaceOperationResult {
    if let Err(failure) = io.sync_after_namespace(transition) {
        return failure.into_result();
    }
    io.release_for_reverification();
    verify_converged(request, io, expected)
}

fn finish_mutation<I: NamespaceIo>(
    request: &NamespaceRetirementRequest,
    transition: NamespaceTransition,
    io: &mut I,
    expected: Decision,
) -> NamespaceOperationResult {
    if let Err(failure) = io.sync_after_namespace(transition) {
        return failure.into_result();
    }
    io.release_for_reverification();
    verify_converged(request, io, expected)
}

fn verify_converged<I: NamespaceIo>(
    request: &NamespaceRetirementRequest,
    io: &mut I,
    expected: Decision,
) -> NamespaceOperationResult {
    let snapshot = match io.snapshot(request.physical_key(), request.ticket().expected_length()) {
        Ok(snapshot) => snapshot,
        Err(failure) => return failure.into_result(),
    };
    if let Some(outcome) = static_rejection(request, snapshot) {
        return outcome;
    }
    let replacement_key = match snapshot.canonical {
        EntryObservation::Missing => None,
        EntryObservation::OtherFile(observed_key) => Some(observed_key),
        EntryObservation::ExpectedFile
        | EntryObservation::ExpectedFileWrongLength(_)
        | EntryObservation::Directory
        | EntryObservation::ReparsePoint => {
            return NamespaceOperationResult::Rejected(NamespacePolicyViolation::NamespaceChangedDuringVerification);
        }
    };
    match expected {
        Decision::Tombstoned if snapshot.tombstone == EntryObservation::ExpectedFile => {
            NamespaceOperationResult::Tombstoned(NamespaceTombstoneProof::new(request, replacement_key))
        }
        Decision::Absent if snapshot.tombstone == EntryObservation::Missing => {
            NamespaceOperationResult::NamespaceAbsentVerified(NamespaceAbsenceProof::new(request, replacement_key))
        }
        Decision::MutateRename | Decision::MutateUnlink(_) | Decision::Return(_) => {
            NamespaceOperationResult::Rejected(NamespacePolicyViolation::NamespaceChangedDuringVerification)
        }
        Decision::Tombstoned | Decision::Absent => {
            NamespaceOperationResult::Rejected(NamespacePolicyViolation::NamespaceChangedDuringVerification)
        }
    }
}

fn decide(
    request: &NamespaceRetirementRequest,
    transition: NamespaceTransition,
    snapshot: NamespaceSnapshot,
) -> Decision {
    if let Some(outcome) = static_rejection(request, snapshot) {
        return Decision::return_result(outcome);
    }

    match transition {
        NamespaceTransition::MoveToTombstone
            if matches!(snapshot.canonical, EntryObservation::OtherFile(_))
                && !replacement_was_recorded(request, snapshot.canonical) =>
        {
            Decision::return_result(superseded(request, snapshot.canonical))
        }
        NamespaceTransition::MoveToTombstone => match (snapshot.canonical, snapshot.tombstone) {
            (EntryObservation::ExpectedFile, EntryObservation::Missing) => Decision::MutateRename,
            (EntryObservation::Missing, EntryObservation::ExpectedFile) => Decision::Tombstoned,
            (EntryObservation::Missing, EntryObservation::Missing) => Decision::Absent,
            (EntryObservation::OtherFile(_), EntryObservation::ExpectedFile) => Decision::Tombstoned,
            (EntryObservation::OtherFile(_), EntryObservation::Missing) => Decision::Absent,
            (EntryObservation::ExpectedFile, EntryObservation::ExpectedFile) => Decision::return_result(
                NamespaceOperationResult::Rejected(NamespacePolicyViolation::TombstoneCollision {
                    observed_key: Some(request.physical_key()),
                }),
            ),
            _ => Decision::return_result(NamespaceOperationResult::Rejected(
                NamespacePolicyViolation::NamespaceChangedDuringVerification,
            )),
        },
        NamespaceTransition::DirectUnlink if matches!(snapshot.canonical, EntryObservation::OtherFile(_)) => {
            Decision::return_result(superseded(request, snapshot.canonical))
        }
        NamespaceTransition::DirectUnlink => match (snapshot.canonical, snapshot.tombstone) {
            (EntryObservation::ExpectedFile, EntryObservation::Missing) => {
                Decision::MutateUnlink(NamespaceEntry::Canonical)
            }
            (EntryObservation::Missing, EntryObservation::Missing) => Decision::Absent,
            (_, EntryObservation::ExpectedFile) => Decision::return_result(NamespaceOperationResult::Rejected(
                NamespacePolicyViolation::TombstoneCollision {
                    observed_key: Some(request.physical_key()),
                },
            )),
            _ => Decision::return_result(NamespaceOperationResult::Rejected(
                NamespacePolicyViolation::NamespaceChangedDuringVerification,
            )),
        },
        NamespaceTransition::RemoveTombstone => match (snapshot.canonical, snapshot.tombstone) {
            (EntryObservation::Missing, EntryObservation::ExpectedFile) => {
                Decision::MutateUnlink(NamespaceEntry::Tombstone)
            }
            (EntryObservation::Missing, EntryObservation::Missing) => Decision::Absent,
            (EntryObservation::OtherFile(_), EntryObservation::ExpectedFile) => {
                Decision::MutateUnlink(NamespaceEntry::Tombstone)
            }
            (EntryObservation::OtherFile(_), EntryObservation::Missing) => Decision::Absent,
            (EntryObservation::ExpectedFile, _) => Decision::return_result(NamespaceOperationResult::Rejected(
                NamespacePolicyViolation::CanonicalRestored,
            )),
            _ => Decision::return_result(NamespaceOperationResult::Rejected(
                NamespacePolicyViolation::NamespaceChangedDuringVerification,
            )),
        },
    }
}

fn replacement_was_recorded(request: &NamespaceRetirementRequest, canonical: EntryObservation) -> bool {
    let EntryObservation::OtherFile(observed_key) = canonical else {
        return false;
    };
    request.recorded_replacement_key() == Some(observed_key)
}

fn static_rejection(
    request: &NamespaceRetirementRequest,
    snapshot: NamespaceSnapshot,
) -> Option<NamespaceOperationResult> {
    match snapshot.canonical {
        EntryObservation::ExpectedFileWrongLength(actual) => {
            return Some(NamespaceOperationResult::Rejected(
                NamespacePolicyViolation::ExpectedLengthMismatch {
                    entry: NamespaceEntry::Canonical,
                    expected: request.ticket().expected_length(),
                    actual,
                },
            ));
        }
        EntryObservation::Directory | EntryObservation::ReparsePoint => {
            return Some(NamespaceOperationResult::Rejected(
                NamespacePolicyViolation::UnexpectedEntryType {
                    entry: NamespaceEntry::Canonical,
                },
            ));
        }
        EntryObservation::Missing | EntryObservation::ExpectedFile | EntryObservation::OtherFile(_) => {}
    }

    match snapshot.tombstone {
        EntryObservation::ExpectedFileWrongLength(actual) => Some(NamespaceOperationResult::Rejected(
            NamespacePolicyViolation::ExpectedLengthMismatch {
                entry: NamespaceEntry::Tombstone,
                expected: request.ticket().expected_length(),
                actual,
            },
        )),
        EntryObservation::OtherFile(observed_key) => Some(NamespaceOperationResult::Rejected(
            NamespacePolicyViolation::TombstoneCollision {
                observed_key: Some(observed_key),
            },
        )),
        EntryObservation::Directory | EntryObservation::ReparsePoint => Some(NamespaceOperationResult::Rejected(
            NamespacePolicyViolation::UnexpectedEntryType {
                entry: NamespaceEntry::Tombstone,
            },
        )),
        EntryObservation::Missing | EntryObservation::ExpectedFile => None,
    }
}

fn superseded(request: &NamespaceRetirementRequest, canonical: EntryObservation) -> NamespaceOperationResult {
    let EntryObservation::OtherFile(observed_key) = canonical else {
        return NamespaceOperationResult::Rejected(NamespacePolicyViolation::NamespaceChangedDuringVerification);
    };
    NamespaceOperationResult::Superseded {
        expected_key: request.physical_key(),
        observed_key,
    }
}
