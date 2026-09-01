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

use thiserror::Error;

use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::IdentityViolation;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;

/// Persisted coordinates that authorize one retirement ticket's namespace names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceTicketBinding {
    ticket_id: TicketId,
    incarnation: FileIncarnationId,
    reason: RetirementReason,
    segment_offset: u64,
    mapping_generation: u64,
    expected_length: u64,
    retirement_nonce: [u8; 16],
}

impl NamespaceTicketBinding {
    pub(crate) fn new(
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        reason: RetirementReason,
        segment_offset: u64,
        mapping_generation: u64,
        expected_length: u64,
        retirement_nonce: [u8; 16],
    ) -> Result<Self, NamespaceRequestViolation> {
        if mapping_generation == 0 {
            return Err(NamespaceRequestViolation::ZeroMappingGeneration);
        }
        if expected_length == 0 {
            return Err(NamespaceRequestViolation::ZeroExpectedLength);
        }
        if retirement_nonce == [0; 16] {
            return Err(NamespaceRequestViolation::ZeroRetirementNonce);
        }
        Ok(Self {
            ticket_id,
            incarnation,
            reason,
            segment_offset,
            mapping_generation,
            expected_length,
            retirement_nonce,
        })
    }

    pub(crate) const fn ticket_id(&self) -> TicketId {
        self.ticket_id
    }

    pub(crate) const fn incarnation(&self) -> FileIncarnationId {
        self.incarnation
    }

    pub(crate) const fn reason(&self) -> RetirementReason {
        self.reason
    }

    pub(crate) const fn segment_offset(&self) -> u64 {
        self.segment_offset
    }

    pub(crate) const fn mapping_generation(&self) -> u64 {
        self.mapping_generation
    }

    pub(crate) const fn expected_length(&self) -> u64 {
        self.expected_length
    }

    pub(crate) const fn retirement_nonce(&self) -> &[u8; 16] {
        &self.retirement_nonce
    }
}

/// Exact immutable path/key request consumed while constructing a verified reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceRetirementRequest {
    ticket: NamespaceTicketBinding,
    physical_key: PhysicalFileKey,
    canonical_path: StoreRelativePath,
    tombstone_path: StoreRelativePath,
    recorded_replacement_key: Option<PhysicalFileKey>,
}

impl NamespaceRetirementRequest {
    pub(crate) fn new(
        ticket: NamespaceTicketBinding,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        tombstone_path: StoreRelativePath,
    ) -> Result<Self, NamespaceRequestViolation> {
        canonical_path.validate_tombstone_binding(
            &tombstone_path,
            ticket.ticket_id,
            ticket.incarnation,
            ticket.segment_offset,
            ticket.mapping_generation,
            &ticket.retirement_nonce,
        )?;
        Ok(Self {
            ticket,
            physical_key,
            canonical_path,
            tombstone_path,
            recorded_replacement_key: None,
        })
    }

    /// Binds a previously durable `SupersededPath` observation to the next namespace attempt.
    ///
    /// The value remains part of the exact authorization request, so a caller cannot substitute a
    /// different replacement after the writer has acknowledged the sticky observation.
    pub(super) fn with_recorded_replacement_key(mut self, observed_key: Option<PhysicalFileKey>) -> Self {
        self.recorded_replacement_key = observed_key;
        self
    }

    pub(crate) const fn ticket(&self) -> &NamespaceTicketBinding {
        &self.ticket
    }

    pub(crate) const fn physical_key(&self) -> PhysicalFileKey {
        self.physical_key
    }

    pub(crate) const fn canonical_path(&self) -> &StoreRelativePath {
        &self.canonical_path
    }

    pub(crate) const fn tombstone_path(&self) -> &StoreRelativePath {
        &self.tombstone_path
    }

    pub(super) const fn recorded_replacement_key(&self) -> Option<PhysicalFileKey> {
        self.recorded_replacement_key
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum NamespaceRequestViolation {
    #[error("retirement mapping generation must be non-zero")]
    ZeroMappingGeneration,
    #[error("retirement expected length must be non-zero")]
    ZeroExpectedLength,
    #[error("retirement nonce must be non-zero")]
    ZeroRetirementNonce,
    #[error("RemoveTombstone requires a durable Tombstoned capability")]
    TombstoneStageRequired,
    #[error(transparent)]
    InvalidIdentity(#[from] IdentityViolation),
}

/// One ledger-ordered namespace transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceTransition {
    /// Linux-only direct canonical unlink (`LogicalRemoved -> NamespaceAbsent`).
    DirectUnlink,
    /// Move the canonical name to its unique no-replace tombstone.
    MoveToTombstone,
    /// Remove the already-observed unique tombstone.
    RemoveTombstone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceEntry {
    Canonical,
    Tombstone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceOperation {
    VerifyRoot,
    OpenParent,
    VerifyCanonical,
    VerifyTombstone,
    Rename,
    Unlink,
    SyncParentOrHandle,
    Reverify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NamespaceFailureClass {
    SharingViolation,
    LockViolation,
    DeletePending,
    PermissionDenied,
    NotFoundNeedsReconciliation,
    Interrupted,
    OtherIo,
}

/// Exact operation/code retained for a retryable or failed namespace attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceFailure {
    operation: NamespaceOperation,
    class: NamespaceFailureClass,
    raw_code: Option<i32>,
}

impl NamespaceFailure {
    pub(super) const fn new(
        operation: NamespaceOperation,
        class: NamespaceFailureClass,
        raw_code: Option<i32>,
    ) -> Self {
        Self {
            operation,
            class,
            raw_code,
        }
    }

    pub(crate) const fn operation(&self) -> NamespaceOperation {
        self.operation
    }

    pub(crate) const fn class(&self) -> NamespaceFailureClass {
        self.class
    }

    pub(crate) const fn raw_code(&self) -> Option<i32> {
        self.raw_code
    }
}

/// Fail-closed rejection that never authorizes a namespace mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NamespacePolicyViolation {
    StoreUuidMismatch {
        root: StoreUuid,
        incarnation: StoreUuid,
    },
    RootIsNotDirectory,
    RootIsReparsePoint,
    ParentEscapedRoot,
    PhysicalKeyPlatformMismatch,
    UnexpectedEntryType {
        entry: NamespaceEntry,
    },
    ExpectedLengthMismatch {
        entry: NamespaceEntry,
        expected: u64,
        actual: u64,
    },
    TombstoneCollision {
        observed_key: Option<PhysicalFileKey>,
    },
    CanonicalRestored,
    UnsupportedTransition {
        transition: NamespaceTransition,
    },
    AuthorizationMismatch,
    NamespaceChangedDuringVerification,
}

/// Proof fields eligible for a following `Tombstoned` ledger append.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NamespaceTombstoneProof {
    request: Box<NamespaceRetirementRequest>,
    replacement_key: Option<PhysicalFileKey>,
}

impl NamespaceTombstoneProof {
    pub(super) fn new(request: &NamespaceRetirementRequest, replacement_key: Option<PhysicalFileKey>) -> Self {
        Self {
            request: Box::new(request.clone()),
            replacement_key,
        }
    }

    pub(crate) fn request(&self) -> &NamespaceRetirementRequest {
        self.request.as_ref()
    }

    pub(crate) const fn replacement_key(&self) -> Option<PhysicalFileKey> {
        self.replacement_key
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement) fn verified_for_test(
        request: &NamespaceRetirementRequest,
        replacement_key: Option<PhysicalFileKey>,
    ) -> Self {
        Self::new(request, replacement_key)
    }
}

/// Positive two-name absence proof eligible for a following `NamespaceAbsent` ledger append.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NamespaceAbsenceProof {
    request: Box<NamespaceRetirementRequest>,
    replacement_key: Option<PhysicalFileKey>,
}

impl NamespaceAbsenceProof {
    pub(super) fn new(request: &NamespaceRetirementRequest, replacement_key: Option<PhysicalFileKey>) -> Self {
        Self {
            request: Box::new(request.clone()),
            replacement_key,
        }
    }

    pub(crate) fn request(&self) -> &NamespaceRetirementRequest {
        self.request.as_ref()
    }

    pub(crate) const fn replacement_key(&self) -> Option<PhysicalFileKey> {
        self.replacement_key
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement) fn verified_for_test(
        request: &NamespaceRetirementRequest,
        replacement_key: Option<PhysicalFileKey>,
    ) -> Self {
        Self::new(request, replacement_key)
    }
}

/// Complete typed result of one authorized transition attempt.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NamespaceTransitionOutcome {
    Tombstoned(NamespaceTombstoneProof),
    NamespaceAbsentVerified(NamespaceAbsenceProof),
    Superseded {
        expected_key: PhysicalFileKey,
        observed_key: PhysicalFileKey,
    },
    Retryable(NamespaceFailure),
    Failed(NamespaceFailure),
    Rejected(NamespacePolicyViolation),
    Unsupported {
        platform: &'static str,
        reason: &'static str,
    },
}

mod private {
    #[derive(Debug)]
    pub(super) struct MutationSeal;
}

/// Write-disabled M3 authorization consumed by the only production mutation entry point.
///
/// The seal has no production constructor. A later Wave-B change must deliberately introduce a
/// reviewed activation capability rather than accidentally reaching the staged backend.
#[derive(Debug)]
pub(crate) struct NamespaceMutationAuthorization {
    _seal: private::MutationSeal,
    request: NamespaceRetirementRequest,
    transition: NamespaceTransition,
}

impl NamespaceMutationAuthorization {
    pub(super) fn from_durable_stage(request: &NamespaceRetirementRequest, transition: NamespaceTransition) -> Self {
        Self {
            _seal: private::MutationSeal,
            request: request.clone(),
            transition,
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(request: &NamespaceRetirementRequest, transition: NamespaceTransition) -> Self {
        Self::from_durable_stage(request, transition)
    }

    pub(super) fn authorizes(&self, request: &NamespaceRetirementRequest, transition: NamespaceTransition) -> bool {
        self.request == *request && self.transition == transition
    }
}
