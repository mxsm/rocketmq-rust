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

use std::fs::File;
use std::io;

use super::identity::StoreRelativePath;

mod creation;
#[allow(dead_code, reason = "M3 stages the namespace engine before Wave-B reaper wiring")]
mod engine;
mod physical_key;
#[allow(dead_code, reason = "M3 stages typed namespace proofs before Wave-B reaper wiring")]
mod types;

#[cfg(target_os = "linux")]
#[allow(dead_code, reason = "M3 stages Linux namespace mutation before Wave-B reaper wiring")]
#[path = "platform/linux.rs"]
mod native;
#[cfg(windows)]
#[allow(
    dead_code,
    reason = "M3 stages Windows namespace mutation before Wave-B reaper wiring"
)]
#[path = "platform/windows.rs"]
mod native;
#[cfg(not(any(target_os = "linux", windows)))]
#[allow(dead_code, reason = "M3 retains a typed unsupported backend for other targets")]
#[path = "platform/unsupported.rs"]
mod native;
#[cfg(all(test, any(target_os = "linux", windows)))]
#[allow(
    dead_code,
    reason = "supported-target tests compile the unsupported backend contract"
)]
#[path = "platform/unsupported.rs"]
mod unsupported_contract;

#[allow(unused_imports, reason = "M3 proof types are staged for the future reaper boundary")]
pub(crate) use types::NamespaceAbsenceProof;
#[allow(unused_imports, reason = "M3 proof types are staged for the future reaper boundary")]
pub(crate) use types::NamespaceFailure;
#[allow(unused_imports, reason = "M3 proof types are staged for the future reaper boundary")]
pub(crate) use types::NamespaceFailureClass;
pub(crate) use types::NamespaceMutationAuthorization;
#[allow(unused_imports, reason = "M3 proof types are staged for the future reaper boundary")]
pub(crate) use types::NamespaceOperation;
#[allow(unused_imports, reason = "M3 proof types are staged for the future reaper boundary")]
pub(crate) use types::NamespacePolicyViolation;
#[allow(
    unused_imports,
    reason = "M3 request errors are staged for the future reaper boundary"
)]
pub(crate) use types::NamespaceRequestViolation;
pub(crate) use types::NamespaceRetirementRequest;
#[allow(
    unused_imports,
    reason = "M3 ticket bindings are staged for the future reaper boundary"
)]
pub(crate) use types::NamespaceTicketBinding;
#[allow(unused_imports, reason = "M3 proof types are staged for the future reaper boundary")]
pub(crate) use types::NamespaceTombstoneProof;
pub(crate) use types::NamespaceTransition;
pub(crate) use types::NamespaceTransitionOutcome;

pub(in crate::mapped_file::retirement) use creation::IncarnationCreationError;
#[cfg(test)]
pub(in crate::mapped_file::retirement) use creation::IncarnationCreationStage;

use super::identity::PhysicalFileKey;
use super::identity::StoreUuid;
use super::registry::LogicalRemovedCapability;
use super::registry::TombstonedCapability;

#[cfg(all(test, any(target_os = "linux", windows)))]
const _: fn(
    &unsupported_contract::NamespaceRoot,
    &StoreRelativePath,
    PhysicalFileKey,
    u64,
) -> Result<File, NamespaceTransitionOutcome> = unsupported_contract::NamespaceRoot::open_active_segment;

/// Exact namespace mutation authority derived from one durable `LogicalRemoved` stage.
///
/// The value is non-cloneable and retains the registry capability. Dropping it before either a
/// retry result or a following durable stage therefore recovery-fences the registry.
#[derive(Debug)]
pub(crate) struct AuthorizedNamespaceTransition<C> {
    capability: C,
    request: NamespaceRetirementRequest,
    transition: NamespaceTransition,
    authorization: NamespaceMutationAuthorization,
}

impl<C> AuthorizedNamespaceTransition<C> {
    pub(crate) const fn request(&self) -> &NamespaceRetirementRequest {
        &self.request
    }

    pub(crate) const fn transition(&self) -> NamespaceTransition {
        self.transition
    }

    pub(in crate::mapped_file::retirement) fn into_capability(self) -> C {
        self.capability
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement) fn into_parts_for_test(self) -> (C, NamespaceRetirementRequest) {
        (self.capability, self.request)
    }
}

/// Reservation failure that preserves the exact unconsumed logical-removal capability.
#[derive(Debug)]
pub(crate) struct NamespaceReservationFailure<C> {
    authorization: Box<AuthorizedNamespaceTransition<C>>,
    error: NamespaceTransitionOutcome,
}

impl<C> NamespaceReservationFailure<C> {
    pub(crate) fn into_parts(self) -> (AuthorizedNamespaceTransition<C>, NamespaceTransitionOutcome) {
        (*self.authorization, self.error)
    }
}

/// Handle-relative reservation paired with its exact durable registry capability.
pub(crate) struct VerifiedAuthorizedNamespaceTransition<C> {
    authorization: AuthorizedNamespaceTransition<C>,
    reservation: VerifiedPathReservation,
}

/// One namespace result paired with the capability needed for the next durable ledger stage.
#[derive(Debug)]
pub(crate) struct AuthorizedNamespaceTransitionResult<C> {
    capability: C,
    outcome: NamespaceTransitionOutcome,
}

impl<C> AuthorizedNamespaceTransitionResult<C> {
    pub(crate) fn into_parts(self) -> (C, NamespaceTransitionOutcome) {
        (self.capability, self.outcome)
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement) fn for_test(capability: C, outcome: NamespaceTransitionOutcome) -> Self {
        Self { capability, outcome }
    }
}

/// Consumes a durable logical-removal capability and derives every namespace field from it.
pub(crate) fn authorize_namespace_transition<O>(
    capability: LogicalRemovedCapability<O>,
    transition: NamespaceTransition,
) -> Result<AuthorizedNamespaceTransition<LogicalRemovedCapability<O>>, NamespaceRequestViolation> {
    if transition == NamespaceTransition::RemoveTombstone {
        return Err(NamespaceRequestViolation::TombstoneStageRequired);
    }
    let binding = capability.binding();
    let ticket = NamespaceTicketBinding::new(
        binding.ticket_id(),
        binding.incarnation(),
        binding.reason(),
        binding.segment_offset(),
        binding.mapping_generation(),
        binding.expected_length(),
        binding.retirement_nonce(),
    )?;
    let tombstone_path = binding.canonical_path().tombstone_path(
        binding.ticket_id(),
        binding.incarnation(),
        binding.segment_offset(),
        binding.mapping_generation(),
        &binding.retirement_nonce(),
    )?;
    let request = NamespaceRetirementRequest::new(
        ticket,
        binding.target_key(),
        binding.canonical_path().clone(),
        tombstone_path,
    )?
    .with_recorded_replacement_key(capability.observed_replacement_key());
    let authorization = NamespaceMutationAuthorization::from_durable_stage(&request, transition);
    Ok(AuthorizedNamespaceTransition {
        capability,
        request,
        transition,
        authorization,
    })
}

/// Consumes a durable tombstone capability and authorizes removal of that exact tombstone only.
pub(crate) fn authorize_tombstone_removal<O>(
    capability: TombstonedCapability<O>,
) -> Result<AuthorizedNamespaceTransition<TombstonedCapability<O>>, NamespaceRequestViolation> {
    let binding = capability.binding();
    let ticket = NamespaceTicketBinding::new(
        binding.ticket_id(),
        binding.incarnation(),
        binding.reason(),
        binding.segment_offset(),
        binding.mapping_generation(),
        binding.expected_length(),
        binding.retirement_nonce(),
    )?;
    let request = NamespaceRetirementRequest::new(
        ticket,
        binding.target_key(),
        binding.canonical_path().clone(),
        capability.tombstone_path().clone(),
    )?;
    let authorization =
        NamespaceMutationAuthorization::from_durable_stage(&request, NamespaceTransition::RemoveTombstone);
    Ok(AuthorizedNamespaceTransition {
        capability,
        request,
        transition: NamespaceTransition::RemoveTombstone,
        authorization,
    })
}

/// Reads a stable physical-file key from an already-open handle.
///
/// The result compares handles only on the same host and mounted filesystem. Backup restore or
/// filesystem migration requires an explicit offline rebind and never silently changes this key.
pub(crate) fn physical_file_key(file: &File) -> io::Result<PhysicalFileKey> {
    physical_key::capture(file)
}

/// Retained, verified Store-root handle from which all namespace reservations are derived.
///
/// Private fields and the validating constructor prevent callers from fabricating a root
/// capability from a path string.
#[allow(dead_code, reason = "M3 stages verified roots before Wave-B reaper wiring")]
pub(crate) struct VerifiedNamespaceRoot {
    store_uuid: StoreUuid,
    native: native::NamespaceRoot,
}

#[allow(dead_code, reason = "M3 stages verified roots before Wave-B reaper wiring")]
impl VerifiedNamespaceRoot {
    /// Constructs a root for native backend tests only.
    ///
    /// Production construction remains unavailable until Wave-B can consume the retained Store
    /// lock, root-identity, activation-fence, and replay-inventory proofs as one opaque capability.
    #[cfg(test)]
    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(crate) fn open(file: File, store_uuid: StoreUuid) -> Result<Self, NamespaceTransitionOutcome> {
        let native = native::NamespaceRoot::open(file)?;
        Ok(Self { store_uuid, native })
    }

    /// Derives namespace authority only from the opaque reconciled session that still owns the
    /// exact Store-root lock and retained root handle.
    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(in crate::mapped_file::retirement) fn from_reconciled_session(
        session: &super::state::reconciliation::ReconciledLifecycleSession,
    ) -> Result<Self, NamespaceTransitionOutcome> {
        let file = session.retained_root().try_clone().map_err(|error| {
            NamespaceTransitionOutcome::Failed(types::NamespaceFailure::new(
                types::NamespaceOperation::VerifyRoot,
                types::NamespaceFailureClass::OtherIo,
                error.raw_os_error(),
            ))
        })?;
        let native = native::NamespaceRoot::open(file)?;
        Ok(Self {
            store_uuid: session.writer_frontier().store_uuid(),
            native,
        })
    }

    /// Opens one replay-authorized active segment for writable mapping from the retained root.
    ///
    /// The native backend performs strict handle-relative, no-follow resolution and revalidates
    /// the exact physical key and durable length before returning the handle.
    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(in crate::mapped_file::retirement) fn open_active_segment(
        &self,
        path: &StoreRelativePath,
        physical_key: PhysicalFileKey,
        expected_length: u64,
    ) -> Result<File, NamespaceTransitionOutcome> {
        self.native.open_active_segment(path, physical_key, expected_length)
    }

    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(crate) fn reserve(
        &self,
        request: NamespaceRetirementRequest,
        transition: NamespaceTransition,
    ) -> Result<VerifiedPathReservation, NamespaceTransitionOutcome> {
        let incarnation_uuid = request.ticket().incarnation().store_uuid();
        if incarnation_uuid != self.store_uuid {
            return Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::StoreUuidMismatch {
                    root: self.store_uuid,
                    incarnation: incarnation_uuid,
                },
            ));
        }
        let native = self.native.reserve(&request, transition)?;
        Ok(VerifiedPathReservation {
            request,
            transition,
            native,
        })
    }

    /// Opens all namespace objects for one exact durable logical-removal authorization.
    #[allow(
        clippy::result_large_err,
        reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
    )]
    pub(crate) fn reserve_authorized<C>(
        &self,
        authorization: AuthorizedNamespaceTransition<C>,
    ) -> Result<VerifiedAuthorizedNamespaceTransition<C>, NamespaceReservationFailure<C>> {
        let request = authorization.request().clone();
        let transition = authorization.transition();
        match self.reserve(request, transition) {
            Ok(reservation) => Ok(VerifiedAuthorizedNamespaceTransition {
                authorization,
                reservation,
            }),
            Err(error) => Err(NamespaceReservationFailure {
                authorization: Box::new(authorization),
                error,
            }),
        }
    }
}

/// Exact path/key/ticket reservation holding the verified parent and target handles.
///
/// It is neither public nor Clone and can only be produced by [`VerifiedNamespaceRoot::reserve`].
#[allow(dead_code, reason = "M3 stages verified reservations before Wave-B reaper wiring")]
pub(crate) struct VerifiedPathReservation {
    request: NamespaceRetirementRequest,
    transition: NamespaceTransition,
    native: native::NamespaceReservation,
}

/// Performs one handle-relative transition and consumes the write-disabled M3 authorization.
///
/// There is deliberately no production constructor for [`NamespaceMutationAuthorization`]. This
/// crate-private entry point therefore cannot enable managed retirement until a reviewed Wave-B
/// activation change supplies a durable writer-derived authorization.
#[allow(dead_code, reason = "M3 stages namespace mutation before Wave-B reaper wiring")]
pub(crate) fn apply_namespace_transition(
    reservation: VerifiedPathReservation,
    authorization: NamespaceMutationAuthorization,
) -> NamespaceTransitionOutcome {
    let VerifiedPathReservation {
        request,
        transition,
        mut native,
    } = reservation;
    engine::advance(&request, transition, &mut native, authorization)
}

/// Performs one authorized transition while preserving the capability for retry or stage append.
pub(crate) fn apply_authorized_namespace_transition<C>(
    verified: VerifiedAuthorizedNamespaceTransition<C>,
) -> AuthorizedNamespaceTransitionResult<C> {
    let VerifiedAuthorizedNamespaceTransition {
        authorization,
        reservation,
    } = verified;
    let AuthorizedNamespaceTransition {
        capability,
        request,
        transition,
        authorization,
    } = authorization;
    let VerifiedPathReservation {
        request: reserved_request,
        transition: reserved_transition,
        mut native,
    } = reservation;
    let outcome = if request == reserved_request && transition == reserved_transition {
        engine::advance(&request, transition, &mut native, authorization)
    } else {
        NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::AuthorizationMismatch)
    };
    AuthorizedNamespaceTransitionResult { capability, outcome }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod native_tests;

#[cfg(test)]
mod physical_key_tests {
    use std::fs::OpenOptions;

    use tempfile::tempdir;

    use super::physical_file_key;

    #[test]
    fn the_same_open_file_has_a_stable_physical_key() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("segment");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create segment");
        let reopened = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("reopen segment");

        let first = physical_file_key(&file).expect("read first physical key");
        let second = physical_file_key(&file).expect("read key again");
        let reopened_key = physical_file_key(&reopened).expect("read reopened physical key");

        assert_eq!(first, second);
        assert_eq!(first, reopened_key);
    }

    #[test]
    fn replacement_at_the_same_path_has_a_different_physical_key() {
        let directory = tempdir().expect("create temporary directory");
        let path = directory.path().join("segment");
        let original = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create original segment");
        let original_key = physical_file_key(&original).expect("read original physical key");

        std::fs::remove_file(&path).expect("unlink original namespace");
        let replacement = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .expect("create replacement segment");
        let replacement_key = physical_file_key(&replacement).expect("read replacement physical key");

        assert_ne!(original_key, replacement_key);
    }
}
