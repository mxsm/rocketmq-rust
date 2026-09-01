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

//! Durable stage advancement for one namespace-reaper observation.

use super::LogicalRemovedCapability;
use super::NamespaceAbsentCapability;
use super::RegistryViolation;
use super::TombstonedCapability;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::io::LedgerIo;
use crate::mapped_file::retirement::platform::apply_authorized_namespace_transition;
use crate::mapped_file::retirement::platform::authorize_namespace_transition;
use crate::mapped_file::retirement::platform::authorize_tombstone_removal;
use crate::mapped_file::retirement::platform::AuthorizedNamespaceTransitionResult;
use crate::mapped_file::retirement::platform::NamespaceFailure;
use crate::mapped_file::retirement::platform::NamespacePolicyViolation;
use crate::mapped_file::retirement::platform::NamespaceRequestViolation;
use crate::mapped_file::retirement::platform::NamespaceTransition;
use crate::mapped_file::retirement::platform::NamespaceTransitionOutcome;
use crate::mapped_file::retirement::platform::VerifiedNamespaceRoot;
use crate::mapped_file::retirement::writer::ManagedLedgerWriter;
use crate::mapped_file::retirement::writer::ManagedLedgerWriterFailure;
use thiserror::Error;

/// Durable or pending result after one logical-removal namespace observation.
pub(in crate::mapped_file::retirement) enum LogicalNamespaceProgress<O> {
    Tombstoned(TombstonedCapability<O>),
    NamespaceAbsent(NamespaceAbsentCapability<O>),
    Pending {
        capability: LogicalRemovedCapability<O>,
        status: NamespacePending,
    },
}

/// Durable or pending result after one tombstone-removal namespace observation.
#[allow(
    clippy::large_enum_variant,
    reason = "the pending arm intentionally retains the durable capability together with the typed namespace disposition"
)]
pub(in crate::mapped_file::retirement) enum TombstoneNamespaceProgress<O> {
    NamespaceAbsent(Box<NamespaceAbsentCapability<O>>),
    Pending {
        capability: Box<TombstonedCapability<O>>,
        status: NamespacePending,
    },
}

/// Namespace work that retains the current durable-stage capability for a later retry.
#[derive(Debug)]
pub(in crate::mapped_file::retirement) enum NamespacePending {
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
    Verification(NamespaceTransitionOutcome),
    UnexpectedOutcome(&'static str),
}

/// Fail-closed errors that consume an ambiguous capability and require replay.
#[derive(Debug, Error)]
pub(in crate::mapped_file::retirement) enum ReaperDriveFailure {
    #[error(transparent)]
    Request(#[from] NamespaceRequestViolation),
    #[error(transparent)]
    Writer(#[from] ManagedLedgerWriterFailure),
}

/// Performs one complete handle-relative attempt for a durable logical-removal capability.
pub(in crate::mapped_file::retirement) fn drive_logical_namespace<I: LedgerIo, O>(
    root: &VerifiedNamespaceRoot,
    writer: &mut ManagedLedgerWriter<I>,
    capability: LogicalRemovedCapability<O>,
    transition: NamespaceTransition,
    observation_time_ns: u64,
) -> Result<LogicalNamespaceProgress<O>, ReaperDriveFailure> {
    let authorization = authorize_namespace_transition(capability, transition)?;
    let verified = match root.reserve_authorized(authorization) {
        Ok(verified) => verified,
        Err(failure) => {
            let (authorization, error) = failure.into_parts();
            return Ok(LogicalNamespaceProgress::Pending {
                capability: authorization.into_capability(),
                status: NamespacePending::Verification(error),
            });
        }
    };
    let result = apply_authorized_namespace_transition(verified);
    commit_logical_namespace_outcome(writer, result, observation_time_ns).map_err(Into::into)
}

/// Performs one complete handle-relative attempt for a durable tombstone capability.
pub(in crate::mapped_file::retirement) fn drive_tombstone_namespace<I: LedgerIo, O>(
    root: &VerifiedNamespaceRoot,
    writer: &mut ManagedLedgerWriter<I>,
    capability: TombstonedCapability<O>,
    observation_time_ns: u64,
) -> Result<TombstoneNamespaceProgress<O>, ReaperDriveFailure> {
    let authorization = authorize_tombstone_removal(capability)?;
    let verified = match root.reserve_authorized(authorization) {
        Ok(verified) => verified,
        Err(failure) => {
            let (authorization, error) = failure.into_parts();
            return Ok(TombstoneNamespaceProgress::Pending {
                capability: Box::new(authorization.into_capability()),
                status: NamespacePending::Verification(error),
            });
        }
    };
    let result = apply_authorized_namespace_transition(verified);
    commit_tombstone_namespace_outcome(writer, result, observation_time_ns).map_err(Into::into)
}

/// Commits the durable record implied by one verified logical-removal namespace result.
pub(in crate::mapped_file::retirement) fn commit_logical_namespace_outcome<I: LedgerIo, O>(
    writer: &mut ManagedLedgerWriter<I>,
    result: AuthorizedNamespaceTransitionResult<LogicalRemovedCapability<O>>,
    observation_time_ns: u64,
) -> Result<LogicalNamespaceProgress<O>, ManagedLedgerWriterFailure> {
    let (capability, outcome) = result.into_parts();
    match outcome {
        NamespaceTransitionOutcome::Tombstoned(proof) => writer
            .append_tombstoned(capability, proof)
            .map(LogicalNamespaceProgress::Tombstoned),
        NamespaceTransitionOutcome::NamespaceAbsentVerified(proof) => writer
            .append_namespace_absent(capability, proof, observation_time_ns)
            .map(LogicalNamespaceProgress::NamespaceAbsent),
        NamespaceTransitionOutcome::Superseded {
            expected_key,
            observed_key,
        } => {
            if expected_key != capability.binding().target_key() {
                return Err(RegistryViolation::NamespaceProofMismatch {
                    ticket_id: capability.binding().ticket_id(),
                }
                .into());
            }
            let capability = writer.append_superseded_path_after_logical(capability, observed_key)?;
            Ok(LogicalNamespaceProgress::Pending {
                capability,
                status: NamespacePending::Superseded {
                    expected_key,
                    observed_key,
                },
            })
        }
        NamespaceTransitionOutcome::Retryable(failure) => Ok(LogicalNamespaceProgress::Pending {
            capability,
            status: NamespacePending::Retryable(failure),
        }),
        NamespaceTransitionOutcome::Failed(failure) => Ok(LogicalNamespaceProgress::Pending {
            capability,
            status: NamespacePending::Failed(failure),
        }),
        NamespaceTransitionOutcome::Rejected(violation) => Ok(LogicalNamespaceProgress::Pending {
            capability,
            status: NamespacePending::Rejected(violation),
        }),
        NamespaceTransitionOutcome::Unsupported { platform, reason } => Ok(LogicalNamespaceProgress::Pending {
            capability,
            status: NamespacePending::Unsupported { platform, reason },
        }),
    }
}

/// Commits the durable absence record implied by one verified tombstone-removal result.
pub(in crate::mapped_file::retirement) fn commit_tombstone_namespace_outcome<I: LedgerIo, O>(
    writer: &mut ManagedLedgerWriter<I>,
    result: AuthorizedNamespaceTransitionResult<TombstonedCapability<O>>,
    observation_time_ns: u64,
) -> Result<TombstoneNamespaceProgress<O>, ManagedLedgerWriterFailure> {
    let (capability, outcome) = result.into_parts();
    match outcome {
        NamespaceTransitionOutcome::NamespaceAbsentVerified(proof) => writer
            .append_namespace_absent_after_tombstone(capability, proof, observation_time_ns)
            .map(|capability| TombstoneNamespaceProgress::NamespaceAbsent(Box::new(capability))),
        NamespaceTransitionOutcome::Retryable(failure) => Ok(TombstoneNamespaceProgress::Pending {
            capability: Box::new(capability),
            status: NamespacePending::Retryable(failure),
        }),
        NamespaceTransitionOutcome::Failed(failure) => Ok(TombstoneNamespaceProgress::Pending {
            capability: Box::new(capability),
            status: NamespacePending::Failed(failure),
        }),
        NamespaceTransitionOutcome::Rejected(violation) => Ok(TombstoneNamespaceProgress::Pending {
            capability: Box::new(capability),
            status: NamespacePending::Rejected(violation),
        }),
        NamespaceTransitionOutcome::Unsupported { platform, reason } => Ok(TombstoneNamespaceProgress::Pending {
            capability: Box::new(capability),
            status: NamespacePending::Unsupported { platform, reason },
        }),
        NamespaceTransitionOutcome::Tombstoned(_) => Ok(TombstoneNamespaceProgress::Pending {
            capability: Box::new(capability),
            status: NamespacePending::UnexpectedOutcome("RemoveTombstone returned Tombstoned"),
        }),
        NamespaceTransitionOutcome::Superseded { .. } => Ok(TombstoneNamespaceProgress::Pending {
            capability: Box::new(capability),
            status: NamespacePending::UnexpectedOutcome("RemoveTombstone returned Superseded"),
        }),
    }
}

#[cfg(test)]
mod tests;
