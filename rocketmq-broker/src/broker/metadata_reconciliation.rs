// Copyright 2023 The RocketMQ Rust Authors
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

//! Business conclusions for Broker metadata writes.
//!
//! `MetadataIoActor` reports what a caller confirmed as a
//! `MetadataIoCommitObservation`. An observation timeout is not a failure: the
//! admitted generation keeps its ordering and byte charge and may still replace
//! the target file. This module turns that observation into the conclusion a
//! Broker owner has to act on, so no caller treats an unobserved write as a
//! definitely-uncommitted one.
//!
//! A conclusion never gates admission. A Broker-wide sticky flag could only be
//! cleared by a restart, which would turn one transient timeout into
//! permanently stopped periodic persistence. The observable record is the
//! conclusion itself: a supervised per-key dirty marker stays set, and the
//! resource and generation are reported through the existing tracing surface so
//! an operator can resolve them against
//! `MetadataIoActor::confirmed_durable_generation`.

use rocketmq_error::SharedError;
use rocketmq_runtime::MetadataGeneration;
use rocketmq_runtime::MetadataIoCommitObservation;
use rocketmq_runtime::MetadataIoCommitOutcome;

use crate::broker_error;

/// Why a metadata write has to be treated as unconfirmed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnconfirmedReason {
    /// The caller's observation deadline elapsed while the admitted generation
    /// continued.
    ObservationExpired,
    /// The target may have been replaced, but the durability protocol did not
    /// complete.
    CommitUnconfirmed,
}

/// What a Broker owner knows about one metadata write after observing it.
#[derive(Debug)]
pub(crate) enum MetadataWriteConclusion {
    /// The generation, or a newer coalesced generation, completed the
    /// persistence protocol.
    Durable(MetadataGeneration),
    /// The snapshot reached the target through a blocking write that carries no
    /// generation identity, such as the legacy path used when no metadata actor
    /// is configured.
    BlockingPersisted,
    /// The target file was not replaced. The change may be retried or
    /// discarded, and a newer snapshot supersedes it.
    FailedBeforeCommit(SharedError),
    /// The generation may still reach the target file. The caller must not
    /// report it as a definite failure, and must not build newer state on it
    /// until the actor confirms the generation.
    Unconfirmed {
        /// The logical resource the generation belongs to.
        resource: &'static str,
        /// The generation whose durability was not confirmed.
        generation: MetadataGeneration,
        /// Why the write is unconfirmed.
        reason: UnconfirmedReason,
        /// The error to report to the caller.
        source: SharedError,
    },
    /// The resource is bound to a different process-local target path.
    TargetConflict(SharedError),
}

impl MetadataWriteConclusion {
    /// Returns whether the write is known to be durable.
    pub(crate) fn is_durable(&self) -> bool {
        matches!(self, Self::Durable(_) | Self::BlockingPersisted)
    }

    /// Returns whether a supervised per-key dirty marker has to stay set.
    ///
    /// The supervised compare-and-set contract is that a client cannot build a
    /// newer state on one whose durability is unknown, so the marker is
    /// released only by a conclusion that is known durable. Every other
    /// conclusion, including an unconfirmed replacement, keeps it. This is the
    /// behavior the existing supervisors already have; an unconfirmed
    /// replacement is now reported as such instead of being indistinguishable
    /// from a definite failure.
    pub(crate) fn retains_dirty_marker(&self) -> bool {
        !self.is_durable()
    }

    /// Builds an unconfirmed conclusion for in-crate tests that only need to
    /// exercise the marker rules.
    #[cfg(test)]
    pub(crate) fn unconfirmed_for_test() -> Self {
        Self::Unconfirmed {
            resource: "test.metadata",
            generation: MetadataGeneration::new(1),
            reason: UnconfirmedReason::ObservationExpired,
            source: broker_error::internal("metadata_io", std::io::Error::other("unconfirmed test write")),
        }
    }
}

/// Classifies one observed metadata write for a Broker owner.
///
/// The unconfirmed case is reported at warn level with the resource and
/// generation, which are bounded values. The record is observational: it never
/// blocks later writes.
pub(crate) fn conclude_metadata_write(
    resource: &'static str,
    observation: MetadataIoCommitObservation,
) -> MetadataWriteConclusion {
    match observation {
        MetadataIoCommitObservation::Settled {
            outcome: MetadataIoCommitOutcome::Durable(generation),
            ..
        } => MetadataWriteConclusion::Durable(generation),
        MetadataIoCommitObservation::Settled {
            outcome: MetadataIoCommitOutcome::FailedBeforeCommit(error),
            ..
        } => MetadataWriteConclusion::FailedBeforeCommit(broker_error::internal("metadata_io", error)),
        MetadataIoCommitObservation::Settled {
            generation,
            outcome: MetadataIoCommitOutcome::CommitOutcomeUnknown(error),
        } => record_unconfirmed(resource, generation, UnconfirmedReason::CommitUnconfirmed, error),
        MetadataIoCommitObservation::Unobserved { generation } => record_unconfirmed(
            resource,
            generation,
            UnconfirmedReason::ObservationExpired,
            rocketmq_runtime::RuntimeError::timed_out(rocketmq_runtime::RuntimeOperation::WaitForDurableMetadata),
        ),
        MetadataIoCommitObservation::TargetConflict(request) => {
            MetadataWriteConclusion::TargetConflict(broker_error::storage_write_failed(
                request.target().display().to_string(),
                "metadata resource target conflict",
            ))
        }
    }
}

fn record_unconfirmed(
    resource: &'static str,
    generation: MetadataGeneration,
    reason: UnconfirmedReason,
    source: rocketmq_runtime::RuntimeError,
) -> MetadataWriteConclusion {
    // A later snapshot of the same resource supersedes this generation, and a
    // confirmed generation resolves it. Until then the write is reported as
    // unconfirmed rather than as definitely uncommitted.
    tracing::warn!(
        resource,
        generation = generation.get(),
        reason = ?reason,
        "broker metadata write is unconfirmed and may still reach the target file"
    );
    MetadataWriteConclusion::Unconfirmed {
        resource,
        generation,
        reason,
        source: broker_error::internal("metadata_io", source),
    }
}

/// Converts a non-durable conclusion into the error the caller has to report.
pub(crate) fn conclusion_error(conclusion: &MetadataWriteConclusion) -> Option<SharedError> {
    match conclusion {
        MetadataWriteConclusion::Durable(_) | MetadataWriteConclusion::BlockingPersisted => None,
        MetadataWriteConclusion::FailedBeforeCommit(error) | MetadataWriteConclusion::TargetConflict(error) => {
            Some(error.clone())
        }
        MetadataWriteConclusion::Unconfirmed { source, .. } => Some(source.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn observation(outcome: MetadataIoCommitOutcome) -> MetadataIoCommitObservation {
        MetadataIoCommitObservation::Settled {
            generation: MetadataGeneration::new(4),
            outcome,
        }
    }

    fn sync_parent_error() -> rocketmq_runtime::RuntimeError {
        rocketmq_runtime::RuntimeError::io(
            rocketmq_runtime::RuntimeOperation::MetadataSyncParent,
            io::Error::other("injected parent directory sync failure"),
        )
    }

    #[test]
    fn every_observation_maps_to_one_conclusion() {
        let durable = conclude_metadata_write(
            "broker.topic-config",
            observation(MetadataIoCommitOutcome::Durable(MetadataGeneration::new(4))),
        );
        assert!(durable.is_durable());
        assert!(!durable.retains_dirty_marker());
        assert!(conclusion_error(&durable).is_none());

        let failed = conclude_metadata_write(
            "broker.topic-config",
            observation(MetadataIoCommitOutcome::FailedBeforeCommit(sync_parent_error())),
        );
        assert!(!failed.is_durable());
        assert!(
            failed.retains_dirty_marker(),
            "a supervised key stays marked until its snapshot is known durable"
        );
        assert!(conclusion_error(&failed).is_some());

        let unknown = conclude_metadata_write(
            "broker.topic-config",
            observation(MetadataIoCommitOutcome::CommitOutcomeUnknown(sync_parent_error())),
        );
        assert!(unknown.retains_dirty_marker());
        assert!(conclusion_error(&unknown).is_some());
        assert!(matches!(
            unknown,
            MetadataWriteConclusion::Unconfirmed {
                resource: "broker.topic-config",
                generation,
                reason: UnconfirmedReason::CommitUnconfirmed,
                ..
            } if generation == MetadataGeneration::new(4)
        ));

        let unobserved = conclude_metadata_write(
            "broker.topic-config",
            MetadataIoCommitObservation::Unobserved {
                generation: MetadataGeneration::new(4),
            },
        );
        assert!(unobserved.retains_dirty_marker());
        assert!(matches!(
            unobserved,
            MetadataWriteConclusion::Unconfirmed {
                reason: UnconfirmedReason::ObservationExpired,
                ..
            }
        ));

        let conflict = conclude_metadata_write(
            "broker.topic-config",
            MetadataIoCommitObservation::TargetConflict(rocketmq_runtime::MetadataWriteRequest::new(
                "broker.topic-config",
                MetadataGeneration::new(4),
                std::path::PathBuf::from("topic-config.json"),
                b"snapshot".to_vec(),
            )),
        );
        assert!(!conflict.is_durable());
        assert!(conflict.retains_dirty_marker());
        assert!(conclusion_error(&conflict).is_some());
    }
}
