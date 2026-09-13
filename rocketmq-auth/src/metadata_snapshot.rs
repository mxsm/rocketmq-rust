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

//! Snapshot persistence classification for the local metadata providers.
//!
//! Both local providers own exactly one metadata resource, so they share one
//! decision. A snapshot that was never admitted, or that failed before the
//! target was replaced, must not be published. A snapshot that may have
//! replaced the target file has to be published anyway: dropping it would
//! leave memory older than the durable state, and the next unrelated mutation
//! would then encode that older snapshot over the newer file. The recorded
//! generation keeps the provider reconciled until the actor confirms it.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use rocketmq_runtime::MetadataGeneration;
use rocketmq_runtime::MetadataIoCommitObservation;
use rocketmq_runtime::MetadataIoCommitOutcome;

use crate::AuthServiceError;

/// What a provider confirmed about one snapshot it asked to persist.
#[derive(Debug)]
pub(crate) enum SnapshotPersistence {
    /// The snapshot, or a newer coalesced snapshot, is durable.
    Durable {
        /// The confirmed generation.
        generation: MetadataGeneration,
    },
    /// The target file was not replaced, so the change must not be published.
    NotWritten(AuthServiceError),
    /// The target file may have been replaced. The caller has to publish the
    /// accepted change and keep the provider reconciled.
    Unconfirmed {
        /// The generation whose durability was not confirmed.
        generation: MetadataGeneration,
        /// The error to report to the caller.
        error: AuthServiceError,
    },
}

impl SnapshotPersistence {
    /// Returns whether the snapshot reached the target file.
    ///
    /// True for a confirmed and for an unconfirmed replacement, because an
    /// unconfirmed one may also have landed.
    pub(crate) fn may_have_written(&self) -> bool {
        !matches!(self, Self::NotWritten(_))
    }

    /// Consumes the outcome and returns the error to report, if any.
    pub(crate) fn into_error(self) -> Option<AuthServiceError> {
        match self {
            Self::Durable { .. } => None,
            Self::NotWritten(error) | Self::Unconfirmed { error, .. } => Some(error),
        }
    }
}

/// Classifies one observed metadata write for a single-resource provider.
pub(crate) fn classify_snapshot_persistence(observation: MetadataIoCommitObservation) -> SnapshotPersistence {
    match observation {
        MetadataIoCommitObservation::Settled {
            outcome: MetadataIoCommitOutcome::Durable(generation),
            ..
        } => SnapshotPersistence::Durable { generation },
        MetadataIoCommitObservation::Settled {
            outcome: MetadataIoCommitOutcome::FailedBeforeCommit(error),
            ..
        } => SnapshotPersistence::NotWritten(AuthServiceError::metadata_io(error)),
        MetadataIoCommitObservation::Settled {
            generation,
            outcome: MetadataIoCommitOutcome::CommitOutcomeUnknown(error),
        } => SnapshotPersistence::Unconfirmed {
            generation,
            error: AuthServiceError::metadata_io(error),
        },
        MetadataIoCommitObservation::Unobserved { generation } => SnapshotPersistence::Unconfirmed {
            generation,
            error: AuthServiceError::metadata_io(rocketmq_runtime::RuntimeError::timed_out(
                rocketmq_runtime::RuntimeOperation::WaitForDurableMetadata,
            )),
        },
        MetadataIoCommitObservation::TargetConflict(_) => {
            SnapshotPersistence::NotWritten(AuthServiceError::storage_conflict())
        }
    }
}

/// The one generation a single-resource provider has not confirmed durable.
///
/// Zero means the provider has nothing to reconcile. The record is released by
/// evidence, not by a restart: a later confirmed generation that is at least
/// the recorded one resolves it.
#[derive(Debug, Default)]
pub(crate) struct ReconciliationGeneration(AtomicU64);

impl ReconciliationGeneration {
    /// Records a generation whose durability was not confirmed.
    pub(crate) fn record(&self, generation: MetadataGeneration) {
        if generation.get() != 0 {
            self.0.store(generation.get(), Ordering::Release);
        }
    }

    /// Releases the record once a generation at least as new is confirmed.
    pub(crate) fn release(&self, generation: MetadataGeneration) {
        let recorded = self.0.load(Ordering::Acquire);
        if recorded != 0 && generation.get() >= recorded {
            self.0.store(0, Ordering::Release);
        }
    }

    /// Returns whether a metadata write is still unconfirmed.
    pub(crate) fn required(&self) -> bool {
        self.0.load(Ordering::Acquire) != 0
    }

    /// Returns the generation whose durability was not confirmed.
    pub(crate) fn generation(&self) -> Option<MetadataGeneration> {
        match self.0.load(Ordering::Acquire) {
            0 => None,
            generation => Some(MetadataGeneration::new(generation)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn runtime_error() -> rocketmq_runtime::RuntimeError {
        rocketmq_runtime::RuntimeError::io(
            rocketmq_runtime::RuntimeOperation::MetadataSyncParent,
            io::Error::other("injected persistence failure"),
        )
    }

    #[test]
    fn only_a_definite_pre_commit_failure_blocks_publication() {
        let durable = classify_snapshot_persistence(MetadataIoCommitObservation::Settled {
            generation: MetadataGeneration::new(3),
            outcome: MetadataIoCommitOutcome::Durable(MetadataGeneration::new(3)),
        });
        assert!(durable.may_have_written());
        assert!(durable.into_error().is_none());

        let failed = classify_snapshot_persistence(MetadataIoCommitObservation::Settled {
            generation: MetadataGeneration::new(3),
            outcome: MetadataIoCommitOutcome::FailedBeforeCommit(runtime_error()),
        });
        assert!(!failed.may_have_written());
        assert!(failed.into_error().is_some());

        let unknown = classify_snapshot_persistence(MetadataIoCommitObservation::Settled {
            generation: MetadataGeneration::new(3),
            outcome: MetadataIoCommitOutcome::CommitOutcomeUnknown(runtime_error()),
        });
        assert!(unknown.may_have_written());
        assert!(matches!(
            unknown,
            SnapshotPersistence::Unconfirmed {
                generation,
                ..
            } if generation == MetadataGeneration::new(3)
        ));

        let unobserved = classify_snapshot_persistence(MetadataIoCommitObservation::Unobserved {
            generation: MetadataGeneration::new(4),
        });
        assert!(unobserved.may_have_written());
        assert!(matches!(unobserved, SnapshotPersistence::Unconfirmed { .. }));
    }

    #[test]
    fn a_recorded_generation_is_released_by_a_newer_confirmed_one() {
        let record = ReconciliationGeneration::default();
        assert!(!record.required());

        record.record(MetadataGeneration::new(5));
        assert!(record.required());
        assert_eq!(record.generation(), Some(MetadataGeneration::new(5)));

        // An older confirmation does not resolve it.
        record.release(MetadataGeneration::new(4));
        assert!(record.required());

        record.release(MetadataGeneration::new(5));
        assert!(!record.required());
        assert_eq!(record.generation(), None);
    }
}
