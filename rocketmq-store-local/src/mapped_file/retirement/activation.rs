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

//! Proof-gated staging for managed mapped-file lifecycle activation.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use thiserror::Error;

use super::io::FileLedgerIo;
use super::platform::NamespaceTransitionOutcome;
use super::platform::VerifiedNamespaceRoot;
use super::registry::ManagedMappedFileQueueGeneration;
use super::registry::RecoveredRetirementWork;
use super::registry::RegistryViolation;
use super::registry::RetirementRegistry;
use super::service::ManagedLifecycleRuntime;
use super::state::reconciliation::ManagedSegmentClaimViolation;
use super::state::reconciliation::ReconciledLifecycleSession;
use super::writer::open_managed_lifecycle_writer;
use super::writer::ManagedLedgerWriter;
use super::writer::ManagedLedgerWriterError;
use crate::mapped_file::queue_io::load_reconciled_mapped_file_queue;
use crate::mapped_file::DefaultMappedFile;

#[derive(Debug, Error)]
enum ManagedLifecycleActivationSource {
    // NamespaceTransitionOutcome is expected disposition data, not a causal source.
    #[error(transparent)]
    Registry(#[from] RegistryViolation),
    #[error(transparent)]
    SegmentClaim(#[from] ManagedSegmentClaimViolation),
    #[error("managed queue construction failed: {0}")]
    QueueLoad(#[source] std::io::Error),
    #[error("managed queue directory was staged more than once: {0}")]
    DuplicateQueue(String),
    #[error("managed queue inventory is invalid: {0}")]
    InvalidQueueInventory(String),
    #[error(transparent)]
    Preflight(#[from] ActivationPreflightViolation),
    #[error(transparent)]
    Writer(#[from] ManagedLedgerWriterError),
    #[error("namespace verification failed: {0:?}")]
    Namespace(NamespaceTransitionOutcome),
}

/// One replay-authorized mapped-file queue that must be staged before activation.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedQueueDescriptor {
    directory: Box<str>,
    expected_file_length: u64,
}

impl ManagedQueueDescriptor {
    /// Store-relative queue directory containing canonical numeric segments.
    pub fn directory(&self) -> &str {
        &self.directory
    }

    /// Exact mapped-file length recorded for every segment in this queue.
    pub const fn expected_file_length(&self) -> u64 {
        self.expected_file_length
    }
}

/// Private activation-orchestration leaf retained as the typed StoreError source.
#[derive(Debug, Error)]
#[error("managed lifecycle activation failed: {source}")]
pub(crate) struct ManagedLifecycleActivationError {
    #[source]
    source: ManagedLifecycleActivationSource,
}

impl From<NamespaceTransitionOutcome> for ManagedLifecycleActivationSource {
    fn from(outcome: NamespaceTransitionOutcome) -> Self {
        Self::Namespace(outcome)
    }
}

impl ManagedLifecycleActivationError {
    fn new(source: impl Into<ManagedLifecycleActivationSource>) -> Self {
        Self { source: source.into() }
    }

    /// Promotes this leaf into the canonical storage facade exactly once.
    ///
    /// Descriptor selection follows the reviewed activation mapping: queue-load
    /// and segment-claim failures are backend unavailability, registry and
    /// staging invariants are corrupted state, and writer or namespace
    /// failures are write failures. The complete leaf is preserved as the
    /// typed source.
    fn into_store_error(self) -> StoreError {
        let descriptor = match &self.source {
            ManagedLifecycleActivationSource::QueueLoad(_) | ManagedLifecycleActivationSource::SegmentClaim(_) => {
                &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE
            }
            ManagedLifecycleActivationSource::Registry(_)
            | ManagedLifecycleActivationSource::DuplicateQueue(_)
            | ManagedLifecycleActivationSource::InvalidQueueInventory(_)
            | ManagedLifecycleActivationSource::Preflight(_) => &rocketmq_error::STORAGE_STATE_CORRUPTED,
            ManagedLifecycleActivationSource::Writer(_) | ManagedLifecycleActivationSource::Namespace(_) => {
                &rocketmq_error::STORAGE_WRITE_FAILED
            }
        };
        StoreError::new(descriptor, StoreOperation::Load)
            .in_component(StoreComponent::MappedFile)
            .with_source(self)
    }
}

/// Builds the promoted activation StoreError from one typed source.
fn activation_store_error(source: impl Into<ManagedLifecycleActivationSource>) -> StoreError {
    ManagedLifecycleActivationError::new(source).into_store_error()
}

/// Reconciled managed state whose queue generations are being staged off to the side.
///
/// This value still grants no lifecycle writes. Dropping it publishes no queue generation and
/// releases the retained Store-root lease with all staged owners.
#[doc(hidden)]
pub struct PreparedManagedLifecycleActivation {
    session: ReconciledLifecycleSession,
    registry: RetirementRegistry<DefaultMappedFile>,
    recovered_work: Vec<RecoveredRetirementWork<DefaultMappedFile>>,
    claimed_directories: BTreeSet<String>,
    store_root: Option<PathBuf>,
    staging_failed: bool,
}

impl std::fmt::Debug for PreparedManagedLifecycleActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedManagedLifecycleActivation")
            .field("unclaimed_active", &self.session.unclaimed_active_count())
            .field("recovered_work", &self.recovered_work.len())
            .field("claimed_directories", &self.claimed_directories.len())
            .field("store_root_bound", &self.store_root.is_some())
            .field("staging_failed", &self.staging_failed)
            .finish_non_exhaustive()
    }
}

/// Rebuilds the registry before any managed queue generation is published.
#[doc(hidden)]
pub fn prepare_managed_lifecycle_activation(
    session: ReconciledLifecycleSession,
) -> Result<PreparedManagedLifecycleActivation, StoreError> {
    let (registry, recovered_work) =
        RetirementRegistry::from_reconciled_state(session.state()).map_err(activation_store_error)?;
    Ok(PreparedManagedLifecycleActivation {
        session,
        registry,
        recovered_work,
        claimed_directories: BTreeSet::new(),
        store_root: None,
        staging_failed: false,
    })
}

impl PreparedManagedLifecycleActivation {
    /// Returns replay-authorized active paths without opening them by pathname.
    #[doc(hidden)]
    pub fn active_segment_paths(&self) -> impl Iterator<Item = &str> {
        self.session.active_segment_paths()
    }

    /// Returns the complete replay-authorized queue inventory without claiming any handles.
    #[doc(hidden)]
    pub fn queue_descriptors(&self) -> Result<Vec<ManagedQueueDescriptor>, StoreError> {
        collect_queue_descriptors(self.session.active_segment_bindings())
            .map_err(ManagedLifecycleActivationError::into_store_error)
    }

    /// Number of active segment handles not yet transferred into a staged queue generation.
    #[doc(hidden)]
    pub fn unclaimed_active_count(&self) -> usize {
        self.session.unclaimed_active_count()
    }

    /// Number of replayed retirement tickets that require a later durable action.
    #[doc(hidden)]
    pub fn recovered_retirement_count(&self) -> usize {
        self.recovered_work.len()
    }

    /// Builds and registers one complete queue generation without publishing it.
    ///
    /// Any failure after segment handles are claimed permanently fails this activation candidate;
    /// callers must drop it and replay again instead of retrying from a partial in-memory view.
    #[doc(hidden)]
    pub fn stage_queue(
        &mut self,
        store_root: &Path,
        directory: &str,
        configured_file_size: u64,
    ) -> Result<ManagedMappedFileQueueGeneration<DefaultMappedFile>, StoreError> {
        if self.staging_failed {
            return Err(activation_store_error(ActivationPreflightViolation::StagingFailed));
        }
        if self.claimed_directories.contains(directory) {
            return Err(activation_store_error(
                ManagedLifecycleActivationSource::DuplicateQueue(directory.to_owned()),
            ));
        }
        match &self.store_root {
            Some(bound) if bound != store_root => {
                self.staging_failed = true;
                return Err(activation_store_error(ActivationPreflightViolation::StoreRootMismatch));
            }
            Some(_) => {}
            None => self.store_root = Some(store_root.to_path_buf()),
        }

        let mut segments = match self
            .session
            .take_active_segments_in_directory(directory, configured_file_size)
        {
            Ok(segments) => segments,
            Err(source) => {
                self.staging_failed = true;
                return Err(activation_store_error(source));
            }
        };
        let namespace_root = VerifiedNamespaceRoot::from_reconciled_session(&self.session).map_err(|source| {
            self.staging_failed = true;
            activation_store_error(ManagedLifecycleActivationSource::Namespace(source))
        })?;
        for segment in &mut segments {
            let writable = namespace_root
                .open_active_segment(
                    segment.canonical_path(),
                    segment.physical_key(),
                    segment.expected_length(),
                )
                .map_err(|source| {
                    self.staging_failed = true;
                    activation_store_error(ManagedLifecycleActivationSource::Namespace(source))
                })?;
            segment.replace_retained_file(writable);
        }
        let generation = match load_reconciled_mapped_file_queue(store_root, segments) {
            Ok(generation) => generation,
            Err(source) => {
                self.staging_failed = true;
                return Err(activation_store_error(ManagedLifecycleActivationSource::QueueLoad(
                    source,
                )));
            }
        };
        if let Err(source) = generation.register_reconciled_members(&self.registry) {
            self.staging_failed = true;
            return Err(activation_store_error(source));
        }
        self.claimed_directories.insert(directory.to_owned());
        Ok(generation)
    }

    /// Activates a completely staged managed lifecycle generation.
    ///
    /// The Store calls this only from its explicit Wave-B mode. Safety remains bound to the
    /// retained exclusive root lease, complete replay/reconciliation, queue staging, and verified
    /// writer/namespace capabilities; no cryptographic signing protocol is required.
    #[doc(hidden)]
    pub fn activate(self) -> Result<ManagedLifecycleRuntime, StoreError> {
        validate_activation_preflight(
            self.session.unclaimed_active_count(),
            self.staging_failed,
            self.store_root.is_some(),
        )
        .map_err(activation_store_error)?;

        let Some(store_root) = self.store_root else {
            return Err(activation_store_error(ActivationPreflightViolation::MissingStoreRoot));
        };

        let writer = open_managed_lifecycle_writer(self.session.retained_root(), self.session.writer_frontier())
            .map_err(activation_store_error)?;
        let namespace_root =
            VerifiedNamespaceRoot::from_reconciled_session(&self.session).map_err(activation_store_error)?;
        Ok(ActiveManagedLifecycle {
            session: self.session,
            store_root,
            registry: self.registry,
            writer,
            namespace_root,
            recovered_work: self.recovered_work,
        }
        .into_runtime())
    }
}

#[allow(
    clippy::result_large_err,
    reason = "the merged namespace outcome intentionally retains typed proof and disposition data"
)]
fn collect_queue_descriptors<'a>(
    bindings: impl IntoIterator<Item = (&'a str, u64)>,
) -> Result<Vec<ManagedQueueDescriptor>, ManagedLifecycleActivationError> {
    let mut queues = BTreeMap::<Box<str>, u64>::new();
    for (path, expected_file_length) in bindings {
        let Some((directory, _file_name)) = path.rsplit_once('/') else {
            return Err(ManagedLifecycleActivationError::new(
                ManagedLifecycleActivationSource::InvalidQueueInventory(format!(
                    "active segment {path:?} has no parent directory"
                )),
            ));
        };
        match queues.entry(directory.into()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(expected_file_length);
            }
            std::collections::btree_map::Entry::Occupied(entry) if *entry.get() != expected_file_length => {
                return Err(ManagedLifecycleActivationError::new(
                    ManagedLifecycleActivationSource::InvalidQueueInventory(format!(
                        "queue {directory:?} contains lengths {} and {expected_file_length}",
                        entry.get()
                    )),
                ));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    Ok(queues
        .into_iter()
        .map(|(directory, expected_file_length)| ManagedQueueDescriptor {
            directory,
            expected_file_length,
        })
        .collect())
}

/// Fully activated in-process authority. Construction remains unreachable in production Wave A.
pub(in crate::mapped_file::retirement) struct ActiveManagedLifecycle {
    pub(super) session: ReconciledLifecycleSession,
    pub(super) store_root: PathBuf,
    pub(super) registry: RetirementRegistry<DefaultMappedFile>,
    pub(super) writer: ManagedLedgerWriter<FileLedgerIo>,
    pub(super) namespace_root: VerifiedNamespaceRoot,
    pub(super) recovered_work: Vec<RecoveredRetirementWork<DefaultMappedFile>>,
}

impl std::fmt::Debug for ActiveManagedLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActiveManagedLifecycle")
            .field("store_uuid", &self.session.writer_frontier().store_uuid())
            .field("recovered_work", &self.recovered_work.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
enum ActivationPreflightViolation {
    #[error("a previous queue-staging attempt failed")]
    StagingFailed,
    #[error("no Store root path was bound while staging managed queues")]
    MissingStoreRoot,
    #[error("managed queues were staged from different Store root paths")]
    StoreRootMismatch,
    #[error("{count} replay-authorized active segments remain unclaimed")]
    UnclaimedSegments { count: usize },
}

fn validate_activation_preflight(
    unclaimed_active: usize,
    staging_failed: bool,
    store_root_bound: bool,
) -> Result<(), ActivationPreflightViolation> {
    if staging_failed {
        return Err(ActivationPreflightViolation::StagingFailed);
    }
    if !store_root_bound {
        return Err(ActivationPreflightViolation::MissingStoreRoot);
    }
    if unclaimed_active != 0 {
        return Err(ActivationPreflightViolation::UnclaimedSegments {
            count: unclaimed_active,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unclaimed_segments_block_activation_before_writer_open() {
        assert_eq!(
            validate_activation_preflight(1, false, true),
            Err(ActivationPreflightViolation::UnclaimedSegments { count: 1 })
        );
    }

    #[test]
    fn an_earlier_staging_failure_permanently_blocks_activation() {
        assert_eq!(
            validate_activation_preflight(0, true, true),
            Err(ActivationPreflightViolation::StagingFailed)
        );
    }

    #[test]
    fn activation_requires_the_exact_store_root_used_for_queue_staging() {
        assert_eq!(
            validate_activation_preflight(0, false, false),
            Err(ActivationPreflightViolation::MissingStoreRoot)
        );
    }

    #[test]
    fn explicit_activation_has_no_signature_or_force_bypass_surface() {
        let source = include_str!("activation.rs").replace("\r\n", "\n");
        let production = source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .expect("tests follow production activation code")
            .0;

        assert!(production.contains("pub fn activate(self)"));
        assert!(!production.contains("ed25519"));
        assert!(!production.contains("signature"));
        assert!(!production.contains("force"));
    }

    #[test]
    fn queue_inventory_groups_segments_by_directory_in_canonical_order() {
        let queues = collect_queue_descriptors([
            ("consumequeue/topic-a/0/00000000000000000000", 300_000),
            ("commitlog/00000000001073741824", 1_073_741_824),
            ("commitlog/00000000000000000000", 1_073_741_824),
        ])
        .expect("valid replay inventory groups by queue directory");

        assert_eq!(
            queues,
            vec![
                ManagedQueueDescriptor {
                    directory: "commitlog".into(),
                    expected_file_length: 1_073_741_824,
                },
                ManagedQueueDescriptor {
                    directory: "consumequeue/topic-a/0".into(),
                    expected_file_length: 300_000,
                },
            ]
        );
    }

    #[test]
    fn queue_inventory_rejects_mixed_segment_lengths_before_claiming_handles() {
        let error = collect_queue_descriptors([
            ("commitlog/00000000000000000000", 1_073_741_824),
            ("commitlog/00000000001073741824", 512),
        ])
        .expect_err("one queue cannot mix mapped-file lengths");

        assert!(matches!(
            error.source,
            ManagedLifecycleActivationSource::InvalidQueueInventory(_)
        ));
    }
}
