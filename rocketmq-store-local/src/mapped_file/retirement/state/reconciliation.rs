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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::TryReserveError;

use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use thiserror::Error;

use super::NeedsReconciliation;
use super::RecoveredLedgerState;
use super::WriterRecoveryFrontier;
use crate::mapped_file::retirement::codec::ContentFingerprint;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;
use crate::mapped_file::retirement::sidecar::IncarnationPhase;
use crate::mapped_file::retirement::sidecar::IncarnationSnapshotEntry;
use crate::mapped_file::retirement::sidecar::QuarantineSnapshotEntry;
use crate::mapped_file::retirement::sidecar::RetirementStage;
use crate::mapped_file::retirement::sidecar::RetirementTicketSnapshotEntry;

mod analysis;
mod inventory;

use analysis::collect_known_paths;
use analysis::parent_directory;
pub(crate) use analysis::reconcile;
pub(crate) use inventory::ReconciliationInventoryLimits;

/// Caller-selected bounds for handle-relative namespace reconciliation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedReconciliationLimits {
    pub max_directories: usize,
    pub max_entries: usize,
    pub max_fingerprint_bytes: u64,
}

impl Default for ManagedReconciliationLimits {
    fn default() -> Self {
        let limits = ReconciliationInventoryLimits::default();
        Self {
            max_directories: limits.max_directories,
            max_entries: limits.max_entries,
            max_fingerprint_bytes: limits.max_fingerprint_bytes,
        }
    }
}

impl From<ManagedReconciliationLimits> for ReconciliationInventoryLimits {
    fn from(limits: ManagedReconciliationLimits) -> Self {
        Self {
            max_directories: limits.max_directories,
            max_entries: limits.max_entries,
            max_fingerprint_bytes: limits.max_fingerprint_bytes,
        }
    }
}

/// One entry from a complete, handle-relative namespace inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NamespaceObject {
    RegularFile {
        physical_key: PhysicalFileKey,
        length: u64,
        content_fingerprint: Option<ContentFingerprint>,
    },
    Directory,
    ReparsePoint,
    Other,
}

/// Complete and stable observations for every segment directory referenced by replay state.
///
/// Production construction is intentionally withheld until the retained-root native scanner is
/// connected. The pure reconciliation engine cannot be turned into publication authority by a
/// caller-provided partial map.
#[derive(Debug)]
pub(crate) struct StableNamespaceInventory {
    store_uuid: StoreUuid,
    complete_directories: BTreeSet<Box<str>>,
    entries: BTreeMap<StoreRelativePath, NamespaceObject>,
    retained_files: BTreeMap<StoreRelativePath, std::fs::File>,
    requires_retained_files: bool,
}

impl StableNamespaceInventory {
    #[cfg(test)]
    fn for_test<const D: usize, const E: usize>(
        store_uuid: StoreUuid,
        complete_directories: [&str; D],
        entries: [(StoreRelativePath, NamespaceObject); E],
    ) -> Self {
        Self {
            store_uuid,
            complete_directories: complete_directories.into_iter().map(Box::<str>::from).collect(),
            entries: entries.into_iter().collect(),
            retained_files: BTreeMap::new(),
            requires_retained_files: false,
        }
    }

    fn observe(&self, path: &StoreRelativePath) -> Result<Option<&NamespaceObject>, ReconciliationViolation> {
        let directory = parent_directory(path);
        if !self.complete_directories.contains(directory) {
            return Err(ReconciliationViolation::IncompleteDirectoryInventory {
                directory: directory.into(),
            });
        }
        Ok(self.entries.get(path))
    }
}

/// One exact active publication binding after replay and namespace reconciliation agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishedIncarnationBinding {
    incarnation: FileIncarnationId,
    physical_key: PhysicalFileKey,
    expected_length: u64,
    segment_offset: u64,
}

impl PublishedIncarnationBinding {
    pub(crate) const fn incarnation(&self) -> FileIncarnationId {
        self.incarnation
    }

    pub(crate) const fn physical_key(&self) -> PhysicalFileKey {
        self.physical_key
    }

    pub(crate) const fn expected_length(&self) -> u64 {
        self.expected_length
    }

    pub(crate) const fn segment_offset(&self) -> u64 {
        self.segment_offset
    }
}

/// Replayed state whose complete namespace inventory is internally consistent.
///
/// This is deliberately not an `ActiveLifecycle` or queue publication permit. Wave-B activation
/// must additionally bind it to the retained Store-root lease and external fleet fence.
#[derive(Debug)]
pub(crate) struct ReconciledLedgerState {
    recovered: RecoveredLedgerState,
    writer_frontier: WriterRecoveryFrontier,
    active: BTreeMap<StoreRelativePath, PublishedIncarnationBinding>,
    retired_paths: BTreeSet<StoreRelativePath>,
    retiring_tickets: BTreeSet<TicketId>,
    completed_revalidated: BTreeSet<TicketId>,
    retained_files: BTreeMap<StoreRelativePath, std::fs::File>,
}

impl ReconciledLedgerState {
    pub(crate) fn active_incarnation(&self, path: &StoreRelativePath) -> Option<&PublishedIncarnationBinding> {
        self.active.get(path)
    }

    pub(crate) fn is_retired_path(&self, path: &StoreRelativePath) -> bool {
        self.retired_paths.contains(path)
    }

    pub(crate) fn completed_is_revalidated(&self, ticket_id: TicketId) -> bool {
        self.completed_revalidated.contains(&ticket_id)
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active.len()
    }

    pub(crate) fn retiring_count(&self) -> usize {
        self.retiring_tickets.len()
    }

    pub(crate) const fn recovered(&self) -> &RecoveredLedgerState {
        &self.recovered
    }

    pub(crate) const fn writer_frontier(&self) -> &WriterRecoveryFrontier {
        &self.writer_frontier
    }

    fn take_active_segments_in_directory(
        &mut self,
        directory: &str,
        configured_file_size: u64,
    ) -> Result<Vec<ReconciledSegmentFile>, ManagedSegmentClaimFault> {
        let directory =
            StoreRelativePath::new(directory).map_err(|_| ManagedSegmentClaimViolation::InvalidDirectory)?;
        let mut paths = Vec::new();
        paths
            .try_reserve_exact(self.active.len())
            .map_err(ManagedSegmentClaimFault::Allocation)?;

        // Validate the complete claim before consuming either map. Queue loading must never observe
        // a partial generation merely because a later binding or retained handle is inconsistent.
        for (path, binding) in &self.active {
            if parent_directory(path) != directory.as_str() {
                continue;
            }
            if binding.expected_length != configured_file_size {
                return Err(ManagedSegmentClaimViolation::ConfiguredLengthMismatch {
                    expected: binding.expected_length,
                    configured: configured_file_size,
                }
                .into());
            }
            if !self.retained_files.contains_key(path) {
                return Err(ManagedSegmentClaimViolation::MissingRetainedHandle.into());
            }
            paths.push(path.clone());
        }

        let mut claimed = Vec::new();
        claimed
            .try_reserve_exact(paths.len())
            .map_err(ManagedSegmentClaimFault::Allocation)?;
        for path in paths {
            // INVARIANT: both maps were preflighted above while this method holds exclusive access
            // to the state; no intervening operation can remove either entry.
            let binding = self
                .active
                .remove(&path)
                .expect("preflighted active binding must remain present");
            let file = self
                .retained_files
                .remove(&path)
                .expect("preflighted retained handle must remain present");
            claimed.push(ReconciledSegmentFile {
                relative_path: path,
                binding,
                file,
            });
        }
        claimed.sort_by_key(|segment| segment.binding.segment_offset);
        Ok(claimed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReconciliationAction {
    ResumeAllocation(FileIncarnationId),
    RecordBound {
        incarnation: FileIncarnationId,
        physical_key: PhysicalFileKey,
    },
    PublishBoundIncarnation(FileIncarnationId),
    RecordPublished(FileIncarnationId),
    RecordLogicalRemoved(TicketId),
    RecordSupersededPath {
        ticket_id: TicketId,
        replacement_key: PhysicalFileKey,
    },
    RecordTombstoned(TicketId),
    RecordNamespaceAbsent {
        ticket_id: TicketId,
        replacement_key: Option<PhysicalFileKey>,
    },
    RecordCompleted(TicketId),
}

/// Ordered, data-only writes required before reconciliation can be accepted.
#[derive(Debug)]
pub(crate) struct ReconciliationPlan {
    recovered: RecoveredLedgerState,
    writer_frontier: WriterRecoveryFrontier,
    actions: Vec<ReconciliationAction>,
    retained_files: BTreeMap<StoreRelativePath, std::fs::File>,
}

impl ReconciliationPlan {
    pub(crate) fn actions(&self) -> &[ReconciliationAction] {
        &self.actions
    }

    pub(crate) const fn recovered(&self) -> &RecoveredLedgerState {
        &self.recovered
    }

    pub(crate) const fn writer_frontier(&self) -> &WriterRecoveryFrontier {
        &self.writer_frontier
    }
}

#[derive(Debug)]
pub(crate) enum ReconciliationDisposition {
    Ready(ReconciledLedgerState),
    RecoveryRequired(ReconciliationPlan),
}

/// Reconciles one session while retaining the exclusive-root proof for the entire result.
#[derive(Debug)]
pub enum ManagedReconciliationDisposition {
    Ready(ReconciledLifecycleSession),
    RecoveryRequired(ManagedRecoverySession),
}

/// Fully reconciled state still scoped to the Store's exclusive root lease.
#[doc(hidden)]
#[derive(Debug)]
pub struct ReconciledLifecycleSession {
    session: Box<crate::mapped_file::retirement::replay::ManagedLifecycleSession>,
    state: ReconciledLedgerState,
}

impl ReconciledLifecycleSession {
    pub(crate) const fn state(&self) -> &ReconciledLedgerState {
        &self.state
    }

    pub(crate) const fn retained_root(&self) -> &std::fs::File {
        self.session.retained_root()
    }

    pub(crate) const fn writer_frontier(&self) -> &WriterRecoveryFrontier {
        self.state.writer_frontier()
    }

    /// Claims every replay-authorized active segment in one exact store-relative directory.
    ///
    /// Returned files are the retained handles used by the A/B/C inventory; no pathname reopen is
    /// required. Retired paths are intentionally absent from the result.
    #[doc(hidden)]
    pub(crate) fn take_active_segments_in_directory(
        &mut self,
        directory: &str,
        configured_file_size: u64,
    ) -> Result<Vec<ReconciledSegmentFile>, ManagedSegmentClaimFault> {
        self.state
            .take_active_segments_in_directory(directory, configured_file_size)
    }

    /// Number of durable active incarnations not yet claimed by a queue loader.
    #[doc(hidden)]
    pub fn unclaimed_active_count(&self) -> usize {
        self.state.active.len()
    }

    /// Enumerates replay-authorized active paths without transferring their retained handles.
    ///
    /// Callers use this read-only view to stage every queue generation before claiming any file.
    #[doc(hidden)]
    pub fn active_segment_paths(&self) -> impl Iterator<Item = &str> {
        self.state.active.keys().map(StoreRelativePath::as_str)
    }

    pub(crate) fn active_segment_bindings(&self) -> impl Iterator<Item = (&str, u64)> {
        self.state
            .active
            .iter()
            .map(|(path, binding)| (path.as_str(), binding.expected_length))
    }
}

/// Recovery plan paired with the exact root/session proof from which it was derived.
#[doc(hidden)]
#[derive(Debug)]
pub struct ManagedRecoverySession {
    session: Box<crate::mapped_file::retirement::replay::ManagedLifecycleSession>,
    plan: ReconciliationPlan,
}

impl ManagedRecoverySession {
    pub(crate) const fn plan(&self) -> &ReconciliationPlan {
        &self.plan
    }

    pub(crate) const fn retained_root(&self) -> &std::fs::File {
        self.session.retained_root()
    }

    /// Number of ordered recovery operations that must complete before activation.
    #[doc(hidden)]
    pub fn required_action_count(&self) -> usize {
        self.plan.actions.len()
    }
}

/// One pathless-reopen segment source claimed from a reconciled lifecycle session.
#[doc(hidden)]
#[derive(Debug)]
pub struct ReconciledSegmentFile {
    relative_path: StoreRelativePath,
    binding: PublishedIncarnationBinding,
    file: std::fs::File,
}

impl ReconciledSegmentFile {
    #[cfg(test)]
    pub(crate) fn for_test(
        relative_path: StoreRelativePath,
        physical_key: PhysicalFileKey,
        expected_length: u64,
        segment_offset: u64,
        file: std::fs::File,
    ) -> Self {
        Self {
            relative_path,
            binding: PublishedIncarnationBinding {
                incarnation: FileIncarnationId::new(
                    StoreUuid::new([1; 16]).expect("test Store UUID is nonzero"),
                    segment_offset.checked_add(1).expect("test segment offset has headroom"),
                )
                .expect("test incarnation is nonzero"),
                physical_key,
                expected_length,
                segment_offset,
            },
            file,
        }
    }

    #[doc(hidden)]
    pub fn relative_path(&self) -> &str {
        self.relative_path.as_str()
    }

    pub(in crate::mapped_file) fn canonical_path(&self) -> &StoreRelativePath {
        &self.relative_path
    }

    pub(in crate::mapped_file) const fn physical_key(&self) -> PhysicalFileKey {
        self.binding.physical_key
    }

    #[doc(hidden)]
    pub const fn expected_length(&self) -> u64 {
        self.binding.expected_length
    }

    #[doc(hidden)]
    pub const fn segment_offset(&self) -> u64 {
        self.binding.segment_offset
    }

    pub(crate) const fn incarnation(&self) -> FileIncarnationId {
        self.binding.incarnation
    }

    pub(in crate::mapped_file::retirement) fn replace_retained_file(&mut self, file: std::fs::File) {
        self.file = file;
    }

    pub(in crate::mapped_file) fn into_parts(self) -> (StoreRelativePath, PublishedIncarnationBinding, std::fs::File) {
        (self.relative_path, self.binding, self.file)
    }
}

#[doc(hidden)]
#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ManagedSegmentClaimViolation {
    #[error("managed queue directory is not a canonical store-relative path")]
    InvalidDirectory,
    #[error("reconciled active binding disappeared before it was claimed")]
    MissingBinding,
    #[error("reconciled active segment lost its retained file handle")]
    MissingRetainedHandle,
    #[error("mapped-file size {configured} differs from durable expected length {expected}")]
    ConfiguredLengthMismatch { expected: u64, configured: u64 },
}

#[derive(Debug, Error)]
pub(crate) enum ManagedSegmentClaimFault {
    #[error("managed segment claim allocation failed")]
    Allocation(#[source] TryReserveError),
    #[error(transparent)]
    Contract(#[from] ManagedSegmentClaimViolation),
}

/// Private reconciliation leaf retained as the typed `StoreError` source.
#[derive(Debug, Error)]
pub(crate) enum ManagedReconciliationFailure {
    #[error("managed session still requires ledger or marker recovery")]
    ReplayRecoveryRequired,
    #[error("managed session is missing its Store UUID")]
    MissingStoreUuid,
    #[error("managed namespace inventory failed")]
    Inventory(#[source] inventory::ReconciliationInventoryFailure),
    #[error("managed replay and namespace state disagree")]
    State(#[source] ReconciliationViolation),
}

fn reconciliation_store_error(error: ManagedReconciliationFailure) -> StoreError {
    let descriptor = match &error {
        ManagedReconciliationFailure::Inventory(_) => &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
        ManagedReconciliationFailure::ReplayRecoveryRequired
        | ManagedReconciliationFailure::MissingStoreUuid
        | ManagedReconciliationFailure::State(_) => &rocketmq_error::STORAGE_STATE_CORRUPTED,
    };
    StoreError::new(descriptor, StoreOperation::Load)
        .in_component(StoreComponent::MappedFile)
        .with_detail("managed lifecycle reconciliation failed")
        .with_source(error)
}

fn reconcile_managed_session(
    session: Box<crate::mapped_file::retirement::replay::ManagedLifecycleSession>,
    limits: ReconciliationInventoryLimits,
) -> Result<ManagedReconciliationDisposition, ManagedReconciliationFailure> {
    let store_uuid = session
        .store_uuid()
        .ok_or(ManagedReconciliationFailure::MissingStoreUuid)?;
    let needs_reconciliation = match session.decision() {
        Some(crate::mapped_file::retirement::replay::RecoveryDecision::NeedsReconciliation(needs)) => needs.clone(),
        _ => {
            return Err(ManagedReconciliationFailure::ReplayRecoveryRequired);
        }
    };
    let inventory = inventory::scan(
        session.retained_root(),
        store_uuid,
        needs_reconciliation.recovered(),
        limits,
    )
    .map_err(ManagedReconciliationFailure::Inventory)?;
    match reconcile(needs_reconciliation, inventory).map_err(ManagedReconciliationFailure::State)? {
        ReconciliationDisposition::Ready(state) => {
            Ok(ManagedReconciliationDisposition::Ready(ReconciledLifecycleSession {
                session,
                state,
            }))
        }
        ReconciliationDisposition::RecoveryRequired(plan) => Ok(ManagedReconciliationDisposition::RecoveryRequired(
            ManagedRecoverySession { session, plan },
        )),
    }
}

impl crate::mapped_file::retirement::replay::ManagedLifecycleSession {
    /// Scans every replay-owned namespace directory and reconciles it before publication.
    #[doc(hidden)]
    pub fn reconcile(
        self: Box<Self>,
        limits: ManagedReconciliationLimits,
    ) -> Result<ManagedReconciliationDisposition, StoreError> {
        reconcile_managed_session(self, limits.into()).map_err(reconciliation_store_error)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ReconciliationViolation {
    #[error("namespace inventory belongs to another Store UUID")]
    StoreUuidMismatch,
    #[error("directory {directory:?} was not included in the stable inventory")]
    IncompleteDirectoryInventory { directory: Box<str> },
    #[error("namespace entry {path:?} was not owned by replay state")]
    UntrackedNamespaceEntry { path: StoreRelativePath },
    #[error("namespace path {path:?} is not a regular no-follow file")]
    UnsafeNamespaceEntry { path: StoreRelativePath },
    #[error("published namespace path {path:?} is absent")]
    MissingPublishedFile { path: StoreRelativePath },
    #[error("namespace path {path:?} resolves to a different physical file")]
    PhysicalKeyMismatch {
        path: StoreRelativePath,
        expected: PhysicalFileKey,
        actual: PhysicalFileKey,
    },
    #[error("namespace path {path:?} has length {actual}, expected {expected}")]
    LengthMismatch {
        path: StoreRelativePath,
        expected: u64,
        actual: u64,
    },
    #[error("physical file {physical_key:?} is aliased by {first:?} and {second:?}")]
    DuplicatePhysicalIdentity {
        physical_key: PhysicalFileKey,
        first: StoreRelativePath,
        second: StoreRelativePath,
    },
    #[error("published incarnation still has create artifact {path:?}")]
    UnexpectedCreateArtifact { path: StoreRelativePath },
    #[error("bound incarnation {incarnation:?} has neither its create nor canonical file")]
    BoundIncarnationMissing { incarnation: FileIncarnationId },
    #[error("allocated incarnation {incarnation:?} unexpectedly has a canonical file")]
    AllocatedCanonicalPresent { incarnation: FileIncarnationId },
    #[error("ticket {ticket_id:?} has a non-canonical tombstone path")]
    TombstonePathMismatch { ticket_id: TicketId },
    #[error("ticket {ticket_id:?} collides with another tombstone object")]
    TombstoneCollision { ticket_id: TicketId },
    #[error("completed ticket {ticket_id:?} has its original target again at {path:?}")]
    CompletedTargetReappeared {
        ticket_id: TicketId,
        path: StoreRelativePath,
    },
    #[error("ticket {ticket_id:?} contradicts its durable namespace stage")]
    DurableStageContradiction { ticket_id: TicketId },
    #[error("quarantine evidence at {path:?} no longer matches its durable observation")]
    QuarantineMismatch { path: StoreRelativePath },
}

#[cfg(test)]
mod tests;
