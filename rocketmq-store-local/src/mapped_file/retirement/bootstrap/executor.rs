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

use std::error::Error;
use std::fs::File;
use std::io;

use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

use super::plan::InitialBootstrapInventoryPlan;
use super::plan::InitialBootstrapPlan;
use super::proof::BootstrapFoundationEvidence;
use super::proof::BootstrapInventoryEvidence;
use super::types::BootstrapAction;
use super::types::BootstrapDecision;
use super::types::BootstrapPlanViolation;
use super::types::BootstrapRecord;
use super::types::DurableUnitProgress;
use super::types::DurableUnitStep;
use super::types::FencedBootstrapEvidence;
#[cfg(test)]
use super::types::ImmutableArtifactProgress;
use super::types::ImmutableArtifactStep;
use super::types::InitialBootstrapProgress;
#[cfg(test)]
use super::types::InitialMarkerProgress;
use super::types::InitialMarkerStep;
use super::types::NeedsRecovery;
use super::types::PlannedAcknowledgedUnit;
use super::types::PlannedInitialMarker;
use super::types::PlannedSnapshot;
use super::types::ReconciliationPhase;
use crate::mapped_file::retirement::identity::IdentityViolation;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::platform::physical_file_key;
use crate::mapped_file::retirement::sidecar::StoreMeta;

use super::inventory::preflight_bootstrap_namespace;
use super::inventory::BootstrapInventoryFailure;
use super::inventory::BootstrapInventoryLimits;
mod durable_unit;
mod platform;

pub use platform::InitialBootstrapCompletion;

#[cfg(test)]
pub(super) use durable_unit::DurableUnitMachine;
#[cfg(all(test, any(target_os = "linux", windows)))]
pub(super) use platform::execute_prepared_initial_bootstrap;
#[cfg(test)]
pub(in crate::mapped_file::retirement::bootstrap) use platform::prepare_initial_bootstrap_foundation;
const MAX_BOOTSTRAP_ACTIONS: usize = 64;

/// Private bootstrap leaf retaining the exact nested source.
#[derive(Debug, Error)]
pub(crate) enum ManagedLifecycleBootstrapFailure {
    #[error("failed to identify the retained Store root")]
    RootIdentity(#[source] io::Error),
    #[error("failed to derive the managed Store identity")]
    Identity(#[source] IdentityViolation),
    #[error("legacy Store namespace is not eligible for managed bootstrap")]
    Inventory(#[source] BootstrapInventoryFailure),
    #[error("failed to prepare the managed lifecycle bootstrap")]
    Foundation(#[source] platform::InitialBootstrapFoundationFailure),
    #[error("failed to execute the managed lifecycle bootstrap")]
    Execution(#[source] InitialBootstrapExecutionFailure<platform::InitialBootstrapFoundationFailure>),
}

fn foundation_descriptor(
    error: &platform::InitialBootstrapFoundationFailure,
) -> &'static rocketmq_error::ErrorDescriptor {
    match error {
        platform::InitialBootstrapFoundationFailure::UnsupportedPlatform(_) => {
            &rocketmq_error::STORAGE_OPERATION_UNSUPPORTED
        }
        platform::InitialBootstrapFoundationFailure::Io(_) => &rocketmq_error::STORAGE_IO_FAILED,
        platform::InitialBootstrapFoundationFailure::Inventory(_) => &rocketmq_error::STORAGE_READ_FAILED,
        platform::InitialBootstrapFoundationFailure::InvalidArtifact(_)
        | platform::InitialBootstrapFoundationFailure::Sidecar(_)
        | platform::InitialBootstrapFoundationFailure::Ledger(_)
        | platform::InitialBootstrapFoundationFailure::DurableUnit(_) => &rocketmq_error::STORAGE_STATE_CORRUPTED,
    }
}

fn bootstrap_descriptor(error: &ManagedLifecycleBootstrapFailure) -> Option<&'static rocketmq_error::ErrorDescriptor> {
    match error {
        ManagedLifecycleBootstrapFailure::RootIdentity(_) => Some(&rocketmq_error::STORAGE_IO_FAILED),
        ManagedLifecycleBootstrapFailure::Identity(_) => Some(&rocketmq_error::STORAGE_STATE_CORRUPTED),
        ManagedLifecycleBootstrapFailure::Inventory(BootstrapInventoryFailure::UnsupportedPlatform)
        | ManagedLifecycleBootstrapFailure::Inventory(BootstrapInventoryFailure::Platform(
            super::super::replay::discovery::platform::PlatformFailure::Unsupported,
        )) => Some(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED),
        ManagedLifecycleBootstrapFailure::Inventory(BootstrapInventoryFailure::Identity(_)) => {
            Some(&rocketmq_error::STORAGE_STATE_CORRUPTED)
        }
        ManagedLifecycleBootstrapFailure::Inventory(_) => Some(&rocketmq_error::STORAGE_READ_FAILED),
        ManagedLifecycleBootstrapFailure::Foundation(source) => Some(foundation_descriptor(source)),
        ManagedLifecycleBootstrapFailure::Execution(InitialBootstrapExecutionFailure::Backend(source)) => {
            Some(foundation_descriptor(source))
        }
        ManagedLifecycleBootstrapFailure::Execution(InitialBootstrapExecutionFailure::NeedsRecovery(_)) => {
            Some(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE)
        }
        ManagedLifecycleBootstrapFailure::Execution(InitialBootstrapExecutionFailure::ActionBoundExceeded) => None,
        ManagedLifecycleBootstrapFailure::Execution(InitialBootstrapExecutionFailure::Plan(_)) => {
            Some(&rocketmq_error::STORAGE_STATE_CORRUPTED)
        }
        ManagedLifecycleBootstrapFailure::Execution(InitialBootstrapExecutionFailure::InvalidPlanAction) => None,
    }
}

fn bootstrap_store_error(error: ManagedLifecycleBootstrapFailure) -> Option<StoreError> {
    Some(
        StoreError::new(bootstrap_descriptor(&error)?, StoreOperation::Load)
            .in_component(StoreComponent::MappedFile)
            .with_detail("managed lifecycle bootstrap failed")
            .with_source(error),
    )
}

/// Bootstraps a legacy Store root into managed lifecycle format.
///
/// Identity is deterministically derived from the retained physical root handle, so interrupted
/// bootstrap can resume without an external signing or request-binding protocol. This function
/// never activates queue mutation; startup must still replay, reconcile, stage every queue, and
/// qualify the platform writer before activation.
///
/// # Safety
///
/// The caller must retain an exclusive Store-root lease for `store_root` for the complete call and
/// must prevent legacy Store components from scanning or mutating the root concurrently.
#[doc(hidden)]
unsafe fn bootstrap_managed_lifecycle_under_exclusive_lock_checked(
    store_root: &File,
) -> Result<InitialBootstrapCompletion, ManagedLifecycleBootstrapFailure> {
    let meta = derive_store_meta(store_root)?;
    preflight_bootstrap_namespace(store_root, BootstrapInventoryLimits::default())
        .map_err(ManagedLifecycleBootstrapFailure::Inventory)?;
    let cloned_root = store_root
        .try_clone()
        .map_err(ManagedLifecycleBootstrapFailure::RootIdentity)?;
    let prepared = platform::prepare_initial_bootstrap_foundation(cloned_root, &meta)
        .map_err(ManagedLifecycleBootstrapFailure::Foundation)?;
    platform::execute_prepared_initial_bootstrap(prepared).map_err(ManagedLifecycleBootstrapFailure::Execution)
}

/// Bootstraps managed lifecycle evidence and promotes its private leaf at the owning facade.
///
/// # Safety
///
/// The caller must retain the exact exclusive Store-root lease for the complete call.
#[doc(hidden)]
pub unsafe fn bootstrap_managed_lifecycle_under_exclusive_lock(
    store_root: &File,
) -> Result<Option<InitialBootstrapCompletion>, StoreError> {
    // SAFETY: the caller upholds the checked bootstrap's retained-root and exclusivity contract.
    match unsafe { bootstrap_managed_lifecycle_under_exclusive_lock_checked(store_root) } {
        Ok(completion) => Ok(Some(completion)),
        Err(error) => match bootstrap_store_error(error) {
            Some(error) => Err(error),
            None => Ok(None),
        },
    }
}

fn derive_store_meta(store_root: &File) -> Result<StoreMeta, ManagedLifecycleBootstrapFailure> {
    let key = physical_file_key(store_root).map_err(ManagedLifecycleBootstrapFailure::RootIdentity)?;
    let mut store_uuid = derive_id(b"rocketmq-managed-store-uuid-v1\0", key);
    if store_uuid == [0; 16] {
        store_uuid[15] = 1;
    }
    let mut bootstrap_id = derive_id(b"rocketmq-managed-bootstrap-id-v1\0", key);
    if bootstrap_id == [0; 16] {
        bootstrap_id[15] = 1;
    }
    Ok(StoreMeta {
        store_uuid: StoreUuid::new(store_uuid).map_err(ManagedLifecycleBootstrapFailure::Identity)?,
        creation_time_ns: 0,
        bootstrap_id,
    })
}

fn derive_id(domain: &[u8], key: PhysicalFileKey) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(domain);
    match key {
        PhysicalFileKey::Unix(key) => {
            digest.update([1]);
            digest.update(key.device().to_le_bytes());
            digest.update(key.inode().to_le_bytes());
        }
        PhysicalFileKey::Windows(key) => {
            digest.update([2]);
            digest.update(key.volume_serial().to_le_bytes());
            digest.update(key.file_id());
        }
    }
    let digest = digest.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

mod private {
    pub trait Sealed {}
}

/// Durable bootstrap operations whose implementation must reclassify bytes before every run.
///
/// An operation error is always ambiguous: the implementation may have advanced durable state
/// before reporting it. The executor therefore returns immediately and a retry starts from a new
/// call to the corresponding `inspect_*` method.
pub(super) trait InitialBootstrapBackend: private::Sealed {
    type Error: Error + Send + Sync + 'static;

    fn inspect_store_initialized(
        &mut self,
        planned: &PlannedAcknowledgedUnit,
    ) -> Result<DurableUnitProgress, Self::Error>;

    fn scan_inventory(&mut self) -> Result<BootstrapInventoryEvidence, Self::Error>;

    fn inspect_inventory_phase(
        &mut self,
        planned: &InitialBootstrapInventoryPlan,
    ) -> Result<InitialBootstrapProgress, Self::Error>;

    fn advance_unit(
        &mut self,
        record: BootstrapRecord,
        planned: &PlannedAcknowledgedUnit,
        step: DurableUnitStep,
    ) -> Result<(), Self::Error>;

    fn advance_snapshot(&mut self, planned: &PlannedSnapshot, step: ImmutableArtifactStep) -> Result<(), Self::Error>;

    fn advance_initial_marker(
        &mut self,
        planned: &PlannedInitialMarker,
        step: InitialMarkerStep,
    ) -> Result<(), Self::Error>;

    fn reconcile(&mut self, phase: ReconciliationPhase) -> Result<(), Self::Error>;
}

#[derive(Debug, Error)]
pub(super) enum InitialBootstrapExecutionFailure<E>
where
    E: Error + Send + Sync + 'static,
{
    #[error(transparent)]
    Plan(#[from] BootstrapPlanViolation),
    #[error("bootstrap backend operation failed")]
    Backend(#[source] E),
    #[error("bootstrap state requires replay/recovery: {0:?}")]
    NeedsRecovery(NeedsRecovery),
    #[error("bootstrap action bound was exceeded without reaching a stable frontier")]
    ActionBoundExceeded,
    #[error("bootstrap planner returned an action for the wrong phase")]
    InvalidPlanAction,
}

pub(super) fn execute_initial_bootstrap<B>(
    foundation: BootstrapFoundationEvidence,
    backend: &mut B,
) -> Result<FencedBootstrapEvidence, InitialBootstrapExecutionFailure<B::Error>>
where
    B: InitialBootstrapBackend,
{
    let initial = InitialBootstrapPlan::new(foundation)?;
    let inventory_plan = execute_store_initialized(initial, backend)?;
    execute_inventory_phase(&inventory_plan, backend)
}

fn execute_store_initialized<B>(
    plan: InitialBootstrapPlan,
    backend: &mut B,
) -> Result<InitialBootstrapInventoryPlan, InitialBootstrapExecutionFailure<B::Error>>
where
    B: InitialBootstrapBackend,
{
    for _ in 0..MAX_BOOTSTRAP_ACTIONS {
        let progress = backend
            .inspect_store_initialized(&plan.store_initialized)
            .map_err(InitialBootstrapExecutionFailure::Backend)?;
        match plan.decide_store_initialized(progress) {
            BootstrapDecision::Execute(BootstrapAction::AdvanceUnit {
                record: BootstrapRecord::StoreInitialized,
                step,
            }) => backend
                .advance_unit(BootstrapRecord::StoreInitialized, &plan.store_initialized, step)
                .map_err(InitialBootstrapExecutionFailure::Backend)?,
            BootstrapDecision::RequireBootstrapInventory => {
                let inventory = backend
                    .scan_inventory()
                    .map_err(InitialBootstrapExecutionFailure::Backend)?;
                return plan.consume_inventory(progress, inventory).map_err(Into::into);
            }
            BootstrapDecision::NeedsRecovery(recovery) => {
                return Err(InitialBootstrapExecutionFailure::NeedsRecovery(recovery));
            }
            _ => return Err(InitialBootstrapExecutionFailure::InvalidPlanAction),
        }
    }
    Err(InitialBootstrapExecutionFailure::ActionBoundExceeded)
}

fn execute_inventory_phase<B>(
    plan: &InitialBootstrapInventoryPlan,
    backend: &mut B,
) -> Result<FencedBootstrapEvidence, InitialBootstrapExecutionFailure<B::Error>>
where
    B: InitialBootstrapBackend,
{
    for _ in 0..MAX_BOOTSTRAP_ACTIONS {
        let progress = backend
            .inspect_inventory_phase(plan)
            .map_err(InitialBootstrapExecutionFailure::Backend)?;
        match plan.decide(progress) {
            BootstrapDecision::Execute(action) => execute_inventory_action(plan, backend, action)?,
            BootstrapDecision::NeedsRecovery(recovery) => {
                return Err(InitialBootstrapExecutionFailure::NeedsRecovery(recovery));
            }
            BootstrapDecision::FencedComplete(evidence) => return Ok(evidence),
            BootstrapDecision::RequireBootstrapInventory => {
                return Err(InitialBootstrapExecutionFailure::InvalidPlanAction);
            }
        }
    }
    Err(InitialBootstrapExecutionFailure::ActionBoundExceeded)
}

fn execute_inventory_action<B>(
    plan: &InitialBootstrapInventoryPlan,
    backend: &mut B,
    action: BootstrapAction,
) -> Result<(), InitialBootstrapExecutionFailure<B::Error>>
where
    B: InitialBootstrapBackend,
{
    match action {
        BootstrapAction::AdvanceSnapshot { step } => backend
            .advance_snapshot(&plan.snapshot, step)
            .map_err(InitialBootstrapExecutionFailure::Backend),
        BootstrapAction::AdvanceInitialMarker { step } => backend
            .advance_initial_marker(&plan.initial_marker, step)
            .map_err(InitialBootstrapExecutionFailure::Backend),
        BootstrapAction::AdvanceUnit { record, step } => {
            let planned = match record {
                BootstrapRecord::BootstrapInstalled => &plan.bootstrap_installed,
                BootstrapRecord::MarkerCommitted => &plan.marker_committed,
                _ => return Err(InitialBootstrapExecutionFailure::InvalidPlanAction),
            };
            backend
                .advance_unit(record, planned, step)
                .map_err(InitialBootstrapExecutionFailure::Backend)
        }
        BootstrapAction::Reconcile { phase } => backend
            .reconcile(phase)
            .map_err(InitialBootstrapExecutionFailure::Backend),
        BootstrapAction::AdvanceMarker { .. } => Err(InitialBootstrapExecutionFailure::InvalidPlanAction),
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelMarkerProgress {
    Missing,
    TemporaryWritten,
    TemporarySynced,
    Published,
    DirectorySynced,
    Verified,
}

#[cfg(test)]
pub(super) struct ModelInitialBootstrapBackend {
    inventory_snapshot: super::super::sidecar::LifecycleSnapshot,
    store_initialized: DurableUnitProgress,
    snapshot: ImmutableArtifactProgress,
    bootstrap_installed: DurableUnitProgress,
    pre_marker_reconciled: bool,
    marker: ModelMarkerProgress,
    marker_committed: DurableUnitProgress,
    post_witness_reconciled: bool,
    actions: Vec<BootstrapAction>,
    fail_after: Option<usize>,
}

#[cfg(test)]
impl private::Sealed for ModelInitialBootstrapBackend {}

#[cfg(test)]
impl ModelInitialBootstrapBackend {
    pub(super) fn new(inventory: BootstrapInventoryEvidence) -> Self {
        Self {
            inventory_snapshot: inventory.snapshot,
            store_initialized: DurableUnitProgress::Missing,
            snapshot: ImmutableArtifactProgress::Missing,
            bootstrap_installed: DurableUnitProgress::Missing,
            pre_marker_reconciled: false,
            marker: ModelMarkerProgress::Missing,
            marker_committed: DurableUnitProgress::Missing,
            post_witness_reconciled: false,
            actions: Vec::new(),
            fail_after: None,
        }
    }

    pub(super) const fn expected_action_count() -> usize {
        41
    }

    pub(super) fn executed_action_count(&self) -> usize {
        self.actions.len()
    }

    pub(super) fn fail_after_action(&mut self, index: usize) {
        self.fail_after = Some(index);
    }

    pub(super) fn clear_failure(&mut self) {
        self.fail_after = None;
    }

    pub(super) fn assert_frozen_order(&self) {
        assert_eq!(self.actions.len(), Self::expected_action_count());
        assert_eq!(
            self.actions.first(),
            Some(&BootstrapAction::AdvanceUnit {
                record: BootstrapRecord::StoreInitialized,
                step: DurableUnitStep::AppendFrame,
            })
        );
        assert_eq!(
            self.actions.last(),
            Some(&BootstrapAction::Reconcile {
                phase: ReconciliationPhase::AfterMarkerWitness,
            })
        );
        let marker_publish = self
            .actions
            .iter()
            .position(|action| {
                *action
                    == BootstrapAction::AdvanceInitialMarker {
                        step: InitialMarkerStep::PublishFinalNoReplace,
                    }
            })
            .expect("initial marker publication is present");
        let witness = self
            .actions
            .iter()
            .position(|action| {
                matches!(
                    action,
                    BootstrapAction::AdvanceUnit {
                        record: BootstrapRecord::MarkerCommitted,
                        step: DurableUnitStep::AppendFrame
                    }
                )
            })
            .expect("marker witness append is present");
        assert!(marker_publish < witness);
    }

    pub(super) fn assert_each_action_at_most_once(&self) {
        for (index, action) in self.actions.iter().enumerate() {
            assert!(
                !self.actions[index + 1..].contains(action),
                "action repeated: {action:?}"
            );
        }
    }

    fn record_action(&mut self, action: BootstrapAction) -> Result<(), std::io::Error> {
        let index = self.actions.len();
        self.actions.push(action);
        if self.fail_after == Some(index) {
            return Err(std::io::Error::other(format!(
                "injected bootstrap failure after action {action:?}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
impl InitialBootstrapBackend for ModelInitialBootstrapBackend {
    type Error = std::io::Error;

    fn inspect_store_initialized(
        &mut self,
        _planned: &PlannedAcknowledgedUnit,
    ) -> Result<DurableUnitProgress, Self::Error> {
        Ok(self.store_initialized)
    }

    fn scan_inventory(&mut self) -> Result<BootstrapInventoryEvidence, Self::Error> {
        Ok(BootstrapInventoryEvidence::verified_for_test(&self.inventory_snapshot)
            .expect("model inventory remains canonical"))
    }

    fn inspect_inventory_phase(
        &mut self,
        planned: &InitialBootstrapInventoryPlan,
    ) -> Result<InitialBootstrapProgress, Self::Error> {
        if self.snapshot != ImmutableArtifactProgress::Verified {
            return Ok(InitialBootstrapProgress::BootstrapSnapshot(self.snapshot));
        }
        if self.bootstrap_installed != DurableUnitProgress::Committed {
            return Ok(InitialBootstrapProgress::BootstrapInstalled(self.bootstrap_installed));
        }
        if !self.pre_marker_reconciled {
            return Ok(InitialBootstrapProgress::BootstrapInstalled(
                DurableUnitProgress::Committed,
            ));
        }
        if self.marker != ModelMarkerProgress::Verified {
            return Ok(match self.marker {
                ModelMarkerProgress::Missing => InitialBootstrapProgress::PreMarkerReconciled,
                ModelMarkerProgress::TemporaryWritten => {
                    InitialBootstrapProgress::InitialMarker(InitialMarkerProgress::TemporaryWritten)
                }
                ModelMarkerProgress::TemporarySynced => {
                    InitialBootstrapProgress::InitialMarker(InitialMarkerProgress::TemporarySynced)
                }
                ModelMarkerProgress::Published => {
                    InitialBootstrapProgress::InitialMarker(InitialMarkerProgress::Published)
                }
                ModelMarkerProgress::DirectorySynced => {
                    InitialBootstrapProgress::InitialMarker(InitialMarkerProgress::DirectorySynced)
                }
                ModelMarkerProgress::Verified => unreachable!("handled above"),
            });
        }
        if self.marker_committed == DurableUnitProgress::Missing {
            let evidence = super::types::InitialMarkerVerificationEvidence::from_reopened_bytes(
                planned.initial_marker.encoded_file,
                &planned.initial_marker,
            )
            .expect("model reopens the exact marker");
            return Ok(InitialBootstrapProgress::InitialMarker(
                InitialMarkerProgress::Verified(Box::new(evidence)),
            ));
        }
        if self.marker_committed != DurableUnitProgress::Committed {
            return Ok(InitialBootstrapProgress::MarkerCommitted(self.marker_committed));
        }
        if !self.post_witness_reconciled {
            return Ok(InitialBootstrapProgress::MarkerCommitted(
                DurableUnitProgress::Committed,
            ));
        }
        Ok(InitialBootstrapProgress::PostWitnessReconciled)
    }

    fn advance_unit(
        &mut self,
        record: BootstrapRecord,
        _planned: &PlannedAcknowledgedUnit,
        step: DurableUnitStep,
    ) -> Result<(), Self::Error> {
        let progress = match record {
            BootstrapRecord::StoreInitialized => &mut self.store_initialized,
            BootstrapRecord::BootstrapInstalled => &mut self.bootstrap_installed,
            BootstrapRecord::MarkerCommitted => &mut self.marker_committed,
            BootstrapRecord::LogOpened => unreachable!("not part of initial bootstrap"),
        };
        *progress = advance_unit_progress(*progress, step);
        self.record_action(BootstrapAction::AdvanceUnit { record, step })
    }

    fn advance_snapshot(&mut self, _planned: &PlannedSnapshot, step: ImmutableArtifactStep) -> Result<(), Self::Error> {
        self.snapshot = match (self.snapshot, step) {
            (ImmutableArtifactProgress::Missing, ImmutableArtifactStep::WriteTemporary) => {
                ImmutableArtifactProgress::TemporaryWritten
            }
            (ImmutableArtifactProgress::TemporaryWritten, ImmutableArtifactStep::SyncTemporary) => {
                ImmutableArtifactProgress::TemporarySynced
            }
            (ImmutableArtifactProgress::TemporarySynced, ImmutableArtifactStep::PublishFinalNoReplace) => {
                ImmutableArtifactProgress::Published
            }
            (ImmutableArtifactProgress::Published, ImmutableArtifactStep::ReopenAndVerify) => {
                ImmutableArtifactProgress::Verified
            }
            _ => unreachable!("planner must advance snapshot monotonically"),
        };
        self.record_action(BootstrapAction::AdvanceSnapshot { step })
    }

    fn advance_initial_marker(
        &mut self,
        _planned: &PlannedInitialMarker,
        step: InitialMarkerStep,
    ) -> Result<(), Self::Error> {
        self.marker = match (self.marker, step) {
            (ModelMarkerProgress::Missing, InitialMarkerStep::WriteTemporary) => ModelMarkerProgress::TemporaryWritten,
            (ModelMarkerProgress::TemporaryWritten, InitialMarkerStep::SyncTemporary) => {
                ModelMarkerProgress::TemporarySynced
            }
            (ModelMarkerProgress::TemporarySynced, InitialMarkerStep::PublishFinalNoReplace) => {
                ModelMarkerProgress::Published
            }
            (ModelMarkerProgress::Published, InitialMarkerStep::SyncLifecycleDirectory) => {
                ModelMarkerProgress::DirectorySynced
            }
            (ModelMarkerProgress::DirectorySynced, InitialMarkerStep::ReopenAndVerifyEntireFile) => {
                ModelMarkerProgress::Verified
            }
            _ => unreachable!("planner must advance initial marker monotonically"),
        };
        self.record_action(BootstrapAction::AdvanceInitialMarker { step })
    }

    fn reconcile(&mut self, phase: ReconciliationPhase) -> Result<(), Self::Error> {
        match phase {
            ReconciliationPhase::BeforeMarker => self.pre_marker_reconciled = true,
            ReconciliationPhase::AfterMarkerWitness => self.post_witness_reconciled = true,
        }
        self.record_action(BootstrapAction::Reconcile { phase })
    }
}

#[cfg(test)]
fn advance_unit_progress(current: DurableUnitProgress, step: DurableUnitStep) -> DurableUnitProgress {
    match (current, step) {
        (DurableUnitProgress::Missing, DurableUnitStep::AppendFrame) => DurableUnitProgress::ExactFramePrefix,
        (DurableUnitProgress::ExactFramePrefix, DurableUnitStep::CompleteFrame) => DurableUnitProgress::FrameWritten,
        (DurableUnitProgress::FrameWritten, DurableUnitStep::SyncFrame) => DurableUnitProgress::FrameSynced,
        (DurableUnitProgress::FrameSynced, DurableUnitStep::WriteAcknowledgementSlot) => {
            DurableUnitProgress::AcknowledgementWritten
        }
        (DurableUnitProgress::AcknowledgementWritten, DurableUnitStep::SyncAcknowledgementSlot) => {
            DurableUnitProgress::AcknowledgementSynced
        }
        (DurableUnitProgress::AcknowledgementSynced, DurableUnitStep::VerifyAcknowledgementSlot) => {
            DurableUnitProgress::AcknowledgementVerified
        }
        (DurableUnitProgress::AcknowledgementVerified, DurableUnitStep::AppendSeal) => {
            DurableUnitProgress::ExactSealPrefix
        }
        (DurableUnitProgress::ExactSealPrefix, DurableUnitStep::CompleteSeal) => DurableUnitProgress::SealWritten,
        (DurableUnitProgress::SealWritten, DurableUnitStep::SyncSeal) => DurableUnitProgress::SealSynced,
        (DurableUnitProgress::SealSynced, DurableUnitStep::VerifySealAndEof) => DurableUnitProgress::Committed,
        _ => unreachable!("planner must advance durable unit monotonically"),
    }
}
