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
use std::fs::File;
use std::io;

use thiserror::Error;

use super::super::inventory::scan_bootstrap_inventory;
use super::super::inventory::BootstrapInventoryError;
use super::super::inventory::BootstrapInventoryLimits;
use super::super::plan::InitialBootstrapInventoryPlan;
use super::super::proof::BootstrapFoundationEvidence;
use super::super::proof::BootstrapInventoryEvidence;
use super::super::types::BootstrapRecord;
use super::super::types::DurableUnitProgress;
use super::super::types::DurableUnitStep;
use super::super::types::FencedBootstrapEvidence;
use super::super::types::ImmutableArtifactStep;
use super::super::types::InitialBootstrapProgress;
use super::super::types::InitialMarkerProgress;
use super::super::types::InitialMarkerStep;
use super::super::types::PlannedAcknowledgedUnit;
use super::super::types::PlannedInitialMarker;
use super::super::types::PlannedSnapshot;
use super::super::types::ReconciliationPhase;
use super::durable_unit::DurableUnitError;
use super::durable_unit::DurableUnitMachine;
use super::private;
use super::InitialBootstrapBackend;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::io::FileLedgerIo;
use crate::mapped_file::retirement::io::LedgerIoError;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::SidecarViolation;
use crate::mapped_file::retirement::sidecar::StoreMeta;

#[cfg(target_os = "linux")]
#[path = "platform/linux.rs"]
mod native;
#[cfg(windows)]
#[path = "platform/windows.rs"]
mod native;
#[cfg(not(any(target_os = "linux", windows)))]
#[path = "platform/unsupported.rs"]
mod native;

/// Bootstrap-foundation failure with its former kind folded in.
#[derive(Debug, Error)]
pub(in crate::mapped_file::retirement::bootstrap) enum InitialBootstrapFoundationError {
    #[error("managed lifecycle bootstrap is unsupported on this platform: {0}")]
    UnsupportedPlatform(&'static str),
    #[error("bootstrap artifact is not the exact resumable prefix: {0}")]
    InvalidArtifact(&'static str),
    #[error("bootstrap sidecar validation failed")]
    Sidecar(#[source] SidecarViolation),
    #[error("bootstrap ledger handle validation failed")]
    Ledger(#[source] LedgerIoError),
    #[error("bootstrap frame/acknowledgement/seal transition failed")]
    DurableUnit(#[source] DurableUnitError),
    #[error("bootstrap numeric-segment inventory failed")]
    Inventory(#[source] BootstrapInventoryError),
    #[error("bootstrap filesystem operation failed")]
    Io(#[source] io::Error),
}

impl InitialBootstrapFoundationError {
    fn unsupported(reason: &'static str) -> Self {
        Self::UnsupportedPlatform(reason)
    }

    fn invalid(detail: &'static str) -> Self {
        Self::InvalidArtifact(detail)
    }

    fn sidecar(source: SidecarViolation) -> Self {
        Self::Sidecar(source)
    }

    fn ledger(source: LedgerIoError) -> Self {
        Self::Ledger(source)
    }

    fn durable_unit(source: DurableUnitError) -> Self {
        Self::DurableUnit(source)
    }

    fn inventory(source: BootstrapInventoryError) -> Self {
        Self::Inventory(source)
    }

    fn io(source: io::Error) -> Self {
        Self::Io(source)
    }
}

pub(in crate::mapped_file::retirement::bootstrap) struct PreparedInitialBootstrapFoundation {
    foundation: Option<BootstrapFoundationEvidence>,
    ledger: DurableUnitMachine<FileLedgerIo>,
    store_root: File,
    artifacts: native::InitialArtifactStore,
    expected_meta: StoreMeta,
    expected_inventory: Option<LifecycleSnapshot>,
    retained_files: BTreeMap<StoreRelativePath, File>,
    pre_marker_reconciled: bool,
    post_witness_reconciled: bool,
}

/// Complete initial bootstrap that remains fenced until startup replay and reconciliation succeed.
#[doc(hidden)]
pub struct InitialBootstrapCompletion {
    evidence: FencedBootstrapEvidence,
}

impl std::fmt::Debug for InitialBootstrapCompletion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InitialBootstrapCompletion")
            .field("store_uuid", &self.evidence.store_uuid)
            .field("witness_sequence", &self.evidence.witness_sequence)
            .field("acknowledgement_epoch", &self.evidence.acknowledgement_epoch)
            .field("marker_epoch", &self.evidence.marker_epoch)
            .finish_non_exhaustive()
    }
}

impl InitialBootstrapCompletion {
    #[doc(hidden)]
    pub fn store_uuid(&self) -> [u8; 16] {
        *self.evidence.store_uuid.as_bytes()
    }

    #[doc(hidden)]
    pub const fn witness_sequence(&self) -> u64 {
        self.evidence.witness_sequence
    }

    #[doc(hidden)]
    pub const fn acknowledgement_epoch(&self) -> u64 {
        self.evidence.acknowledgement_epoch
    }

    #[doc(hidden)]
    pub const fn marker_epoch(&self) -> u64 {
        self.evidence.marker_epoch
    }
}

impl std::fmt::Debug for PreparedInitialBootstrapFoundation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedInitialBootstrapFoundation")
            .field(
                "store_uuid",
                &self
                    .foundation
                    .as_ref()
                    .map(|foundation| foundation.store_meta.meta.store_uuid),
            )
            .finish_non_exhaustive()
    }
}

impl PreparedInitialBootstrapFoundation {
    fn new(
        foundation: BootstrapFoundationEvidence,
        ledger: FileLedgerIo,
        store_root: File,
        artifacts: native::InitialArtifactStore,
        expected_meta: StoreMeta,
    ) -> Self {
        Self {
            foundation: Some(foundation),
            ledger: DurableUnitMachine::new(ledger),
            store_root,
            artifacts,
            expected_meta,
            expected_inventory: None,
            retained_files: BTreeMap::new(),
            pre_marker_reconciled: false,
            post_witness_reconciled: false,
        }
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement::bootstrap) fn store_uuid_for_test(
        &self,
    ) -> crate::mapped_file::retirement::identity::StoreUuid {
        self.foundation
            .as_ref()
            .expect("test observes foundation before execution")
            .store_meta
            .meta
            .store_uuid
    }
}

pub(in crate::mapped_file::retirement::bootstrap) fn execute_prepared_initial_bootstrap(
    mut prepared: PreparedInitialBootstrapFoundation,
) -> Result<InitialBootstrapCompletion, super::InitialBootstrapExecutionError<InitialBootstrapFoundationError>> {
    let foundation = prepared.foundation.take().ok_or_else(|| {
        super::InitialBootstrapExecutionError::Backend(InitialBootstrapFoundationError::invalid(
            "bootstrap foundation was already consumed",
        ))
    })?;
    let evidence = super::execute_initial_bootstrap(foundation, &mut prepared)?;
    Ok(InitialBootstrapCompletion { evidence })
}

impl private::Sealed for PreparedInitialBootstrapFoundation {}

impl InitialBootstrapBackend for PreparedInitialBootstrapFoundation {
    type Error = InitialBootstrapFoundationError;

    fn inspect_store_initialized(
        &mut self,
        planned: &PlannedAcknowledgedUnit,
    ) -> Result<DurableUnitProgress, Self::Error> {
        self.ledger
            .inspect(BootstrapRecord::StoreInitialized, planned)
            .map_err(InitialBootstrapFoundationError::durable_unit)
    }

    fn scan_inventory(&mut self) -> Result<BootstrapInventoryEvidence, Self::Error> {
        let stable = scan_bootstrap_inventory(
            &self.store_root,
            &self.expected_meta,
            BootstrapInventoryLimits::default(),
        )
        .map_err(InitialBootstrapFoundationError::inventory)?;
        self.expected_inventory = Some(stable.snapshot().clone());
        let (evidence, retained_files) = stable.into_parts();
        self.retained_files = retained_files;
        Ok(evidence)
    }

    fn inspect_inventory_phase(
        &mut self,
        planned: &InitialBootstrapInventoryPlan,
    ) -> Result<InitialBootstrapProgress, Self::Error> {
        let snapshot = self.artifacts.inspect_snapshot(&planned.snapshot)?;
        if snapshot != super::super::types::ImmutableArtifactProgress::Verified {
            return Ok(InitialBootstrapProgress::BootstrapSnapshot(snapshot));
        }
        let bootstrap_installed = self
            .ledger
            .inspect(BootstrapRecord::BootstrapInstalled, &planned.bootstrap_installed)
            .map_err(InitialBootstrapFoundationError::durable_unit)?;
        if bootstrap_installed != DurableUnitProgress::Committed || !self.pre_marker_reconciled {
            return Ok(InitialBootstrapProgress::BootstrapInstalled(bootstrap_installed));
        }
        let marker = self.artifacts.inspect_initial_marker(&planned.initial_marker)?;
        if !matches!(&marker, InitialMarkerProgress::Verified(_)) {
            return Ok(if matches!(&marker, InitialMarkerProgress::Missing) {
                InitialBootstrapProgress::PreMarkerReconciled
            } else {
                InitialBootstrapProgress::InitialMarker(marker)
            });
        }
        let marker_committed = self
            .ledger
            .inspect(BootstrapRecord::MarkerCommitted, &planned.marker_committed)
            .map_err(InitialBootstrapFoundationError::durable_unit)?;
        if marker_committed == DurableUnitProgress::Missing {
            return Ok(InitialBootstrapProgress::InitialMarker(marker));
        }
        if marker_committed != DurableUnitProgress::Committed || !self.post_witness_reconciled {
            return Ok(InitialBootstrapProgress::MarkerCommitted(marker_committed));
        }
        Ok(InitialBootstrapProgress::PostWitnessReconciled)
    }

    fn advance_unit(
        &mut self,
        record: BootstrapRecord,
        planned: &PlannedAcknowledgedUnit,
        step: DurableUnitStep,
    ) -> Result<(), Self::Error> {
        self.ledger
            .advance(record, planned, step)
            .map_err(InitialBootstrapFoundationError::durable_unit)
    }

    fn advance_snapshot(&mut self, planned: &PlannedSnapshot, step: ImmutableArtifactStep) -> Result<(), Self::Error> {
        self.artifacts.advance_snapshot(planned, step)
    }

    fn advance_initial_marker(
        &mut self,
        planned: &PlannedInitialMarker,
        step: InitialMarkerStep,
    ) -> Result<(), Self::Error> {
        self.artifacts.advance_initial_marker(planned, step)
    }

    fn reconcile(&mut self, phase: ReconciliationPhase) -> Result<(), Self::Error> {
        let expected = self.expected_inventory.as_ref().ok_or_else(|| {
            InitialBootstrapFoundationError::invalid("inventory was not retained before reconciliation")
        })?;
        let stable = scan_bootstrap_inventory(
            &self.store_root,
            &self.expected_meta,
            BootstrapInventoryLimits::default(),
        )
        .map_err(InitialBootstrapFoundationError::inventory)?;
        if stable.snapshot() != expected || stable.retained_file_count_for_reconciliation() != self.retained_files.len()
        {
            return Err(InitialBootstrapFoundationError::invalid(
                "numeric segment inventory changed before bootstrap reconciliation",
            ));
        }
        match phase {
            ReconciliationPhase::BeforeMarker => self.pre_marker_reconciled = true,
            ReconciliationPhase::AfterMarkerWitness => self.post_witness_reconciled = true,
        }
        Ok(())
    }
}

pub(in crate::mapped_file::retirement::bootstrap) fn prepare_initial_bootstrap_foundation(
    store_root: File,
    expected_meta: &StoreMeta,
) -> Result<PreparedInitialBootstrapFoundation, InitialBootstrapFoundationError> {
    native::prepare(store_root, expected_meta)
}
