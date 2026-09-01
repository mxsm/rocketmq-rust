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

use super::super::codec::crc32;
use super::super::codec::encode_acknowledgement_slot;
use super::super::codec::encode_commit_seal;
use super::super::codec::encode_ledger_frame;
use super::super::codec::AcknowledgementSlot;
use super::super::codec::CodecViolation;
use super::super::codec::CommitSeal;
use super::super::codec::LedgerRecord;
use super::super::codec::COMMIT_SEAL_LENGTH;
use super::super::identity::StoreUuid;
use super::super::sidecar::encode_enabled_marker_file;
use super::super::sidecar::encode_enabled_marker_slot;
use super::super::sidecar::EnabledMarkerFile;
use super::super::sidecar::EnabledMarkerSlot;
use super::super::sidecar::SidecarViolation;
use super::super::sidecar::StoreMeta;
use super::proof::BootstrapFoundationEvidence;
use super::proof::BootstrapInventoryEvidence;
use super::proof::GenerationSwitchFoundationEvidence;
use super::types::BootstrapAction;
use super::types::BootstrapAmbiguity;
use super::types::BootstrapCheckpoint;
use super::types::BootstrapCrashBoundary;
use super::types::BootstrapDecision;
use super::types::BootstrapFlow;
use super::types::BootstrapPlanViolation;
use super::types::BootstrapRecord;
use super::types::BootstrapRecoveryReason;
use super::types::DurableUnitProgress;
use super::types::DurableUnitStep;
use super::types::FencedBootstrapEvidence;
use super::types::GenerationSwitchProgress;
use super::types::ImmutableArtifactProgress;
use super::types::ImmutableArtifactStep;
use super::types::InitialBootstrapProgress;
use super::types::InitialMarkerProgress;
use super::types::InitialMarkerStep;
use super::types::MarkerSlotProgress;
use super::types::MarkerSlotStep;
use super::types::NeedsRecovery;
use super::types::PlannedAcknowledgedUnit;
use super::types::PlannedInitialMarker;
use super::types::PlannedMarkerSlot;
use super::types::PlannedSnapshot;
use super::types::ReconciliationPhase;

mod validation;

use validation::invalid_switch;
use validation::planned_generation_snapshot;
use validation::planned_inventory_snapshot;
use validation::validate_canonical_store_meta;
use validation::validate_generation_foundation;
use validation::validate_initial_inventory;

/// The only initial-bootstrap phase constructible before durable `StoreInitialized`.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct InitialBootstrapPlan {
    foundation: BootstrapFoundationEvidence,
    pub(super) store_initialized: PlannedAcknowledgedUnit,
}

impl InitialBootstrapPlan {
    pub(super) fn new(foundation: BootstrapFoundationEvidence) -> Result<Self, BootstrapPlanViolation> {
        let meta = validate_canonical_store_meta(&foundation.store_meta)?;
        let record = LedgerRecord::StoreInitialized {
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            creation_time_ns: meta.creation_time_ns,
        };
        let store_initialized = plan_unit(meta, record, 1, 0, 0, 1, false, 0)?;
        Ok(Self {
            foundation,
            store_initialized,
        })
    }

    pub(super) fn decide_store_initialized(&self, progress: DurableUnitProgress) -> BootstrapDecision {
        match unit_step(progress) {
            Some(step) => execute_unit(BootstrapRecord::StoreInitialized, step),
            None => BootstrapDecision::RequireBootstrapInventory,
        }
    }

    /// Consumes the no-follow inventory proof only after `StoreInitialized` is durable.
    pub(super) fn consume_inventory(
        self,
        store_initialized: DurableUnitProgress,
        inventory: BootstrapInventoryEvidence,
    ) -> Result<InitialBootstrapInventoryPlan, BootstrapPlanViolation> {
        if store_initialized != DurableUnitProgress::Committed {
            return Err(BootstrapPlanViolation::StoreInitializedNotDurable);
        }
        let meta = validate_canonical_store_meta(&self.foundation.store_meta)?;
        validate_initial_inventory(&inventory, meta)?;
        let snapshot = planned_inventory_snapshot(inventory)?;

        let bootstrap_installed_record = LedgerRecord::BootstrapInstalled {
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            snapshot_generation: 0,
            snapshot_base_sequence: 1,
            snapshot_file_length: snapshot_length(&snapshot)?,
            snapshot_file_crc32: snapshot.file_crc32,
            inventory_count: snapshot.inventory_count,
            create_high_water: snapshot.create_high_water,
            ticket_high_water: snapshot.ticket_high_water,
        };
        let bootstrap_installed = plan_unit(
            meta,
            bootstrap_installed_record,
            2,
            0,
            self.store_initialized.sealed_log_length,
            2,
            false,
            0,
        )?;
        let initial_marker = plan_initial_marker(
            meta,
            2,
            snapshot_length(&snapshot)?,
            snapshot.file_crc32,
            crc32(&bootstrap_installed.frame),
        )?;
        let marker_committed = plan_unit(
            meta,
            initial_marker_committed_record(meta.store_uuid, &initial_marker),
            3,
            0,
            bootstrap_installed.sealed_log_length,
            3,
            true,
            1,
        )?;
        let fenced_evidence = evidence(BootstrapFlow::Initial, meta.store_uuid, 0, 1, &marker_committed);

        Ok(InitialBootstrapInventoryPlan {
            snapshot,
            bootstrap_installed,
            initial_marker,
            marker_committed,
            fenced_evidence,
        })
    }
}

/// Initial-bootstrap phase that can exist only after consuming an inventory proof.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct InitialBootstrapInventoryPlan {
    pub(super) snapshot: PlannedSnapshot,
    pub(super) bootstrap_installed: PlannedAcknowledgedUnit,
    pub(super) initial_marker: PlannedInitialMarker,
    pub(super) marker_committed: PlannedAcknowledgedUnit,
    pub(super) fenced_evidence: FencedBootstrapEvidence,
}

impl InitialBootstrapInventoryPlan {
    pub(super) fn decide(&self, progress: InitialBootstrapProgress) -> BootstrapDecision {
        match progress {
            InitialBootstrapProgress::BootstrapSnapshot(progress) => artifact_or(
                progress,
                BootstrapAction::AdvanceUnit {
                    record: BootstrapRecord::BootstrapInstalled,
                    step: DurableUnitStep::AppendFrame,
                },
            ),
            InitialBootstrapProgress::BootstrapInstalled(progress) => unit_or(
                BootstrapRecord::BootstrapInstalled,
                progress,
                BootstrapAction::Reconcile {
                    phase: ReconciliationPhase::BeforeMarker,
                },
            ),
            InitialBootstrapProgress::PreMarkerReconciled => execute(BootstrapAction::AdvanceInitialMarker {
                step: InitialMarkerStep::WriteTemporary,
            }),
            InitialBootstrapProgress::InitialMarker(progress) => initial_marker_or(
                &self.initial_marker,
                progress,
                BootstrapAction::AdvanceUnit {
                    record: BootstrapRecord::MarkerCommitted,
                    step: DurableUnitStep::AppendFrame,
                },
            ),
            InitialBootstrapProgress::MarkerCommitted(progress) => unit_or(
                BootstrapRecord::MarkerCommitted,
                progress,
                BootstrapAction::Reconcile {
                    phase: ReconciliationPhase::AfterMarkerWitness,
                },
            ),
            InitialBootstrapProgress::PostWitnessReconciled => BootstrapDecision::FencedComplete(self.fenced_evidence),
            InitialBootstrapProgress::Ambiguous { checkpoint, evidence } => needs_recovery(
                BootstrapFlow::Initial,
                checkpoint,
                BootstrapRecoveryReason::AmbiguousPersistedState(evidence),
            ),
        }
    }
}

/// Marker-switch phase minted only from verified predecessor and published-anchor evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GenerationSwitchPlan {
    pub(super) snapshot: PlannedSnapshot,
    pub(super) log_opened: PlannedAcknowledgedUnit,
    pub(super) marker_slot: PlannedMarkerSlot,
    pub(super) marker_committed: PlannedAcknowledgedUnit,
    pub(super) fenced_evidence: FencedBootstrapEvidence,
}

impl GenerationSwitchPlan {
    pub(super) fn new(foundation: GenerationSwitchFoundationEvidence) -> Result<Self, BootstrapPlanViolation> {
        validate_generation_foundation(&foundation)?;
        let common = foundation.common();
        let meta = &common.store_meta.meta;
        let snapshot = planned_generation_snapshot(common)?;
        let anchor_sequence =
            common
                .predecessor_terminal_sequence
                .checked_add(1)
                .ok_or(BootstrapPlanViolation::ArithmeticOverflow {
                    field: "LogOpened sequence",
                })?;
        let anchor_acknowledgement_epoch = common.predecessor_acknowledgement_epoch.checked_add(1).ok_or(
            BootstrapPlanViolation::ArithmeticOverflow {
                field: "LogOpened acknowledgement epoch",
            },
        )?;
        let log_opened = plan_existing_frame_unit(
            meta,
            common.log_opened_record.clone(),
            common.canonical_log_opened_frame.clone(),
            anchor_sequence,
            common.snapshot.generation,
            0,
            anchor_acknowledgement_epoch,
            true,
            common.marker_epoch,
        )?;
        let marker_slot = plan_marker(
            meta,
            common.marker_epoch,
            common.snapshot.generation,
            anchor_sequence,
            snapshot_length(&snapshot)?,
            snapshot.file_crc32,
            common.log_opened_frame_crc32,
        )?;
        let witness_sequence = anchor_sequence
            .checked_add(1)
            .ok_or(BootstrapPlanViolation::ArithmeticOverflow {
                field: "MarkerCommitted sequence",
            })?;
        let witness_acknowledgement_epoch =
            anchor_acknowledgement_epoch
                .checked_add(1)
                .ok_or(BootstrapPlanViolation::ArithmeticOverflow {
                    field: "MarkerCommitted acknowledgement epoch",
                })?;
        let marker_committed = plan_unit(
            meta,
            marker_committed_record(meta.store_uuid, &marker_slot),
            witness_sequence,
            common.snapshot.generation,
            log_opened.sealed_log_length,
            witness_acknowledgement_epoch,
            true,
            common.marker_epoch,
        )?;
        let fenced_evidence = evidence(
            BootstrapFlow::GenerationSwitch,
            meta.store_uuid,
            common.snapshot.generation,
            common.marker_epoch,
            &marker_committed,
        );

        Ok(Self {
            snapshot,
            log_opened,
            marker_slot,
            marker_committed,
            fenced_evidence,
        })
    }

    pub(super) fn decide(&self, progress: GenerationSwitchProgress) -> BootstrapDecision {
        match progress {
            GenerationSwitchProgress::LogOpenedBeforeMarker(progress) => match progress {
                DurableUnitProgress::FrameSynced => execute(BootstrapAction::AdvanceMarker {
                    step: MarkerSlotStep::WriteInactiveSlot,
                }),
                DurableUnitProgress::Missing
                | DurableUnitProgress::ExactFramePrefix
                | DurableUnitProgress::FrameWritten => needs_recovery(
                    BootstrapFlow::GenerationSwitch,
                    BootstrapCheckpoint::LogOpened,
                    BootstrapRecoveryReason::FoundationAnchorNotDurable,
                ),
                _ => needs_recovery(
                    BootstrapFlow::GenerationSwitch,
                    BootstrapCheckpoint::LogOpened,
                    BootstrapRecoveryReason::AnchorAcknowledgedBeforeMarker,
                ),
            },
            GenerationSwitchProgress::MarkerSlot(progress) => marker_or(
                progress,
                BootstrapAction::AdvanceUnit {
                    record: BootstrapRecord::LogOpened,
                    step: DurableUnitStep::WriteAcknowledgementSlot,
                },
            ),
            GenerationSwitchProgress::LogOpenedAfterMarker(progress) => match progress {
                DurableUnitProgress::Missing
                | DurableUnitProgress::ExactFramePrefix
                | DurableUnitProgress::FrameWritten => needs_recovery(
                    BootstrapFlow::GenerationSwitch,
                    BootstrapCheckpoint::LogOpened,
                    BootstrapRecoveryReason::AnchorMissingAfterMarker,
                ),
                progress => unit_or(
                    BootstrapRecord::LogOpened,
                    progress,
                    BootstrapAction::AdvanceUnit {
                        record: BootstrapRecord::MarkerCommitted,
                        step: DurableUnitStep::AppendFrame,
                    },
                ),
            },
            GenerationSwitchProgress::MarkerCommitted(progress) => unit_or(
                BootstrapRecord::MarkerCommitted,
                progress,
                BootstrapAction::Reconcile {
                    phase: ReconciliationPhase::AfterMarkerWitness,
                },
            ),
            GenerationSwitchProgress::PostWitnessReconciled => BootstrapDecision::FencedComplete(self.fenced_evidence),
            GenerationSwitchProgress::Ambiguous { checkpoint, evidence } => needs_recovery(
                BootstrapFlow::GenerationSwitch,
                checkpoint,
                BootstrapRecoveryReason::AmbiguousPersistedState(evidence),
            ),
        }
    }
}

pub(super) const fn decision_after_failure(
    flow: BootstrapFlow,
    checkpoint: BootstrapCheckpoint,
    boundary: BootstrapCrashBoundary,
) -> BootstrapDecision {
    needs_recovery(flow, checkpoint, BootstrapRecoveryReason::OperationFailed(boundary))
}

fn snapshot_length(snapshot: &PlannedSnapshot) -> Result<u64, BootstrapPlanViolation> {
    u64::try_from(snapshot.encoded.len()).map_err(|_| BootstrapPlanViolation::ArithmeticOverflow {
        field: "snapshot file length",
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the planned unit mirrors the persisted frame, acknowledgement, and seal bindings"
)]
fn plan_unit(
    meta: &StoreMeta,
    record: LedgerRecord,
    sequence: u64,
    log_generation: u64,
    frame_start_offset: u64,
    acknowledgement_epoch: u64,
    activated: bool,
    marker_epoch: u64,
) -> Result<PlannedAcknowledgedUnit, BootstrapPlanViolation> {
    let frame = encode_ledger_frame(&record, sequence, log_generation)?;
    plan_unit_from_frame(
        meta,
        record,
        frame,
        sequence,
        log_generation,
        frame_start_offset,
        acknowledgement_epoch,
        activated,
        marker_epoch,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the existing-frame plan verifies all persisted frame, acknowledgement, and seal bindings"
)]
fn plan_existing_frame_unit(
    meta: &StoreMeta,
    record: LedgerRecord,
    frame: Vec<u8>,
    sequence: u64,
    log_generation: u64,
    frame_start_offset: u64,
    acknowledgement_epoch: u64,
    activated: bool,
    marker_epoch: u64,
) -> Result<PlannedAcknowledgedUnit, BootstrapPlanViolation> {
    if encode_ledger_frame(&record, sequence, log_generation)? != frame {
        return Err(invalid_switch("published LogOpened frame differs from its record"));
    }
    plan_unit_from_frame(
        meta,
        record,
        frame,
        sequence,
        log_generation,
        frame_start_offset,
        acknowledgement_epoch,
        activated,
        marker_epoch,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the planned unit mirrors the persisted frame, acknowledgement, and seal bindings"
)]
fn plan_unit_from_frame(
    meta: &StoreMeta,
    record: LedgerRecord,
    frame: Vec<u8>,
    sequence: u64,
    log_generation: u64,
    frame_start_offset: u64,
    acknowledgement_epoch: u64,
    activated: bool,
    marker_epoch: u64,
) -> Result<PlannedAcknowledgedUnit, BootstrapPlanViolation> {
    let frame_length =
        u64::try_from(frame.len()).map_err(|_| BootstrapPlanViolation::ArithmeticOverflow { field: "frame length" })?;
    let frame_end_offset =
        frame_start_offset
            .checked_add(frame_length)
            .ok_or(BootstrapPlanViolation::ArithmeticOverflow {
                field: "frame end offset",
            })?;
    let slot_index = ((acknowledgement_epoch
        .checked_sub(1)
        .ok_or(CodecViolation::ZeroAcknowledgementEpoch)?)
        & 1) as u8;
    let slot = AcknowledgementSlot {
        slot_index,
        activated,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        acknowledgement_epoch,
        marker_epoch,
        log_generation,
        frame_sequence: sequence,
        frame_end_offset,
        frame_crc32: crc32(&frame),
    };
    let acknowledgement_slot = encode_acknowledgement_slot(&slot)?;
    let commit_seal = CommitSeal::from_acknowledgement_slot(&slot, &acknowledgement_slot)?;
    let seal = encode_commit_seal(&commit_seal)?;
    let sealed_log_length =
        frame_end_offset
            .checked_add(COMMIT_SEAL_LENGTH as u64)
            .ok_or(BootstrapPlanViolation::ArithmeticOverflow {
                field: "sealed log length",
            })?;
    Ok(PlannedAcknowledgedUnit {
        record,
        frame,
        acknowledgement_slot,
        seal,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the marker plan mirrors the seven independent marker binding fields"
)]
fn plan_marker(
    meta: &StoreMeta,
    marker_epoch: u64,
    generation: u64,
    anchor_sequence: u64,
    snapshot_file_length: u64,
    snapshot_file_crc32: u32,
    anchor_frame_crc32: u32,
) -> Result<PlannedMarkerSlot, BootstrapPlanViolation> {
    let slot_index = ((marker_epoch.checked_sub(1).ok_or(SidecarViolation::ZeroMarkerEpoch)?) & 1) as u8;
    let slot = EnabledMarkerSlot {
        slot_index,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        marker_epoch,
        snapshot_generation: generation,
        log_generation: generation,
        anchor_sequence,
        snapshot_file_length,
        snapshot_file_crc32,
        anchor_frame_crc32,
    };
    let encoded = encode_enabled_marker_slot(&slot)?;
    let stored_crc32 = u32::from_le_bytes([encoded[100], encoded[101], encoded[102], encoded[103]]);
    Ok(PlannedMarkerSlot {
        slot,
        encoded,
        stored_crc32,
    })
}

fn plan_initial_marker(
    meta: &StoreMeta,
    anchor_sequence: u64,
    snapshot_file_length: u64,
    snapshot_file_crc32: u32,
    anchor_frame_crc32: u32,
) -> Result<PlannedInitialMarker, BootstrapPlanViolation> {
    let planned_slot = plan_marker(
        meta,
        1,
        0,
        anchor_sequence,
        snapshot_file_length,
        snapshot_file_crc32,
        anchor_frame_crc32,
    )?;
    let encoded_file = encode_enabled_marker_file(&EnabledMarkerFile {
        slots: [Some(planned_slot.slot.clone()), None],
    })?;
    Ok(PlannedInitialMarker {
        slot0: planned_slot.slot,
        file_crc32: crc32(&encoded_file),
        encoded_file,
        slot0_stored_crc32: planned_slot.stored_crc32,
    })
}

fn marker_committed_record(store_uuid: StoreUuid, marker: &PlannedMarkerSlot) -> LedgerRecord {
    LedgerRecord::MarkerCommitted {
        store_uuid,
        marker_epoch: marker.slot.marker_epoch,
        snapshot_generation: marker.slot.snapshot_generation,
        log_generation: marker.slot.log_generation,
        anchor_sequence: marker.slot.anchor_sequence,
        slot_index: marker.slot.slot_index,
        slot_crc32: marker.stored_crc32,
    }
}

fn initial_marker_committed_record(store_uuid: StoreUuid, marker: &PlannedInitialMarker) -> LedgerRecord {
    LedgerRecord::MarkerCommitted {
        store_uuid,
        marker_epoch: marker.slot0.marker_epoch,
        snapshot_generation: marker.slot0.snapshot_generation,
        log_generation: marker.slot0.log_generation,
        anchor_sequence: marker.slot0.anchor_sequence,
        slot_index: marker.slot0.slot_index,
        slot_crc32: marker.slot0_stored_crc32,
    }
}

const fn evidence(
    flow: BootstrapFlow,
    store_uuid: StoreUuid,
    log_generation: u64,
    marker_epoch: u64,
    witness: &PlannedAcknowledgedUnit,
) -> FencedBootstrapEvidence {
    FencedBootstrapEvidence {
        flow,
        store_uuid,
        log_generation,
        marker_epoch,
        witness_sequence: witness.sequence,
        acknowledgement_epoch: witness.acknowledgement_epoch,
        sealed_log_length: witness.sealed_log_length,
    }
}

fn unit_or(record: BootstrapRecord, progress: DurableUnitProgress, next: BootstrapAction) -> BootstrapDecision {
    match unit_step(progress) {
        Some(step) => execute_unit(record, step),
        None => execute(next),
    }
}

const fn unit_step(progress: DurableUnitProgress) -> Option<DurableUnitStep> {
    match progress {
        DurableUnitProgress::Missing => Some(DurableUnitStep::AppendFrame),
        DurableUnitProgress::ExactFramePrefix => Some(DurableUnitStep::CompleteFrame),
        DurableUnitProgress::FrameWritten => Some(DurableUnitStep::SyncFrame),
        DurableUnitProgress::FrameSynced => Some(DurableUnitStep::WriteAcknowledgementSlot),
        DurableUnitProgress::AcknowledgementWritten => Some(DurableUnitStep::SyncAcknowledgementSlot),
        DurableUnitProgress::AcknowledgementSynced => Some(DurableUnitStep::VerifyAcknowledgementSlot),
        DurableUnitProgress::AcknowledgementVerified => Some(DurableUnitStep::AppendSeal),
        DurableUnitProgress::ExactSealPrefix => Some(DurableUnitStep::CompleteSeal),
        DurableUnitProgress::SealWritten => Some(DurableUnitStep::SyncSeal),
        DurableUnitProgress::SealSynced => Some(DurableUnitStep::VerifySealAndEof),
        DurableUnitProgress::Committed => None,
    }
}

fn artifact_or(progress: ImmutableArtifactProgress, next: BootstrapAction) -> BootstrapDecision {
    let step = match progress {
        ImmutableArtifactProgress::Missing => Some(ImmutableArtifactStep::WriteTemporary),
        ImmutableArtifactProgress::TemporaryWritten => Some(ImmutableArtifactStep::SyncTemporary),
        ImmutableArtifactProgress::TemporarySynced => Some(ImmutableArtifactStep::PublishFinalNoReplace),
        ImmutableArtifactProgress::Published => Some(ImmutableArtifactStep::ReopenAndVerify),
        ImmutableArtifactProgress::Verified => None,
    };
    match step {
        Some(step) => execute(BootstrapAction::AdvanceSnapshot { step }),
        None => execute(next),
    }
}

fn marker_or(progress: MarkerSlotProgress, next: BootstrapAction) -> BootstrapDecision {
    let step = match progress {
        MarkerSlotProgress::Missing => Some(MarkerSlotStep::WriteInactiveSlot),
        MarkerSlotProgress::ExactSlotPrefix => Some(MarkerSlotStep::CompleteInactiveSlot),
        MarkerSlotProgress::SlotWritten => Some(MarkerSlotStep::SyncMarkerFile),
        MarkerSlotProgress::SlotSynced => Some(MarkerSlotStep::ReopenAndVerifySlot),
        MarkerSlotProgress::Verified => None,
    };
    match step {
        Some(step) => execute(BootstrapAction::AdvanceMarker { step }),
        None => execute(next),
    }
}

fn initial_marker_or(
    planned: &PlannedInitialMarker,
    progress: InitialMarkerProgress,
    next: BootstrapAction,
) -> BootstrapDecision {
    match progress {
        InitialMarkerProgress::Missing => execute(BootstrapAction::AdvanceInitialMarker {
            step: InitialMarkerStep::WriteTemporary,
        }),
        InitialMarkerProgress::TemporaryWritten => execute(BootstrapAction::AdvanceInitialMarker {
            step: InitialMarkerStep::SyncTemporary,
        }),
        InitialMarkerProgress::TemporarySynced => execute(BootstrapAction::AdvanceInitialMarker {
            step: InitialMarkerStep::PublishFinalNoReplace,
        }),
        InitialMarkerProgress::Published => execute(BootstrapAction::AdvanceInitialMarker {
            step: InitialMarkerStep::SyncLifecycleDirectory,
        }),
        InitialMarkerProgress::DirectorySynced => execute(BootstrapAction::AdvanceInitialMarker {
            step: InitialMarkerStep::ReopenAndVerifyEntireFile,
        }),
        InitialMarkerProgress::Verified(evidence) if evidence.matches(planned) => execute(next),
        InitialMarkerProgress::Verified(_) => needs_recovery(
            BootstrapFlow::Initial,
            BootstrapCheckpoint::InitialMarker,
            BootstrapRecoveryReason::AmbiguousPersistedState(BootstrapAmbiguity::InitialMarkerArtifact),
        ),
    }
}

const fn execute(action: BootstrapAction) -> BootstrapDecision {
    BootstrapDecision::Execute(action)
}

const fn execute_unit(record: BootstrapRecord, step: DurableUnitStep) -> BootstrapDecision {
    execute(BootstrapAction::AdvanceUnit { record, step })
}

const fn needs_recovery(
    flow: BootstrapFlow,
    checkpoint: BootstrapCheckpoint,
    reason: BootstrapRecoveryReason,
) -> BootstrapDecision {
    BootstrapDecision::NeedsRecovery(NeedsRecovery {
        flow,
        checkpoint,
        reason,
    })
}
