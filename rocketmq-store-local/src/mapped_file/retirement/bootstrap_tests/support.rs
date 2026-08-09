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

use super::*;
use crate::mapped_file::retirement::codec::decode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::decode_commit_seal;
use crate::mapped_file::retirement::codec::decode_next_frame;
use crate::mapped_file::retirement::codec::AcknowledgementSlotState;
use crate::mapped_file::retirement::codec::DecodeOutcome;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::sidecar::StoreMeta;

pub(super) fn store_meta() -> StoreMeta {
    StoreMeta {
        store_uuid: store_uuid(),
        creation_time_ns: 7,
        bootstrap_id: std::array::from_fn(|index| index as u8 + 0x10),
    }
}

pub(super) fn bootstrap_snapshot() -> LifecycleSnapshot {
    LifecycleSnapshot {
        mode: SnapshotMode::BootstrapInventory,
        store_uuid: store_uuid(),
        generation: 0,
        log_generation: 0,
        predecessor_log_generation: u64::MAX,
        base_sequence: 1,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    }
}

pub(super) fn compaction_snapshot() -> LifecycleSnapshot {
    LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 20,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    }
}

pub(super) fn tail_snapshot() -> LifecycleSnapshot {
    LifecycleSnapshot {
        mode: SnapshotMode::TailRepair,
        ..compaction_snapshot()
    }
}

pub(super) fn foundation(meta: &StoreMeta) -> BootstrapFoundationEvidence {
    BootstrapFoundationEvidence::verified_for_test(meta).expect("foundation is canonical")
}

pub(super) fn inventory(snapshot: &LifecycleSnapshot) -> BootstrapInventoryEvidence {
    BootstrapInventoryEvidence::verified_for_test(snapshot).expect("inventory is canonical")
}

pub(super) fn initial_store_plan() -> InitialBootstrapPlan {
    let meta = store_meta();
    InitialBootstrapPlan::new(foundation(&meta)).expect("initial StoreInitialized plan is valid")
}

pub(super) fn initial_inventory_plan() -> InitialBootstrapInventoryPlan {
    initial_store_plan()
        .consume_inventory(DurableUnitProgress::Committed, inventory(&bootstrap_snapshot()))
        .expect("inventory proof advances initial bootstrap")
}

pub(super) fn compaction_proof() -> GenerationSwitchFoundationEvidence {
    GenerationSwitchFoundationEvidence::compaction_for_test(&store_meta(), &compaction_snapshot(), 4096, 0x0102_0304)
        .expect("compaction foundation is canonical")
}

pub(super) fn tail_proof(suffix: Vec<u8>) -> GenerationSwitchFoundationEvidence {
    GenerationSwitchFoundationEvidence::tail_repair_for_test(&store_meta(), &tail_snapshot(), 4096, 0x0102_0304, suffix)
        .expect("tail-repair foundation is canonical")
}

pub(super) fn switch_plan() -> GenerationSwitchPlan {
    GenerationSwitchPlan::new(compaction_proof()).expect("generation switch plan is valid")
}

pub(super) fn decode_record(unit: &PlannedAcknowledgedUnit, generation: u64) -> LedgerRecord {
    let DecodeOutcome::Frame(frame) = decode_next_frame(&unit.frame, unit.sequence, generation).expect("frame decodes")
    else {
        panic!("planned frame must be complete");
    };
    frame
        .decode_record()
        .expect("record decodes")
        .expect("bootstrap records are known")
}

pub(super) fn assert_slot_and_seal(
    unit: &PlannedAcknowledgedUnit,
    expected_slot: u8,
    activated: bool,
    marker_epoch: u64,
) {
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(&unit.acknowledgement_slot).expect("slot decodes")
    else {
        panic!("planned acknowledgement must be populated");
    };
    assert_eq!(slot.slot_index, expected_slot);
    assert_eq!(slot.activated, activated);
    assert_eq!(slot.marker_epoch, marker_epoch);
    assert_eq!(slot.frame_sequence, unit.sequence);
    assert_eq!(slot.frame_end_offset, unit.frame_end_offset);
    let seal = decode_commit_seal(&unit.seal).expect("seal decodes");
    assert_eq!(seal.acknowledgement_slot_index, slot.slot_index);
    assert_eq!(seal.acknowledgement_epoch, slot.acknowledgement_epoch);
    assert_eq!(seal.frame_end_offset, slot.frame_end_offset);
}

pub(super) fn unit_progress_steps() -> [(DurableUnitProgress, DurableUnitStep); 10] {
    [
        (DurableUnitProgress::Missing, DurableUnitStep::AppendFrame),
        (DurableUnitProgress::ExactFramePrefix, DurableUnitStep::CompleteFrame),
        (DurableUnitProgress::FrameWritten, DurableUnitStep::SyncFrame),
        (
            DurableUnitProgress::FrameSynced,
            DurableUnitStep::WriteAcknowledgementSlot,
        ),
        (
            DurableUnitProgress::AcknowledgementWritten,
            DurableUnitStep::SyncAcknowledgementSlot,
        ),
        (
            DurableUnitProgress::AcknowledgementSynced,
            DurableUnitStep::VerifyAcknowledgementSlot,
        ),
        (
            DurableUnitProgress::AcknowledgementVerified,
            DurableUnitStep::AppendSeal,
        ),
        (DurableUnitProgress::ExactSealPrefix, DurableUnitStep::CompleteSeal),
        (DurableUnitProgress::SealWritten, DurableUnitStep::SyncSeal),
        (DurableUnitProgress::SealSynced, DurableUnitStep::VerifySealAndEof),
    ]
}

pub(super) fn marker_progress_steps() -> [(MarkerSlotProgress, MarkerSlotStep); 4] {
    [
        (MarkerSlotProgress::Missing, MarkerSlotStep::WriteInactiveSlot),
        (
            MarkerSlotProgress::ExactSlotPrefix,
            MarkerSlotStep::CompleteInactiveSlot,
        ),
        (MarkerSlotProgress::SlotWritten, MarkerSlotStep::SyncMarkerFile),
        (MarkerSlotProgress::SlotSynced, MarkerSlotStep::ReopenAndVerifySlot),
    ]
}

pub(super) fn initial_marker_progress_steps() -> [(InitialMarkerProgress, InitialMarkerStep); 5] {
    [
        (InitialMarkerProgress::Missing, InitialMarkerStep::WriteTemporary),
        (
            InitialMarkerProgress::TemporaryWritten,
            InitialMarkerStep::SyncTemporary,
        ),
        (
            InitialMarkerProgress::TemporarySynced,
            InitialMarkerStep::PublishFinalNoReplace,
        ),
        (
            InitialMarkerProgress::Published,
            InitialMarkerStep::SyncLifecycleDirectory,
        ),
        (
            InitialMarkerProgress::DirectorySynced,
            InitialMarkerStep::ReopenAndVerifyEntireFile,
        ),
    ]
}

pub(super) fn crash_boundaries() -> [BootstrapCrashBoundary; 22] {
    [
        BootstrapCrashBoundary::FrameAppend,
        BootstrapCrashBoundary::FrameSync,
        BootstrapCrashBoundary::AcknowledgementSlotWrite,
        BootstrapCrashBoundary::AcknowledgementSlotSync,
        BootstrapCrashBoundary::AcknowledgementSlotReread,
        BootstrapCrashBoundary::SealAppend,
        BootstrapCrashBoundary::SealSync,
        BootstrapCrashBoundary::SealReread,
        BootstrapCrashBoundary::EofVerification,
        BootstrapCrashBoundary::SnapshotWrite,
        BootstrapCrashBoundary::SnapshotSync,
        BootstrapCrashBoundary::SnapshotPublish,
        BootstrapCrashBoundary::SnapshotReopen,
        BootstrapCrashBoundary::InitialMarkerTemporaryWrite,
        BootstrapCrashBoundary::InitialMarkerTemporarySync,
        BootstrapCrashBoundary::InitialMarkerPublish,
        BootstrapCrashBoundary::InitialMarkerDirectorySync,
        BootstrapCrashBoundary::InitialMarkerReopen,
        BootstrapCrashBoundary::MarkerSlotWrite,
        BootstrapCrashBoundary::MarkerSync,
        BootstrapCrashBoundary::MarkerReread,
        BootstrapCrashBoundary::Reconciliation,
    ]
}

pub(super) fn ambiguities() -> [BootstrapAmbiguity; 7] {
    [
        BootstrapAmbiguity::UnexpectedArtifact,
        BootstrapAmbiguity::IdentityMismatch,
        BootstrapAmbiguity::NonDeterministicBytes,
        BootstrapAmbiguity::AcknowledgementOrSeal,
        BootstrapAmbiguity::Snapshot,
        BootstrapAmbiguity::InitialMarkerArtifact,
        BootstrapAmbiguity::Marker,
    ]
}

pub(super) const fn execute_unit(record: BootstrapRecord, step: DurableUnitStep) -> BootstrapDecision {
    BootstrapDecision::Execute(BootstrapAction::AdvanceUnit { record, step })
}

pub(super) fn store_uuid() -> StoreUuid {
    StoreUuid::new(std::array::from_fn(|index| index as u8)).expect("UUID is nonzero")
}
