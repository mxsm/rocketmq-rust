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

use crate::mapped_file::retirement::codec::ContentFingerprint;
use crate::mapped_file::retirement::codec::GenerationAbortReason;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::OpenReason;
use crate::mapped_file::retirement::codec::QuarantineEntityKind;
use crate::mapped_file::retirement::codec::QuarantineReason;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;
use crate::mapped_file::retirement::sidecar::EnabledMarkerSlot;
use crate::mapped_file::retirement::sidecar::IncarnationPhase;
use crate::mapped_file::retirement::sidecar::IncarnationSnapshotEntry;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::QuarantineSnapshotEntry;
use crate::mapped_file::retirement::sidecar::RetirementStage;
use crate::mapped_file::retirement::sidecar::RetirementTicketSnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::sidecar::StoreMeta;

pub(super) fn sample_records() -> Vec<(&'static str, LedgerRecord, u64, u64)> {
    record_map()
        .into_iter()
        .enumerate()
        .map(|(index, (name, record))| match name {
            "store-initialized" => (name, record, 1, 0),
            "bootstrap-installed" => (name, record, 2, 0),
            "log-opened" => (name, record, 21, 3),
            "generation-prepared" => (name, record, 10, 2),
            "generation-aborted" => (name, record, 11, 2),
            "marker-committed" => (name, record, 3, 0),
            _ => (name, record, 100 + index as u64, 3),
        })
        .collect()
}

pub(super) fn record_map() -> Vec<(&'static str, LedgerRecord)> {
    let canonical_path = canonical_path();
    let create_file_path = create_path();
    let tombstone_path = tombstone_path();
    let unix_key = unix_key();
    let windows_key = windows_key();
    vec![
        (
            "store-initialized",
            LedgerRecord::StoreInitialized {
                store_uuid: sample_store_uuid(),
                bootstrap_id: bootstrap_id(),
                creation_time_ns: 11,
            },
        ),
        (
            "bootstrap-installed",
            LedgerRecord::BootstrapInstalled {
                store_uuid: sample_store_uuid(),
                bootstrap_id: bootstrap_id(),
                snapshot_generation: 0,
                snapshot_base_sequence: 1,
                snapshot_file_length: 108,
                snapshot_file_crc32: 0x1234_5678,
                inventory_count: 3,
                create_high_water: 7,
                ticket_high_water: 42,
            },
        ),
        (
            "log-opened",
            LedgerRecord::LogOpened {
                store_uuid: sample_store_uuid(),
                generation: 3,
                snapshot_generation: 3,
                predecessor_log_generation: 2,
                predecessor_terminal_acknowledged_sequence: 20,
                snapshot_base_sequence: 20,
                snapshot_file_length: 108,
                snapshot_file_crc32: 0x1111_1111,
                predecessor_prefix_crc32: 0x2222_2222,
                validated_prefix_length: 1_000,
                unacknowledged_suffix_length: 0,
                unacknowledged_suffix_crc32: 0,
                open_reason: OpenReason::Compaction,
                predecessor_acknowledgement_epoch: 9,
            },
        ),
        (
            "generation-prepared",
            LedgerRecord::GenerationPrepared {
                store_uuid: sample_store_uuid(),
                source_generation: 2,
                target_generation: 3,
                target_snapshot_generation: 3,
                open_reason: OpenReason::Compaction,
            },
        ),
        (
            "generation-aborted",
            LedgerRecord::GenerationAborted {
                store_uuid: sample_store_uuid(),
                source_generation: 2,
                target_generation: 3,
                prepared_sequence: 10,
                abort_reason: GenerationAbortReason::Io,
            },
        ),
        (
            "marker-committed",
            LedgerRecord::MarkerCommitted {
                store_uuid: sample_store_uuid(),
                marker_epoch: 1,
                snapshot_generation: 0,
                log_generation: 0,
                anchor_sequence: 2,
                slot_index: 0,
                slot_crc32: 0x3344_5566,
            },
        ),
        (
            "allocate-incarnation",
            LedgerRecord::AllocateIncarnation {
                incarnation: incarnation(),
                segment_offset: 0,
                expected_length: 1_024,
                create_nonce: [0x20; 16],
                canonical_path: canonical_path.clone(),
                create_file_path: create_file_path.clone(),
            },
        ),
        (
            "bind-incarnation-unix",
            LedgerRecord::BindIncarnation {
                incarnation: incarnation(),
                expected_length: 1_024,
                physical_key: unix_key,
                canonical_path: canonical_path.clone(),
                create_file_path: create_file_path.clone(),
            },
        ),
        (
            "publish-incarnation-windows",
            LedgerRecord::PublishIncarnation {
                incarnation: incarnation(),
                expected_length: 1_024,
                physical_key: windows_key,
                canonical_path: canonical_path.clone(),
                create_file_path,
            },
        ),
        (
            "retirement-intent",
            LedgerRecord::RetirementIntent {
                ticket_id: ticket(),
                incarnation: incarnation(),
                reason: RetirementReason::TtlExpired,
                mapping_generation: 3,
                segment_offset: 0,
                expected_length: 1_024,
                retirement_nonce: [0x40; 16],
                target_key: unix_key,
                canonical_path: canonical_path.clone(),
            },
        ),
        (
            "logical-removed",
            LedgerRecord::LogicalRemoved {
                ticket_id: ticket(),
                incarnation: incarnation(),
                target_key: unix_key,
                canonical_path: canonical_path.clone(),
            },
        ),
        (
            "tombstoned",
            LedgerRecord::Tombstoned {
                ticket_id: ticket(),
                incarnation: incarnation(),
                target_key: unix_key,
                retirement_nonce: [0x40; 16],
                canonical_path: canonical_path.clone(),
                tombstone_path: tombstone_path.clone(),
            },
        ),
        (
            "namespace-absent",
            LedgerRecord::NamespaceAbsent {
                ticket_id: ticket(),
                incarnation: incarnation(),
                replacement_observed: true,
                observation_time_ns: 55,
                target_key: unix_key,
                canonical_path: canonical_path.clone(),
                tombstone_path: Some(tombstone_path.clone()),
            },
        ),
        (
            "completed",
            LedgerRecord::Completed {
                ticket_id: ticket(),
                incarnation: incarnation(),
                completion_time_ns: 66,
                namespace_absent_sequence: 99,
            },
        ),
        (
            "superseded-path",
            LedgerRecord::SupersededPath {
                ticket_id: ticket(),
                incarnation: incarnation(),
                expected_target_key: unix_key,
                observed_replacement_key: windows_key,
                canonical_path,
            },
        ),
        (
            "quarantined",
            LedgerRecord::Quarantined {
                entity_kind: QuarantineEntityKind::Tombstone,
                reason: QuarantineReason::KeyMismatch,
                sequence_at_observation: 77,
                physical_key: Some(windows_key),
                content_fingerprint: Some(ContentFingerprint {
                    length: 1_024,
                    crc32: 0x1234_5678,
                }),
                source_path: tombstone_path,
                destination_path: Some(path(".rocketmq-lifecycle/quarantine/tombstone.bin")),
            },
        ),
    ]
}

pub(super) fn sample_store_meta() -> StoreMeta {
    StoreMeta {
        store_uuid: sample_store_uuid(),
        creation_time_ns: 0x0102_0304_0506_0708,
        bootstrap_id: bootstrap_id(),
    }
}

pub(super) fn marker_slot(
    slot_index: u8,
    marker_epoch: u64,
    generation: u64,
    anchor_sequence: u64,
    snapshot_file_length: u64,
    snapshot_file_crc32: u32,
    anchor_frame_crc32: u32,
) -> EnabledMarkerSlot {
    EnabledMarkerSlot {
        slot_index,
        store_uuid: sample_store_uuid(),
        bootstrap_id: bootstrap_id(),
        marker_epoch,
        snapshot_generation: generation,
        log_generation: generation,
        anchor_sequence,
        snapshot_file_length,
        snapshot_file_crc32,
        anchor_frame_crc32,
    }
}

pub(super) fn snapshot(mode: SnapshotMode, entries: Vec<SnapshotEntry>) -> LifecycleSnapshot {
    let (generation, predecessor_log_generation, base_sequence) = match mode {
        SnapshotMode::BootstrapInventory => (0, u64::MAX, 1),
        SnapshotMode::OrdinaryCompaction => (1, 0, 100),
        SnapshotMode::TailRepair => (2, 1, 100),
    };
    LifecycleSnapshot {
        mode,
        store_uuid: sample_store_uuid(),
        generation,
        log_generation: generation,
        predecessor_log_generation,
        base_sequence,
        create_high_water: 7,
        ticket_high_water: 42,
        entries,
    }
}

pub(super) fn sample_incarnation_entry() -> IncarnationSnapshotEntry {
    IncarnationSnapshotEntry {
        incarnation: incarnation(),
        phase: IncarnationPhase::Published,
        segment_offset: 0,
        expected_file_length: 1_024,
        create_nonce: [0x20; 16],
        physical_key: Some(unix_key()),
        canonical_path: canonical_path(),
        create_file_path: create_path(),
    }
}

pub(super) fn sample_retirement_entry(stage: RetirementStage) -> RetirementTicketSnapshotEntry {
    RetirementTicketSnapshotEntry {
        ticket_id: ticket(),
        incarnation: incarnation(),
        stage,
        superseded_path_observed: true,
        quarantined: false,
        reason: RetirementReason::TtlExpired,
        stage_sequence: 99,
        mapping_generation: 3,
        segment_offset: 0,
        expected_file_length: 1_024,
        retirement_nonce: [0x40; 16],
        target_key: unix_key(),
        canonical_path: canonical_path(),
        tombstone_path: match stage {
            RetirementStage::Tombstoned | RetirementStage::NamespaceAbsent | RetirementStage::CompletedRetained => {
                Some(tombstone_path())
            }
            RetirementStage::IntentDurable | RetirementStage::LogicalRemoved => None,
        },
    }
}

pub(super) fn sample_quarantine_entry() -> QuarantineSnapshotEntry {
    QuarantineSnapshotEntry {
        entity_kind: QuarantineEntityKind::Sidecar,
        reason: QuarantineReason::UnknownOwner,
        sequence_at_observation: 88,
        physical_key: Some(windows_key()),
        content_fingerprint: Some(ContentFingerprint {
            length: 1_234,
            crc32: 0xaabb_ccdd,
        }),
        source_path: path(".rocketmq-lifecycle/orphan.tmp"),
        destination_path: Some(path(".rocketmq-lifecycle/quarantine/orphan.bin")),
    }
}

pub(super) fn sample_store_uuid() -> StoreUuid {
    StoreUuid::new(std::array::from_fn(|index| index as u8)).expect("sample UUID is nonzero")
}

pub(super) fn bootstrap_id() -> [u8; 16] {
    std::array::from_fn(|index| 0x10 + index as u8)
}

pub(super) fn incarnation() -> FileIncarnationId {
    FileIncarnationId::new(sample_store_uuid(), 7).expect("sample incarnation is nonzero")
}

pub(super) fn ticket() -> TicketId {
    TicketId::new(42).expect("sample ticket is nonzero")
}

pub(super) fn unix_key() -> PhysicalFileKey {
    PhysicalFileKey::unix(0x0102_0304_0506_0708, 0x1112_1314_1516_1718)
}

pub(super) fn windows_key() -> PhysicalFileKey {
    PhysicalFileKey::windows(0x2122_2324_2526_2728, std::array::from_fn(|index| 0x30 + index as u8))
}

pub(super) fn path(value: &str) -> StoreRelativePath {
    StoreRelativePath::new(value).expect("fixture path is canonical")
}

pub(super) fn canonical_path() -> StoreRelativePath {
    path("commitlog/00000000000000000000")
}

pub(super) fn create_path() -> StoreRelativePath {
    path("commitlog/.create.i0000000000000007.s00000000000000000000.n20202020202020202020202020202020")
}

pub(super) fn tombstone_path() -> StoreRelativePath {
    path(
        "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n40404040404040404040404040404040",
    )
}

pub(super) fn maximum_path() -> StoreRelativePath {
    let raw = std::iter::repeat_n("a".repeat(240), 17).collect::<Vec<_>>().join("/");
    StoreRelativePath::new(&raw).expect("4096-byte path is valid")
}
