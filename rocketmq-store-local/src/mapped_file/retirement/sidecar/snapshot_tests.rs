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
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::ContentFingerprint;
use crate::mapped_file::retirement::codec::QuarantineEntityKind;
use crate::mapped_file::retirement::codec::QuarantineReason;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::IdentityError;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;

const ENTRY_OFFSET: usize = SNAPSHOT_HEADER_LENGTH;
const ENTRY_PAYLOAD_OFFSET: usize = ENTRY_OFFSET + 8;

#[test]
fn incarnation_payload_rejects_every_flag_reserved_key_and_identity_corruption_class() {
    let encoded = encode_one(SnapshotEntry::Incarnation(incarnation_entry()));
    for (payload_offset, replacement, expected) in [
        (25, vec![2], "flags"),
        (26, vec![1], "reserved"),
        (60, vec![9], "key_kind"),
        (61, vec![1], "key_reserved"),
        (44, vec![0; 16], "nonce"),
        (94, vec![b'\\'], "path"),
    ] {
        let damaged = mutate_payload(&encoded, payload_offset, &replacement);
        let error = decode_snapshot(&damaged).expect_err("payload corruption must fail");
        let matches = match expected {
            "flags" => matches!(error, SidecarError::InvalidFlags { .. }),
            "reserved" => matches!(error, SidecarError::NonZeroReserved { .. }),
            "key_kind" => matches!(error, SidecarError::InvalidPhysicalFileKeyKind { kind: 9 }),
            "key_reserved" => matches!(error, SidecarError::NonZeroPhysicalFileKeyReserved),
            "nonce" => matches!(error, SidecarError::ZeroOpaqueIdentifier { field: "create_nonce" }),
            "path" => matches!(
                error,
                SidecarError::InvalidIdentity {
                    field: "canonical_path",
                    ..
                }
            ),
            _ => false,
        };
        assert!(matches, "class={expected}, error={error:?}");
    }

    let phase_without_absent_key = mutate_payload(&encoded, 24, &[1]);
    assert!(matches!(
        decode_snapshot(&phase_without_absent_key),
        Err(SidecarError::IncarnationPhaseKeyMismatch)
    ));
    let zero_length = mutate_payload(&encoded, 36, &[0; 8]);
    assert!(matches!(
        decode_snapshot(&zero_length),
        Err(SidecarError::ZeroExpectedFileLength)
    ));
}

#[test]
fn retirement_payload_rejects_enums_flags_sequences_nonce_key_and_optional_path_mismatch() {
    let encoded = encode_one(SnapshotEntry::RetirementTicket(retirement_entry()));
    for (payload_offset, replacement, expected) in [
        (32, vec![6], "stage"),
        (33, vec![8], "flags"),
        (34, vec![10, 0], "reason"),
        (36, vec![0; 8], "stage_sequence"),
        (44, vec![0; 8], "mapping_generation"),
        (60, vec![0; 8], "expected_length"),
        (68, vec![0; 16], "nonce"),
        (84, vec![9], "key"),
        (33, vec![6], "tombstone_path"),
    ] {
        let damaged = mutate_payload(&encoded, payload_offset, &replacement);
        let error = decode_snapshot(&damaged).expect_err("payload corruption must fail");
        let matches = match expected {
            "stage" => matches!(
                error,
                SidecarError::InvalidEnumValue {
                    field: "retirement_stage",
                    value: 6
                }
            ),
            "flags" => matches!(error, SidecarError::InvalidFlags { .. }),
            "reason" => matches!(
                error,
                SidecarError::InvalidEnumValue {
                    field: "retirement_reason",
                    value: 10
                }
            ),
            "stage_sequence" => matches!(error, SidecarError::StageSequenceOutOfRange { .. }),
            "mapping_generation" => matches!(error, SidecarError::ZeroMappingGeneration),
            "expected_length" => matches!(error, SidecarError::ZeroExpectedFileLength),
            "nonce" => matches!(
                error,
                SidecarError::ZeroOpaqueIdentifier {
                    field: "retirement_nonce"
                }
            ),
            "key" => matches!(error, SidecarError::InvalidPhysicalFileKeyKind { kind: 9 }),
            "tombstone_path" => matches!(
                error,
                SidecarError::OptionalPathFlagMismatch {
                    field: "tombstone_path"
                }
            ),
            _ => false,
        };
        assert!(matches, "class={expected}, error={error:?}");
    }
}

#[test]
fn quarantine_payload_rejects_enums_flags_presence_reserved_and_observation_sequence() {
    let encoded = encode_one(SnapshotEntry::Quarantine(quarantine_entry()));
    for (payload_offset, replacement, expected) in [
        (0, vec![5], "entity_kind"),
        (1, vec![5], "reason"),
        (2, vec![8, 0], "flags"),
        (4, vec![0; 8], "sequence"),
        (2, vec![6, 0], "key_presence"),
        (2, vec![5, 0], "content_presence"),
        (2, vec![3, 0], "destination_presence"),
        (56, vec![1], "reserved"),
    ] {
        let damaged = mutate_payload(&encoded, payload_offset, &replacement);
        let error = decode_snapshot(&damaged).expect_err("payload corruption must fail");
        let matches = match expected {
            "entity_kind" => matches!(
                error,
                SidecarError::InvalidEnumValue {
                    field: "quarantine_entity_kind",
                    value: 5
                }
            ),
            "reason" => matches!(
                error,
                SidecarError::InvalidEnumValue {
                    field: "quarantine_reason",
                    value: 5
                }
            ),
            "flags" | "content_presence" => {
                matches!(error, SidecarError::InvalidQuarantineFields { .. })
            }
            "sequence" => matches!(error, SidecarError::ObservationSequenceOutOfRange { .. }),
            "key_presence" => matches!(error, SidecarError::InvalidAbsentPhysicalFileKey),
            "destination_presence" => matches!(
                error,
                SidecarError::OptionalPathFlagMismatch {
                    field: "destination_path"
                }
            ),
            "reserved" => matches!(error, SidecarError::NonZeroReserved { .. }),
            _ => false,
        };
        assert!(matches, "class={expected}, error={error:?}");
    }
}

#[test]
fn snapshot_decoder_enforces_each_kind_specific_payload_max_before_truncation() {
    for (entry, maximum) in [
        (SnapshotEntry::Incarnation(incarnation_entry()), 8_288_usize),
        (SnapshotEntry::RetirementTicket(retirement_entry()), 8_312_usize),
        (SnapshotEntry::Quarantine(quarantine_entry()), 8_256_usize),
    ] {
        let mut encoded = encode_one(entry);
        let kind = u16::from_le_bytes(encoded[ENTRY_OFFSET..ENTRY_OFFSET + 2].try_into().unwrap());
        encoded[ENTRY_OFFSET + 4..ENTRY_OFFSET + 8].copy_from_slice(&u32::try_from(maximum + 1).unwrap().to_le_bytes());
        rewrite_body_crc(&mut encoded);
        assert!(matches!(
            decode_snapshot(&encoded),
            Err(SidecarError::SnapshotEntryPayloadTooLarge {
                kind: actual_kind,
                length,
                maximum: actual_maximum,
            }) if actual_kind == kind && length == maximum + 1 && actual_maximum == maximum
        ));
    }
}

#[test]
fn snapshot_modes_and_retirement_tombstone_stage_rules_fail_closed() {
    let mut bootstrap = snapshot_with(SnapshotEntry::Incarnation(incarnation_entry()));
    bootstrap.mode = SnapshotMode::BootstrapInventory;
    assert!(matches!(
        encode_snapshot(&bootstrap),
        Err(SidecarError::SnapshotModeGenerationMismatch {
            mode: "bootstrap_inventory",
            generation: 1,
        })
    ));

    let mut retirement = retirement_entry();
    retirement.stage = RetirementStage::LogicalRemoved;
    assert!(matches!(
        encode_snapshot(&snapshot_with(SnapshotEntry::RetirementTicket(retirement))),
        Err(SidecarError::RetirementTombstoneStageMismatch)
    ));

    let mut direct_absence = retirement_entry();
    direct_absence.stage = RetirementStage::NamespaceAbsent;
    direct_absence.tombstone_path = None;
    let snapshot = snapshot_with(SnapshotEntry::RetirementTicket(direct_absence));
    let encoded = encode_snapshot(&snapshot).expect("direct-unlink stage four is valid");
    assert_eq!(decode_snapshot(&encoded), Ok(snapshot));
}

#[test]
fn snapshot_paths_are_bound_to_offsets_ids_generations_nonces_and_parent_directory() {
    let mut wrong_canonical = incarnation_entry();
    wrong_canonical.segment_offset = 1;
    assert!(matches!(
        encode_snapshot(&snapshot_with(SnapshotEntry::Incarnation(wrong_canonical))),
        Err(SidecarError::InvalidIdentity {
            field: "canonical_path",
            source: IdentityError::CanonicalSegmentPathIdentityMismatch,
        })
    ));

    let mut wrong_create = incarnation_entry();
    wrong_create.create_file_path =
        path("consumequeue/.create.i0000000000000007.s00000000000000000000.n01010101010101010101010101010101");
    assert!(matches!(
        encode_snapshot(&snapshot_with(SnapshotEntry::Incarnation(wrong_create))),
        Err(SidecarError::InvalidIdentity {
            field: "create_file_path",
            source: IdentityError::CreateFilePathIdentityMismatch,
        })
    ));

    let mut wrong_tombstone = retirement_entry();
    wrong_tombstone.tombstone_path = Some(path(
        "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000004.n02020202020202020202020202020202",
    ));
    assert!(matches!(
        encode_snapshot(&snapshot_with(SnapshotEntry::RetirementTicket(wrong_tombstone))),
        Err(SidecarError::InvalidIdentity {
            field: "tombstone_path",
            source: IdentityError::TombstonePathIdentityMismatch,
        })
    ));

    let encoded_incarnation = encode_one(SnapshotEntry::Incarnation(incarnation_entry()));
    let corrupted_canonical = mutate_payload(&encoded_incarnation, 94 + 29, b"1");
    assert!(matches!(
        decode_snapshot(&corrupted_canonical),
        Err(SidecarError::InvalidIdentity {
            field: "canonical_path",
            source: IdentityError::CanonicalSegmentPathIdentityMismatch,
        })
    ));

    let encoded_retirement = encode_one(SnapshotEntry::RetirementTicket(retirement_entry()));
    let tombstone_length = retirement_entry()
        .tombstone_path
        .as_ref()
        .expect("test retirement has a tombstone")
        .as_bytes()
        .len();
    let corrupted_tombstone = mutate_payload(&encoded_retirement, 150 + tombstone_length - 1, b"3");
    assert!(matches!(
        decode_snapshot(&corrupted_tombstone),
        Err(SidecarError::InvalidIdentity {
            field: "tombstone_path",
            source: IdentityError::TombstonePathIdentityMismatch,
        })
    ));
}

fn encode_one(entry: SnapshotEntry) -> Vec<u8> {
    encode_snapshot(&snapshot_with(entry)).expect("valid one-entry snapshot encodes")
}

fn snapshot_with(entry: SnapshotEntry) -> LifecycleSnapshot {
    let (create_high_water, ticket_high_water) = match &entry {
        SnapshotEntry::Incarnation(entry) => (entry.incarnation.create_seq(), 0),
        SnapshotEntry::RetirementTicket(entry) => (entry.incarnation.create_seq(), entry.ticket_id.get()),
        SnapshotEntry::Quarantine(_) => (0, 0),
    };
    LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 100,
        create_high_water,
        ticket_high_water,
        entries: vec![entry],
    }
}

fn incarnation_entry() -> IncarnationSnapshotEntry {
    IncarnationSnapshotEntry {
        incarnation: incarnation(),
        phase: IncarnationPhase::Published,
        segment_offset: 0,
        expected_file_length: 1_024,
        create_nonce: [1; 16],
        physical_key: Some(key()),
        canonical_path: path("commitlog/00000000000000000000"),
        create_file_path: path(
            "commitlog/.create.i0000000000000007.s00000000000000000000.n01010101010101010101010101010101",
        ),
    }
}

fn retirement_entry() -> RetirementTicketSnapshotEntry {
    RetirementTicketSnapshotEntry {
        ticket_id: TicketId::new(42).expect("ticket is non-zero"),
        incarnation: incarnation(),
        stage: RetirementStage::CompletedRetained,
        superseded_path_observed: true,
        quarantined: true,
        reason: RetirementReason::TtlExpired,
        stage_sequence: 99,
        mapping_generation: 3,
        segment_offset: 0,
        expected_file_length: 1_024,
        retirement_nonce: [2; 16],
        target_key: key(),
        canonical_path: path("commitlog/00000000000000000000"),
        tombstone_path: Some(path(
            "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n02020202020202020202020202020202",
        )),
    }
}

fn quarantine_entry() -> QuarantineSnapshotEntry {
    QuarantineSnapshotEntry {
        entity_kind: QuarantineEntityKind::Sidecar,
        reason: QuarantineReason::UnknownOwner,
        sequence_at_observation: 88,
        physical_key: Some(key()),
        content_fingerprint: Some(ContentFingerprint {
            length: 1_024,
            crc32: 0x1234_5678,
        }),
        source_path: path(".rocketmq-lifecycle/orphan.tmp"),
        destination_path: Some(path(".rocketmq-lifecycle/quarantine/orphan.bin")),
    }
}

fn mutate_payload(snapshot: &[u8], payload_offset: usize, replacement: &[u8]) -> Vec<u8> {
    let mut output = snapshot.to_vec();
    let start = ENTRY_PAYLOAD_OFFSET + payload_offset;
    output[start..start + replacement.len()].copy_from_slice(replacement);
    rewrite_entry_crc(&mut output);
    rewrite_body_crc(&mut output);
    output
}

fn rewrite_entry_crc(snapshot: &mut [u8]) {
    let payload_length = u32::from_le_bytes(
        snapshot[ENTRY_OFFSET + 4..ENTRY_OFFSET + 8]
            .try_into()
            .expect("payload length field"),
    ) as usize;
    let crc_offset = ENTRY_PAYLOAD_OFFSET + payload_length;
    let checksum = crc32(&snapshot[ENTRY_OFFSET..crc_offset]);
    snapshot[crc_offset..crc_offset + 4].copy_from_slice(&checksum.to_le_bytes());
}

fn rewrite_body_crc(snapshot: &mut [u8]) {
    let body_length = u64::from_le_bytes(snapshot[88..96].try_into().expect("body length field")) as usize;
    let body_end = SNAPSHOT_HEADER_LENGTH + body_length;
    let checksum = crc32(&snapshot[SNAPSHOT_HEADER_LENGTH..body_end]);
    snapshot[body_end..body_end + 4].copy_from_slice(&checksum.to_le_bytes());
}

fn store_uuid() -> StoreUuid {
    StoreUuid::new([1; 16]).expect("UUID is non-zero")
}

fn incarnation() -> FileIncarnationId {
    FileIncarnationId::new(store_uuid(), 7).expect("incarnation is non-zero")
}

fn key() -> PhysicalFileKey {
    PhysicalFileKey::unix(11, 22)
}

fn path(value: &str) -> StoreRelativePath {
    StoreRelativePath::new(value).expect("path is canonical")
}
