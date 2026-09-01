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
use crate::mapped_file::retirement::codec::encode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::encode_commit_seal;
use crate::mapped_file::retirement::codec::encode_ledger_frame;
use crate::mapped_file::retirement::codec::CodecViolation;
use crate::mapped_file::retirement::codec::CommitSeal;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_slot;
use crate::mapped_file::retirement::sidecar::encode_snapshot;
use crate::mapped_file::retirement::sidecar::EnabledMarkerSlot;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::SnapshotMode;

#[test]
fn clean_bootstrap_chain_recovers_only_to_needs_reconciliation() {
    let fixture = BootstrapFixture::new();
    let decision = replay(fixture.input()).expect("valid sealed bootstrap chain replays");

    let RecoveryDecision::NeedsReconciliation(needs) = decision else {
        panic!("M3.2 must stop at namespace reconciliation")
    };
    assert_eq!(needs.recovered().generation(), 0);
    assert_eq!(needs.recovered().last_sequence(), 3);
    assert_eq!(needs.recovered().acknowledgement_epoch(), 3);
    assert_eq!(needs.recovered().marker_epoch(), 1);
    let frontier = needs.writer_frontier();
    assert_eq!(frontier.bootstrap_id(), fixture.meta.bootstrap_id);
    assert_eq!(frontier.log_generation(), 0);
    assert_eq!(frontier.next_sequence(), 4);
    assert_eq!(frontier.next_acknowledgement_epoch(), 4);
    assert_eq!(frontier.sealed_log_length(), fixture.evidence[2].sealed_log_length);
    assert_eq!(frontier.marker_epoch(), 1);
}

#[test]
fn all_zero_acknowledgement_slots_fail_when_marker_is_present() {
    let fixture = BootstrapFixture::new();
    let zero = [0_u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH];
    let mut input = fixture.input();
    input.acknowledgement_slots = [&zero, &zero];

    assert_eq!(replay(input), Err(ReplayViolation::NoAcknowledgedFrame));
}

#[test]
fn replay_limits_fail_before_unbounded_generation_or_unit_collection() {
    let fixture = BootstrapFixture::new();
    let mut generation_limited = fixture.input();
    generation_limited.limits.max_generations = 0;
    assert_eq!(
        replay(generation_limited),
        Err(ReplayViolation::LimitExceeded {
            limit: "generations",
            actual: 1,
            maximum: 0,
        })
    );

    let mut unit_limited = fixture.input();
    unit_limited.limits.max_sealed_units = 2;
    assert_eq!(
        replay(unit_limited),
        Err(ReplayViolation::LimitExceeded {
            limit: "sealed_units",
            actual: 3,
            maximum: 2,
        })
    );
}

#[test]
fn torn_slot_reconstructs_only_from_one_adjacent_highest_seal() {
    let fixture = BootstrapFixture::new();
    let mut torn = fixture.slot0;
    torn[88] ^= 1;
    let resolved = resolve_acknowledgement([&torn, &fixture.slot1], &fixture.evidence)
        .expect("epoch 3 seal uniquely reconstructs torn physical slot 0");

    assert_eq!(resolved.authoritative.acknowledgement_epoch, 3);
    assert_eq!(resolved.authoritative.slot_index, 0);
    assert_eq!(resolved.encoded_authoritative, fixture.slot0);
}

#[test]
fn torn_slot_without_candidate_and_with_multiple_candidates_fails_closed() {
    let fixture = BootstrapFixture::new();
    let mut torn = fixture.slot0;
    torn[88] ^= 1;

    assert_eq!(
        resolve_acknowledgement([&torn, &fixture.slot1], &fixture.evidence[1..2]),
        Err(ReplayViolation::UnreconstructableAcknowledgementSlot { slot_index: 0 })
    );

    let mut ambiguous = fixture.evidence.clone();
    ambiguous.push(fixture.evidence[2].clone());
    assert_eq!(
        resolve_acknowledgement([&torn, &fixture.slot1], &ambiguous),
        Err(ReplayViolation::AmbiguousAcknowledgementSlot {
            slot_index: 0,
            candidates: 2,
        })
    );
}

#[test]
fn torn_slot_can_reconstruct_the_unique_older_adjacent_epoch() {
    let fixture = BootstrapFixture::new();
    let mut torn_old = fixture.evidence[0].encoded_slot;
    torn_old[88] ^= 1;

    let resolved = resolve_acknowledgement([&torn_old, &fixture.evidence[1].encoded_slot], &fixture.evidence[..2])
        .expect("the unique epoch-1 seal completes the highest epoch-1/2 chain");
    assert_eq!(resolved.authoritative.acknowledgement_epoch, 2);
    assert_eq!(resolved.authoritative.slot_index, 1);
}

#[test]
fn every_populated_acknowledgement_slot_requires_one_exact_seal() {
    let fixture = BootstrapFixture::new();
    assert_eq!(
        resolve_acknowledgement([&fixture.slot0, &fixture.slot1], &fixture.evidence[2..]),
        Err(ReplayViolation::MissingAcknowledgementSealEvidence { slot_index: 1 })
    );

    let mut duplicate = fixture.evidence.clone();
    duplicate.push(fixture.evidence[1].clone());
    assert_eq!(
        resolve_acknowledgement([&fixture.slot0, &fixture.slot1], &duplicate),
        Err(ReplayViolation::AmbiguousAcknowledgementSealEvidence {
            slot_index: 1,
            candidates: 2,
        })
    );
}

#[test]
fn acknowledgement_sequence_or_epoch_break_fails_closed() {
    let mut fixture = BootstrapFixture::new();
    fixture.log = fixture.log_with_epoch_override(1, 3);
    let input = fixture.input();

    assert!(matches!(
        replay(input),
        Err(ReplayViolation::BrokenSealChain { generation: 0 })
            | Err(ReplayViolation::InvalidLog { generation: 0, .. })
    ));
}

#[test]
fn partial_acknowledged_seal_returns_data_only_completion_decision() {
    let mut fixture = BootstrapFixture::new();
    fixture.log.truncate(fixture.log.len() - 17);
    let decision = replay(fixture.input()).expect("deterministic partial seal is recoverable");

    let RecoveryDecision::CompleteSeal(plan) = decision else {
        panic!("partial final seal must produce a completion plan")
    };
    assert_eq!(plan.generation(), 0);
    assert_eq!(plan.frame_sequence(), 3);
    assert_eq!(
        plan.available_seal_bytes(),
        super::super::codec::COMMIT_SEAL_LENGTH - 17
    );
    assert_eq!(plan.expected_seal(), &fixture.seals[2]);
}

#[test]
fn selected_marker_with_anchor_only_returns_witness_completion_data() {
    let mut fixture = BootstrapFixture::new();
    fixture.log.truncate(fixture.evidence[1].sealed_log_length as usize);
    fixture.slot0 = fixture.evidence[0].encoded_slot;
    fixture.slot1 = fixture.evidence[1].encoded_slot;
    let expected_frame = fixture.frames[2].clone();
    fixture.log.extend_from_slice(&expected_frame[..17]);

    let decision = replay(fixture.input()).expect("exact witness prefix is resumable");
    let RecoveryDecision::CompleteMarkerWitness(plan) = decision else {
        panic!("anchor-only marker selection must not enter normal replay")
    };
    assert_eq!(plan.generation(), 0);
    assert_eq!(plan.anchor_sequence(), 2);
    assert_eq!(plan.available_frame_bytes(), 17);
    assert_eq!(plan.expected_frame(), expected_frame.as_slice());
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(plan.expected_acknowledgement_slot()).expect("planned witness slot decodes")
    else {
        panic!("planned witness slot is populated")
    };
    assert_eq!(slot.acknowledgement_epoch, 3);
    assert!(slot.activated);
    assert_eq!(slot.marker_epoch, 1);
    assert_eq!(plan.expected_seal().len(), super::super::codec::COMMIT_SEAL_LENGTH);
}

#[test]
fn valid_unacknowledged_tail_returns_a_bounded_tail_repair_plan() {
    let mut fixture = BootstrapFixture::new();
    fixture.log.extend_from_slice(b"RMLC");

    let decision = replay(fixture.input()).expect("valid partial frame prefix is classified");
    let RecoveryDecision::TailRepair(plan) = decision else {
        panic!("unacknowledged bytes after the sealed watermark require tail repair")
    };
    assert_eq!(plan.generation(), 0);
    assert_eq!(plan.acknowledged_prefix_length(), fixture.evidence[2].sealed_log_length);
    assert_eq!(plan.suffix_length(), 4);
    assert_eq!(plan.suffix_crc32(), crc32(b"RMLC"));
}

#[test]
fn complete_invalid_seal_after_old_watermark_is_corruption_not_tail_repair() {
    let mut fixture = BootstrapFixture::new();
    let frame = encode_ledger_frame(
        &LedgerRecord::GenerationPrepared {
            store_uuid: fixture.meta.store_uuid,
            source_generation: 0,
            target_generation: 1,
            target_snapshot_generation: 1,
            open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
        },
        4,
        0,
    )
    .expect("generation preparation frame encodes");
    fixture.log.extend_from_slice(&frame);
    fixture
        .log
        .extend_from_slice(&[0; super::super::codec::COMMIT_SEAL_LENGTH]);

    assert!(matches!(
        replay(fixture.input()),
        Err(ReplayViolation::InvalidLog {
            generation: 0,
            source: CodecViolation::InvalidCommitSealMagic { .. },
            ..
        })
    ));
}

#[test]
fn every_sealed_unit_must_obey_activation_phase() {
    let fixture = BootstrapFixture::new();
    let generation = fixture.input().generations[0];
    let mut evidence = Vec::new();
    let parsed_generation =
        parsing::parse_generation(generation, &fixture.meta, ReplayLimits::default(), &mut evidence)
            .expect("fixture parses");
    evidence[1].slot.activated = true;
    evidence[1].slot.marker_epoch = 1;
    let parsed = BTreeMap::from([(0, parsed_generation)]);

    assert_eq!(
        validation::validate_all_sealed_units(&parsed, &evidence),
        Err(ReplayViolation::BrokenSealChain { generation: 0 })
    );
}

#[test]
fn terminal_generation_prepared_returns_resume_data_without_publication_authority() {
    let mut fixture = BootstrapFixture::new();
    fixture.frames.push(
        encode_ledger_frame(
            &LedgerRecord::GenerationPrepared {
                store_uuid: fixture.meta.store_uuid,
                source_generation: 0,
                target_generation: 1,
                target_snapshot_generation: 1,
                open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
            },
            4,
            0,
        )
        .expect("generation preparation frame encodes"),
    );
    fixture.rebuild(&[1, 2, 3, 4]);

    let decision = replay(fixture.input()).expect("terminal preparation is classified");
    let RecoveryDecision::ResumeGeneration(plan) = decision else {
        panic!("unmatched preparation must remain fenced")
    };
    assert_eq!(plan.source_generation(), 0);
    assert_eq!(plan.target_generation(), 1);
}

#[test]
fn invalid_generation_prepared_never_reaches_resume_decision() {
    for (target_snapshot_generation, open_reason) in [
        (2_u64, crate::mapped_file::retirement::codec::OpenReason::Compaction),
        (1_u64, crate::mapped_file::retirement::codec::OpenReason::TailRepair),
    ] {
        let mut fixture = BootstrapFixture::new();
        let mut frame = encode_ledger_frame(
            &LedgerRecord::GenerationPrepared {
                store_uuid: fixture.meta.store_uuid,
                source_generation: 0,
                target_generation: 1,
                target_snapshot_generation: 1,
                open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
            },
            4,
            0,
        )
        .expect("valid preparation fixture encodes");
        let payload_offset = super::super::codec::MIN_HEADER_LENGTH;
        frame[payload_offset + 32..payload_offset + 40].copy_from_slice(&target_snapshot_generation.to_le_bytes());
        frame[payload_offset + 48] = match open_reason {
            crate::mapped_file::retirement::codec::OpenReason::Compaction => 0,
            crate::mapped_file::retirement::codec::OpenReason::TailRepair => 1,
        };
        let payload_end = frame.len() - 4;
        let payload_crc32 = crc32(&frame[payload_offset..payload_end]);
        frame[payload_end..].copy_from_slice(&payload_crc32.to_le_bytes());
        fixture.frames.push(frame);
        fixture.rebuild_without_evidence(&[1, 2, 3, 4]);

        let result = replay(fixture.input());
        match (open_reason, result) {
            (
                crate::mapped_file::retirement::codec::OpenReason::Compaction,
                Err(ReplayViolation::InvalidLog {
                    source: CodecViolation::InvalidGenerationRelationship { .. },
                    ..
                }),
            )
            | (
                crate::mapped_file::retirement::codec::OpenReason::TailRepair,
                Err(ReplayViolation::InvalidLog {
                    source: CodecViolation::InvalidEnvelopeRelationship { .. },
                    ..
                }),
            ) => {}
            (_, other) => panic!("unexpected replay result: {other:?}"),
        }
    }
}

#[test]
fn early_tail_decision_cannot_bypass_semantically_invalid_sealed_record() {
    let mut fixture = BootstrapFixture::new();
    fixture.frames.push(
        encode_ledger_frame(
            &LedgerRecord::GenerationAborted {
                store_uuid: fixture.meta.store_uuid,
                source_generation: 0,
                target_generation: 1,
                prepared_sequence: 3,
                abort_reason: crate::mapped_file::retirement::codec::GenerationAbortReason::Validation,
            },
            4,
            0,
        )
        .expect("structurally encodable abort fixture"),
    );
    fixture.rebuild(&[1, 2, 3, 4]);
    fixture.log.extend_from_slice(b"RMLC");

    assert_eq!(
        replay(fixture.input()),
        Err(ReplayViolation::State(StateViolation::IllegalGenerationAdministration))
    );
}

#[test]
fn newer_selected_marker_with_predecessor_ack_returns_anchor_acknowledgement_data() {
    let mut fixture = BootstrapFixture::new();
    fixture.frames.push(
        encode_ledger_frame(
            &LedgerRecord::GenerationPrepared {
                store_uuid: fixture.meta.store_uuid,
                source_generation: 0,
                target_generation: 1,
                target_snapshot_generation: 1,
                open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
            },
            4,
            0,
        )
        .expect("generation preparation frame encodes"),
    );
    fixture.rebuild(&[1, 2, 3, 4]);

    let target_snapshot_state = LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: fixture.meta.store_uuid,
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 4,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    };
    let target_snapshot = encode_snapshot(&target_snapshot_state).expect("target snapshot encodes");
    let target_log = compaction_target_log(&fixture, &target_snapshot, 4, 0, 0);
    let old_marker = fixture.marker.slots[0].clone().expect("bootstrap marker exists");
    let marker = EnabledMarkerFile {
        slots: [
            Some(old_marker),
            Some(EnabledMarkerSlot {
                slot_index: 1,
                store_uuid: fixture.meta.store_uuid,
                bootstrap_id: fixture.meta.bootstrap_id,
                marker_epoch: 2,
                snapshot_generation: 1,
                log_generation: 1,
                anchor_sequence: 5,
                snapshot_file_length: target_snapshot.len() as u64,
                snapshot_file_crc32: crc32(&target_snapshot),
                anchor_frame_crc32: crc32(&target_log),
            }),
        ],
    };
    let decision = replay(ReplayInput {
        store_meta: &fixture.meta,
        marker: &marker,
        acknowledgement_slots: [&fixture.slot0, &fixture.slot1],
        generations: vec![
            GenerationBytes {
                generation: 0,
                snapshot: &fixture.snapshot,
                log: &fixture.log,
            },
            GenerationBytes {
                generation: 1,
                snapshot: &target_snapshot,
                log: &target_log,
            },
        ],
        limits: ReplayLimits::default(),
    })
    .expect("the explicit marker-ahead switch row is recoverable");
    let RecoveryDecision::AcknowledgeSelectedAnchor(plan) = decision else {
        panic!("marker-ahead state must return an anchor acknowledgement plan")
    };
    assert_eq!(plan.source_generation(), 0);
    assert_eq!(plan.generation(), 1);
    assert_eq!(plan.frame_sequence(), 5);
    assert_eq!(plan.frame_end_offset(), target_log.len() as u64);
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(plan.expected_acknowledgement_slot()).expect("planned slot decodes")
    else {
        panic!("planned slot is populated")
    };
    assert_eq!(slot.acknowledgement_epoch, 5);
    assert_eq!(slot.marker_epoch, 2);
    assert_eq!(slot.log_generation, 1);
    assert_eq!(plan.expected_seal().len(), super::super::codec::COMMIT_SEAL_LENGTH);

    let mut invalid_history = marker.clone();
    let historical = invalid_history.slots[0].as_mut().expect("historical marker exists");
    historical.anchor_sequence = 4;
    historical.anchor_frame_crc32 = crc32(&fixture.frames[3]);
    assert_eq!(
        replay(ReplayInput {
            store_meta: &fixture.meta,
            marker: &invalid_history,
            acknowledgement_slots: [&fixture.slot0, &fixture.slot1],
            generations: vec![
                GenerationBytes {
                    generation: 0,
                    snapshot: &fixture.snapshot,
                    log: &fixture.log,
                },
                GenerationBytes {
                    generation: 1,
                    snapshot: &target_snapshot,
                    log: &target_log,
                },
            ],
            limits: ReplayLimits::default(),
        }),
        Err(ReplayViolation::Marker(
            crate::mapped_file::retirement::sidecar::SidecarViolation::InvalidMarkerSlotHistory
        ))
    );

    for (invalid_target_log, source) in [
        (
            compaction_target_log(&fixture, &target_snapshot, 3, 0, 0),
            CodecViolation::InvalidEnvelopeRelationship {
                detail: "LogOpened must be the first sequence after its snapshot base",
            },
        ),
        (
            compaction_target_log(&fixture, &target_snapshot, 4, 0, 1),
            CodecViolation::InvalidTailRepairFields,
        ),
    ] {
        let mut invalid_marker = marker.clone();
        let selected = invalid_marker.slots[1].as_mut().expect("selected marker exists");
        selected.anchor_frame_crc32 = crc32(&invalid_target_log);
        assert_eq!(
            replay(ReplayInput {
                store_meta: &fixture.meta,
                marker: &invalid_marker,
                acknowledgement_slots: [&fixture.slot0, &fixture.slot1],
                generations: vec![
                    GenerationBytes {
                        generation: 0,
                        snapshot: &fixture.snapshot,
                        log: &fixture.log,
                    },
                    GenerationBytes {
                        generation: 1,
                        snapshot: &target_snapshot,
                        log: &invalid_target_log,
                    },
                ],
                limits: ReplayLimits::default(),
            }),
            Err(ReplayViolation::InvalidLog {
                generation: 1,
                offset: 0,
                source,
            })
        );
    }
}

#[test]
fn selected_ordinary_compaction_requires_exact_terminal_generation_prepared() {
    let fixture = BootstrapFixture::new();
    let (snapshot, log, marker, slots) = selected_compaction_artifacts(&fixture);
    assert_eq!(
        replay(ReplayInput {
            store_meta: &fixture.meta,
            marker: &marker,
            acknowledgement_slots: [&slots[0], &slots[1]],
            generations: vec![
                GenerationBytes {
                    generation: 0,
                    snapshot: &fixture.snapshot,
                    log: &fixture.log,
                },
                GenerationBytes {
                    generation: 1,
                    snapshot: &snapshot,
                    log: &log,
                },
            ],
            limits: ReplayLimits::default(),
        }),
        Err(ReplayViolation::GenerationBindingMismatch)
    );

    let mut prepared = BootstrapFixture::new();
    prepared.frames.push(
        encode_ledger_frame(
            &LedgerRecord::GenerationPrepared {
                store_uuid: prepared.meta.store_uuid,
                source_generation: 0,
                target_generation: 1,
                target_snapshot_generation: 1,
                open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
            },
            4,
            0,
        )
        .expect("preparation frame encodes"),
    );
    prepared.rebuild(&[1, 2, 3, 4]);
    let (snapshot, log, marker, slots) = selected_compaction_artifacts(&prepared);
    assert!(matches!(
        replay(ReplayInput {
            store_meta: &prepared.meta,
            marker: &marker,
            acknowledgement_slots: [&slots[0], &slots[1]],
            generations: vec![
                GenerationBytes {
                    generation: 0,
                    snapshot: &prepared.snapshot,
                    log: &prepared.log,
                },
                GenerationBytes {
                    generation: 1,
                    snapshot: &snapshot,
                    log: &log,
                },
            ],
            limits: ReplayLimits::default(),
        }),
        Ok(RecoveryDecision::NeedsReconciliation(_))
    ));
}

#[test]
fn tail_repair_marker_switch_with_predecessor_ack_is_recoverable() {
    let mut fixture = BootstrapFixture::new();
    let acknowledged_prefix_length = fixture.log.len();
    let unacknowledged_suffix = b"RMLC";
    fixture.log.extend_from_slice(unacknowledged_suffix);
    let target_snapshot_state = LifecycleSnapshot {
        mode: SnapshotMode::TailRepair,
        store_uuid: fixture.meta.store_uuid,
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 3,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    };
    let target_snapshot = encode_snapshot(&target_snapshot_state).expect("target snapshot encodes");
    let target_log = encode_ledger_frame(
        &LedgerRecord::LogOpened {
            store_uuid: fixture.meta.store_uuid,
            generation: 1,
            snapshot_generation: 1,
            predecessor_log_generation: 0,
            predecessor_terminal_acknowledged_sequence: 3,
            snapshot_base_sequence: 3,
            snapshot_file_length: target_snapshot.len() as u64,
            snapshot_file_crc32: crc32(&target_snapshot),
            predecessor_prefix_crc32: crc32(&fixture.log[..acknowledged_prefix_length]),
            validated_prefix_length: acknowledged_prefix_length as u64,
            unacknowledged_suffix_length: unacknowledged_suffix.len() as u32,
            unacknowledged_suffix_crc32: crc32(unacknowledged_suffix),
            open_reason: crate::mapped_file::retirement::codec::OpenReason::TailRepair,
            predecessor_acknowledgement_epoch: 3,
        },
        4,
        1,
    )
    .expect("tail-repair LogOpened encodes");
    let marker = EnabledMarkerFile {
        slots: [
            fixture.marker.slots[0].clone(),
            Some(EnabledMarkerSlot {
                slot_index: 1,
                store_uuid: fixture.meta.store_uuid,
                bootstrap_id: fixture.meta.bootstrap_id,
                marker_epoch: 2,
                snapshot_generation: 1,
                log_generation: 1,
                anchor_sequence: 4,
                snapshot_file_length: target_snapshot.len() as u64,
                snapshot_file_crc32: crc32(&target_snapshot),
                anchor_frame_crc32: crc32(&target_log),
            }),
        ],
    };

    let decision = replay(ReplayInput {
        store_meta: &fixture.meta,
        marker: &marker,
        acknowledgement_slots: [&fixture.slot0, &fixture.slot1],
        generations: vec![
            GenerationBytes {
                generation: 0,
                snapshot: &fixture.snapshot,
                log: &fixture.log,
            },
            GenerationBytes {
                generation: 1,
                snapshot: &target_snapshot,
                log: &target_log,
            },
        ],
        limits: ReplayLimits::default(),
    })
    .expect("tail-repair marker-ahead row is recoverable");
    let RecoveryDecision::AcknowledgeSelectedAnchor(plan) = decision else {
        panic!("tail-repair anchor needs acknowledgement")
    };
    assert_eq!(plan.source_generation(), 0);
    assert_eq!(plan.generation(), 1);
    assert_eq!(plan.frame_sequence(), 4);
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(plan.expected_acknowledgement_slot()).expect("planned slot decodes")
    else {
        panic!("planned slot is populated")
    };
    assert_eq!(slot.acknowledgement_epoch, 4);
    assert_eq!(slot.marker_epoch, 2);
}

#[test]
fn bytes_after_terminal_generation_prepared_never_become_generic_tail_repair() {
    let mut fixture = BootstrapFixture::new();
    fixture.frames.push(
        encode_ledger_frame(
            &LedgerRecord::GenerationPrepared {
                store_uuid: fixture.meta.store_uuid,
                source_generation: 0,
                target_generation: 1,
                target_snapshot_generation: 1,
                open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
            },
            4,
            0,
        )
        .expect("generation preparation frame encodes"),
    );
    fixture.rebuild(&[1, 2, 3, 4]);
    fixture.log.extend_from_slice(b"RMLC");

    assert_eq!(replay(fixture.input()), Err(ReplayViolation::GenerationBindingMismatch));
}

#[test]
fn corrupt_partial_acknowledged_seal_fails_closed() {
    let mut fixture = BootstrapFixture::new();
    fixture.log.truncate(fixture.log.len() - 17);
    let final_byte = fixture.log.len() - 1;
    fixture.log[final_byte] ^= 1;

    assert_eq!(replay(fixture.input()), Err(ReplayViolation::PartialSealMismatch));
}

#[test]
fn unacknowledged_tail_boundary_is_strictly_less_than_one_sealed_unit() {
    assert_eq!(
        validate_unacknowledged_suffix_length(super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH - 1),
        Ok((super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH - 1) as u32)
    );
    assert_eq!(
        validate_unacknowledged_suffix_length(super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH),
        Err(ReplayViolation::InvalidUnacknowledgedSuffixLength {
            length: super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH,
            maximum: super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH,
        })
    );
}

#[test]
fn marker_and_generation_mismatch_fails_closed() {
    let fixture = BootstrapFixture::new();
    let wrong = GenerationBytes {
        generation: 1,
        snapshot: &fixture.snapshot,
        log: &fixture.log,
    };
    let mut input = fixture.input();
    input.generations = vec![wrong];

    assert_eq!(replay(input), Err(ReplayViolation::GenerationBindingMismatch));
}

#[test]
fn valid_older_generation_pairs_are_retained_gc_backlog_not_selection_ambiguity() {
    let store_uuid = StoreUuid::new([1; 16]).expect("test UUID is nonzero");
    let parsed = (0..=2)
        .map(|generation| {
            (
                generation,
                ParsedGeneration {
                    bytes: GenerationBytes {
                        generation,
                        snapshot: &[],
                        log: &[],
                    },
                    snapshot: LifecycleSnapshot {
                        mode: if generation == 0 {
                            SnapshotMode::BootstrapInventory
                        } else {
                            SnapshotMode::OrdinaryCompaction
                        },
                        store_uuid,
                        generation,
                        log_generation: generation,
                        predecessor_log_generation: generation.checked_sub(1).unwrap_or(u64::MAX),
                        base_sequence: generation + 1,
                        create_high_water: 0,
                        ticket_high_water: 0,
                        entries: Vec::new(),
                    },
                    evidence_range: 0..0,
                    tail: None,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(validation::validate_generation_set(2, &parsed), Ok(()));
}

#[test]
fn snapshot_semantics_and_marker_epoch_relation_are_validated_before_decisions() {
    let fixture = BootstrapFixture::new();
    let generation = fixture.input().generations[0];
    let mut evidence = Vec::new();
    let mut parsed_generation =
        parsing::parse_generation(generation, &fixture.meta, ReplayLimits::default(), &mut evidence)
            .expect("fixture parses");
    parsed_generation.snapshot.base_sequence = 0;
    let parsed = BTreeMap::from([(0, parsed_generation)]);
    assert_eq!(
        validation::validate_all_snapshot_states(&parsed),
        Err(ReplayViolation::State(StateViolation::InvalidSnapshotState))
    );

    let mut impossible_epoch = fixture.marker.slots[0].clone().expect("marker exists");
    impossible_epoch.marker_epoch = 3;
    let mut valid_evidence = Vec::new();
    let valid_generation =
        parsing::parse_generation(generation, &fixture.meta, ReplayLimits::default(), &mut valid_evidence)
            .expect("fixture parses");
    assert_eq!(
        validation::validate_marker_binding(&impossible_epoch, &fixture.meta, &valid_generation),
        Err(ReplayViolation::GenerationBindingMismatch)
    );
}

#[test]
fn gc_backlog_reconstructs_canonical_marker_and_exact_second_frame() {
    let fixture = BootstrapFixture::new();
    let generation = fixture.input().generations[0];
    let mut evidence = Vec::new();
    let parsed_generation =
        parsing::parse_generation(generation, &fixture.meta, ReplayLimits::default(), &mut evidence)
            .expect("fixture parses");
    let parsed = BTreeMap::from([(0, parsed_generation)]);
    let no_marker_references = EnabledMarkerFile { slots: [None, None] };

    validation::validate_gc_backlog_markers(&no_marker_references, &fixture.meta, 1, &parsed, &evidence)
        .expect("canonical bootstrap marker and witness are reconstructed");
    evidence[2].encoded_frame[48] ^= 1;
    assert_eq!(
        validation::validate_gc_backlog_markers(&no_marker_references, &fixture.meta, 1, &parsed, &evidence,),
        Err(ReplayViolation::GenerationBindingMismatch)
    );
}

#[test]
fn switch_headroom_reserves_anchor_witness_and_following_cursor() {
    assert_eq!(require_recovery_headroom(u64::MAX - 3, u64::MAX - 3, 3), Ok(()));
    assert_eq!(
        require_recovery_headroom(u64::MAX - 2, u64::MAX - 3, 3),
        Err(ReplayViolation::State(StateViolation::SequenceOverflow))
    );
    assert_eq!(
        require_recovery_headroom(u64::MAX - 3, u64::MAX - 2, 3),
        Err(ReplayViolation::BrokenAcknowledgementChain)
    );
}

#[test]
fn unknown_critical_record_fails_closed_before_state_application() {
    let mut fixture = BootstrapFixture::new();
    fixture.replace_marker_committed_with_unknown_critical();

    assert!(matches!(
        replay(fixture.input()),
        Err(ReplayViolation::InvalidLog {
            generation: 0,
            source: CodecViolation::UnknownCriticalRecordType { record_type: 0x7777 },
            ..
        })
    ));
}

#[derive(Debug)]
struct BootstrapFixture {
    meta: StoreMeta,
    marker: EnabledMarkerFile,
    snapshot: Vec<u8>,
    log: Vec<u8>,
    slot0: [u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
    slot1: [u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
    evidence: Vec<SealEvidence>,
    frames: Vec<Vec<u8>>,
    seals: Vec<[u8; super::super::codec::COMMIT_SEAL_LENGTH]>,
}

impl BootstrapFixture {
    fn new() -> Self {
        let store_uuid = StoreUuid::new([1; 16]).expect("test UUID is nonzero");
        let bootstrap_id = [2; 16];
        let meta = StoreMeta {
            store_uuid,
            creation_time_ns: 7,
            bootstrap_id,
        };
        let snapshot_state = LifecycleSnapshot {
            mode: SnapshotMode::BootstrapInventory,
            store_uuid,
            generation: 0,
            log_generation: 0,
            predecessor_log_generation: u64::MAX,
            base_sequence: 1,
            create_high_water: 0,
            ticket_high_water: 0,
            entries: Vec::new(),
        };
        let snapshot = encode_snapshot(&snapshot_state).expect("bootstrap snapshot encodes");
        let bootstrap_records = [
            LedgerRecord::StoreInitialized {
                store_uuid,
                bootstrap_id,
                creation_time_ns: 7,
            },
            LedgerRecord::BootstrapInstalled {
                store_uuid,
                bootstrap_id,
                snapshot_generation: 0,
                snapshot_base_sequence: 1,
                snapshot_file_length: snapshot.len() as u64,
                snapshot_file_crc32: crc32(&snapshot),
                inventory_count: 0,
                create_high_water: 0,
                ticket_high_water: 0,
            },
        ];
        let mut frames = bootstrap_records
            .iter()
            .enumerate()
            .map(|(index, record)| encode_ledger_frame(record, index as u64 + 1, 0).expect("frame encodes"))
            .collect::<Vec<_>>();
        let marker_slot = EnabledMarkerSlot {
            slot_index: 0,
            store_uuid,
            bootstrap_id,
            marker_epoch: 1,
            snapshot_generation: 0,
            log_generation: 0,
            anchor_sequence: 2,
            snapshot_file_length: snapshot.len() as u64,
            snapshot_file_crc32: crc32(&snapshot),
            anchor_frame_crc32: crc32(&frames[1]),
        };
        let encoded_marker = encode_enabled_marker_slot(&marker_slot).expect("marker slot encodes");
        let marker_slot_crc32 = u32::from_le_bytes(
            encoded_marker[100..104]
                .try_into()
                .expect("stored marker CRC is four bytes"),
        );
        frames.push(
            encode_ledger_frame(
                &LedgerRecord::MarkerCommitted {
                    store_uuid,
                    marker_epoch: 1,
                    snapshot_generation: 0,
                    log_generation: 0,
                    anchor_sequence: 2,
                    slot_index: 0,
                    slot_crc32: marker_slot_crc32,
                },
                3,
                0,
            )
            .expect("marker witness frame encodes"),
        );
        let marker = EnabledMarkerFile {
            slots: [Some(marker_slot), None],
        };

        let (log, slots, seals, evidence) = build_log(&meta, &frames, &[1, 2, 3]);
        Self {
            meta,
            marker,
            snapshot,
            log,
            slot0: slots[0],
            slot1: slots[1],
            evidence,
            frames,
            seals,
        }
    }

    fn input(&self) -> ReplayInput<'_> {
        ReplayInput {
            store_meta: &self.meta,
            marker: &self.marker,
            acknowledgement_slots: [&self.slot0, &self.slot1],
            generations: vec![GenerationBytes {
                generation: 0,
                snapshot: &self.snapshot,
                log: &self.log,
            }],
            limits: ReplayLimits::default(),
        }
    }

    fn log_with_epoch_override(&self, frame_index: usize, epoch: u64) -> Vec<u8> {
        let mut epochs = [1, 2, 3];
        epochs[frame_index] = epoch;
        build_log(&self.meta, &self.frames, &epochs).0
    }

    fn replace_marker_committed_with_unknown_critical(&mut self) {
        let mut frame = self.frames[2].clone();
        frame[8..10].copy_from_slice(&0x7777_u16.to_le_bytes());
        let header_crc32 = crc32(&frame[..36]);
        frame[36..40].copy_from_slice(&header_crc32.to_le_bytes());
        self.frames[2] = frame;
        let (log, slots, seals, evidence) = build_log_inner(&self.meta, &self.frames, &[1, 2, 3], false);
        self.log = log;
        self.slot0 = slots[0];
        self.slot1 = slots[1];
        self.seals = seals;
        self.evidence = evidence;
    }

    fn rebuild_without_evidence(&mut self, epochs: &[u64]) {
        let (log, slots, seals, evidence) = build_log_inner(&self.meta, &self.frames, epochs, false);
        self.log = log;
        self.slot0 = slots[0];
        self.slot1 = slots[1];
        self.seals = seals;
        self.evidence = evidence;
    }

    fn rebuild(&mut self, epochs: &[u64]) {
        let (log, slots, seals, evidence) = build_log(&self.meta, &self.frames, epochs);
        self.log = log;
        self.slot0 = slots[0];
        self.slot1 = slots[1];
        self.seals = seals;
        self.evidence = evidence;
    }
}

fn compaction_target_log(
    fixture: &BootstrapFixture,
    target_snapshot: &[u8],
    snapshot_base_sequence: u64,
    unacknowledged_suffix_length: u32,
    unacknowledged_suffix_crc32: u32,
) -> Vec<u8> {
    let mut frame = encode_ledger_frame(
        &LedgerRecord::LogOpened {
            store_uuid: fixture.meta.store_uuid,
            generation: 1,
            snapshot_generation: 1,
            predecessor_log_generation: 0,
            predecessor_terminal_acknowledged_sequence: 4,
            snapshot_base_sequence: 4,
            snapshot_file_length: target_snapshot.len() as u64,
            snapshot_file_crc32: crc32(target_snapshot),
            predecessor_prefix_crc32: crc32(&fixture.log),
            validated_prefix_length: fixture.log.len() as u64,
            unacknowledged_suffix_length: 0,
            unacknowledged_suffix_crc32: 0,
            open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
            predecessor_acknowledgement_epoch: 4,
        },
        5,
        1,
    )
    .expect("canonical target LogOpened encodes");
    let payload_offset = super::super::codec::MIN_HEADER_LENGTH;
    frame[payload_offset + 48..payload_offset + 56].copy_from_slice(&snapshot_base_sequence.to_le_bytes());
    frame[payload_offset + 80..payload_offset + 84].copy_from_slice(&unacknowledged_suffix_length.to_le_bytes());
    frame[payload_offset + 84..payload_offset + 88].copy_from_slice(&unacknowledged_suffix_crc32.to_le_bytes());
    let payload_end = frame.len() - 4;
    let payload_crc32 = crc32(&frame[payload_offset..payload_end]);
    frame[payload_end..].copy_from_slice(&payload_crc32.to_le_bytes());
    frame
}

fn selected_compaction_artifacts(
    fixture: &BootstrapFixture,
) -> (
    Vec<u8>,
    Vec<u8>,
    EnabledMarkerFile,
    [[u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]; 2],
) {
    let source_sequence = fixture.frames.len() as u64;
    let snapshot_state = LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: fixture.meta.store_uuid,
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: source_sequence,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    };
    let snapshot = encode_snapshot(&snapshot_state).expect("target snapshot encodes");
    let anchor_sequence = source_sequence + 1;
    let anchor = encode_ledger_frame(
        &LedgerRecord::LogOpened {
            store_uuid: fixture.meta.store_uuid,
            generation: 1,
            snapshot_generation: 1,
            predecessor_log_generation: 0,
            predecessor_terminal_acknowledged_sequence: source_sequence,
            snapshot_base_sequence: source_sequence,
            snapshot_file_length: snapshot.len() as u64,
            snapshot_file_crc32: crc32(&snapshot),
            predecessor_prefix_crc32: crc32(&fixture.log),
            validated_prefix_length: fixture.log.len() as u64,
            unacknowledged_suffix_length: 0,
            unacknowledged_suffix_crc32: 0,
            open_reason: crate::mapped_file::retirement::codec::OpenReason::Compaction,
            predecessor_acknowledgement_epoch: source_sequence,
        },
        anchor_sequence,
        1,
    )
    .expect("target anchor encodes");
    let selected_marker = EnabledMarkerSlot {
        slot_index: 1,
        store_uuid: fixture.meta.store_uuid,
        bootstrap_id: fixture.meta.bootstrap_id,
        marker_epoch: 2,
        snapshot_generation: 1,
        log_generation: 1,
        anchor_sequence,
        snapshot_file_length: snapshot.len() as u64,
        snapshot_file_crc32: crc32(&snapshot),
        anchor_frame_crc32: crc32(&anchor),
    };
    let witness = encode_ledger_frame(
        &validation::expected_marker_record(&selected_marker).expect("marker witness derives"),
        anchor_sequence + 1,
        1,
    )
    .expect("marker witness encodes");
    let (log, slots) = build_generation_log(
        &fixture.meta,
        1,
        2,
        anchor_sequence,
        &[anchor, witness],
        &[source_sequence + 1, source_sequence + 2],
    );
    (
        snapshot,
        log,
        EnabledMarkerFile {
            slots: [fixture.marker.slots[0].clone(), Some(selected_marker)],
        },
        slots,
    )
}

fn build_generation_log(
    meta: &StoreMeta,
    generation: u64,
    marker_epoch: u64,
    first_sequence: u64,
    frames: &[Vec<u8>],
    epochs: &[u64],
) -> (Vec<u8>, [[u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]; 2]) {
    let mut log = Vec::new();
    let mut slots = [[0_u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]; 2];
    for (index, frame) in frames.iter().enumerate() {
        let epoch = epochs[index];
        let slot_index = ((epoch - 1) & 1) as u8;
        let frame_end_offset = (log.len() + frame.len()) as u64;
        let slot = AcknowledgementSlot {
            slot_index,
            activated: true,
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            acknowledgement_epoch: epoch,
            marker_epoch,
            log_generation: generation,
            frame_sequence: first_sequence + index as u64,
            frame_end_offset,
            frame_crc32: crc32(frame),
        };
        let encoded_slot = encode_acknowledgement_slot(&slot).expect("slot encodes");
        let seal = CommitSeal::from_acknowledgement_slot(&slot, &encoded_slot).expect("seal derives");
        log.extend_from_slice(frame);
        log.extend_from_slice(&encode_commit_seal(&seal).expect("seal encodes"));
        slots[slot_index as usize] = encoded_slot;
    }
    (log, slots)
}

fn build_log(meta: &StoreMeta, frames: &[Vec<u8>], epochs: &[u64]) -> BuiltLog {
    build_log_inner(meta, frames, epochs, true)
}

type BuiltLog = (
    Vec<u8>,
    [[u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]; 2],
    Vec<[u8; super::super::codec::COMMIT_SEAL_LENGTH]>,
    Vec<SealEvidence>,
);

fn build_log_inner(meta: &StoreMeta, frames: &[Vec<u8>], epochs: &[u64], capture_evidence: bool) -> BuiltLog {
    let mut log = Vec::new();
    let mut slots = [[0_u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]; 2];
    let mut seals = Vec::new();
    let mut evidence = Vec::new();
    for (index, frame) in frames.iter().enumerate() {
        let sequence = index as u64 + 1;
        let epoch = epochs[index];
        let slot_index = ((epoch - 1) & 1) as u8;
        let frame_start = log.len() as u64;
        let frame_end = (log.len() + frame.len()) as u64;
        let slot = AcknowledgementSlot {
            slot_index,
            activated: sequence >= 3,
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            acknowledgement_epoch: epoch,
            marker_epoch: if sequence >= 3 { 1 } else { 0 },
            log_generation: 0,
            frame_sequence: sequence,
            frame_end_offset: frame_end,
            frame_crc32: crc32(frame),
        };
        let encoded_slot = encode_acknowledgement_slot(&slot).expect("slot encodes");
        let seal = CommitSeal::from_acknowledgement_slot(&slot, &encoded_slot).expect("seal derives from slot");
        let encoded_seal = encode_commit_seal(&seal).expect("seal encodes");
        log.extend_from_slice(frame);
        log.extend_from_slice(&encoded_seal);
        slots[slot_index as usize] = encoded_slot;
        seals.push(encoded_seal);
        if capture_evidence {
            let DecodeOutcome::Frame(decoded) = decode_next_frame(frame, sequence, 0).expect("frame decodes") else {
                panic!("test frame is complete")
            };
            let record = decoded.decode_record().expect("typed record decodes");
            evidence.push(SealEvidence {
                slot,
                encoded_slot,
                generation: 0,
                sealed_log_length: log.len() as u64,
                frame_start_offset: frame_start,
                encoded_frame: frame.clone(),
                record,
            });
        }
    }
    (log, slots, seals, evidence)
}
