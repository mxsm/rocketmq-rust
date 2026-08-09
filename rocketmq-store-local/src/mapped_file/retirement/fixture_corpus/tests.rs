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

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use sha2::Digest;
use sha2::Sha256;

use super::build::corpus;
use super::Fixture;
use super::FixtureValidation;
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::decode_acknowledgement_file;
use crate::mapped_file::retirement::codec::decode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::decode_commit_seal;
use crate::mapped_file::retirement::codec::decode_next_frame;
use crate::mapped_file::retirement::codec::validate_acknowledged_frame;
use crate::mapped_file::retirement::codec::AcknowledgementSlotState;
use crate::mapped_file::retirement::codec::DecodeOutcome;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::OpenReason;
use crate::mapped_file::retirement::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
use crate::mapped_file::retirement::codec::COMMIT_SEAL_LENGTH;
use crate::mapped_file::retirement::sidecar::decode_enabled_marker_file;
use crate::mapped_file::retirement::sidecar::decode_enabled_marker_slot;
use crate::mapped_file::retirement::sidecar::decode_snapshot;
use crate::mapped_file::retirement::sidecar::decode_store_meta;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_slot;

const REGENERATE_ENV: &str = "ROCKETMQ_REGENERATE_LIFECYCLE_FIXTURES";
const MANIFEST_NAME: &str = "SHA256SUMS";

#[test]
fn checked_in_lifecycle_corpus_is_byte_exact_decodable_and_hash_complete() {
    let fixtures = corpus();
    assert_required_edge_coverage(&fixtures);
    assert_generation_frontier_relationships(&fixtures);
    let directory = fixture_directory();
    let expected_manifest = manifest(&fixtures);

    if std::env::var_os(REGENERATE_ENV).as_deref() == Some(std::ffi::OsStr::new("1")) {
        write_corpus(&directory, &fixtures, &expected_manifest);
    }

    let actual_manifest = fs::read_to_string(directory.join(MANIFEST_NAME)).unwrap_or_else(|error| {
        panic!(
            "read {} (run with {REGENERATE_ENV}=1 only after format-owner review): {error}",
            directory.join(MANIFEST_NAME).display()
        )
    });
    assert_eq!(
        actual_manifest.replace("\r\n", "\n"),
        expected_manifest,
        "fixture manifest changed"
    );

    let expected_names = fixtures
        .iter()
        .map(|fixture| fixture.name.clone())
        .chain(std::iter::once(MANIFEST_NAME.to_owned()))
        .collect::<BTreeSet<_>>();
    let actual_names = fs::read_dir(&directory)
        .expect("read lifecycle fixture directory")
        .map(|entry| {
            entry
                .expect("read lifecycle fixture entry")
                .file_name()
                .into_string()
                .expect("fixture names are UTF-8")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_names, expected_names,
        "fixture directory has missing or untracked files"
    );

    for fixture in &fixtures {
        let path = directory.join(&fixture.name);
        let actual = fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        assert_eq!(actual, fixture.bytes, "fixture bytes changed: {}", fixture.name);
        validate_fixture(fixture, &actual);
    }

    assert_eq!(
        sha256(
            &fixtures
                .iter()
                .find(|fixture| fixture.name == "completed.frame.bin")
                .expect("worked frame fixture exists")
                .bytes
        ),
        "38b8ae1b8279222529f89270c7ffc5853146940c4abd91c24a6f3af1990af2b1"
    );
    assert_eq!(
        sha256(
            &fixtures
                .iter()
                .find(|fixture| fixture.name == "completed.acknowledged-unit.bin")
                .expect("worked acknowledged unit fixture exists")
                .bytes
        ),
        "5c243cc25589cb54c89c49afc796e6e41a12a19604a8eecf0242776797e5bdf2"
    );
}

fn assert_required_edge_coverage(fixtures: &[Fixture]) {
    let names = fixtures
        .iter()
        .map(|fixture| fixture.name.as_str())
        .collect::<BTreeSet<_>>();
    for required in [
        "acknowledgement.all-zero.bin",
        "acknowledgement.slot1.bin",
        "enabled.newer-slot-missing-pair.bin",
        "invalid.acknowledgement-nonconsecutive.bin",
        "invalid.mid-log-damage.log.bin",
        "invalid.oversized-header.bin",
        "invalid.oversized-payload.bin",
        "invalid.seal-slot-crc-mismatch.bundle.bin",
        "invalid.sequence-duplicate.log.bin",
        "invalid.sequence-gap.log.bin",
        "invalid.sequence-overflow.log.bin",
        "invalid.snapshot-oversized-body.bin",
        "invalid.snapshot-reserved.bin",
        "invalid.snapshot-too-many-entries.bin",
        "recovery.ack-seal-reconstruction.bundle.bin",
        "recovery.acknowledged-unit-all-truncations.bin",
        "recovery.valid-ack-missing-seal.log.bin",
        "recovery.valid-ack-partial-seal.log.bin",
        "compaction.source-prepared.acknowledged-unit.bundle.bin",
        "compaction.target-snapshot.bin",
        "compaction.target-log-opened.unsealed.bin",
        "compaction.target-log-opened.acknowledged-unit.bundle.bin",
        "compaction.marker-switched.bin",
        "compaction.marker-committed.unsealed.bin",
        "compaction.marker-committed.acknowledged-unit.bundle.bin",
        "tail-repair.target-log-opened.unsealed.bin",
        "tail-repair.target-snapshot.bin",
        "tail-repair.quarantine-suffix.bin",
        "tail-repair.target-log-opened.acknowledged-unit.bundle.bin",
        "tail-repair.marker-switched.bin",
        "tail-repair.marker-committed.unsealed.bin",
        "tail-repair.marker-committed.acknowledged-unit.bundle.bin",
        "generation.orphan-higher.snapshot.bin",
        "generation.orphan-higher.log.bin",
        "invalid.acknowledged-complete-suffix-loss.bundle.bin",
        "invalid.log-opened-overlong-suffix.bin",
    ] {
        assert!(
            names.contains(required),
            "required lifecycle fixture is missing: {required}"
        );
    }
}

fn assert_generation_frontier_relationships(fixtures: &[Fixture]) {
    for (prefix, expected_reason, suffix_name) in [
        ("compaction", OpenReason::Compaction, None),
        (
            "tail-repair",
            OpenReason::TailRepair,
            Some("tail-repair.quarantine-suffix.bin"),
        ),
    ] {
        let snapshot_bytes = fixture_bytes(fixtures, &format!("{prefix}.target-snapshot.bin"));
        let snapshot = decode_snapshot(snapshot_bytes).expect("frontier target snapshot decodes");
        let opened_bytes = fixture_bytes(fixtures, &format!("{prefix}.target-log-opened.unsealed.bin"));
        let opened_sequence = snapshot
            .base_sequence
            .checked_add(1)
            .expect("frontier sequence advances");
        let DecodeOutcome::Frame(opened_frame) =
            decode_next_frame(opened_bytes, opened_sequence, snapshot.log_generation)
                .expect("frontier LogOpened frame decodes")
        else {
            panic!("frontier LogOpened is complete");
        };
        let Some(LedgerRecord::LogOpened {
            generation,
            snapshot_generation,
            predecessor_log_generation,
            predecessor_terminal_acknowledged_sequence,
            snapshot_base_sequence,
            snapshot_file_length,
            snapshot_file_crc32,
            unacknowledged_suffix_length,
            unacknowledged_suffix_crc32,
            open_reason,
            ..
        }) = opened_frame
            .decode_record()
            .expect("frontier LogOpened payload decodes")
        else {
            panic!("frontier anchor is LogOpened");
        };
        assert_eq!(generation, snapshot.generation);
        assert_eq!(snapshot_generation, snapshot.generation);
        assert_eq!(predecessor_log_generation, snapshot.predecessor_log_generation);
        assert_eq!(predecessor_terminal_acknowledged_sequence, snapshot.base_sequence);
        assert_eq!(snapshot_base_sequence, snapshot.base_sequence);
        assert_eq!(snapshot_file_length, snapshot_bytes.len() as u64);
        assert_eq!(snapshot_file_crc32, crc32(snapshot_bytes));
        assert_eq!(open_reason, expected_reason);

        match suffix_name {
            Some(name) => {
                let suffix = fixture_bytes(fixtures, name);
                assert_eq!(unacknowledged_suffix_length, suffix.len() as u32);
                assert_eq!(unacknowledged_suffix_crc32, crc32(suffix));
            }
            None => {
                assert_eq!(unacknowledged_suffix_length, 0);
                assert_eq!(unacknowledged_suffix_crc32, 0);
            }
        }

        let marker_bytes = fixture_bytes(fixtures, &format!("{prefix}.marker-switched.bin"));
        let marker = decode_enabled_marker_file(marker_bytes).expect("frontier marker decodes");
        let selected = marker.selected_slot().expect("frontier marker selects its target");
        assert_eq!(selected.snapshot_generation, snapshot.generation);
        assert_eq!(selected.log_generation, snapshot.log_generation);
        assert_eq!(selected.anchor_sequence, opened_sequence);
        assert_eq!(selected.snapshot_file_length, snapshot_bytes.len() as u64);
        assert_eq!(selected.snapshot_file_crc32, crc32(snapshot_bytes));
        assert_eq!(selected.anchor_frame_crc32, crc32(opened_bytes));

        assert_marker_witness(
            fixture_bytes(fixtures, &format!("{prefix}.marker-committed.unsealed.bin")),
            snapshot.log_generation,
            opened_sequence,
            selected,
        );
    }
}

fn assert_marker_witness(
    log: &[u8],
    generation: u64,
    opened_sequence: u64,
    selected: &crate::mapped_file::retirement::sidecar::EnabledMarkerSlot,
) {
    let DecodeOutcome::Frame(opened) =
        decode_next_frame(log, opened_sequence, generation).expect("witness log anchor decodes")
    else {
        panic!("witness log starts with a complete anchor");
    };
    let witness_offset = opened.encoded_len() + COMMIT_SEAL_LENGTH;
    let witness_sequence = opened_sequence.checked_add(1).expect("witness sequence advances");
    let DecodeOutcome::Frame(witness) = decode_next_frame(&log[witness_offset..], witness_sequence, generation)
        .expect("MarkerCommitted witness decodes")
    else {
        panic!("frontier contains a complete MarkerCommitted witness");
    };
    assert_eq!(witness_offset + witness.encoded_len(), log.len());
    let Some(LedgerRecord::MarkerCommitted {
        marker_epoch,
        snapshot_generation,
        log_generation,
        anchor_sequence,
        slot_index,
        slot_crc32,
        ..
    }) = witness.decode_record().expect("MarkerCommitted payload decodes")
    else {
        panic!("frontier witness is MarkerCommitted");
    };
    let encoded_slot = encode_enabled_marker_slot(selected).expect("selected marker slot re-encodes");
    assert_eq!(marker_epoch, selected.marker_epoch);
    assert_eq!(snapshot_generation, selected.snapshot_generation);
    assert_eq!(log_generation, selected.log_generation);
    assert_eq!(anchor_sequence, selected.anchor_sequence);
    assert_eq!(slot_index, selected.slot_index);
    assert_eq!(
        slot_crc32,
        u32::from_le_bytes(encoded_slot[100..104].try_into().expect("stored marker slot CRC"))
    );
}

fn fixture_bytes<'a>(fixtures: &'a [Fixture], name: &str) -> &'a [u8] {
    fixtures
        .iter()
        .find(|fixture| fixture.name == name)
        .unwrap_or_else(|| panic!("frontier fixture is present: {name}"))
        .bytes
        .as_slice()
}

fn validate_fixture(fixture: &Fixture, bytes: &[u8]) {
    match fixture.validation {
        FixtureValidation::StoreMeta => {
            decode_store_meta(bytes).expect("store.meta fixture decodes");
        }
        FixtureValidation::MarkerSlot { physical_slot } => {
            decode_enabled_marker_slot(bytes, physical_slot).expect("marker slot fixture decodes");
        }
        FixtureValidation::MarkerFile => {
            decode_enabled_marker_file(bytes).expect("marker file fixture decodes");
        }
        FixtureValidation::AcknowledgementSlot => {
            decode_acknowledgement_slot(bytes).expect("acknowledgement slot fixture decodes");
        }
        FixtureValidation::AcknowledgementFile => {
            decode_acknowledgement_file(bytes).expect("acknowledgement file fixture decodes");
        }
        FixtureValidation::AcknowledgementFileWithoutAuthoritative => {
            let file = decode_acknowledgement_file(bytes).expect("empty acknowledgement file decodes structurally");
            assert!(file.authoritative().is_none());
        }
        FixtureValidation::CommitSeal => {
            decode_commit_seal(bytes).expect("commit-seal fixture decodes");
        }
        FixtureValidation::Snapshot => {
            decode_snapshot(bytes).expect("snapshot fixture decodes");
        }
        FixtureValidation::TailEvidence { length, crc32 } => {
            assert_eq!(bytes.len(), length, "tail evidence length is exact");
            assert_eq!(crate::mapped_file::retirement::codec::crc32(bytes), crc32);
        }
        FixtureValidation::LedgerFrame { sequence, generation } => validate_frame(bytes, sequence, generation),
        FixtureValidation::InvalidMarker => {
            decode_enabled_marker_file(bytes).expect_err("corrupt marker fixture is rejected");
        }
        FixtureValidation::InvalidAcknowledgement => {
            decode_acknowledgement_slot(bytes).expect_err("corrupt acknowledgement fixture is rejected");
        }
        FixtureValidation::InvalidAcknowledgementFile => {
            decode_acknowledgement_file(bytes).expect_err("invalid acknowledgement history is rejected");
        }
        FixtureValidation::InvalidCommitSeal => {
            decode_commit_seal(bytes).expect_err("corrupt commit-seal fixture is rejected");
        }
        FixtureValidation::InvalidSnapshot => {
            decode_snapshot(bytes).expect_err("corrupt snapshot fixture is rejected");
        }
        FixtureValidation::InvalidLedgerFrame { sequence, generation } => {
            decode_next_frame(bytes, sequence, generation).expect_err("corrupt frame fixture is rejected");
        }
        FixtureValidation::InvalidTypedLedgerFrame { sequence, generation } => {
            let DecodeOutcome::Frame(frame) =
                decode_next_frame(bytes, sequence, generation).expect("invalid typed payload has a valid envelope")
            else {
                panic!("invalid typed fixture must contain one complete frame");
            };
            frame.decode_record().expect_err("invalid typed payload is rejected");
        }
        FixtureValidation::LedgerFrameStream {
            first_sequence,
            generation,
        } => validate_frame_stream(bytes, first_sequence, generation),
        FixtureValidation::InvalidLedgerFrameStream {
            first_sequence,
            generation,
        } => validate_invalid_frame_stream(bytes, first_sequence, generation),
        FixtureValidation::SequenceOverflowStream { generation } => {
            validate_sequence_overflow_stream(bytes, generation)
        }
        FixtureValidation::LedgerFrameThenPartialSeal { sequence, generation } => {
            validate_frame_then_partial_seal(bytes, sequence, generation)
        }
        FixtureValidation::AcknowledgementReconstructionBundle { sequence, generation } => {
            validate_reconstruction_bundle(bytes, sequence, generation)
        }
        FixtureValidation::InvalidAcknowledgedUnitBinding { sequence, generation } => {
            validate_invalid_acknowledged_unit_binding(bytes, sequence, generation)
        }
        FixtureValidation::AcknowledgedLogBundle {
            first_sequence,
            final_sequence,
            generation,
        } => validate_acknowledged_log_bundle(bytes, first_sequence, final_sequence, generation),
        FixtureValidation::AcknowledgedSuffixLossBundle {
            first_sequence,
            generation,
        } => validate_acknowledged_suffix_loss_bundle(bytes, first_sequence, generation),
        FixtureValidation::SealedUnitsWithUnsealedFinal {
            first_sequence,
            generation,
            sealed_units,
        } => validate_sealed_units_with_unsealed_final(bytes, first_sequence, generation, sealed_units),
        FixtureValidation::TruncatedLedgerFrames { sequence, generation } => {
            validate_truncation_set(bytes, sequence, generation)
        }
        FixtureValidation::TruncatedSealedUnit {
            sequence,
            generation,
            frame_length,
        } => validate_sealed_unit_truncation_set(bytes, sequence, generation, frame_length),
    }
}

fn validate_invalid_frame_stream(bytes: &[u8], first_sequence: u64, generation: u64) {
    let DecodeOutcome::Frame(first) =
        decode_next_frame(bytes, first_sequence, generation).expect("first stream frame decodes")
    else {
        panic!("invalid stream fixture starts with a partial frame");
    };
    let rest = &bytes[first.encoded_len()..];
    let expected = first_sequence.checked_add(1).expect("non-overflow fixture sequence");
    decode_next_frame(rest, expected, generation).expect_err("second stream frame is intentionally invalid");
}

fn validate_sequence_overflow_stream(bytes: &[u8], generation: u64) {
    let DecodeOutcome::Frame(first) =
        decode_next_frame(bytes, u64::MAX, generation).expect("maximum sequence frame decodes")
    else {
        panic!("overflow stream starts with a partial frame");
    };
    assert!(
        !bytes[first.encoded_len()..].is_empty(),
        "overflow fixture retains a following frame"
    );
    assert!(u64::MAX.checked_add(1).is_none());
}

fn validate_frame_then_partial_seal(bytes: &[u8], sequence: u64, generation: u64) {
    let DecodeOutcome::Frame(frame) = decode_next_frame(bytes, sequence, generation).expect("frame envelope decodes")
    else {
        panic!("partial-seal fixture must contain a complete frame");
    };
    let seal = &bytes[frame.encoded_len()..];
    assert!(!seal.is_empty());
    assert!(seal.len() < COMMIT_SEAL_LENGTH);
    decode_commit_seal(seal).expect_err("partial seal is not a complete durable witness");
}

struct BundleParts<'a> {
    frame: &'a [u8],
    first_slot: &'a [u8],
    expected_slot: Option<&'a [u8]>,
    seal: &'a [u8],
}

fn bundle_parts(bytes: &[u8], include_torn_slot: bool) -> BundleParts<'_> {
    let frame_length = u32::from_le_bytes(bytes[..4].try_into().expect("bundle frame length")) as usize;
    let frame_start = 4;
    let frame_end = frame_start + frame_length;
    let first_slot_end = frame_end + ACKNOWLEDGEMENT_SLOT_LENGTH;
    let (first_slot, expected_slot, seal_start) = if include_torn_slot {
        let expected_slot_end = first_slot_end + ACKNOWLEDGEMENT_SLOT_LENGTH;
        (
            &bytes[frame_end..first_slot_end],
            Some(&bytes[first_slot_end..expected_slot_end]),
            expected_slot_end,
        )
    } else {
        (&bytes[frame_end..first_slot_end], None, first_slot_end)
    };
    BundleParts {
        frame: &bytes[frame_start..frame_end],
        first_slot,
        expected_slot,
        seal: &bytes[seal_start..],
    }
}

fn validate_reconstruction_bundle(bytes: &[u8], sequence: u64, generation: u64) {
    let parts = bundle_parts(bytes, true);
    decode_acknowledgement_slot(parts.first_slot).expect_err("torn nonzero slot is rejected");
    let expected_slot: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH] = parts
        .expected_slot
        .expect("reconstruction bundle includes expected slot")
        .try_into()
        .expect("expected slot length");
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(expected_slot).expect("reconstructed slot decodes")
    else {
        panic!("reconstructed slot is populated");
    };
    let seal = decode_commit_seal(parts.seal).expect("reconstruction seal decodes");
    let DecodeOutcome::Frame(frame) =
        decode_next_frame(parts.frame, sequence, generation).expect("reconstruction frame decodes")
    else {
        panic!("reconstruction bundle contains a complete frame");
    };
    validate_acknowledged_frame(&frame, parts.frame, 0, &slot, &seal, expected_slot)
        .expect("reconstructed slot binds the exact frame and seal");
}

fn validate_invalid_acknowledged_unit_binding(bytes: &[u8], sequence: u64, generation: u64) {
    let parts = bundle_parts(bytes, false);
    let encoded_slot: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH] =
        parts.first_slot.try_into().expect("acknowledgement slot length");
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(encoded_slot).expect("acknowledgement slot decodes")
    else {
        panic!("acknowledgement slot is populated");
    };
    let seal = decode_commit_seal(parts.seal).expect("mismatched seal remains structurally valid");
    let DecodeOutcome::Frame(frame) =
        decode_next_frame(parts.frame, sequence, generation).expect("acknowledged frame decodes")
    else {
        panic!("binding fixture contains a complete frame");
    };
    validate_acknowledged_frame(&frame, parts.frame, 0, &slot, &seal, encoded_slot)
        .expect_err("slot CRC mismatch breaks the acknowledgement binding");
}

fn validate_acknowledged_log_bundle(bytes: &[u8], first_sequence: u64, final_sequence: u64, generation: u64) {
    let prefix_length = read_bundle_length(bytes, 0, "acknowledged prefix length");
    let frame_length = read_bundle_length(bytes, 4, "acknowledged frame length");
    let prefix_start = 8;
    let prefix_end = prefix_start + prefix_length;
    let frame_end = prefix_end + frame_length;
    let seal_end = frame_end + COMMIT_SEAL_LENGTH;
    let slot_end = seal_end + ACKNOWLEDGEMENT_SLOT_LENGTH;
    assert_eq!(slot_end, bytes.len(), "acknowledged bundle has exact length");

    let (next_sequence, prefix_offset) =
        validate_sealed_unit_stream(&bytes[prefix_start..prefix_end], first_sequence, generation);
    assert_eq!(
        next_sequence, final_sequence,
        "sealed prefix ends immediately before the final frame"
    );
    assert_eq!(prefix_offset, prefix_length as u64);
    let DecodeOutcome::Frame(frame) = decode_next_frame(&bytes[prefix_end..frame_end], final_sequence, generation)
        .expect("final acknowledged frame decodes")
    else {
        panic!("acknowledged bundle contains a complete final frame");
    };
    assert_eq!(frame.encoded_len(), frame_length);
    frame.decode_record().expect("final acknowledged record decodes");
    let seal = decode_commit_seal(&bytes[frame_end..seal_end]).expect("final commit seal decodes");
    let encoded_slot: &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH] = bytes[seal_end..slot_end]
        .try_into()
        .expect("final acknowledgement slot length");
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(encoded_slot).expect("final acknowledgement slot decodes")
    else {
        panic!("final acknowledgement slot is populated");
    };
    validate_acknowledged_frame(
        &frame,
        &bytes[prefix_end..frame_end],
        prefix_length as u64,
        &slot,
        &seal,
        encoded_slot,
    )
    .expect("final frame, acknowledgement slot, and seal are byte-exact bound");
}

fn validate_acknowledged_suffix_loss_bundle(bytes: &[u8], first_sequence: u64, generation: u64) {
    let log_length = read_bundle_length(bytes, 0, "retained log length");
    let log_end = 4 + log_length;
    assert_eq!(
        log_end + 2 * ACKNOWLEDGEMENT_SLOT_LENGTH,
        bytes.len(),
        "suffix-loss bundle has exact length"
    );
    let log = &bytes[4..log_end];
    let (next_sequence, retained_length) = validate_sealed_unit_stream(log, first_sequence, generation);
    assert_eq!(
        next_sequence,
        first_sequence.checked_add(1).expect("fixture sequence advances")
    );
    assert_eq!(retained_length, log_length as u64);
    let acknowledgement =
        decode_acknowledgement_file(&bytes[log_end..]).expect("suffix-loss acknowledgement history decodes");
    let authoritative = acknowledgement
        .authoritative()
        .expect("suffix-loss bundle has an authoritative slot");
    assert_eq!(
        authoritative.frame_sequence,
        first_sequence.checked_add(1).expect("fixture sequence advances")
    );
    assert!(
        authoritative.sealed_log_length().expect("sealed length is bounded") > log_length as u64,
        "authoritative acknowledgement proves a complete missing suffix"
    );
}

fn validate_sealed_units_with_unsealed_final(bytes: &[u8], first_sequence: u64, generation: u64, sealed_units: usize) {
    let mut remaining = bytes;
    let mut sequence = first_sequence;
    for _ in 0..sealed_units {
        let DecodeOutcome::Frame(frame) =
            decode_next_frame(remaining, sequence, generation).expect("sealed prefix frame decodes")
        else {
            panic!("sealed prefix contains a complete frame");
        };
        frame.decode_record().expect("sealed prefix record decodes");
        let frame_length = frame.encoded_len();
        let seal_end = frame_length + COMMIT_SEAL_LENGTH;
        decode_commit_seal(&remaining[frame_length..seal_end]).expect("sealed prefix witness decodes");
        remaining = &remaining[seal_end..];
        sequence = sequence.checked_add(1).expect("fixture sequence advances");
    }
    let DecodeOutcome::Frame(frame) =
        decode_next_frame(remaining, sequence, generation).expect("unsealed final frame decodes")
    else {
        panic!("frontier contains a complete unsealed final frame");
    };
    frame.decode_record().expect("unsealed final record decodes");
    assert_eq!(
        frame.encoded_len(),
        remaining.len(),
        "no bytes follow the unsealed frontier"
    );
}

fn validate_sealed_unit_stream(mut bytes: &[u8], mut sequence: u64, generation: u64) -> (u64, u64) {
    let mut offset = 0_u64;
    while !bytes.is_empty() {
        let DecodeOutcome::Frame(frame) =
            decode_next_frame(bytes, sequence, generation).expect("sealed stream frame decodes")
        else {
            panic!("sealed stream contains a complete frame");
        };
        frame.decode_record().expect("sealed stream record decodes");
        let frame_length = frame.encoded_len();
        let seal_end = frame_length + COMMIT_SEAL_LENGTH;
        let seal = decode_commit_seal(&bytes[frame_length..seal_end]).expect("sealed stream witness decodes");
        assert_eq!(seal.log_generation, generation);
        assert_eq!(seal.frame_sequence, sequence);
        assert_eq!(seal.frame_end_offset, offset + frame_length as u64);
        bytes = &bytes[seal_end..];
        offset = offset
            .checked_add(seal_end as u64)
            .expect("fixture log offset advances");
        sequence = sequence.checked_add(1).expect("fixture sequence advances");
    }
    (sequence, offset)
}

fn read_bundle_length(bytes: &[u8], offset: usize, field: &str) -> usize {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .unwrap_or_else(|_| panic!("{field} is a u32")),
    ) as usize
}

fn validate_frame(bytes: &[u8], sequence: u64, generation: u64) {
    let DecodeOutcome::Frame(frame) =
        decode_next_frame(bytes, sequence, generation).expect("ledger-frame fixture envelope decodes")
    else {
        panic!("ledger-frame fixture must contain a complete frame");
    };
    frame.decode_record().expect("ledger-frame fixture payload decodes");
}

fn validate_frame_stream(mut bytes: &[u8], mut sequence: u64, generation: u64) {
    let mut count = 0;
    while !bytes.is_empty() {
        let DecodeOutcome::Frame(frame) =
            decode_next_frame(bytes, sequence, generation).expect("stream frame envelope decodes")
        else {
            panic!("stream fixture contains a partial frame");
        };
        frame.decode_record().expect("stream frame payload decodes");
        let encoded_len = frame.encoded_len();
        bytes = &bytes[encoded_len..];
        sequence = sequence.checked_add(1).expect("fixture sequence does not overflow");
        count += 1;
    }
    assert!(count >= 4, "retirement chain contains every required durable stage");
}

fn validate_truncation_set(mut bytes: &[u8], sequence: u64, generation: u64) {
    let mut expected_length = 0_usize;
    while !bytes.is_empty() {
        let (length_bytes, rest) = bytes.split_at(2);
        let length = usize::from(u16::from_le_bytes(
            length_bytes.try_into().expect("two-byte truncation length"),
        ));
        assert_eq!(length, expected_length, "truncation corpus is gap-free");
        let (truncated, rest) = rest.split_at(length);
        match (length, decode_next_frame(truncated, sequence, generation)) {
            (0, Ok(DecodeOutcome::EndOfInput)) => {}
            (_, Ok(DecodeOutcome::TrailingPartial(partial))) => {
                assert_eq!(partial.available, length);
                assert!(partial.required > partial.available);
            }
            (_, outcome) => panic!("valid frame prefix at length {length} was not classified partial: {outcome:?}"),
        }
        bytes = rest;
        expected_length += 1;
    }
    assert!(
        expected_length > 40,
        "truncation corpus reaches beyond the fixed header"
    );
}

fn validate_sealed_unit_truncation_set(mut bytes: &[u8], sequence: u64, generation: u64, frame_length: usize) {
    let mut expected_length = 0_usize;
    while !bytes.is_empty() {
        let (length_bytes, rest) = bytes.split_at(2);
        let length = usize::from(u16::from_le_bytes(
            length_bytes.try_into().expect("two-byte sealed-unit length"),
        ));
        assert_eq!(length, expected_length, "sealed-unit truncation corpus is gap-free");
        let (prefix, rest) = rest.split_at(length);
        if length < frame_length {
            match (length, decode_next_frame(prefix, sequence, generation)) {
                (0, Ok(DecodeOutcome::EndOfInput)) => {}
                (_, Ok(DecodeOutcome::TrailingPartial(_))) => {}
                (_, outcome) => panic!("frame prefix at length {length} was not partial: {outcome:?}"),
            }
        } else {
            let DecodeOutcome::Frame(frame) =
                decode_next_frame(prefix, sequence, generation).expect("complete frame prefix decodes")
            else {
                panic!("complete frame prefix was classified partial");
            };
            assert_eq!(frame.encoded_len(), frame_length);
            let seal = &prefix[frame_length..];
            if seal.len() == COMMIT_SEAL_LENGTH {
                decode_commit_seal(seal).expect("complete commit seal decodes");
            } else if !seal.is_empty() {
                decode_commit_seal(seal).expect_err("partial commit seal is not durable");
            }
        }
        bytes = rest;
        expected_length += 1;
    }
    assert_eq!(expected_length, frame_length + COMMIT_SEAL_LENGTH + 1);
}

fn fixture_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("mapped_file_lifecycle")
}

fn write_corpus(directory: &Path, fixtures: &[Fixture], expected_manifest: &str) {
    fs::create_dir_all(directory).expect("create lifecycle fixture directory");
    for fixture in fixtures {
        fs::write(directory.join(&fixture.name), &fixture.bytes).expect("write lifecycle fixture");
    }
    fs::write(directory.join(MANIFEST_NAME), expected_manifest).expect("write lifecycle fixture manifest");
}

fn manifest(fixtures: &[Fixture]) -> String {
    fixtures
        .iter()
        .map(|fixture| format!("{}  {}\n", sha256(&fixture.bytes), fixture.name))
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
