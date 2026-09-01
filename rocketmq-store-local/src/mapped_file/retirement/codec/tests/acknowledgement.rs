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

use super::super::super::identity::StoreUuid;
use super::super::*;
use super::sample_nonce;
use super::sample_store_uuid;
use super::COMPLETED_FRAME;

const ACKNOWLEDGEMENT_SLOT: [u8; 104] = [
    0x52, 0x4d, 0x41, 0x43, 0x01, 0x00, 0x00, 0x00, 0x68, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
    0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
    0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x4d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x35, 0xdb, 0x50, 0xac, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x3b, 0x6b, 0x90, 0xbb,
];

const COMMIT_SEAL: [u8; 72] = [
    0x52, 0x4d, 0x43, 0x53, 0x01, 0x00, 0x00, 0x00, 0x48, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x4d, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22,
    0x35, 0xdb, 0x50, 0x3b, 0x6b, 0x90, 0xbb, 0x00, 0x00, 0x00, 0x00, 0xef, 0x9d, 0xc0, 0xc9,
];

#[test]
fn acknowledgement_slot_and_commit_seal_match_worked_golden_unit() {
    let slot = worked_acknowledgement_slot();

    assert_eq!(encode_acknowledgement_slot(&slot), Ok(ACKNOWLEDGEMENT_SLOT));
    assert_eq!(
        decode_acknowledgement_slot(&ACKNOWLEDGEMENT_SLOT),
        Ok(AcknowledgementSlotState::Populated(slot.clone()))
    );

    let seal = CommitSeal::from_acknowledgement_slot(&slot, &ACKNOWLEDGEMENT_SLOT)
        .expect("golden acknowledgement slot must produce its deterministic seal");
    assert_eq!(encode_commit_seal(&seal), Ok(COMMIT_SEAL));
    assert_eq!(decode_commit_seal(&COMMIT_SEAL), Ok(seal.clone()));
    assert_eq!(
        validate_commit_seal_against_slot(&seal, &slot, &ACKNOWLEDGEMENT_SLOT),
        Ok(())
    );

    let mut unit = Vec::from(COMPLETED_FRAME);
    unit.extend_from_slice(&COMMIT_SEAL);
    assert_eq!(unit.len(), 172);
    assert_eq!(crc32(&COMPLETED_FRAME), 0x50db_3522);
}

#[test]
fn acknowledgement_file_selects_only_a_consecutive_non_regressing_history() {
    let store_uuid = sample_store_uuid();
    let bootstrap_id = sample_nonce(1);
    let first = AcknowledgementSlot {
        slot_index: 0,
        activated: true,
        store_uuid,
        bootstrap_id,
        acknowledgement_epoch: 1,
        marker_epoch: 1,
        log_generation: 0,
        frame_sequence: 1,
        frame_end_offset: 108,
        frame_crc32: 7,
    };
    let second = AcknowledgementSlot {
        slot_index: 1,
        activated: true,
        store_uuid,
        bootstrap_id,
        acknowledgement_epoch: 2,
        marker_epoch: 1,
        log_generation: 0,
        frame_sequence: 2,
        frame_end_offset: 268,
        frame_crc32: 8,
    };
    let mut file = [0_u8; ACKNOWLEDGEMENT_FILE_LENGTH];
    file[..ACKNOWLEDGEMENT_SLOT_LENGTH]
        .copy_from_slice(&encode_acknowledgement_slot(&first).expect("first slot encodes"));
    file[ACKNOWLEDGEMENT_SLOT_LENGTH..]
        .copy_from_slice(&encode_acknowledgement_slot(&second).expect("second slot encodes"));

    let decoded = decode_acknowledgement_file(&file).expect("consecutive slots are valid");
    assert_eq!(decoded.authoritative(), Some(&second));

    let mut regressed = second;
    regressed.activated = false;
    regressed.marker_epoch = 0;
    file[ACKNOWLEDGEMENT_SLOT_LENGTH..]
        .copy_from_slice(&encode_acknowledgement_slot(&regressed).expect("standalone slot encodes"));
    assert_eq!(
        decode_acknowledgement_file(&file),
        Err(CodecViolation::AcknowledgementActivationRegressed)
    );
}

#[test]
fn acknowledgement_file_codec_enforces_position_history_and_torn_slot_rules() {
    let unused = [AcknowledgementSlotState::Unused, AcknowledgementSlotState::Unused];
    let zero_file = encode_acknowledgement_file(&unused).expect("the initial file is all zero");
    assert_eq!(zero_file, [0; ACKNOWLEDGEMENT_FILE_LENGTH]);
    assert_eq!(
        decode_acknowledgement_file(&zero_file)
            .expect("the initial file is valid")
            .authoritative(),
        None
    );

    let first = acknowledgement_slot(0, 1, false, 0);
    let second = acknowledgement_slot(1, 2, false, 0);
    let states = [
        AcknowledgementSlotState::Populated(first.clone()),
        AcknowledgementSlotState::Populated(second.clone()),
    ];
    let encoded = encode_acknowledgement_file(&states).expect("a consecutive positional history encodes");
    let decoded = decode_acknowledgement_file(&encoded).expect("the encoded history decodes");
    assert_eq!(decoded.slots(), &states);
    assert_eq!(decoded.authoritative(), Some(&second));

    let mut swapped = [0_u8; ACKNOWLEDGEMENT_FILE_LENGTH];
    swapped[..ACKNOWLEDGEMENT_SLOT_LENGTH]
        .copy_from_slice(&encode_acknowledgement_slot(&second).expect("slot encodes"));
    swapped[ACKNOWLEDGEMENT_SLOT_LENGTH..].copy_from_slice(&encode_acknowledgement_slot(&first).expect("slot encodes"));
    assert_eq!(
        decode_acknowledgement_file(&swapped),
        Err(CodecViolation::AcknowledgementSlotPositionMismatch {
            physical_slot_index: 0,
            encoded_slot_index: 1,
        })
    );

    let mut missing_history = [0_u8; ACKNOWLEDGEMENT_FILE_LENGTH];
    missing_history[ACKNOWLEDGEMENT_SLOT_LENGTH..]
        .copy_from_slice(&encode_acknowledgement_slot(&second).expect("slot encodes"));
    assert_eq!(
        decode_acknowledgement_file(&missing_history),
        Err(CodecViolation::AcknowledgementHistoryMissing {
            acknowledgement_epoch: 2,
        })
    );

    let mut nonconsecutive = acknowledgement_slot(0, 5, false, 0);
    nonconsecutive.frame_sequence = 5;
    let gap_states = [
        AcknowledgementSlotState::Populated(nonconsecutive),
        AcknowledgementSlotState::Populated(second),
    ];
    assert!(matches!(
        encode_acknowledgement_file(&gap_states),
        Err(CodecViolation::NonConsecutiveAcknowledgementEpochs { .. })
    ));

    let mut torn = encoded;
    torn[ACKNOWLEDGEMENT_SLOT_LENGTH + 88] ^= 1;
    assert!(matches!(
        decode_acknowledgement_file(&torn),
        Err(CodecViolation::AcknowledgementSlotCrcMismatch { .. })
    ));
}

#[test]
fn acknowledgement_slot_and_seal_reject_structural_and_crc_corruption() {
    assert!(matches!(
        decode_acknowledgement_slot(&ACKNOWLEDGEMENT_SLOT[..103]),
        Err(CodecViolation::InvalidFixedStructureLength {
            structure: "acknowledgement slot",
            expected: ACKNOWLEDGEMENT_SLOT_LENGTH,
            actual: 103,
        })
    ));

    let mut bad_slot_magic = ACKNOWLEDGEMENT_SLOT;
    bad_slot_magic[0] ^= 1;
    assert!(matches!(
        decode_acknowledgement_slot(&bad_slot_magic),
        Err(CodecViolation::InvalidAcknowledgementMagic { .. })
    ));

    let mut bad_slot_flags = ACKNOWLEDGEMENT_SLOT;
    bad_slot_flags[11] |= 0x80;
    assert_eq!(
        decode_acknowledgement_slot(&bad_slot_flags),
        Err(CodecViolation::InvalidAcknowledgementFlags { flags: 0x81 })
    );

    let mut bad_sealed_length = ACKNOWLEDGEMENT_SLOT;
    bad_sealed_length[92..100].copy_from_slice(&173_u64.to_le_bytes());
    assert_eq!(
        decode_acknowledgement_slot(&bad_sealed_length),
        Err(CodecViolation::SealedLogLengthMismatch {
            expected: 172,
            actual: 173,
        })
    );

    let mut bad_slot_crc = ACKNOWLEDGEMENT_SLOT;
    bad_slot_crc[100] ^= 1;
    assert!(matches!(
        decode_acknowledgement_slot(&bad_slot_crc),
        Err(CodecViolation::AcknowledgementSlotCrcMismatch { .. })
    ));

    assert!(matches!(
        decode_commit_seal(&COMMIT_SEAL[..71]),
        Err(CodecViolation::InvalidFixedStructureLength {
            structure: "commit seal",
            expected: COMMIT_SEAL_LENGTH,
            actual: 71,
        })
    ));

    let mut bad_seal_reserved = COMMIT_SEAL;
    bad_seal_reserved[64] = 1;
    assert!(matches!(
        decode_commit_seal(&bad_seal_reserved),
        Err(CodecViolation::NonZeroReserved {
            field: "commit_seal_reserved",
            value: 1,
        })
    ));

    let mut bad_seal_crc = COMMIT_SEAL;
    bad_seal_crc[68] ^= 1;
    assert!(matches!(
        decode_commit_seal(&bad_seal_crc),
        Err(CodecViolation::CommitSealCrcMismatch { .. })
    ));
}

#[test]
fn acknowledged_frame_slot_and_seal_bind_byte_exactly() {
    let DecodeOutcome::Frame(frame) = decode_next_frame(&COMPLETED_FRAME, 100, 2).expect("golden frame decodes") else {
        panic!("golden frame must be complete");
    };
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(&ACKNOWLEDGEMENT_SLOT).expect("golden slot decodes")
    else {
        panic!("golden slot must be populated");
    };
    let seal = decode_commit_seal(&COMMIT_SEAL).expect("golden seal decodes");

    assert_eq!(
        validate_acknowledged_frame(&frame, &COMPLETED_FRAME, 0, &slot, &seal, &ACKNOWLEDGEMENT_SLOT,),
        Ok(())
    );

    let mut wrong_end = slot.clone();
    wrong_end.frame_end_offset += 1;
    assert_eq!(
        validate_acknowledged_frame(&frame, &COMPLETED_FRAME, 0, &wrong_end, &seal, &ACKNOWLEDGEMENT_SLOT,),
        Err(CodecViolation::AcknowledgedFrameBindingMismatch {
            field: "frame_end_offset",
        })
    );
    assert_eq!(
        validate_acknowledged_frame(&frame, &COMPLETED_FRAME, u64::MAX, &slot, &seal, &ACKNOWLEDGEMENT_SLOT,),
        Err(CodecViolation::AcknowledgedFrameOffsetOverflow)
    );
}

#[test]
fn acknowledgement_encoder_rejects_invalid_identity_bounds_and_epoch_overflow() {
    let mut slot = worked_acknowledgement_slot();
    slot.acknowledgement_epoch = 0;
    assert_eq!(
        encode_acknowledgement_slot(&slot),
        Err(CodecViolation::ZeroAcknowledgementEpoch)
    );

    slot.acknowledgement_epoch = 77;
    slot.slot_index = 2;
    assert_eq!(
        encode_acknowledgement_slot(&slot),
        Err(CodecViolation::InvalidAcknowledgementSlotIndex { slot_index: 2 })
    );

    slot.slot_index = 0;
    slot.activated = false;
    assert_eq!(
        encode_acknowledgement_slot(&slot),
        Err(CodecViolation::AcknowledgementActivationMarkerMismatch)
    );

    slot.activated = true;
    slot.frame_end_offset = u64::MAX;
    assert_eq!(
        encode_acknowledgement_slot(&slot),
        Err(CodecViolation::SealedLogLengthOverflow)
    );

    slot.frame_end_offset = 100;
    slot.acknowledgement_epoch = u64::MAX;
    assert_eq!(
        slot.next_acknowledgement_epoch(),
        Err(CodecViolation::AcknowledgementEpochOverflow)
    );
}

fn worked_acknowledgement_slot() -> AcknowledgementSlot {
    AcknowledgementSlot {
        slot_index: 0,
        activated: true,
        store_uuid: StoreUuid::new(std::array::from_fn(|index| index as u8)).expect("golden UUID is nonzero"),
        bootstrap_id: std::array::from_fn(|index| index as u8 + 0x10),
        acknowledgement_epoch: 77,
        marker_epoch: 5,
        log_generation: 2,
        frame_sequence: 100,
        frame_end_offset: 100,
        frame_crc32: 0x50db_3522,
    }
}

fn acknowledgement_slot(
    slot_index: u8,
    acknowledgement_epoch: u64,
    activated: bool,
    marker_epoch: u64,
) -> AcknowledgementSlot {
    AcknowledgementSlot {
        slot_index,
        activated,
        store_uuid: sample_store_uuid(),
        bootstrap_id: sample_nonce(1),
        acknowledgement_epoch,
        marker_epoch,
        log_generation: 0,
        frame_sequence: acknowledgement_epoch,
        frame_end_offset: acknowledgement_epoch * 100,
        frame_crc32: acknowledgement_epoch as u32,
    }
}
