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

pub(super) fn parse_generation<'a>(
    bytes: GenerationBytes<'a>,
    meta: &StoreMeta,
    limits: ReplayLimits,
    evidence: &mut Vec<SealEvidence>,
) -> Result<ParsedGeneration<'a>, ReplayViolation> {
    let snapshot = decode_snapshot(bytes.snapshot).map_err(ReplayViolation::Snapshot)?;
    if bytes.generation != snapshot.generation
        || snapshot.log_generation != bytes.generation
        || snapshot.store_uuid != meta.store_uuid
    {
        return Err(ReplayViolation::GenerationBindingMismatch);
    }
    let evidence_start = evidence.len();
    let mut offset = 0_usize;
    let mut expected_sequence = if bytes.generation == 0 {
        1
    } else {
        snapshot
            .base_sequence
            .checked_add(1)
            .ok_or(ReplayViolation::GenerationBindingMismatch)?
    };
    let mut tail = None;

    while offset < bytes.log.len() {
        let suffix = bytes.log.get(offset..).ok_or(ReplayViolation::BrokenSealChain {
            generation: bytes.generation,
        })?;
        let outcome = decode_next_frame(suffix, expected_sequence, bytes.generation).map_err(|source| {
            ReplayViolation::InvalidLog {
                generation: bytes.generation,
                offset: offset as u64,
                source,
            }
        })?;
        let frame = match outcome {
            DecodeOutcome::EndOfInput => break,
            DecodeOutcome::TrailingPartial(_) => {
                validate_unacknowledged_suffix_length(suffix.len())?;
                tail = Some(LogTail {
                    offset: offset as u64,
                    bytes: suffix.to_vec(),
                    complete_frame: None,
                });
                break;
            }
            DecodeOutcome::Frame(frame) => frame,
        };
        let frame_length = frame.encoded_len();
        let frame_end = offset
            .checked_add(frame_length)
            .ok_or(ReplayViolation::BrokenSealChain {
                generation: bytes.generation,
            })?;
        let encoded_frame = bytes
            .log
            .get(offset..frame_end)
            .ok_or(ReplayViolation::BrokenSealChain {
                generation: bytes.generation,
            })?;
        let after_frame = bytes.log.get(frame_end..).ok_or(ReplayViolation::BrokenSealChain {
            generation: bytes.generation,
        })?;
        if after_frame.len() < super::super::codec::COMMIT_SEAL_LENGTH {
            let record = frame.decode_record().map_err(|source| ReplayViolation::InvalidLog {
                generation: bytes.generation,
                offset: offset as u64,
                source,
            })?;
            tail = Some(complete_frame_tail(
                offset,
                frame_end,
                encoded_frame,
                record,
                after_frame,
                bytes.log,
            )?);
            break;
        }
        let encoded_seal = &after_frame[..super::super::codec::COMMIT_SEAL_LENGTH];
        let seal = decode_commit_seal(encoded_seal).map_err(|source| ReplayViolation::InvalidLog {
            generation: bytes.generation,
            offset: frame_end as u64,
            source,
        })?;
        if evidence.len() >= limits.max_sealed_units {
            return Err(ReplayViolation::LimitExceeded {
                limit: "sealed_units",
                actual: evidence.len().saturating_add(1),
                maximum: limits.max_sealed_units,
            });
        }
        let record = frame.decode_record().map_err(|source| ReplayViolation::InvalidLog {
            generation: bytes.generation,
            offset: offset as u64,
            source,
        })?;
        let slot = slot_from_seal(&seal, meta)?;
        let encoded_slot = encode_acknowledgement_slot(&slot).map_err(|source| ReplayViolation::InvalidLog {
            generation: bytes.generation,
            offset: frame_end as u64,
            source,
        })?;
        validate_acknowledged_frame(&frame, encoded_frame, offset as u64, &slot, &seal, &encoded_slot).map_err(
            |source| ReplayViolation::InvalidLog {
                generation: bytes.generation,
                offset: frame_end as u64,
                source,
            },
        )?;
        validate_seal_progression(bytes.generation, record.as_ref(), evidence, evidence_start, &slot)?;
        let sealed_log_length = slot.sealed_log_length().map_err(|source| ReplayViolation::InvalidLog {
            generation: bytes.generation,
            offset: frame_end as u64,
            source,
        })?;
        evidence.push(SealEvidence {
            slot,
            encoded_slot,
            generation: bytes.generation,
            sealed_log_length,
            frame_start_offset: offset as u64,
            encoded_frame: encoded_frame.to_vec(),
            record,
        });
        offset =
            frame_end
                .checked_add(super::super::codec::COMMIT_SEAL_LENGTH)
                .ok_or(ReplayViolation::BrokenSealChain {
                    generation: bytes.generation,
                })?;
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(ReplayViolation::BrokenSealChain {
                generation: bytes.generation,
            })?;
    }
    Ok(ParsedGeneration {
        bytes,
        snapshot,
        evidence_range: evidence_start..evidence.len(),
        tail,
    })
}

fn complete_frame_tail(
    frame_start: usize,
    frame_end: usize,
    encoded_frame: &[u8],
    record: Option<LedgerRecord>,
    following: &[u8],
    log: &[u8],
) -> Result<LogTail, ReplayViolation> {
    let suffix = log
        .get(frame_start..)
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?;
    validate_unacknowledged_suffix_length(suffix.len())?;
    Ok(LogTail {
        offset: frame_start as u64,
        bytes: suffix.to_vec(),
        complete_frame: Some(TrailingFrame {
            sequence: record_sequence(encoded_frame)?,
            frame_end_offset: frame_end as u64,
            encoded_frame: encoded_frame.to_vec(),
            record,
            following_bytes: following.to_vec(),
        }),
    })
}

fn record_sequence(encoded_frame: &[u8]) -> Result<u64, ReplayViolation> {
    let bytes: [u8; 8] = encoded_frame
        .get(20..28)
        .ok_or(ReplayViolation::AuthoritativeFrameMissing)?
        .try_into()
        .map_err(|_| ReplayViolation::AuthoritativeFrameMissing)?;
    Ok(u64::from_le_bytes(bytes))
}

fn slot_from_seal(seal: &CommitSeal, meta: &StoreMeta) -> Result<AcknowledgementSlot, ReplayViolation> {
    let slot = AcknowledgementSlot {
        slot_index: seal.acknowledgement_slot_index,
        activated: seal.activated,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        acknowledgement_epoch: seal.acknowledgement_epoch,
        marker_epoch: seal.marker_epoch,
        log_generation: seal.log_generation,
        frame_sequence: seal.frame_sequence,
        frame_end_offset: seal.frame_end_offset,
        frame_crc32: seal.frame_crc32,
    };
    let encoded = encode_acknowledgement_slot(&slot).map_err(|source| ReplayViolation::InvalidLog {
        generation: seal.log_generation,
        offset: seal.frame_end_offset,
        source,
    })?;
    let expected =
        CommitSeal::from_acknowledgement_slot(&slot, &encoded).map_err(|source| ReplayViolation::InvalidLog {
            generation: seal.log_generation,
            offset: seal.frame_end_offset,
            source,
        })?;
    if &expected != seal {
        return Err(ReplayViolation::BrokenSealChain {
            generation: seal.log_generation,
        });
    }
    Ok(slot)
}

fn validate_seal_progression(
    generation: u64,
    first_record: Option<&LedgerRecord>,
    evidence: &[SealEvidence],
    generation_start: usize,
    slot: &AcknowledgementSlot,
) -> Result<(), ReplayViolation> {
    if let Some(previous) = evidence.get(generation_start..).and_then(|items| items.last()) {
        if previous.slot.acknowledgement_epoch.checked_add(1) != Some(slot.acknowledgement_epoch)
            || previous.slot.frame_sequence.checked_add(1) != Some(slot.frame_sequence)
        {
            return Err(ReplayViolation::BrokenSealChain { generation });
        }
        return Ok(());
    }
    if generation == 0 {
        if slot.acknowledgement_epoch != 1 || slot.frame_sequence != 1 {
            return Err(ReplayViolation::BrokenSealChain { generation });
        }
        return Ok(());
    }
    let Some(LedgerRecord::LogOpened {
        predecessor_acknowledgement_epoch,
        ..
    }) = first_record
    else {
        return Err(ReplayViolation::BrokenSealChain { generation });
    };
    if predecessor_acknowledgement_epoch.checked_add(1) != Some(slot.acknowledgement_epoch) {
        return Err(ReplayViolation::BrokenSealChain { generation });
    }
    Ok(())
}
