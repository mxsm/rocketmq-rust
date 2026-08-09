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

enum RawSlot {
    Unused,
    Valid {
        slot: AcknowledgementSlot,
        encoded: [u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
    },
    Invalid,
}

pub(super) fn resolve_acknowledgement(
    raw_slots: [&[u8]; 2],
    seal_evidence: &[SealEvidence],
) -> Result<ResolvedAcknowledgement, ReplayError> {
    let mut slots = [classify(raw_slots[0], 0), classify(raw_slots[1], 1)];
    let first_invalid = matches!(&slots[0], RawSlot::Invalid);
    let second_invalid = matches!(&slots[1], RawSlot::Invalid);
    if first_invalid && second_invalid {
        reconstruct_pair(&mut slots, seal_evidence)?;
    } else if first_invalid {
        let replacement = reconstruct_one(0, &slots[1], seal_evidence)?;
        slots[0] = replacement;
    } else if second_invalid {
        let replacement = reconstruct_one(1, &slots[0], seal_evidence)?;
        slots[1] = replacement;
    } else if matches!(&slots[0], RawSlot::Unused) && matches!(&slots[1], RawSlot::Unused) {
        return Err(ReplayError::NoAcknowledgedFrame);
    }

    let encoded = materialize_file(&slots)?;
    let decoded = decode_acknowledgement_file(&encoded).map_err(|_| ReplayError::BrokenAcknowledgementChain)?;
    let authoritative = decoded
        .authoritative()
        .cloned()
        .ok_or(ReplayError::NoAcknowledgedFrame)?;
    for (physical_index, state) in decoded.slots().iter().enumerate() {
        let AcknowledgementSlotState::Populated(slot) = state else {
            continue;
        };
        let start = physical_index * super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
        let encoded_slot: [u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH] = encoded
            [start..start + super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]
            .try_into()
            .map_err(|_| ReplayError::BrokenAcknowledgementChain)?;
        let candidates = seal_evidence
            .iter()
            .filter(|evidence| {
                evidence.generation == slot.log_generation
                    && &evidence.slot == slot
                    && evidence.encoded_slot == encoded_slot
            })
            .count();
        if candidates > 1 {
            return Err(ReplayError::AmbiguousAcknowledgementSealEvidence {
                slot_index: slot.slot_index,
                candidates,
            });
        }
        if candidates == 0 && slot.slot_index != authoritative.slot_index {
            return Err(ReplayError::MissingAcknowledgementSealEvidence {
                slot_index: slot.slot_index,
            });
        }
    }
    let start = usize::from(authoritative.slot_index) * super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
    let encoded_authoritative = encoded[start..start + super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]
        .try_into()
        .map_err(|_| ReplayError::BrokenAcknowledgementChain)?;

    if seal_evidence.iter().any(|evidence| {
        evidence.slot.acknowledgement_epoch > authoritative.acknowledgement_epoch
            && evidence.slot.store_uuid == authoritative.store_uuid
            && evidence.slot.bootstrap_id == authoritative.bootstrap_id
    }) {
        return Err(ReplayError::BrokenAcknowledgementChain);
    }
    Ok(ResolvedAcknowledgement {
        authoritative,
        encoded_authoritative,
    })
}

pub(super) fn validate_unacknowledged_suffix_length(length: usize) -> Result<u32, ReplayError> {
    if length == 0 || length >= super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH {
        return Err(ReplayError::InvalidUnacknowledgedSuffixLength {
            length,
            maximum: super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH,
        });
    }
    u32::try_from(length).map_err(|_| ReplayError::InvalidUnacknowledgedSuffixLength {
        length,
        maximum: super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH,
    })
}

fn classify(raw: &[u8], physical_index: u8) -> RawSlot {
    let Ok(encoded) = <[u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH]>::try_from(raw) else {
        return RawSlot::Invalid;
    };
    match decode_acknowledgement_slot(&encoded) {
        Ok(AcknowledgementSlotState::Unused) => RawSlot::Unused,
        Ok(AcknowledgementSlotState::Populated(slot)) if slot.slot_index == physical_index => {
            RawSlot::Valid { slot, encoded }
        }
        Ok(AcknowledgementSlotState::Populated(_)) | Err(_) => RawSlot::Invalid,
    }
}

fn reconstruct_one(physical_index: u8, other: &RawSlot, evidence: &[SealEvidence]) -> Result<RawSlot, ReplayError> {
    let mut candidate = None;
    let mut candidates = 0_usize;
    let mut highest_chain_epoch = 0_u64;
    for current in evidence
        .iter()
        .filter(|current| current.slot.slot_index == physical_index)
    {
        let chain_epoch = match other {
            RawSlot::Unused if current.slot.acknowledgement_epoch == 1 => Some(1),
            RawSlot::Unused => None,
            RawSlot::Valid { slot, .. } => {
                let newer = slot.acknowledgement_epoch.checked_add(1) == Some(current.slot.acknowledgement_epoch)
                    && slot.frame_sequence.checked_add(1) == Some(current.slot.frame_sequence)
                    && (!slot.activated || current.slot.activated);
                let older = current.slot.acknowledgement_epoch.checked_add(1) == Some(slot.acknowledgement_epoch)
                    && current.slot.frame_sequence.checked_add(1) == Some(slot.frame_sequence)
                    && (!current.slot.activated || slot.activated);
                if slot.store_uuid != current.slot.store_uuid || slot.bootstrap_id != current.slot.bootstrap_id {
                    None
                } else if newer {
                    Some(current.slot.acknowledgement_epoch)
                } else if older {
                    Some(slot.acknowledgement_epoch)
                } else {
                    None
                }
            }
            RawSlot::Invalid => None,
        };
        if let Some(chain_epoch) = chain_epoch {
            if chain_epoch > highest_chain_epoch {
                highest_chain_epoch = chain_epoch;
                candidates = 0;
                candidate = None;
            }
            if chain_epoch != highest_chain_epoch {
                continue;
            }
            candidates = candidates.saturating_add(1);
            candidate.get_or_insert(current);
        }
    }
    match (candidates, candidate) {
        (0, _) => Err(ReplayError::UnreconstructableAcknowledgementSlot {
            slot_index: physical_index,
        }),
        (1, Some(candidate)) => Ok(RawSlot::Valid {
            slot: candidate.slot.clone(),
            encoded: candidate.encoded_slot,
        }),
        _ => Err(ReplayError::AmbiguousAcknowledgementSlot {
            slot_index: physical_index,
            candidates,
        }),
    }
}

fn reconstruct_pair(slots: &mut [RawSlot; 2], evidence: &[SealEvidence]) -> Result<(), ReplayError> {
    let mut by_epoch = BTreeMap::<u64, Vec<&SealEvidence>>::new();
    for current in evidence {
        by_epoch
            .entry(current.slot.acknowledgement_epoch)
            .or_default()
            .push(current);
    }
    let mut selected = None;
    let mut candidates = 0_usize;
    for (newer_epoch, newer_items) in by_epoch.iter().rev() {
        let Some(older_epoch) = newer_epoch.checked_sub(1) else {
            continue;
        };
        let Some(older_items) = by_epoch.get(&older_epoch) else {
            continue;
        };
        for newer in newer_items {
            for older in older_items {
                if older.slot.frame_sequence.checked_add(1) == Some(newer.slot.frame_sequence)
                    && older.slot.slot_index != newer.slot.slot_index
                    && older.slot.store_uuid == newer.slot.store_uuid
                    && older.slot.bootstrap_id == newer.slot.bootstrap_id
                    && (!older.slot.activated || newer.slot.activated)
                {
                    candidates = candidates.saturating_add(1);
                    selected.get_or_insert((*older, *newer));
                }
            }
        }
        if candidates != 0 {
            break;
        }
    }
    let Some((older, newer)) = selected else {
        return Err(ReplayError::UnreconstructableAcknowledgementSlot { slot_index: 0 });
    };
    if candidates != 1 {
        return Err(ReplayError::AmbiguousAcknowledgementSlot {
            slot_index: 0,
            candidates,
        });
    }
    for evidence in [older, newer] {
        slots[usize::from(evidence.slot.slot_index)] = RawSlot::Valid {
            slot: evidence.slot.clone(),
            encoded: evidence.encoded_slot,
        };
    }
    Ok(())
}

fn materialize_file(
    slots: &[RawSlot; 2],
) -> Result<[u8; super::super::codec::ACKNOWLEDGEMENT_FILE_LENGTH], ReplayError> {
    let mut encoded = [0; super::super::codec::ACKNOWLEDGEMENT_FILE_LENGTH];
    for (index, slot) in slots.iter().enumerate() {
        match slot {
            RawSlot::Unused => {}
            RawSlot::Valid { encoded: slot, .. } => {
                let start = index * super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
                encoded[start..start + super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH].copy_from_slice(slot);
            }
            RawSlot::Invalid => {
                return Err(ReplayError::UnreconstructableAcknowledgementSlot {
                    slot_index: index as u8,
                });
            }
        }
    }
    Ok(encoded)
}
