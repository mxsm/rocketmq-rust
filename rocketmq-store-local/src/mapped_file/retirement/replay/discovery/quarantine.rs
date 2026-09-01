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

use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::decode_next_frame;
use crate::mapped_file::retirement::codec::DecodeOutcome;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::OpenReason;
use crate::mapped_file::retirement::codec::MAX_SEALED_RECORD_UNIT_LENGTH;
use crate::mapped_file::retirement::sidecar::decode_snapshot;
use crate::mapped_file::retirement::sidecar::SnapshotMode;

use super::corruption;
use super::limit_error;
use super::map_codec_error;
use super::map_sidecar_error;
use super::platform;
use super::read_exact_file;
use super::ManagedLifecycleReadError;
use super::ManagedLifecycleReadSource;
use super::OwnedGeneration;
use super::GENERATION_DIGITS;
use super::LOG_PREFIX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TailEvidenceCoordinates {
    pub(super) predecessor_generation: u64,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) crc32: u32,
}

#[derive(Debug)]
pub(super) struct QuarantinePlan {
    tails: Vec<(usize, TailEvidenceCoordinates)>,
}

pub(super) struct QuarantineRead {
    pub(super) first: platform::InventorySnapshot,
    pub(super) opened: Vec<platform::OpenedEntry>,
    pub(super) plan: QuarantinePlan,
}

pub(super) struct OwnedTailEvidence {
    pub(super) coordinates: TailEvidenceCoordinates,
    pub(super) bytes: Vec<u8>,
}

impl QuarantinePlan {
    pub(super) fn parse(
        root: &platform::InventorySnapshot,
        quarantine: &platform::InventorySnapshot,
    ) -> Result<Self, ManagedLifecycleReadError> {
        let mut case_folded = BTreeMap::<String, &str>::new();
        let mut physical_files = BTreeMap::<(u64, [u8; 16]), &str>::new();
        for entry in &root.entries {
            if entry.kind == platform::EntryKind::File {
                physical_files.insert((entry.stamp.volume, entry.stamp.file_id), &entry.name);
            }
        }
        let mut tails = Vec::new();
        tails
            .try_reserve_exact(quarantine.entries.len())
            .map_err(|_| limit_error("quarantine entries", quarantine.entries.len(), quarantine.entries.len()))?;
        for (index, entry) in quarantine.entries.iter().enumerate() {
            if entry.kind == platform::EntryKind::Reparse {
                return Err(ManagedLifecycleReadError::new(
                    ManagedLifecycleReadSource::UnsafeNamespace(format!(
                        "quarantine entry {:?} is a symlink or reparse point",
                        entry.name
                    )),
                ));
            }
            if entry.kind != platform::EntryKind::File {
                return Err(ManagedLifecycleReadError::new(
                    ManagedLifecycleReadSource::UnsafeNamespace(format!(
                        "quarantine entry {:?} is not a regular file",
                        entry.name
                    )),
                ));
            }
            if entry.stamp.link_count != 1 {
                return Err(ManagedLifecycleReadError::new(
                    ManagedLifecycleReadSource::UnsafeNamespace(format!(
                        "quarantine file {:?} has {} hard links; exactly one is required",
                        entry.name, entry.stamp.link_count
                    )),
                ));
            }
            let folded = entry.name.to_ascii_lowercase();
            if let Some(previous) = case_folded.insert(folded, &entry.name) {
                return Err(corruption(format!(
                    "case-fold collision between quarantine files {previous:?} and {:?}",
                    entry.name
                )));
            }
            let physical_id = (entry.stamp.volume, entry.stamp.file_id);
            if let Some(previous) = physical_files.insert(physical_id, &entry.name) {
                return Err(ManagedLifecycleReadError::new(
                    ManagedLifecycleReadSource::UnsafeNamespace(format!(
                        "lifecycle/quarantine files {previous:?} and {:?} are hard-link aliases",
                        entry.name
                    )),
                ));
            }
            if let Some(coordinates) = tail_evidence_coordinates(&entry.name)? {
                if coordinates.length == 0
                    || coordinates.length >= MAX_SEALED_RECORD_UNIT_LENGTH as u64
                    || entry.stamp.length != coordinates.length
                {
                    return Err(corruption(format!(
                        "tail evidence {:?} length does not match its bounded filename",
                        entry.name
                    )));
                }
                tails.push((index, coordinates));
            }
        }
        Ok(Self { tails })
    }
}

fn tail_evidence_coordinates(name: &str) -> Result<Option<TailEvidenceCoordinates>, ManagedLifecycleReadError> {
    let Some(rest) = name.strip_prefix(LOG_PREFIX) else {
        return Ok(None);
    };
    if !rest.contains(".tail.") {
        return Ok(None);
    }
    let Some((generation, rest)) = rest.split_once(".tail.o") else {
        return Err(corruption(format!("malformed tail evidence name {name:?}")));
    };
    let Some((offset, rest)) = rest.split_once(".l") else {
        return Err(corruption(format!("malformed tail evidence name {name:?}")));
    };
    let Some((length, rest)) = rest.split_once(".c") else {
        return Err(corruption(format!("malformed tail evidence name {name:?}")));
    };
    let Some(crc) = rest.strip_suffix(".bin") else {
        return Err(corruption(format!("malformed tail evidence name {name:?}")));
    };
    let predecessor_generation = fixed_decimal(generation, GENERATION_DIGITS)
        .ok_or_else(|| corruption(format!("malformed tail evidence generation in {name:?}")))?;
    let offset =
        fixed_decimal(offset, 20).ok_or_else(|| corruption(format!("malformed tail evidence offset in {name:?}")))?;
    let length =
        fixed_decimal(length, 20).ok_or_else(|| corruption(format!("malformed tail evidence length in {name:?}")))?;
    if crc.len() != 8
        || !crc
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(corruption(format!("malformed tail evidence CRC in {name:?}")));
    }
    let crc32 =
        u32::from_str_radix(crc, 16).map_err(|_| corruption(format!("malformed tail evidence CRC in {name:?}")))?;
    Ok(Some(TailEvidenceCoordinates {
        predecessor_generation,
        offset,
        length,
        crc32,
    }))
}

fn fixed_decimal(value: &str, width: usize) -> Option<u64> {
    if value.len() != width || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let parsed = value.parse::<u64>().ok()?;
    (format!("{parsed:0width$}") == value).then_some(parsed)
}

pub(super) fn read_tail_evidence(
    plan: &QuarantinePlan,
    opened: &mut [platform::OpenedEntry],
    total_read: &mut u64,
    max_total_read_bytes: u64,
) -> Result<Vec<OwnedTailEvidence>, ManagedLifecycleReadError> {
    let mut evidence = Vec::new();
    evidence
        .try_reserve_exact(plan.tails.len())
        .map_err(|_| limit_error("tail evidence", plan.tails.len(), plan.tails.len()))?;
    for (index, coordinates) in &plan.tails {
        let expected_length = usize::try_from(coordinates.length).map_err(|_| {
            limit_error(
                "tail evidence bytes",
                coordinates.length,
                MAX_SEALED_RECORD_UNIT_LENGTH - 1,
            )
        })?;
        let bytes = read_exact_file(
            &mut opened[*index],
            Some(expected_length),
            (MAX_SEALED_RECORD_UNIT_LENGTH - 1) as u64,
            total_read,
            max_total_read_bytes,
        )?;
        if crc32(&bytes) != coordinates.crc32 {
            return Err(corruption("tail evidence content CRC differs from its filename"));
        }
        evidence.push(OwnedTailEvidence {
            coordinates: *coordinates,
            bytes,
        });
    }
    Ok(evidence)
}

pub(super) fn validate_required_tail_evidence(
    generations: &[OwnedGeneration],
    evidence: &[OwnedTailEvidence],
) -> Result<(), ManagedLifecycleReadError> {
    for generation in generations {
        let snapshot = decode_snapshot(&generation.snapshot).map_err(map_sidecar_error)?;
        if snapshot.mode != SnapshotMode::TailRepair {
            continue;
        }
        let expected_sequence = snapshot
            .base_sequence
            .checked_add(1)
            .ok_or_else(|| corruption("tail-repair LogOpened sequence overflow"))?;
        let frame = match decode_next_frame(&generation.log, expected_sequence, generation.generation)
            .map_err(map_codec_error)?
        {
            DecodeOutcome::Frame(frame) => frame,
            DecodeOutcome::EndOfInput | DecodeOutcome::TrailingPartial(_) => {
                return Err(corruption(
                    "tail-repair generation does not contain its complete LogOpened frame",
                ));
            }
        };
        let record = frame.decode_record().map_err(map_codec_error)?;
        let Some(LedgerRecord::LogOpened {
            predecessor_log_generation,
            validated_prefix_length,
            unacknowledged_suffix_length,
            unacknowledged_suffix_crc32,
            open_reason: OpenReason::TailRepair,
            ..
        }) = record
        else {
            return Err(corruption(
                "tail-repair snapshot is not paired with a tail-repair LogOpened frame",
            ));
        };
        let expected = TailEvidenceCoordinates {
            predecessor_generation: predecessor_log_generation,
            offset: validated_prefix_length,
            length: u64::from(unacknowledged_suffix_length),
            crc32: unacknowledged_suffix_crc32,
        };
        let mut matches = evidence.iter().filter(|candidate| candidate.coordinates == expected);
        let candidate = matches
            .next()
            .ok_or_else(|| corruption("tail-repair generation is missing its exact quarantine evidence"))?;
        if matches.next().is_some() {
            return Err(corruption(
                "tail-repair generation has ambiguous duplicate quarantine evidence",
            ));
        }
        if let Some(predecessor) = generations
            .iter()
            .find(|candidate| candidate.generation == predecessor_log_generation)
        {
            let start = usize::try_from(validated_prefix_length)
                .map_err(|_| corruption("tail evidence offset does not fit memory"))?;
            let end = start
                .checked_add(candidate.bytes.len())
                .ok_or_else(|| corruption("tail evidence range overflow"))?;
            if predecessor.log.get(start..end) != Some(candidate.bytes.as_slice()) || end != predecessor.log.len() {
                return Err(corruption(
                    "tail evidence bytes differ from the immutable predecessor suffix",
                ));
            }
        }
    }
    Ok(())
}
