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
use crate::mapped_file::retirement::codec;
use crate::mapped_file::retirement::platform;
use crate::mapped_file::retirement::writer;

pub(super) fn merge_observed_replacement_key(
    ticket_id: TicketId,
    recorded: Option<PhysicalFileKey>,
    observed: Option<PhysicalFileKey>,
) -> Result<Option<PhysicalFileKey>, RegistryViolation> {
    match (recorded, observed) {
        (Some(recorded), Some(observed)) if recorded != observed => {
            Err(RegistryViolation::NamespaceProofMismatch { ticket_id })
        }
        (Some(key), _) | (_, Some(key)) => Ok(Some(key)),
        (None, None) => Ok(None),
    }
}

pub(in crate::mapped_file::retirement) fn commit_writer_intent<O>(
    intent: RetirementIntentAppend<'_, O>,
    proof: writer::WriterDurabilityProof,
) -> Result<DurableRetirementToken<O>, RegistryViolation> {
    let (
        record,
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    ) = proof.into_parts();
    let binding = RetirementIntentBinding::from_record(record)?;
    let durability = DurabilityCoordinates::verified(
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    )?;
    intent.commit(DurableIntentEvidence {
        binding,
        durability,
        source: DurableEvidenceSource::Writer,
    })
}

/// Consumes an exact handoff capability and commits its immediately succeeding durable stage.
pub(in crate::mapped_file::retirement) fn commit_writer_logical_removed<O>(
    capability: RetirementHandoffCapability<O>,
    proof: writer::WriterDurabilityProof,
) -> Result<LogicalRemovedCapability<O>, RegistryViolation> {
    let (
        record,
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    ) = proof.into_parts();
    let ticket_id = capability.binding.ticket_id();
    if record != capability.logical_removed_record() {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    let durability = DurabilityCoordinates::verified(
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    )?;
    let authority = Arc::clone(&capability.authority);
    authority.commit_logical_removed(capability, durability)
}

pub(in crate::mapped_file::retirement) fn superseded_path_record<O>(
    capability: &LogicalRemovedCapability<O>,
    observed_replacement_key: PhysicalFileKey,
) -> Result<codec::LedgerRecord, RegistryViolation> {
    let binding = capability.binding();
    merge_observed_replacement_key(
        binding.ticket_id(),
        capability.observed_replacement_key,
        Some(observed_replacement_key),
    )?;
    Ok(codec::LedgerRecord::SupersededPath {
        ticket_id: binding.ticket_id(),
        incarnation: binding.incarnation(),
        expected_target_key: binding.target_key(),
        observed_replacement_key,
        canonical_path: binding.canonical_path().clone(),
    })
}

pub(in crate::mapped_file::retirement) fn commit_writer_superseded_path_after_logical<O>(
    capability: LogicalRemovedCapability<O>,
    proof: writer::WriterDurabilityProof,
) -> Result<LogicalRemovedCapability<O>, RegistryViolation> {
    let (
        record,
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    ) = proof.into_parts();
    let ticket_id = capability.binding.ticket_id();
    let codec::LedgerRecord::SupersededPath {
        ticket_id: record_ticket,
        incarnation,
        expected_target_key,
        observed_replacement_key,
        canonical_path,
    } = record
    else {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    };
    if record_ticket != ticket_id
        || incarnation != capability.binding.incarnation()
        || expected_target_key != capability.binding.target_key()
        || canonical_path != *capability.binding.canonical_path()
    {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    let durability = DurabilityCoordinates::verified(
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    )?;
    let authority = Arc::clone(&capability.authority);
    authority.commit_superseded_path_after_logical(capability, durability, observed_replacement_key)
}

pub(in crate::mapped_file::retirement) fn namespace_absent_record<O>(
    capability: &LogicalRemovedCapability<O>,
    proof: &platform::NamespaceAbsenceProof,
    observation_time_ns: u64,
) -> Result<codec::LedgerRecord, RegistryViolation> {
    let request = proof.request();
    let binding = capability.binding();
    if !namespace_request_matches(binding, request) {
        return Err(RegistryViolation::NamespaceProofMismatch {
            ticket_id: binding.ticket_id(),
        });
    }
    let observed_replacement_key = merge_observed_replacement_key(
        binding.ticket_id(),
        capability.observed_replacement_key,
        proof.replacement_key(),
    )?;
    Ok(codec::LedgerRecord::NamespaceAbsent {
        ticket_id: binding.ticket_id(),
        incarnation: binding.incarnation(),
        replacement_observed: observed_replacement_key.is_some(),
        observation_time_ns,
        target_key: binding.target_key(),
        canonical_path: binding.canonical_path().clone(),
        tombstone_path: None,
    })
}

pub(in crate::mapped_file::retirement) fn tombstoned_record<O>(
    capability: &LogicalRemovedCapability<O>,
    proof: &platform::NamespaceTombstoneProof,
) -> Result<codec::LedgerRecord, RegistryViolation> {
    let request = proof.request();
    let binding = capability.binding();
    if !namespace_request_matches(binding, request) {
        return Err(RegistryViolation::NamespaceProofMismatch {
            ticket_id: binding.ticket_id(),
        });
    }
    merge_observed_replacement_key(
        binding.ticket_id(),
        capability.observed_replacement_key,
        proof.replacement_key(),
    )?;
    Ok(codec::LedgerRecord::Tombstoned {
        ticket_id: binding.ticket_id(),
        incarnation: binding.incarnation(),
        target_key: binding.target_key(),
        retirement_nonce: binding.retirement_nonce(),
        canonical_path: binding.canonical_path().clone(),
        tombstone_path: request.tombstone_path().clone(),
    })
}

pub(in crate::mapped_file::retirement) fn commit_writer_tombstoned<O>(
    capability: LogicalRemovedCapability<O>,
    proof: writer::WriterDurabilityProof,
    observed_replacement_key: Option<PhysicalFileKey>,
) -> Result<TombstonedCapability<O>, RegistryViolation> {
    let (
        record,
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    ) = proof.into_parts();
    let ticket_id = capability.binding.ticket_id();
    let codec::LedgerRecord::Tombstoned {
        ticket_id: record_ticket,
        incarnation,
        target_key,
        retirement_nonce,
        canonical_path,
        tombstone_path,
    } = &record
    else {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    };
    if *record_ticket != ticket_id
        || *incarnation != capability.binding.incarnation()
        || *target_key != capability.binding.target_key()
        || *retirement_nonce != capability.binding.retirement_nonce()
        || canonical_path != capability.binding.canonical_path()
    {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    let durability = DurabilityCoordinates::verified(
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    )?;
    let authority = Arc::clone(&capability.authority);
    authority.commit_tombstoned(capability, durability, tombstone_path.clone(), observed_replacement_key)
}

pub(in crate::mapped_file::retirement) fn namespace_absent_after_tombstone_record<O>(
    capability: &TombstonedCapability<O>,
    proof: &platform::NamespaceAbsenceProof,
    observation_time_ns: u64,
) -> Result<codec::LedgerRecord, RegistryViolation> {
    let request = proof.request();
    let binding = capability.binding();
    if !namespace_request_matches(binding, request) || request.tombstone_path() != capability.tombstone_path() {
        return Err(RegistryViolation::NamespaceProofMismatch {
            ticket_id: binding.ticket_id(),
        });
    }
    let observed_replacement_key = merge_observed_replacement_key(
        binding.ticket_id(),
        capability.observed_replacement_key,
        proof.replacement_key(),
    )?;
    Ok(codec::LedgerRecord::NamespaceAbsent {
        ticket_id: binding.ticket_id(),
        incarnation: binding.incarnation(),
        replacement_observed: observed_replacement_key.is_some(),
        observation_time_ns,
        target_key: binding.target_key(),
        canonical_path: binding.canonical_path().clone(),
        tombstone_path: Some(capability.tombstone_path.clone()),
    })
}

pub(in crate::mapped_file::retirement) fn commit_writer_namespace_absent_after_tombstone<O>(
    capability: TombstonedCapability<O>,
    proof: writer::WriterDurabilityProof,
    observed_replacement_key: Option<PhysicalFileKey>,
) -> Result<NamespaceAbsentCapability<O>, RegistryViolation> {
    let (
        record,
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    ) = proof.into_parts();
    let ticket_id = capability.binding.ticket_id();
    let codec::LedgerRecord::NamespaceAbsent {
        ticket_id: record_ticket,
        incarnation,
        replacement_observed,
        target_key,
        canonical_path,
        tombstone_path,
        ..
    } = &record
    else {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    };
    if *record_ticket != ticket_id
        || *incarnation != capability.binding.incarnation()
        || *target_key != capability.binding.target_key()
        || canonical_path != capability.binding.canonical_path()
        || tombstone_path.as_ref() != Some(&capability.tombstone_path)
    {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    let durability = DurabilityCoordinates::verified(
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    )?;
    let authority = Arc::clone(&capability.authority);
    let expected_replacement_observed =
        merge_observed_replacement_key(ticket_id, capability.observed_replacement_key, observed_replacement_key)?
            .is_some();
    if *replacement_observed != expected_replacement_observed {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    authority.commit_namespace_absent_after_tombstone(capability, durability, observed_replacement_key)
}

pub(in crate::mapped_file::retirement) fn commit_writer_namespace_absent<O>(
    capability: LogicalRemovedCapability<O>,
    proof: writer::WriterDurabilityProof,
    observed_replacement_key: Option<PhysicalFileKey>,
) -> Result<NamespaceAbsentCapability<O>, RegistryViolation> {
    let (
        record,
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    ) = proof.into_parts();
    let ticket_id = capability.binding.ticket_id();
    let codec::LedgerRecord::NamespaceAbsent {
        ticket_id: record_ticket,
        incarnation,
        replacement_observed,
        target_key,
        canonical_path,
        tombstone_path,
        ..
    } = &record
    else {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    };
    if *record_ticket != ticket_id
        || *incarnation != capability.binding.incarnation()
        || *target_key != capability.binding.target_key()
        || canonical_path != capability.binding.canonical_path()
        || tombstone_path.is_some()
    {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    let durability = DurabilityCoordinates::verified(
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    )?;
    let authority = Arc::clone(&capability.authority);
    let expected_replacement_observed =
        merge_observed_replacement_key(ticket_id, capability.observed_replacement_key, observed_replacement_key)?
            .is_some();
    if *replacement_observed != expected_replacement_observed {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    authority.commit_namespace_absent(capability, durability, None, observed_replacement_key)
}

pub(in crate::mapped_file::retirement) fn completed_record<O>(
    capability: &NamespaceAbsentCapability<O>,
    completion_time_ns: u64,
) -> codec::LedgerRecord {
    codec::LedgerRecord::Completed {
        ticket_id: capability.binding.ticket_id(),
        incarnation: capability.binding.incarnation(),
        completion_time_ns,
        namespace_absent_sequence: capability.durability.sequence(),
    }
}

pub(in crate::mapped_file::retirement) fn commit_writer_completed<O>(
    capability: NamespaceAbsentCapability<O>,
    proof: writer::WriterDurabilityProof,
) -> Result<CompletedRetirementReceipt, RegistryViolation> {
    let (
        record,
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    ) = proof.into_parts();
    let ticket_id = capability.binding.ticket_id();
    let codec::LedgerRecord::Completed {
        ticket_id: record_ticket,
        incarnation,
        namespace_absent_sequence,
        ..
    } = record
    else {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    };
    if record_ticket != ticket_id
        || incarnation != capability.binding.incarnation()
        || namespace_absent_sequence != capability.durability.sequence()
    {
        return Err(RegistryViolation::DurableStageEvidenceMismatch { ticket_id });
    }
    let durability = DurabilityCoordinates::verified(
        ledger_generation,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    )?;
    let authority = Arc::clone(&capability.authority);
    authority.commit_completed(capability, durability)
}

fn namespace_request_matches(
    binding: &RetirementIntentBinding,
    request: &platform::NamespaceRetirementRequest,
) -> bool {
    let ticket = request.ticket();
    ticket.ticket_id() == binding.ticket_id()
        && ticket.incarnation() == binding.incarnation()
        && ticket.reason() == binding.reason()
        && ticket.segment_offset() == binding.segment_offset()
        && ticket.mapping_generation() == binding.mapping_generation()
        && ticket.expected_length() == binding.expected_length()
        && ticket.retirement_nonce() == &binding.retirement_nonce()
        && request.physical_key() == binding.target_key()
        && request.canonical_path() == binding.canonical_path()
}
