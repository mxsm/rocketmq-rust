// Copyright 2023 The RocketMQ Rust Authors
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

use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::EvidenceId;

use super::PackVersion;

/// Fail-closed diagnostic evaluation errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiagnosticError {
    UnknownPack { id: String },
    UnknownPackVersion { id: String, version: PackVersion },
    InvalidEvidenceHash { evidence_id: EvidenceId },
    DuplicateEvidenceId { evidence_id: EvidenceId },
    MixedTenantScope,
    MixedClusterScope,
    InvalidEvidenceCitation { pack_id: String, evidence_id: EvidenceId },
    ConclusionWithoutEvidence { pack_id: String, reason_code: String },
    PackReturnedNoConclusion { pack_id: String },
    UndeclaredReasonCode { pack_id: String, reason_code: String },
    MessageBodyRejected { evidence_id: EvidenceId },
    MessageMetadataReferenceRejected { evidence_id: EvidenceId },
}

impl fmt::Display for DiagnosticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPack { id } => write!(formatter, "diagnostic pack `{id}` is not registered"),
            Self::UnknownPackVersion { id, version } => {
                write!(
                    formatter,
                    "diagnostic pack `{id}` version `{version}` is not registered"
                )
            }
            Self::InvalidEvidenceHash { evidence_id } => {
                write!(formatter, "evidence `{evidence_id}` failed content hash verification")
            }
            Self::DuplicateEvidenceId { evidence_id } => {
                write!(formatter, "evidence ID `{evidence_id}` is duplicated")
            }
            Self::MixedTenantScope => formatter.write_str("diagnostic evidence crosses tenant scope"),
            Self::MixedClusterScope => formatter.write_str("diagnostic evidence crosses cluster scope"),
            Self::InvalidEvidenceCitation { pack_id, evidence_id } => {
                write!(
                    formatter,
                    "diagnostic pack `{pack_id}` cited unknown evidence `{evidence_id}`"
                )
            }
            Self::ConclusionWithoutEvidence { pack_id, reason_code } => {
                write!(
                    formatter,
                    "diagnostic pack `{pack_id}` returned `{reason_code}` without evidence"
                )
            }
            Self::PackReturnedNoConclusion { pack_id } => {
                write!(
                    formatter,
                    "diagnostic pack `{pack_id}` returned no conclusion despite complete required evidence"
                )
            }
            Self::UndeclaredReasonCode { pack_id, reason_code } => {
                write!(
                    formatter,
                    "diagnostic pack `{pack_id}` returned undeclared reason code `{reason_code}`"
                )
            }
            Self::MessageBodyRejected { evidence_id } => {
                write!(
                    formatter,
                    "message-path evidence `{evidence_id}` contains forbidden message content"
                )
            }
            Self::MessageMetadataReferenceRejected { evidence_id } => {
                write!(
                    formatter,
                    "message-path evidence `{evidence_id}` is an opaque reference and cannot be proven body-free"
                )
            }
        }
    }
}

impl Error for DiagnosticError {}
