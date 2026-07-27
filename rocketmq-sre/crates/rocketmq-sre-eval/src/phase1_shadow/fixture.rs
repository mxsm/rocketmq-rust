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

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use chrono::TimeZone;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use rocketmq_sre_core::diagnostics::DiagnosticStatus;
use serde::Deserialize;
use serde_json::Value;

use super::ShadowEvalError;

#[derive(Debug, Deserialize)]
struct RawFixture {
    pack: String,
    scenario: String,
    #[serde(default = "default_inconclusive")]
    expected_status: String,
    #[serde(default)]
    expected_reason_codes: BTreeSet<String>,
    evidence: Vec<RawEvidence>,
}

#[derive(Debug, Deserialize)]
struct RawEvidence {
    evidence_id: EvidenceId,
    source: String,
    resource: String,
    content: Value,
    #[serde(default)]
    partial: bool,
    #[serde(default = "default_available")]
    coverage: CoverageStatus,
}

/// Fully materialized deterministic diagnostic fixture.
#[derive(Clone, Debug)]
pub struct DiagnosticReplayFixture {
    pub pack: String,
    pub scenario: String,
    pub expected_status: DiagnosticStatus,
    pub expected_reason_codes: BTreeSet<String>,
    pub evidence: Vec<EvidenceSnapshot>,
}

/// Loads a compact diagnostic fixture into canonical Evidence snapshots.
///
/// # Errors
///
/// Returns a bounded fixture error when the file, JSON, timestamp, or
/// canonical Evidence hash is invalid.
pub fn load_diagnostic_fixture(
    path: &Path,
    tenant_id: TenantId,
    cluster_id: ClusterId,
) -> Result<DiagnosticReplayFixture, ShadowEvalError> {
    let raw = fs::read_to_string(path).map_err(|source| ShadowEvalError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let raw = serde_json::from_str::<RawFixture>(&raw).map_err(|error| ShadowEvalError::InvalidFixture {
        path: path.to_path_buf(),
        detail: error.to_string(),
    })?;
    let expected_status = parse_status(&raw.expected_status).ok_or_else(|| ShadowEvalError::InvalidFixture {
        path: path.to_path_buf(),
        detail: format!("unsupported expected status `{}`", raw.expected_status),
    })?;
    let observed_at =
        Utc.with_ymd_and_hms(2026, 7, 27, 0, 0, 0)
            .single()
            .ok_or_else(|| ShadowEvalError::InvalidFixture {
                path: path.to_path_buf(),
                detail: "fixture timestamp is invalid".to_owned(),
            })?;

    let evidence = raw
        .evidence
        .into_iter()
        .map(|item| {
            let query = EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id,
                cluster_id,
                source: item.source,
                resource: item.resource,
                time_range: TimeRange::new(observed_at, observed_at).map_err(|error| {
                    ShadowEvalError::InvalidFixture {
                        path: path.to_path_buf(),
                        detail: error.to_string(),
                    }
                })?,
            };
            let mut snapshot = EvidenceSnapshot::capture(
                query,
                current_evidence_schema(),
                observed_at,
                EvidenceContent::Inline(item.content),
            )
            .map_err(|error| ShadowEvalError::InvalidFixture {
                path: path.to_path_buf(),
                detail: error.to_string(),
            })?;
            snapshot.evidence_id = item.evidence_id;
            snapshot.partial = item.partial;
            snapshot.coverage = item.coverage;
            Ok(snapshot)
        })
        .collect::<Result<Vec<_>, ShadowEvalError>>()?;

    Ok(DiagnosticReplayFixture {
        pack: raw.pack,
        scenario: raw.scenario,
        expected_status,
        expected_reason_codes: raw.expected_reason_codes,
        evidence,
    })
}

pub(super) const fn status_name(status: DiagnosticStatus) -> &'static str {
    match status {
        DiagnosticStatus::Healthy => "healthy",
        DiagnosticStatus::Fault => "fault",
        DiagnosticStatus::Inconclusive => "inconclusive",
        DiagnosticStatus::Unsupported => "unsupported",
    }
}

fn parse_status(value: &str) -> Option<DiagnosticStatus> {
    match value {
        "healthy" => Some(DiagnosticStatus::Healthy),
        "fault" => Some(DiagnosticStatus::Fault),
        "inconclusive" => Some(DiagnosticStatus::Inconclusive),
        "unsupported" => Some(DiagnosticStatus::Unsupported),
        _ => None,
    }
}

fn default_inconclusive() -> String {
    "inconclusive".to_owned()
}

const fn default_available() -> CoverageStatus {
    CoverageStatus::Available
}
