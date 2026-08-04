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

use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::TenantId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

pub const QUALIFICATION_SCHEMA: &str = "rocketmq-sre.diagnostic-pack-qualification.v1";
pub const QUALIFICATION_REPORT_SCHEMA: &str = "rocketmq-sre.diagnostic-pack-qualification-report.v1";
pub const QUALIFICATION_PACK_COUNT: usize = 32;
pub const QUALIFICATION_SCENARIO_COUNT: usize = 3;

/// Stable scenario names shared by fixtures, the manifest, and live reports.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationScenario {
    Normal,
    Fault,
    Missing,
}

impl QualificationScenario {
    pub const ALL: [Self; QUALIFICATION_SCENARIO_COUNT] = [Self::Normal, Self::Fault, Self::Missing];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Fault => "fault",
            Self::Missing => "missing",
        }
    }
}

/// One Evidence requirement recorded in the versioned qualification manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QualificationEvidenceRequirement {
    pub key: String,
    pub source: String,
    pub resource_prefix: String,
}

/// Expected deterministic result for one pack and scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QualificationExpectation {
    pub scenario: QualificationScenario,
    pub expected_status: String,
    pub expected_reason_codes: Vec<String>,
    pub partial: bool,
    pub execution_eligible: bool,
}

/// Qualification definition for one built-in diagnostic pack.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QualifiedDiagnosticPack {
    pub id: String,
    pub inspection_template: String,
    pub required_evidence: Vec<QualificationEvidenceRequirement>,
    pub scenarios: Vec<QualificationExpectation>,
}

/// Committed, generated contract for all live diagnostic-pack scenarios.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticQualificationManifest {
    pub schema_version: String,
    pub operating_mode: String,
    pub model_provider_network_calls: bool,
    pub target_mutation_calls: u32,
    pub execution_eligible: bool,
    pub pack_count: usize,
    pub scenario_count: usize,
    pub pack_scenario_count: usize,
    pub inspection_templates: Vec<String>,
    pub fixture_assets: Vec<String>,
    pub packs: Vec<QualifiedDiagnosticPack>,
}

/// Secret-bearing configuration for a disposable live qualification run.
///
/// The type intentionally has no `Debug` implementation so a token or database
/// URL cannot be logged through routine diagnostic formatting.
pub struct LiveQualificationConfig {
    pub public_url: String,
    pub connector_url: String,
    pub database_url: String,
    pub token: String,
    pub tenant_id: TenantId,
    pub revision: String,
    pub environment: String,
}

/// One successfully validated pack/scenario result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QualifiedPackScenarioResult {
    pub pack_id: String,
    pub scenario: QualificationScenario,
    pub status: String,
    pub reason_codes: Vec<String>,
    pub cited_evidence_count: usize,
    pub persisted_run_count: usize,
    pub partial: bool,
    pub execution_eligible: bool,
}

/// Redacted machine-local evidence emitted by the live qualification harness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiagnosticQualificationReport {
    pub schema_version: String,
    pub revision: String,
    pub environment: String,
    pub database: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: String,
    pub operating_mode: String,
    pub pack_count: usize,
    pub scenario_count: usize,
    pub pack_scenario_count: usize,
    pub model_provider_network_calls: u64,
    pub target_mutation_calls: u64,
    pub execution_records: u64,
    pub cross_cluster_access_rejected: bool,
    pub schema_drift_rejected: bool,
    pub results: Vec<QualifiedPackScenarioResult>,
}

/// Stable failure categories for manifest and live qualification.
#[derive(Debug, Error)]
pub enum DiagnosticQualificationError {
    #[error("failed to access `{path}`: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid qualification JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid diagnostic qualification manifest: {0}")]
    InvalidManifest(String),
    #[error("invalid diagnostic fixture: {0}")]
    InvalidFixture(String),
    #[error("live qualification HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("live qualification database query failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("diagnostic qualification assertion failed: {0}")]
    Assertion(String),
}

impl DiagnosticQualificationError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } | Self::Http(_) | Self::Database(_) => "source_unavailable",
            Self::Json(_) | Self::InvalidManifest(_) => "invalid_qualification_manifest",
            Self::InvalidFixture(_) => "invalid_evidence_fixture",
            Self::Assertion(_) => "diagnostic_qualification_failed",
        }
    }
}
