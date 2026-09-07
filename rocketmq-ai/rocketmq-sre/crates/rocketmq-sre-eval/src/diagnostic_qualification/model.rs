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

/// Private mixed completion channel. It deliberately does not implement
/// [`std::error::Error`]; expected validation and assertion results are
/// projected as [`crate::EvalRejection`] at the public boundary.
pub(crate) enum DiagnosticQualificationFailure {
    Io { _path: PathBuf, source: std::io::Error },
    Json(serde_json::Error),
    InvalidManifest(String),
    InvalidFixture(String),
    FixtureDecode(serde_json::Error),
    FixtureContract(rocketmq_sre_contracts::SreContractError),
    Http(reqwest::Error),
    Database(sqlx::Error),
    Assertion(String),
    AssertionContract(rocketmq_sre_contracts::SreContractError),
}

impl std::fmt::Debug for DiagnosticQualificationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DiagnosticQualificationFailure")
    }
}

#[derive(Debug, Error)]
pub(crate) enum DiagnosticQualificationSource {
    #[error("diagnostic qualification source is unavailable")]
    Io {
        _path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("diagnostic qualification JSON is invalid")]
    Json(#[source] serde_json::Error),
    #[error("diagnostic qualification fixture cannot be decoded")]
    FixtureDecode(#[source] serde_json::Error),
    #[error("diagnostic qualification HTTP request failed")]
    Http(#[source] reqwest::Error),
    #[error("diagnostic qualification database query failed")]
    Database(#[source] sqlx::Error),
    #[error("diagnostic qualification fixture contract operation failed")]
    FixtureContract(#[source] rocketmq_sre_contracts::SreContractError),
    #[error("diagnostic qualification assertion contract operation failed")]
    AssertionContract(#[source] rocketmq_sre_contracts::SreContractError),
}

impl DiagnosticQualificationFailure {
    pub(crate) fn into_boundary<T>(self) -> Result<crate::EvalOutcome<T>, crate::EvalError> {
        match self {
            Self::InvalidManifest(detail) => {
                let _ = detail;
                Ok(crate::EvalOutcome::Rejected(
                    crate::EvalRejection::InvalidQualificationManifest,
                ))
            }
            Self::InvalidFixture(detail) => {
                let _ = detail;
                Ok(crate::EvalOutcome::Rejected(
                    crate::EvalRejection::InvalidEvidenceFixture,
                ))
            }
            Self::FixtureContract(source) => {
                if std::error::Error::source(&source).is_some() {
                    Err(crate::EvalError::diagnostic_qualification_source(
                        DiagnosticQualificationSource::FixtureContract(source),
                    ))
                } else {
                    Ok(crate::EvalOutcome::Rejected(
                        crate::EvalRejection::InvalidEvidenceFixture,
                    ))
                }
            }
            Self::Assertion(detail) => {
                let _ = detail;
                Ok(crate::EvalOutcome::Rejected(
                    crate::EvalRejection::QualificationAssertion,
                ))
            }
            Self::AssertionContract(source) => {
                if std::error::Error::source(&source).is_some() {
                    Err(crate::EvalError::diagnostic_qualification_source(
                        DiagnosticQualificationSource::AssertionContract(source),
                    ))
                } else {
                    Ok(crate::EvalOutcome::Rejected(
                        crate::EvalRejection::QualificationAssertion,
                    ))
                }
            }
            Self::Io { _path, source } => Err(crate::EvalError::diagnostic_qualification_source(
                DiagnosticQualificationSource::Io { _path, source },
            )),
            Self::Json(source) => Err(crate::EvalError::diagnostic_qualification_source(
                DiagnosticQualificationSource::Json(source),
            )),
            Self::FixtureDecode(source) => Err(crate::EvalError::diagnostic_qualification_source(
                DiagnosticQualificationSource::FixtureDecode(source),
            )),
            Self::Http(source) => Err(crate::EvalError::diagnostic_qualification_source(
                DiagnosticQualificationSource::Http(source),
            )),
            Self::Database(source) => Err(crate::EvalError::diagnostic_qualification_source(
                DiagnosticQualificationSource::Database(source),
            )),
        }
    }
}

impl From<serde_json::Error> for DiagnosticQualificationFailure {
    fn from(source: serde_json::Error) -> Self {
        Self::Json(source)
    }
}

impl From<reqwest::Error> for DiagnosticQualificationFailure {
    fn from(source: reqwest::Error) -> Self {
        Self::Http(source)
    }
}

impl From<sqlx::Error> for DiagnosticQualificationFailure {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source)
    }
}

impl DiagnosticQualificationSource {
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::Io { .. }
            | Self::Http(_)
            | Self::Database(_)
            | Self::FixtureContract(_)
            | Self::AssertionContract(_) => "source_unavailable",
            Self::Json(_) => "invalid_qualification_manifest",
            Self::FixtureDecode(_) => "invalid_evidence_fixture",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use rocketmq_sre_contracts::PublicErrorCode;
    use rocketmq_sre_contracts::SreContractError;

    use super::*;

    #[test]
    fn fixture_contract_without_source_is_a_closed_rejection() {
        let outcome =
            DiagnosticQualificationFailure::FixtureContract(SreContractError::new(PublicErrorCode::InvalidDescriptor))
                .into_boundary::<()>()
                .expect("source-free contract failure must not be an operational error");

        assert_eq!(
            outcome,
            crate::EvalOutcome::Rejected(crate::EvalRejection::InvalidEvidenceFixture)
        );
    }

    #[test]
    fn assertion_contract_with_source_preserves_the_typed_chain() {
        let error = DiagnosticQualificationFailure::AssertionContract(SreContractError::with_source(
            PublicErrorCode::SourceUnavailable,
            std::io::Error::other("private assertion source"),
        ))
        .into_boundary::<()>()
        .expect_err("source-bearing contract failure must remain operational");

        let qualification = error.source().expect("qualification source");
        assert!(qualification.is::<DiagnosticQualificationSource>());
        let contract = qualification.source().expect("contract source");
        assert!(contract.is::<SreContractError>());
        assert!(contract.source().is_some_and(|source| source.is::<std::io::Error>()));
    }

    #[test]
    fn fixture_contract_with_source_preserves_the_typed_chain() {
        let error = DiagnosticQualificationFailure::FixtureContract(SreContractError::with_source(
            PublicErrorCode::SourceUnavailable,
            std::io::Error::other("private fixture source"),
        ))
        .into_boundary::<()>()
        .expect_err("source-bearing fixture failure must remain operational");

        let qualification = error.source().expect("qualification source");
        assert!(qualification.is::<DiagnosticQualificationSource>());
        let contract = qualification.source().expect("contract source");
        assert!(contract.is::<SreContractError>());
        assert!(contract.source().is_some_and(|source| source.is::<std::io::Error>()));
    }
}
