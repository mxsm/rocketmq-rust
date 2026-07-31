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

use std::fmt;

use rocketmq_sre_contracts::DiagnosticEvidence;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceRelation;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::SchemaVersion;

use super::DiagnosticContext;
use super::DiagnosticError;

/// Schema family emitted by every deterministic diagnostic pack.
pub const DIAGNOSTIC_OUTPUT_SCHEMA_FAMILY: &str = "rocketmq-sre.diagnostic-result";

/// Current diagnostic result schema major.
pub const DIAGNOSTIC_OUTPUT_SCHEMA_MAJOR: u16 = 1;

/// Current diagnostic result schema minor.
pub const DIAGNOSTIC_OUTPUT_SCHEMA_MINOR: u16 = 0;

/// Semantic version of a diagnostic pack implementation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PackVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl PackVersion {
    /// Creates a pack version.
    #[must_use]
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self { major, minor, patch }
    }
}

impl fmt::Display for PackVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Evidence required or optionally consumed by a pack.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvidenceRequirement {
    /// Stable key used by pack rules.
    pub key: &'static str,
    /// Canonical evidence source name.
    pub source: &'static str,
    /// Prefix matched against [`EvidenceSnapshot::resource`].
    pub resource_prefix: &'static str,
    /// Operator-facing explanation of why the evidence is used.
    pub purpose: &'static str,
}

/// Bounded follow-up query suggested when evidence is absent or inconclusive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FollowUpQuery {
    pub source: &'static str,
    pub resource_template: &'static str,
    pub reason: &'static str,
}

/// Severity of one deterministic finding.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// Rule outcome used to distinguish healthy conclusions from faults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FindingOutcome {
    Healthy,
    Fault,
    Inconclusive,
}

/// Confidence band derived from an integer score.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ConfidenceBand {
    Low,
    Medium,
    High,
}

/// Deterministic confidence score with a reproducible explanation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfidenceScore {
    pub percent: u8,
    pub band: ConfidenceBand,
    pub explanation: String,
}

/// Evidence selected by a rule before the engine seals the final score.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleEvidence {
    pub evidence_id: EvidenceId,
    pub rationale: String,
}

impl RuleEvidence {
    /// Creates a rule-level evidence citation.
    #[must_use]
    pub fn new(evidence_id: EvidenceId, rationale: impl Into<String>) -> Self {
        Self {
            evidence_id,
            rationale: rationale.into(),
        }
    }
}

/// Rule output before deterministic confidence is calculated.
///
/// Packs deliberately cannot provide a confidence percentage. They expose
/// matched signal counts and citations; the engine owns confidence scoring.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleMatch {
    pub reason_code: &'static str,
    pub root_cause: String,
    pub severity: Severity,
    pub outcome: FindingOutcome,
    pub supporting_evidence: Vec<RuleEvidence>,
    pub counter_evidence: Vec<RuleEvidence>,
    pub missing_evidence: Vec<String>,
    pub matched_signal_count: u8,
}

impl RuleMatch {
    /// Starts a deterministic rule match.
    #[must_use]
    pub fn new(
        reason_code: &'static str,
        root_cause: impl Into<String>,
        severity: Severity,
        outcome: FindingOutcome,
    ) -> Self {
        Self {
            reason_code,
            root_cause: root_cause.into(),
            severity,
            outcome,
            supporting_evidence: Vec::new(),
            counter_evidence: Vec::new(),
            missing_evidence: Vec::new(),
            matched_signal_count: 1,
        }
    }

    /// Adds evidence that supports the candidate conclusion.
    #[must_use]
    pub fn with_support(mut self, evidence: RuleEvidence) -> Self {
        self.supporting_evidence.push(evidence);
        self
    }

    /// Adds evidence that contradicts the candidate conclusion.
    #[must_use]
    pub fn with_counter(mut self, evidence: RuleEvidence) -> Self {
        self.counter_evidence.push(evidence);
        self
    }

    /// Records evidence still needed to strengthen or reject the conclusion.
    #[must_use]
    pub fn with_missing(mut self, requirement: impl Into<String>) -> Self {
        self.missing_evidence.push(requirement.into());
        self
    }

    /// Records how many independent rule predicates matched.
    #[must_use]
    pub const fn with_matched_signals(mut self, count: u8) -> Self {
        self.matched_signal_count = count;
        self
    }
}

/// Final explainable finding produced by the engine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticFinding {
    pub reason_code: String,
    pub root_cause: String,
    pub severity: Severity,
    pub outcome: FindingOutcome,
    pub confidence: ConfidenceScore,
    pub supporting_evidence: Vec<DiagnosticEvidence>,
    pub counter_evidence: Vec<DiagnosticEvidence>,
    pub missing_evidence: Vec<String>,
}

/// Overall state of one pack evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticStatus {
    Healthy,
    Fault,
    Inconclusive,
    Unsupported,
}

/// Versioned, deterministic output of one diagnostic pack.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticReport {
    pub pack_id: String,
    pub pack_version: PackVersion,
    pub output_schema: SchemaVersion,
    pub status: DiagnosticStatus,
    pub findings: Vec<DiagnosticFinding>,
    pub missing_required_evidence: Vec<String>,
    pub missing_optional_evidence: Vec<String>,
    pub follow_up_queries: Vec<FollowUpQuery>,
}

/// Versioned, implementation-neutral diagnostic pack.
pub trait DiagnosticPack: fmt::Debug + Send + Sync {
    /// Stable base ID without the `.vN` major suffix.
    fn id(&self) -> &'static str;

    /// Semantic implementation version.
    fn version(&self) -> PackVersion;

    /// RocketMQ or platform components to which the pack applies.
    fn applicable_components(&self) -> &'static [&'static str];

    /// Evidence without which the pack cannot make a high-confidence finding.
    fn required_evidence(&self) -> &'static [EvidenceRequirement];

    /// Evidence that improves attribution or provides counter-evidence.
    fn optional_evidence(&self) -> &'static [EvidenceRequirement];

    /// Stable reason codes emitted by this pack.
    fn rule_codes(&self) -> &'static [&'static str];

    /// Queries the orchestrator may issue after an inconclusive result.
    fn follow_up_queries(&self) -> &'static [FollowUpQuery];

    /// Maximum acceptable age of evidence consumed by this pack.
    ///
    /// Evidence older than this bound is treated as missing so a stale
    /// snapshot cannot produce a healthy or high-confidence conclusion.
    fn max_evidence_freshness_seconds(&self) -> u64 {
        300
    }

    /// Versioned schema emitted by this pack.
    fn output_schema(&self) -> SchemaVersion {
        SchemaVersion::new(
            DIAGNOSTIC_OUTPUT_SCHEMA_FAMILY,
            DIAGNOSTIC_OUTPUT_SCHEMA_MAJOR,
            DIAGNOSTIC_OUTPUT_SCHEMA_MINOR,
        )
    }

    /// Validates pack-specific safety invariants before rules run.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed diagnostic error when evidence is unsafe or
    /// incompatible with the pack.
    fn validate_evidence(&self, _evidence: &[EvidenceSnapshot]) -> Result<(), DiagnosticError> {
        Ok(())
    }

    /// Evaluates deterministic rules without making external calls.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic error when evidence cannot be safely evaluated.
    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, DiagnosticError>;

    /// Returns the stable major-qualified pack ID.
    #[must_use]
    fn qualified_id(&self) -> String {
        format!("{}.v{}", self.id(), self.version().major)
    }
}

pub(super) fn seal_evidence(
    evidence: RuleEvidence,
    relation: EvidenceRelation,
    confidence_percent: u8,
) -> DiagnosticEvidence {
    DiagnosticEvidence {
        evidence_id: evidence.evidence_id,
        relation,
        rationale: evidence.rationale,
        confidence_percent,
    }
}
