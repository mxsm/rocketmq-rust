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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceRelation;
use rocketmq_sre_contracts::EvidenceSnapshot;

use super::ConfidenceInputs;
use super::DiagnosticError;
use super::DiagnosticFinding;
use super::DiagnosticPack;
use super::DiagnosticPackRegistry;
use super::DiagnosticReport;
use super::DiagnosticStatus;
use super::EvidenceRequirement;
use super::FindingOutcome;
use super::PackVersion;
use super::RuleEvidence;
use super::calculate_confidence;
use super::types::seal_evidence;

#[derive(Debug, Default)]
struct RequirementMatch<'a> {
    usable: Vec<&'a EvidenceSnapshot>,
    unsupported: bool,
}

/// Read-only view over evidence matched to one pack's declared requirements.
#[derive(Debug)]
pub struct DiagnosticContext<'a> {
    required: &'static [EvidenceRequirement],
    optional: &'static [EvidenceRequirement],
    matches: BTreeMap<&'static str, RequirementMatch<'a>>,
}

impl<'a> DiagnosticContext<'a> {
    fn new(pack: &dyn DiagnosticPack, evidence: &'a [EvidenceSnapshot]) -> Self {
        let required = pack.required_evidence();
        let optional = pack.optional_evidence();
        let mut matches = BTreeMap::new();

        for requirement in required.iter().chain(optional) {
            let mut matched = RequirementMatch::default();
            for snapshot in evidence.iter().filter(|snapshot| {
                snapshot.source == requirement.source && snapshot.resource.starts_with(requirement.resource_prefix)
            }) {
                match snapshot.coverage {
                    CoverageStatus::Available | CoverageStatus::Partial
                        if matches!(snapshot.content, EvidenceContent::Inline(_)) =>
                    {
                        matched.usable.push(snapshot);
                    }
                    CoverageStatus::NotProductionVerified => matched.unsupported = true,
                    CoverageStatus::Available | CoverageStatus::Partial | CoverageStatus::Missing => {}
                }
            }
            matched.usable.sort_by_key(|snapshot| snapshot.evidence_id);
            matches.insert(requirement.key, matched);
        }

        Self {
            required,
            optional,
            matches,
        }
    }

    /// Returns the first usable snapshot for a requirement in stable ID order.
    #[must_use]
    pub fn evidence(&self, requirement_key: &str) -> Option<&'a EvidenceSnapshot> {
        self.matches.get(requirement_key)?.usable.first().copied()
    }

    /// Returns all usable snapshots for a requirement in stable ID order.
    #[must_use]
    pub fn all_evidence(&self, requirement_key: &str) -> &[&'a EvidenceSnapshot] {
        self.matches
            .get(requirement_key)
            .map_or(&[], |matched| matched.usable.as_slice())
    }

    /// Whether at least one queryable inline snapshot satisfies a requirement.
    #[must_use]
    pub fn is_available(&self, requirement_key: &str) -> bool {
        self.evidence(requirement_key).is_some()
    }

    /// Reads a boolean from an inline evidence object using a dotted path.
    #[must_use]
    pub fn bool(&self, requirement_key: &str, path: &str) -> Option<bool> {
        let snapshot = self.evidence(requirement_key)?;
        let EvidenceContent::Inline(content) = &snapshot.content else {
            return None;
        };
        let mut value = content;
        for segment in path.split('.') {
            value = value.get(segment)?;
        }
        value.as_bool()
    }

    /// Reads a number from an inline evidence object using a dotted path.
    #[must_use]
    pub fn number(&self, requirement_key: &str, path: &str) -> Option<f64> {
        let snapshot = self.evidence(requirement_key)?;
        let EvidenceContent::Inline(content) = &snapshot.content else {
            return None;
        };
        let mut value = content;
        for segment in path.split('.') {
            value = value.get(segment)?;
        }
        value.as_f64()
    }

    /// Reads a string from an inline evidence object using a dotted path.
    #[must_use]
    pub fn text(&self, requirement_key: &str, path: &str) -> Option<&str> {
        let snapshot = self.evidence(requirement_key)?;
        let EvidenceContent::Inline(content) = &snapshot.content else {
            return None;
        };
        let mut value = content;
        for segment in path.split('.') {
            value = value.get(segment)?;
        }
        value.as_str()
    }

    /// Creates a citation to the first usable snapshot for a requirement.
    #[must_use]
    pub fn cite(&self, requirement_key: &str, rationale: impl Into<String>) -> Option<RuleEvidence> {
        self.evidence(requirement_key)
            .map(|snapshot| RuleEvidence::new(snapshot.evidence_id, rationale))
    }

    fn missing_required(&self) -> Vec<String> {
        self.required
            .iter()
            .filter(|requirement| !self.is_available(requirement.key))
            .map(|requirement| requirement.key.to_owned())
            .collect()
    }

    fn missing_optional(&self) -> Vec<String> {
        self.optional
            .iter()
            .filter(|requirement| !self.is_available(requirement.key))
            .map(|requirement| requirement.key.to_owned())
            .collect()
    }

    fn unsupported_required(&self) -> Vec<String> {
        self.required
            .iter()
            .filter(|requirement| {
                self.matches
                    .get(requirement.key)
                    .is_some_and(|matched| matched.unsupported && matched.usable.is_empty())
            })
            .map(|requirement| requirement.key.to_owned())
            .collect()
    }

    fn coverage_inputs(&self) -> ConfidenceInputs {
        let required_available = self
            .required
            .iter()
            .filter(|requirement| self.is_available(requirement.key))
            .count();
        let optional_available = self
            .optional
            .iter()
            .filter(|requirement| self.is_available(requirement.key))
            .count();
        let partial_evidence = self
            .matches
            .values()
            .flat_map(|matched| &matched.usable)
            .filter(|snapshot| snapshot.partial || snapshot.coverage == CoverageStatus::Partial)
            .count();
        let unsupported_required = self.unsupported_required().len();

        ConfidenceInputs {
            required_total: as_u16(self.required.len()),
            required_available: as_u16(required_available),
            optional_total: as_u16(self.optional.len()),
            optional_available: as_u16(optional_available),
            partial_evidence: as_u16(partial_evidence),
            missing_required: as_u16(self.missing_required().len()),
            unsupported_required: as_u16(unsupported_required),
            ..ConfidenceInputs::default()
        }
    }
}

/// Offline-capable deterministic diagnostic engine.
#[derive(Debug)]
pub struct DiagnosticEngine {
    registry: DiagnosticPackRegistry,
}

impl DiagnosticEngine {
    /// Creates an engine from an explicit pack registry.
    #[must_use]
    pub const fn new(registry: DiagnosticPackRegistry) -> Self {
        Self { registry }
    }

    /// Returns the immutable versioned registry.
    #[must_use]
    pub const fn registry(&self) -> &DiagnosticPackRegistry {
        &self.registry
    }

    /// Evaluates a base or major-qualified pack reference.
    ///
    /// # Errors
    ///
    /// Fails closed on an unknown pack, invalid evidence hash, mixed scope,
    /// unsafe message content, or a pack conclusion with missing citations.
    pub fn evaluate(
        &self,
        pack_reference: &str,
        evidence: &[EvidenceSnapshot],
    ) -> Result<DiagnosticReport, DiagnosticError> {
        let pack = self
            .registry
            .resolve(pack_reference)
            .ok_or_else(|| DiagnosticError::UnknownPack {
                id: pack_reference.to_owned(),
            })?;
        self.evaluate_pack(pack, evidence)
    }

    /// Evaluates an exact semantic version.
    ///
    /// # Errors
    ///
    /// Returns [`DiagnosticError::UnknownPackVersion`] when the version is not
    /// registered, otherwise the same fail-closed errors as [`Self::evaluate`].
    pub fn evaluate_version(
        &self,
        id: &str,
        version: PackVersion,
        evidence: &[EvidenceSnapshot],
    ) -> Result<DiagnosticReport, DiagnosticError> {
        let pack = self
            .registry
            .get(id, version)
            .ok_or_else(|| DiagnosticError::UnknownPackVersion {
                id: id.to_owned(),
                version,
            })?;
        self.evaluate_pack(pack, evidence)
    }

    fn evaluate_pack(
        &self,
        pack: &dyn DiagnosticPack,
        evidence: &[EvidenceSnapshot],
    ) -> Result<DiagnosticReport, DiagnosticError> {
        validate_evidence_set(evidence)?;
        pack.validate_evidence(evidence)?;
        let context = DiagnosticContext::new(pack, evidence);
        let missing_required = context.missing_required();
        let missing_optional = context.missing_optional();
        let unsupported_required = context.unsupported_required();

        if !unsupported_required.is_empty() {
            return Ok(DiagnosticReport {
                pack_id: pack.qualified_id(),
                pack_version: pack.version(),
                output_schema: pack.output_schema(),
                status: DiagnosticStatus::Unsupported,
                findings: Vec::new(),
                missing_required_evidence: missing_required,
                missing_optional_evidence: missing_optional,
                follow_up_queries: pack.follow_up_queries().to_vec(),
            });
        }

        let rule_matches = pack.evaluate(&context)?;
        if missing_required.is_empty() && rule_matches.is_empty() {
            return Err(DiagnosticError::PackReturnedNoConclusion {
                pack_id: pack.qualified_id(),
            });
        }

        let evidence_ids = evidence
            .iter()
            .map(|snapshot| snapshot.evidence_id)
            .collect::<BTreeSet<_>>();
        let mut findings = Vec::with_capacity(rule_matches.len());
        for rule_match in rule_matches {
            if !pack.rule_codes().contains(&rule_match.reason_code) {
                return Err(DiagnosticError::UndeclaredReasonCode {
                    pack_id: pack.qualified_id(),
                    reason_code: rule_match.reason_code.to_owned(),
                });
            }
            findings.push(seal_finding(
                pack,
                rule_match,
                &context,
                &missing_required,
                &evidence_ids,
            )?);
        }
        findings.sort_by(|left, right| {
            right
                .severity
                .cmp(&left.severity)
                .then_with(|| left.reason_code.cmp(&right.reason_code))
        });

        let status = if !missing_required.is_empty() {
            DiagnosticStatus::Inconclusive
        } else if findings.iter().any(|finding| finding.outcome == FindingOutcome::Fault) {
            DiagnosticStatus::Fault
        } else if findings
            .iter()
            .any(|finding| finding.outcome == FindingOutcome::Inconclusive)
        {
            DiagnosticStatus::Inconclusive
        } else {
            DiagnosticStatus::Healthy
        };

        Ok(DiagnosticReport {
            pack_id: pack.qualified_id(),
            pack_version: pack.version(),
            output_schema: pack.output_schema(),
            status,
            findings,
            missing_required_evidence: missing_required,
            missing_optional_evidence: missing_optional,
            follow_up_queries: pack.follow_up_queries().to_vec(),
        })
    }
}

fn validate_evidence_set(evidence: &[EvidenceSnapshot]) -> Result<(), DiagnosticError> {
    let mut ids = BTreeSet::new();
    let mut tenant = None;
    let mut cluster = None;

    for snapshot in evidence {
        if snapshot.verify_content_hash().is_err() {
            return Err(DiagnosticError::InvalidEvidenceHash {
                evidence_id: snapshot.evidence_id,
            });
        }
        if !ids.insert(snapshot.evidence_id) {
            return Err(DiagnosticError::DuplicateEvidenceId {
                evidence_id: snapshot.evidence_id,
            });
        }
        if tenant.is_some_and(|expected| expected != snapshot.tenant_id) {
            return Err(DiagnosticError::MixedTenantScope);
        }
        if cluster.is_some_and(|expected| expected != snapshot.cluster_id) {
            return Err(DiagnosticError::MixedClusterScope);
        }
        tenant.get_or_insert(snapshot.tenant_id);
        cluster.get_or_insert(snapshot.cluster_id);
    }
    Ok(())
}

fn seal_finding(
    pack: &dyn DiagnosticPack,
    rule_match: super::RuleMatch,
    context: &DiagnosticContext<'_>,
    missing_required: &[String],
    evidence_ids: &BTreeSet<EvidenceId>,
) -> Result<DiagnosticFinding, DiagnosticError> {
    let pack_id = pack.qualified_id();
    if rule_match.supporting_evidence.is_empty() {
        return Err(DiagnosticError::ConclusionWithoutEvidence {
            pack_id,
            reason_code: rule_match.reason_code.to_owned(),
        });
    }
    for citation in rule_match
        .supporting_evidence
        .iter()
        .chain(&rule_match.counter_evidence)
    {
        if !evidence_ids.contains(&citation.evidence_id) {
            return Err(DiagnosticError::InvalidEvidenceCitation {
                pack_id: pack.qualified_id(),
                evidence_id: citation.evidence_id,
            });
        }
    }

    let mut confidence_inputs = context.coverage_inputs();
    confidence_inputs.supporting_signals =
        u16::from(rule_match.matched_signal_count).max(as_u16(rule_match.supporting_evidence.len()));
    confidence_inputs.counter_signals = as_u16(rule_match.counter_evidence.len());
    confidence_inputs.missing_required = confidence_inputs
        .missing_required
        .saturating_add(as_u16(rule_match.missing_evidence.len()));
    let confidence = calculate_confidence(confidence_inputs);
    let mut missing_evidence = rule_match.missing_evidence;
    missing_evidence.extend(missing_required.iter().cloned());
    missing_evidence.sort();
    missing_evidence.dedup();

    let supporting_evidence = rule_match
        .supporting_evidence
        .into_iter()
        .map(|evidence| seal_evidence(evidence, EvidenceRelation::Supports, confidence.percent))
        .collect();
    let counter_evidence = rule_match
        .counter_evidence
        .into_iter()
        .map(|evidence| seal_evidence(evidence, EvidenceRelation::Contradicts, confidence.percent))
        .collect();

    Ok(DiagnosticFinding {
        reason_code: rule_match.reason_code.to_owned(),
        root_cause: rule_match.root_cause,
        severity: rule_match.severity,
        outcome: rule_match.outcome,
        confidence,
        supporting_evidence,
        counter_evidence,
        missing_evidence,
    })
}

fn as_u16(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}
