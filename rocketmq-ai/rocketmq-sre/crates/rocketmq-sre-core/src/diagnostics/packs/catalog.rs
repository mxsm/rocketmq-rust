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

use super::super::DiagnosticContext;
use super::super::DiagnosticError;
use super::super::DiagnosticPack;
use super::super::EvidenceRequirement;
use super::super::FindingOutcome;
use super::super::FollowUpQuery;
use super::super::PackVersion;
use super::super::RuleMatch;
use super::super::Severity;
use super::common;

/// One compiled predicate. Configuration files document thresholds and
/// windows, but cannot introduce new executable predicates.
#[derive(Clone, Copy, Debug)]
pub(super) enum Condition {
    Boolean { path: &'static str, expected: bool },
    NumberAtLeast { path: &'static str, threshold: f64 },
    NumberBelow { path: &'static str, threshold: f64 },
    TextEquals { path: &'static str, expected: &'static str },
}

impl Condition {
    fn path(self) -> &'static str {
        match self {
            Self::Boolean { path, .. }
            | Self::NumberAtLeast { path, .. }
            | Self::NumberBelow { path, .. }
            | Self::TextEquals { path, .. } => path,
        }
    }

    fn evaluate(self, context: &DiagnosticContext<'_>, requirement: &str) -> Option<bool> {
        match self {
            Self::Boolean { path, expected } => context.bool(requirement, path).map(|value| value == expected),
            Self::NumberAtLeast { path, threshold } => {
                context.number(requirement, path).map(|value| value >= threshold)
            }
            Self::NumberBelow { path, threshold } => context.number(requirement, path).map(|value| value < threshold),
            Self::TextEquals { path, expected } => context.text(requirement, path).map(|value| value == expected),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RuleSpec {
    pub reason_code: &'static str,
    pub root_cause: &'static str,
    pub rationale: &'static str,
    pub severity: Severity,
    pub condition: Condition,
}

#[derive(Debug)]
pub(super) struct PackSpec {
    pub id: &'static str,
    pub components: &'static [&'static str],
    pub required: &'static [EvidenceRequirement],
    pub optional: &'static [EvidenceRequirement],
    pub rules: &'static [RuleSpec],
    pub rule_codes: &'static [&'static str],
    pub healthy_code: &'static str,
    pub healthy_summary: &'static str,
    pub incomplete_code: &'static str,
    pub follow_up: &'static [FollowUpQuery],
    pub max_freshness_seconds: u64,
}

/// Concrete implementation backed only by compiled Rust predicates.
#[derive(Clone, Copy, Debug)]
pub(super) struct CatalogPack {
    spec: &'static PackSpec,
}

impl CatalogPack {
    pub(super) const fn new(spec: &'static PackSpec) -> Self {
        Self { spec }
    }
}

impl DiagnosticPack for CatalogPack {
    fn id(&self) -> &'static str {
        self.spec.id
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        self.spec.components
    }

    fn required_evidence(&self) -> &'static [EvidenceRequirement] {
        self.spec.required
    }

    fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
        self.spec.optional
    }

    fn rule_codes(&self) -> &'static [&'static str] {
        self.spec.rule_codes
    }

    fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
        self.spec.follow_up
    }

    fn max_evidence_freshness_seconds(&self) -> u64 {
        self.spec.max_freshness_seconds
    }

    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, DiagnosticError> {
        let primary = self.spec.required[0].key;
        if !context.is_available(primary) {
            return Ok(Vec::new());
        }

        let missing_fields = self
            .spec
            .rules
            .iter()
            .filter_map(|rule| {
                rule.condition
                    .evaluate(context, primary)
                    .is_none()
                    .then_some(rule.condition.path())
            })
            .collect::<Vec<_>>();
        if !missing_fields.is_empty() {
            return Ok(
                common::incomplete(context, primary, self.spec.incomplete_code, &missing_fields)
                    .into_iter()
                    .collect(),
            );
        }

        let mut findings = self
            .spec
            .rules
            .iter()
            .filter(|rule| rule.condition.evaluate(context, primary) == Some(true))
            .filter_map(|rule| {
                common::conclusion(
                    context,
                    primary,
                    rule.reason_code,
                    rule.root_cause,
                    rule.severity,
                    FindingOutcome::Fault,
                    rule.rationale,
                )
                .map(|finding| add_counter_evidence(context, self.spec.optional, finding))
            })
            .collect::<Vec<_>>();

        if findings.is_empty() {
            findings.extend(common::conclusion(
                context,
                primary,
                self.spec.healthy_code,
                self.spec.healthy_summary,
                Severity::Info,
                FindingOutcome::Healthy,
                "Every compiled fault predicate evaluated to false",
            ));
        }
        Ok(findings)
    }
}

fn add_counter_evidence(
    context: &DiagnosticContext<'_>,
    optional: &[EvidenceRequirement],
    mut finding: RuleMatch,
) -> RuleMatch {
    for requirement in optional {
        if context.bool(requirement.key, "counter_signal") == Some(true)
            && let Some(citation) = context.cite(
                requirement.key,
                "Independent optional evidence does not confirm the candidate cause",
            )
        {
            finding = finding.with_counter(citation);
        }
    }
    finding
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::CoverageStatus;
    use rocketmq_sre_contracts::EvidenceContent;
    use rocketmq_sre_contracts::EvidenceQuery;
    use rocketmq_sre_contracts::EvidenceSnapshot;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::TimeRange;
    use rocketmq_sre_contracts::current_evidence_schema;
    use serde_json::Map;
    use serde_json::Value;
    use serde_json::json;

    use super::super::super::DiagnosticEngine;
    use super::super::super::DiagnosticStatus;
    use super::super::super::FindingOutcome;
    use super::super::full_registry;
    use super::super::prevention;
    use super::super::wave_b_specs;
    use super::*;

    #[test]
    fn every_wave_b_and_c_pack_replays_normal_fault_and_missing_cases() {
        let engine = DiagnosticEngine::new(full_registry().expect("complete registry"));
        let specs = wave_b_specs().iter().chain(prevention::specs());

        for spec in specs {
            let healthy = snapshot(spec, healthy_content(spec), CoverageStatus::Available, 0);
            let healthy_report = engine
                .evaluate(&format!("{}.v1", spec.id), std::slice::from_ref(&healthy))
                .unwrap_or_else(|error| panic!("{} healthy fixture should evaluate: {error}", spec.id));
            assert_eq!(healthy_report.status, DiagnosticStatus::Healthy, "{}", spec.id);
            assert_eq!(healthy_report.findings[0].reason_code, spec.healthy_code);

            let fault = snapshot(spec, fault_content(spec, 0), CoverageStatus::Available, 0);
            let counter = optional_counter(spec);
            let mut fault_evidence = vec![fault];
            if let Some(counter) = counter {
                fault_evidence.push(counter);
            }
            let fault_report = engine
                .evaluate(&format!("{}.v1", spec.id), &fault_evidence)
                .unwrap_or_else(|error| panic!("{} fault fixture should evaluate: {error}", spec.id));
            assert_eq!(fault_report.status, DiagnosticStatus::Fault, "{}", spec.id);
            assert_eq!(fault_report.findings[0].reason_code, spec.rules[0].reason_code);
            assert_eq!(fault_report.findings[0].outcome, FindingOutcome::Fault);
            if !spec.optional.is_empty() {
                assert_eq!(fault_report.findings[0].counter_evidence.len(), 1, "{}", spec.id);
            }

            let missing_report = engine
                .evaluate(&format!("{}.v1", spec.id), &[])
                .unwrap_or_else(|error| panic!("{} missing fixture should evaluate: {error}", spec.id));
            assert_eq!(missing_report.status, DiagnosticStatus::Inconclusive, "{}", spec.id);
            assert_eq!(missing_report.missing_required_evidence, vec![spec.required[0].key]);
        }
    }

    #[test]
    fn partial_stale_and_local_only_evidence_degrade_fail_closed() {
        let engine = DiagnosticEngine::new(full_registry().expect("complete registry"));
        let spec = &super::super::security_runtime::RUNTIME_SATURATION;
        let content = fault_content(spec, 0);

        let available = snapshot(spec, content.clone(), CoverageStatus::Available, 0);
        let partial = snapshot(spec, content.clone(), CoverageStatus::Partial, 0);
        let stale = snapshot(
            spec,
            content.clone(),
            CoverageStatus::Available,
            spec.max_freshness_seconds + 1,
        );
        let local_only = snapshot(spec, content, CoverageStatus::NotProductionVerified, 0);

        let available_report = engine
            .evaluate("runtime-saturation.v1", &[available])
            .expect("available fixture");
        let partial_report = engine
            .evaluate("runtime-saturation.v1", &[partial])
            .expect("partial fixture");
        let stale_report = engine
            .evaluate("runtime-saturation.v1", &[stale])
            .expect("stale fixture");
        let local_only_report = engine
            .evaluate("runtime-saturation.v1", &[local_only])
            .expect("local-only fixture");

        assert_eq!(partial_report.status, DiagnosticStatus::Fault);
        assert!(partial_report.findings[0].confidence.percent < available_report.findings[0].confidence.percent);
        assert_eq!(stale_report.status, DiagnosticStatus::Inconclusive);
        assert_eq!(stale_report.missing_required_evidence, vec!["runtime-saturation"]);
        assert_eq!(local_only_report.status, DiagnosticStatus::Unsupported);
    }

    #[test]
    fn controller_leader_unknown_and_quorum_insufficient_are_independent_faults() {
        let engine = DiagnosticEngine::new(full_registry().expect("complete registry"));
        let spec = &super::super::broker_ha::CONTROLLER_HA;
        let mut content = healthy_content(spec);
        set_path(&mut content, "leader_known", Value::Bool(false));
        set_path(&mut content, "quorum_healthy", Value::Bool(false));
        let evidence = snapshot(spec, content, CoverageStatus::Available, 0);

        let report = engine
            .evaluate("controller-ha.v1", &[evidence])
            .expect("controller fault fixture");
        let codes = report
            .findings
            .iter()
            .map(|finding| finding.reason_code.as_str())
            .collect::<Vec<_>>();

        assert_eq!(report.status, DiagnosticStatus::Fault);
        assert!(codes.contains(&"CONTROLLER_LEADER_UNKNOWN"));
        assert!(codes.contains(&"CONTROLLER_QUORUM_INSUFFICIENT"));
    }

    fn healthy_content(spec: &PackSpec) -> Value {
        let mut content = Value::Object(Map::new());
        for rule in spec.rules {
            let value = match rule.condition {
                Condition::Boolean { expected, .. } => Value::Bool(!expected),
                Condition::NumberAtLeast { threshold, .. } => json!(threshold - 1.0),
                Condition::NumberBelow { threshold, .. } => json!(threshold + 1.0),
                Condition::TextEquals { expected, .. } => {
                    Value::String(if expected == "healthy" { "normal" } else { "healthy" }.to_owned())
                }
            };
            set_path(&mut content, rule.condition.path(), value);
        }
        content
    }

    fn fault_content(spec: &PackSpec, rule_index: usize) -> Value {
        let mut content = healthy_content(spec);
        let condition = spec.rules[rule_index].condition;
        let value = match condition {
            Condition::Boolean { expected, .. } => Value::Bool(expected),
            Condition::NumberAtLeast { threshold, .. } => json!(threshold),
            Condition::NumberBelow { threshold, .. } => json!(threshold - 0.1),
            Condition::TextEquals { expected, .. } => Value::String(expected.to_owned()),
        };
        set_path(&mut content, condition.path(), value);
        content
    }

    fn set_path(root: &mut Value, path: &str, value: Value) {
        let segments = path.split('.').collect::<Vec<_>>();
        let mut current = root;
        for segment in &segments[..segments.len() - 1] {
            let object = current.as_object_mut().expect("fixture node should be an object");
            current = object
                .entry((*segment).to_owned())
                .or_insert_with(|| Value::Object(Map::new()));
        }
        current
            .as_object_mut()
            .expect("fixture leaf parent should be an object")
            .insert(segments[segments.len() - 1].to_owned(), value);
    }

    fn snapshot(spec: &PackSpec, content: Value, coverage: CoverageStatus, freshness_seconds: u64) -> EvidenceSnapshot {
        let observed_at = Utc
            .with_ymd_and_hms(2026, 7, 27, 0, 0, 0)
            .single()
            .expect("valid fixture timestamp");
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixed_tenant(),
            cluster_id: fixed_cluster(),
            source: spec.required[0].source.to_owned(),
            resource: format!("{}fixture", spec.required[0].resource_prefix),
            time_range: TimeRange::new(observed_at, observed_at).expect("valid range"),
        };
        let mut snapshot = EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            observed_at,
            EvidenceContent::Inline(content),
        )
        .expect("fixture should canonicalize");
        snapshot.coverage = coverage;
        snapshot.partial = coverage == CoverageStatus::Partial;
        snapshot.freshness_seconds = freshness_seconds;
        snapshot
    }

    fn optional_counter(spec: &PackSpec) -> Option<EvidenceSnapshot> {
        let requirement = spec.optional.first()?;
        let observed_at = Utc
            .with_ymd_and_hms(2026, 7, 27, 0, 0, 0)
            .single()
            .expect("valid fixture timestamp");
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixed_tenant(),
            cluster_id: fixed_cluster(),
            source: requirement.source.to_owned(),
            resource: format!("{}fixture", requirement.resource_prefix),
            time_range: TimeRange::new(observed_at, observed_at).expect("valid range"),
        };
        EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            observed_at,
            EvidenceContent::Inline(json!({"counter_signal": true})),
        )
        .ok()
    }

    fn fixed_tenant() -> TenantId {
        "00000000-0000-4000-8000-000000000001".parse().expect("valid tenant ID")
    }

    fn fixed_cluster() -> ClusterId {
        "00000000-0000-4000-8000-000000000002"
            .parse()
            .expect("valid cluster ID")
    }
}
