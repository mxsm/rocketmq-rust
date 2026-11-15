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

mod normalized;
mod observation;
#[cfg(test)]
mod tests;

pub use observation::{
    NamesrvComparisonState, NamesrvObservationIssue, NamesrvRouteObservation, assess_namesrv_route_observation,
    compare_namesrv_route_content, namesrv_route_domain,
};

use super::super::{
    DiagnosticContext, DiagnosticPack, EvidenceRequirement, EvidenceTemporalPolicy, FindingOutcome, FollowUpQuery,
    PackVersion, RuleMatch, Severity,
};
use rocketmq_sre_contracts::{EvidenceFieldContract, EvidenceFieldKind, EvidenceSnapshot, SreContractError};

pub const NAMESRV_ROUTE_SCHEMA: &str = "rocketmq.sre-namesrv-route-observation.v1";
pub const NAMESRV_MISSING_FACTS: [&str; 9] = [
    "registration.first_registration_time",
    "registration.last_registration_time",
    "registration.last_heartbeat_time",
    "route.source_generation_time",
    "route.source_generation",
    "route.source_instance_id",
    "route.local_origin_atomic_proof",
    "broker.scope_bound_reachability",
    "historical_window_coverage",
];
const REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "namesrv-route",
    source: "admin-query",
    resource_prefix: "namesrv-route/",
    purpose: "Configured independent NameServer current route observations; registration and historical coverage remain unknown",
}];
const CODES: &[&str] = &[
    "NAMESRV_CURRENT_ROUTES_EQUAL_OBSERVED",
    "NAMESRV_CURRENT_ROUTE_DIFFERENCE_OBSERVED",
    "NAMESRV_CURRENT_ROUTE_CHANGE_OBSERVED",
    "NAMESRV_CURRENT_ROUTES_INCOMPARABLE",
    "NAMESRV_ROUTE_CONTEXT_INCOMPLETE",
];

/// Current point-state advisories only. This pack never infers historical
/// route state, registration freshness, Broker reachability or a root cause.
#[derive(Debug)]
pub struct NamesrvRouteV2;

impl DiagnosticPack for NamesrvRouteV2 {
    fn id(&self) -> &'static str {
        "namesrv-route"
    }
    fn version(&self) -> PackVersion {
        PackVersion::new(2, 0, 0)
    }
    fn applicable_components(&self) -> &'static [&'static str] {
        &["nameserver", "broker"]
    }
    fn required_evidence(&self) -> &'static [EvidenceRequirement] {
        REQUIRED
    }
    fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
        &[]
    }
    fn rule_codes(&self) -> &'static [&'static str] {
        CODES
    }
    fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
        &[]
    }
    fn max_evidence_freshness_seconds(&self) -> u64 {
        300
    }
    fn temporal_evidence_policy(&self) -> EvidenceTemporalPolicy {
        EvidenceTemporalPolicy::CurrentContextOnly
    }
    fn evidence_fields(&self, key: &str) -> Vec<EvidenceFieldContract> {
        if key != "namesrv-route" {
            return vec![];
        }
        [
            "schema_version",
            "profile",
            "tenant_id",
            "cluster_id",
            "topic",
            "temporal_semantics",
            "historical_coverage",
        ]
        .into_iter()
        .map(|path| EvidenceFieldContract {
            path: path.to_owned(),
            kind: EvidenceFieldKind::Text,
        })
        .collect()
    }
    fn missing_evidence_facts(&self, key: &str, snapshot: &EvidenceSnapshot) -> Vec<String> {
        if key != "namesrv-route" {
            return vec![];
        }
        let mut missing: Vec<_> = NAMESRV_MISSING_FACTS.iter().map(|fact| (*fact).to_owned()).collect();
        if let Err(issue) = assess_namesrv_route_observation(snapshot, snapshot.observed_at.timestamp_millis()) {
            missing.push(format!("namesrv-route.{}", issue.reason_code()));
        }
        missing
    }
    fn accepts_partial_context(&self, key: &str, snapshot: &EvidenceSnapshot, as_of_unix_ms: i64) -> bool {
        key == "namesrv-route" && assess_namesrv_route_observation(snapshot, as_of_unix_ms).is_ok()
    }
    fn semantic_evidence_fingerprint(&self, snapshot: &EvidenceSnapshot) -> Result<Option<String>, SreContractError> {
        observation::semantic_fingerprint(snapshot)
    }
    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, SreContractError> {
        let Some(snapshot) = context.evidence("namesrv-route") else {
            return Ok(vec![]);
        };
        let assessment = assess_namesrv_route_observation(snapshot, snapshot.observed_at.timestamp_millis());
        let (code, summary, severity) = match assessment {
            Ok(NamesrvRouteObservation {
                state: NamesrvComparisonState::Equal,
                ..
            }) => (
                CODES[0],
                "Configured NameServers returned equal current point states; historical coverage is unknown",
                Severity::Info,
            ),
            Ok(NamesrvRouteObservation {
                state: NamesrvComparisonState::Difference,
                ..
            }) => (
                CODES[1],
                "A current route difference was observed at the sampled NameServers; historical cause is unknown",
                Severity::Warning,
            ),
            Ok(NamesrvRouteObservation {
                state: NamesrvComparisonState::Transient,
                ..
            }) => (
                CODES[2],
                "Current route or view changed between the two sampled rounds",
                Severity::Warning,
            ),
            Ok(_) => (
                CODES[3],
                "The configured NameServer observations cannot be completely compared",
                Severity::Info,
            ),
            Err(_) => (
                CODES[4],
                "Current route context is unavailable or invalid; historical state remains unknown",
                Severity::Info,
            ),
        };
        let Some(citation) = context.cite(
            "namesrv-route",
            "Current acquisition context only; not a historical route, registration, reachability or causal proof",
        ) else {
            return Ok(vec![]);
        };
        let mut finding = RuleMatch::new(code, summary, severity, FindingOutcome::Inconclusive).with_support(citation);
        finding.missing_evidence = NAMESRV_MISSING_FACTS.iter().map(|fact| (*fact).to_owned()).collect();
        Ok(vec![finding])
    }
}
