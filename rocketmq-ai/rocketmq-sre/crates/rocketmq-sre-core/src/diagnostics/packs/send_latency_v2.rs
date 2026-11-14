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

mod estimate;
mod observation;
#[cfg(test)]
mod tests;

pub use estimate::{
    LATENCY_GRID, LatencyBucket, LatencyCounterSample, LatencyHistogram, LatencyQuantile, LatencyResolution,
    LatencyWindowAssessment, LatencyWindowIssue, LatencyWindowState, assess_latency_histograms,
};
pub use observation::{
    LATENCY_FAMILIES, LatencyObservationIssue, LatencyPolicyState, SendLatencyObservation,
    assess_send_latency_observation,
};

use super::super::{
    DiagnosticContext, DiagnosticPack, EvidenceRequirement, FindingOutcome, FollowUpQuery, PackVersion, RuleMatch,
    Severity,
};
use rocketmq_sre_contracts::{
    ContractJsonValue as Value, EvidenceContent, EvidenceFieldContract, EvidenceFieldKind, EvidenceSnapshot,
    SreContractError,
};

const REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "send-latency",
    source: "prometheus",
    resource_prefix: "send-latency/",
    purpose: "Exact-topic recorded Broker histogram window with explicit missing path segments",
}];
const MISSING: [&str; 5] = [
    "send-latency.client_exact_topic_latency",
    "send-latency.proxy_exact_topic_latency",
    "send-latency.remoting_exact_topic_latency",
    "send-latency.store_exact_topic_latency",
    "send-latency.correlated_segment_population",
];
const CODES: &[&str] = &[
    "BROKER_SEND_LATENCY_ESTIMATE_EXCEEDS_POLICY",
    "BROKER_SEND_LATENCY_WINDOW_OBSERVED",
    "SEND_LATENCY_PATH_INCOMPLETE",
];

/// Version-owned numeric interpretation of the observed Broker population.
/// Four uncorrelated exact-topic segments remain unsupported in this source.
#[derive(Debug)]
pub struct SendLatencyV2;

impl DiagnosticPack for SendLatencyV2 {
    fn id(&self) -> &'static str {
        "send-latency"
    }
    fn version(&self) -> PackVersion {
        PackVersion::new(2, 0, 0)
    }
    fn applicable_components(&self) -> &'static [&'static str] {
        &["client", "proxy", "broker", "store"]
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
    fn evidence_fields(&self, key: &str) -> Vec<EvidenceFieldContract> {
        if key != "send-latency" {
            return vec![];
        }
        ["schema_version", "profile", "observation_schema"]
            .into_iter()
            .map(|path| EvidenceFieldContract {
                path: path.to_owned(),
                kind: EvidenceFieldKind::Text,
            })
            .collect()
    }
    fn missing_evidence_facts(&self, key: &str, snapshot: &EvidenceSnapshot) -> Vec<String> {
        if key != "send-latency" {
            return vec![];
        }
        let mut missing: Vec<_> = MISSING.iter().map(|fact| (*fact).to_owned()).collect();
        match assess_send_latency_observation(snapshot, snapshot.observed_at.timestamp_millis()) {
            Err(issue) => missing.push(format!("send-latency.{}", issue.reason_code())),
            Ok(observation) if observation.window.state != LatencyWindowState::Available => {
                missing.push("send-latency.broker_histogram_estimate_not_qualified".to_owned());
            }
            Ok(_) => {}
        }
        missing
    }
    fn accepts_partial_context(&self, key: &str, snapshot: &EvidenceSnapshot, as_of_unix_ms: i64) -> bool {
        key == "send-latency" && assess_send_latency_observation(snapshot, as_of_unix_ms).is_ok()
    }
    fn semantic_evidence_fingerprint(&self, snapshot: &EvidenceSnapshot) -> Result<Option<String>, SreContractError> {
        let EvidenceContent::Inline(content) = &snapshot.content else {
            return Ok(None);
        };
        if content["observation_schema"].as_str() != Some("rocketmq.send-latency-window.v1") {
            return Ok(None);
        }
        let mut facts = content.clone();
        if let Some(root) = facts.as_object_mut() {
            for field in [
                "collected_at",
                "collection_started_at",
                "source_age_seconds",
                "segments",
                "http_statistics",
            ] {
                root.remove(field);
            }
            if let Some(policy) = root.get_mut("policy").and_then(Value::as_object_mut) {
                policy.remove("status");
                policy.remove("reason_code");
            }
        }
        rocketmq_sre_contracts::canonical_sha256(&(
            &snapshot.source,
            &snapshot.resource,
            &facts,
            snapshot.coverage,
            snapshot.partial,
        ))
        .map(Some)
    }
    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, SreContractError> {
        let mut incomplete = RuleMatch::new(
            CODES[2],
            "The send path has uncorrelated or unavailable topic-scoped segments",
            Severity::Warning,
            FindingOutcome::Inconclusive,
        );
        for fact in MISSING {
            incomplete = incomplete.with_missing(fact);
        }
        let Some(snapshot) = context.evidence("send-latency") else {
            return Ok(vec![]);
        };
        if let Some(citation) = context.cite(
            "send-latency",
            "The captured Broker population does not establish the missing topic-scoped path segments",
        ) {
            incomplete = incomplete.with_support(citation);
        }
        let assessment = match assess_send_latency_observation(snapshot, snapshot.observed_at.timestamp_millis()) {
            Ok(assessment) => assessment,
            Err(issue) => {
                return Ok(vec![
                    incomplete.with_missing(format!("send-latency.{}", issue.reason_code())),
                ]);
            }
        };
        let crossed = assessment.policy == LatencyPolicyState::Exceeded;
        let (code, text, severity, outcome) = if crossed {
            (
                CODES[0],
                "The observed Broker Store-submit/result P99 estimate exceeds the operator-declared predicate",
                Severity::Warning,
                FindingOutcome::Fault,
            )
        } else {
            (
                CODES[1],
                "A bounded Broker histogram window is observed; no whole-path conclusion is established",
                Severity::Info,
                FindingOutcome::Inconclusive,
            )
        };
        let mut finding = RuleMatch::new(code, text, severity, outcome);
        if let Some(citation)=context.cite("send-latency",format!("Observed recorded Broker population only; estimate {:?} ms, source state {:?}, operator predicate {:?}. Client/Proxy/Remoting/Store populations are not correlated.",assessment.window.quantile.map(|value|value.estimate_millis),assessment.window.state,assessment.threshold_millis)){
            finding=finding.with_support(citation);
        }
        Ok(vec![finding, incomplete])
    }
}
