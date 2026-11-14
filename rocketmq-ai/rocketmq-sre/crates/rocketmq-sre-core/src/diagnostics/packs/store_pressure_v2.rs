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

mod context;
mod observation;
#[cfg(test)]
mod tests;

pub use observation::{StorePressureSignal, StorePressureSignalState, assess_store_pressure_observation};

use rocketmq_sre_contracts::{
    EvidenceContent, EvidenceFieldContract, EvidenceFieldKind, EvidenceSnapshot, SreContractError,
};

use super::super::{
    DiagnosticContext, DiagnosticPack, EvidenceRequirement, FindingOutcome, FollowUpQuery, PackVersion, RuleMatch,
    Severity,
};

const REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "store-pressure",
    source: "admin-query",
    resource_prefix: "store-pressure/",
    purpose: "Per-instance Store disk probes, actual cleanup policy and admission pressure; missing progress is explicit",
}];

const CODES: &[&str] = &[
    "STORE_DISK_WARNING_RATIO_EXCEEDED",
    "STORE_FORCIBLE_CLEANUP_RATIO_EXCEEDED",
    "STORE_RECLAIM_RATIO_EXCEEDED",
    "STORE_SYNC_FLUSH_ADMISSION_PRESSURE",
    "STORE_DISPATCH_ADMISSION_PRESSURE",
    "STORE_PAGE_CACHE_BUSY",
    "STORE_TRANSIENT_POOL_DEFICIENT",
    "STORE_LAST_FLUSH_FAILURE_RECORDED",
    "STORE_WRITE_ADMISSION_CLOSED",
    "STORE_PRESSURE_OBSERVATION_INCOMPLETE",
];

/// Real instantaneous Store predicates. This version intentionally cannot
/// certify sustained pipeline or WAL health without their missing producers.
#[derive(Debug)]
pub struct StorePressureV2;

impl DiagnosticPack for StorePressureV2 {
    fn id(&self) -> &'static str {
        "store-pressure"
    }
    fn version(&self) -> PackVersion {
        PackVersion::new(2, 0, 0)
    }
    fn applicable_components(&self) -> &'static [&'static str] {
        &["broker", "store", "tiered-store"]
    }
    fn required_evidence(&self) -> &'static [EvidenceRequirement] {
        REQUIRED
    }
    fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
        &[]
    }
    fn evidence_fields(&self, key: &str) -> Vec<EvidenceFieldContract> {
        if key != "store-pressure" {
            return vec![];
        }
        [
            ("schema_version", EvidenceFieldKind::Text),
            ("profile", EvidenceFieldKind::Text),
            ("observed_brokers", EvidenceFieldKind::Number),
        ]
        .into_iter()
        .map(|(path, kind)| EvidenceFieldContract {
            path: path.to_owned(),
            kind,
        })
        .collect()
    }
    fn rule_codes(&self) -> &'static [&'static str] {
        CODES
    }
    fn missing_evidence_facts(&self, key: &str, _snapshot: &EvidenceSnapshot) -> Vec<String> {
        if key == "store-pressure" {
            // This public source has no progress interval or warranted stall
            // policy. A caller-provided complete flag cannot supply them.
            vec!["store-pressure.flush_dispatch_monotonic_progress_and_policy".to_owned()]
        } else {
            vec![]
        }
    }
    fn accepts_partial_context(&self, key: &str, snapshot: &EvidenceSnapshot, as_of_unix_ms: i64) -> bool {
        key == "store-pressure" && context::accepts(snapshot, as_of_unix_ms)
    }
    fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
        &[]
    }

    fn semantic_evidence_fingerprint(&self, snapshot: &EvidenceSnapshot) -> Result<Option<String>, SreContractError> {
        context::fingerprint(snapshot)
    }

    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, SreContractError> {
        let Some(snapshot) = context.evidence("store-pressure") else {
            return Ok(vec![]);
        };
        let EvidenceContent::Inline(content) = &snapshot.content else {
            return Ok(vec![]);
        };
        let mut missing = vec!["store-pressure.flush_dispatch_progress".to_owned()];
        let mut findings = vec![];
        if content.get("schema_version").and_then(|value| value.as_str()) != Some("rocketmq.sre-diagnostic-source.v1")
            || content.get("profile").and_then(|value| value.as_str()) != Some("store_pressure")
        {
            missing.push("store-pressure.observation_schema".to_owned());
        } else if let Some(brokers) = content
            .get("brokers")
            .and_then(|value| value.as_array())
            .filter(|rows| !rows.is_empty())
        {
            let expected = snapshot
                .resource
                .strip_prefix("store-pressure/")
                .filter(|target| !target.is_empty() && !target.contains('/'));
            let mut seen = std::collections::BTreeSet::new();
            let all_targeted_unique = expected.is_some()
                && brokers.iter().all(|row| {
                    row.get("broker_name").and_then(|value| value.as_str()) == expected
                        && row
                            .get("broker_id")
                            .and_then(|value| value.as_u64())
                            .is_some_and(|id| seen.insert(id))
                });
            for row in brokers {
                let broker_id = row.get("broker_id").and_then(|value| value.as_u64());
                let observation = &row["store_pressure"];
                let point = observation
                    .get("observed_at_millis")
                    .and_then(|value| value.as_u64())
                    .and_then(|point| i64::try_from(point).ok());
                let in_window = point.is_some_and(|point| {
                    let point = (point.div_euclid(1000), (point.rem_euclid(1000) as u32) * 1_000_000);
                    point
                        >= (
                            snapshot.time_range.start.timestamp(),
                            snapshot.time_range.start.timestamp_subsec_nanos(),
                        )
                        && point
                            <= (
                                snapshot.time_range.end.timestamp(),
                                snapshot.time_range.end.timestamp_subsec_nanos(),
                            )
                });
                let fresh = point
                    .and_then(|point| {
                        (snapshot.observed_at.timestamp() as i128)
                            .checked_mul(1_000_000_000)
                            .and_then(|time| time.checked_add(snapshot.observed_at.timestamp_subsec_nanos() as i128))
                            .and_then(|time| {
                                (point as i128)
                                    .checked_mul(1_000_000)
                                    .and_then(|point| time.checked_sub(point))
                            })
                    })
                    .filter(|age| *age >= 0)
                    .is_some_and(|age| age <= 300_000_000_000);
                if !all_targeted_unique {
                    missing.push("store-pressure.exact_broker_or_unique_instance_mismatch".to_owned());
                    continue;
                }
                if !in_window || !fresh {
                    missing.push("store-pressure.instance_or_point_in_window_unavailable".to_owned());
                    continue;
                }
                if let Some(generation) = row
                    .get("config")
                    .and_then(|config| config.get("generation"))
                    .and_then(|value| value.as_u64())
                    && observation
                        .get("configuration_generation")
                        .and_then(|value| value.as_u64())
                        != Some(generation)
                {
                    missing.push("store-pressure.response_configuration_generation_mismatch".to_owned());
                    continue;
                }
                for signal in assess_store_pressure_observation(observation) {
                    match signal.state {
                        StorePressureSignalState::Crossed => {
                            let Some((code, summary)) = fault(signal.key) else {
                                continue;
                            };
                            if let Some(citation) = context.cite(
                                "store-pressure",
                                format!(
                                    "Broker instance {}: {}; {}",
                                    broker_id.unwrap_or_default(),
                                    signal.key,
                                    signal.reason_code
                                ),
                            ) {
                                findings.push(
                                    RuleMatch::new(code, summary, Severity::Warning, FindingOutcome::Fault)
                                        .with_support(citation),
                                );
                            }
                        }
                        StorePressureSignalState::Unknown | StorePressureSignalState::NotConfigured => {
                            missing.push(format!("store-pressure.{}:{}", signal.key, signal.reason_code));
                        }
                        StorePressureSignalState::WithinObservedPolicy => {}
                    }
                }
            }
        } else {
            missing.push("store-pressure.broker_observations".to_owned());
        }
        missing.sort();
        missing.dedup();
        let mut incomplete = RuleMatch::new(
            "STORE_PRESSURE_OBSERVATION_INCOMPLETE",
            "Instantaneous Store observations do not establish sustained flush, dispatch or tiered WAL health",
            Severity::Info,
            FindingOutcome::Inconclusive,
        );
        if let Some(citation) = context.cite(
            "store-pressure",
            "Missing dimensions remain unknown; positive totals and queue ages are not stall proofs",
        ) {
            incomplete = incomplete.with_support(citation);
        }
        incomplete.missing_evidence = missing;
        findings.push(incomplete);
        Ok(findings)
    }
}

fn fault(key: &str) -> Option<(&'static str, &'static str)> {
    Some(match key {
        "disk_warning" => (
            CODES[0],
            "A sampled configured Store root exceeds the actual disk-warning ratio",
        ),
        "disk_forcible_cleanup" => (
            CODES[1],
            "A sampled Store root exceeds the configured forcible-cleanup ratio",
        ),
        "disk_reclaim" => (
            CODES[2],
            "A sampled Store root exceeds the configured cleanup-start ratio",
        ),
        "sync_flush_admission" => (CODES[3], "A configured synchronous-flush admission limit is reached"),
        "dispatch_admission" => (
            CODES[4],
            "The configured dispatch byte-backlog admission limit is reached",
        ),
        "page_cache_busy" => (CODES[5], "The Store reports its page-cache lock-age busy predicate"),
        "transient_pool_deficient" => (CODES[6], "The Store reports transient-buffer pool deficiency"),
        "last_flush_error" => (CODES[7], "A last-flush failure is retained in the Store recorder"),
        "write_admission" => (
            CODES[8],
            "The Store reports primary-write admission closed; its cause is not established",
        ),
        _ => return None,
    })
}
