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

use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_core::diagnostics::DiagnosticReport;

use crate::ControlPlaneError;

/// Verifies that every deterministic conclusion cites evidence in the
/// authorized input pack.
pub(super) fn validate_report_citations(
    report: &DiagnosticReport,
    evidence: &[EvidenceSnapshot],
) -> Result<Vec<EvidenceId>, ControlPlaneError> {
    let authorized = evidence
        .iter()
        .map(|snapshot| snapshot.evidence_id)
        .collect::<BTreeSet<_>>();
    let mut cited = BTreeSet::new();

    for finding in &report.findings {
        for citation in finding.supporting_evidence.iter().chain(&finding.counter_evidence) {
            if !authorized.contains(&citation.evidence_id) {
                return Err(ControlPlaneError::validation(
                    "invalid_evidence_citation",
                    "diagnosis cited evidence outside the authorized evidence pack",
                ));
            }
            cited.insert(citation.evidence_id);
        }
    }
    Ok(cited.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CorrelationId;
    use rocketmq_sre_contracts::EvidenceContent;
    use rocketmq_sre_contracts::EvidenceQuery;
    use rocketmq_sre_contracts::QueryId;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::TimeRange;
    use rocketmq_sre_contracts::current_evidence_schema;
    use rocketmq_sre_core::diagnostics::DiagnosticEngine;
    use rocketmq_sre_core::diagnostics::wave_a_registry;
    use serde_json::json;

    use super::*;

    #[test]
    fn accepts_only_citations_from_the_evaluated_pack() {
        let now = Utc::now();
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            source: "rocketmq-mcp".to_owned(),
            resource: "cluster/topology".to_owned(),
            time_range: TimeRange::new(now, now).expect("valid time"),
        };
        let snapshot = EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            now,
            EvidenceContent::Inline(json!({"reachable": true, "node_count": 3})),
        )
        .expect("valid snapshot");
        let engine = DiagnosticEngine::new(wave_a_registry().expect("valid registry"));
        let report = engine
            .evaluate("cluster-topology.v1", std::slice::from_ref(&snapshot))
            .expect("diagnostic evaluation");

        let cited = validate_report_citations(&report, std::slice::from_ref(&snapshot))
            .expect("generated citations should be authorized");
        assert!(cited.iter().all(|id| *id == snapshot.evidence_id));
    }
}
