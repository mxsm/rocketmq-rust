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

//! Canonical, body-free Evidence projection for synthetic scenarios.

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::Sensitivity;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::current_evidence_schema;
use serde_json::json;

use crate::ProbePlan;
use crate::scenario::ProbeRunResult;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProbeEvidenceFailure {
    #[error("probe result time range is invalid")]
    InvalidTimeRange(#[source] rocketmq_sre_contracts::SreContractError),
    #[error("probe result could not be sealed as canonical Evidence")]
    Capture(#[source] rocketmq_sre_contracts::SreContractError),
}

/// Converts a completed probe into canonical metadata-only Evidence.
///
/// The serialized content contains counts, timing, trace identity, status, and
/// cleanup state. Synthetic message bytes are intentionally absent.
///
fn capture_probe_evidence_inner(
    tenant_id: TenantId,
    correlation_id: CorrelationId,
    plan: &ProbePlan,
    result: &ProbeRunResult,
) -> Result<EvidenceSnapshot, ProbeEvidenceFailure> {
    let time_range =
        TimeRange::new(result.started_at, result.finished_at).map_err(ProbeEvidenceFailure::InvalidTimeRange)?;
    let query = EvidenceQuery {
        query_id: QueryId::new(),
        correlation_id,
        tenant_id,
        cluster_id: plan.cluster_id,
        source: "synthetic-probe".to_owned(),
        resource: format!("probe/{}/{}", result.scenario.as_str(), result.probe_id),
        time_range,
    };
    let content = json!({
        "scenario": result.scenario,
        "status": result.status,
        "trace_id": result.trace_id,
        "sent_messages": result.sent_messages,
        "received_messages": result.received_messages,
        "acknowledged_messages": result.acknowledged_messages,
        "stages": result.stages,
        "cleanup": result.cleanup,
        "error_code": result.error_code,
    });
    let mut evidence = EvidenceSnapshot::capture(
        query,
        current_evidence_schema(),
        result.finished_at,
        EvidenceContent::Inline(content),
    )
    .map_err(ProbeEvidenceFailure::Capture)?;
    evidence.sensitivity = Sensitivity::Internal;
    evidence.exposure = EvidenceExposure::Synthetic;
    evidence.partial = result.cleanup.partial;
    Ok(evidence)
}

/// Converts a completed probe into canonical metadata-only Evidence.
///
/// The public boundary preserves the driver facade and does not expose
/// serialization, evidence sealing, or local timing internals.
pub fn capture_probe_evidence(
    tenant_id: TenantId,
    correlation_id: CorrelationId,
    plan: &ProbePlan,
    result: &ProbeRunResult,
) -> Result<EvidenceSnapshot, crate::scenario::ProbeDriverError> {
    capture_probe_evidence_inner(tenant_id, correlation_id, plan, result)
        .map_err(crate::scenario::ProbeDriverError::evidence)
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use chrono::TimeZone;
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use serde_json::Value;
    use uuid::Uuid;

    use super::*;
    use crate::ProbeConfig;
    use crate::cleanup::ProbeCleanupResult;
    use crate::scenario::ProbeRunStatus;
    use crate::scenario::ProbeScenario;

    #[test]
    fn evidence_contains_timing_and_counts_but_no_message_body() {
        let plan = ProbeConfig {
            cluster_id: ClusterId::new(),
            max_messages: 1,
            max_messages_per_second: 1,
            max_payload_bytes: 16,
            max_duration_seconds: 2,
        }
        .plan(Uuid::nil())
        .expect("plan");
        let at = Utc.timestamp_opt(1_735_689_600, 0).single().expect("timestamp");
        let result = ProbeRunResult {
            probe_id: Uuid::nil().to_string(),
            scenario: ProbeScenario::SendConsumeAck,
            status: ProbeRunStatus::Succeeded,
            started_at: at,
            finished_at: at,
            trace_id: "probe-fixture".to_owned(),
            stages: Vec::new(),
            sent_messages: 1,
            received_messages: 1,
            acknowledged_messages: 1,
            error_code: None,
            cleanup: ProbeCleanupResult::default(),
        };

        let evidence = capture_probe_evidence(TenantId::new(), CorrelationId::new(), &plan, &result).expect("evidence");
        assert_eq!(evidence.exposure, EvidenceExposure::Synthetic);
        assert!(evidence.verify_content_hash().is_ok());
        let EvidenceContent::Inline(Value::Object(content)) = evidence.content else {
            panic!("probe Evidence must be inline");
        };
        assert!(!content.contains_key("body"));
        assert!(!content.contains_key("payload"));
        assert_eq!(content["acknowledged_messages"], 1);
    }

    #[test]
    fn evidence_failure_retains_contract_source_behind_redacted_driver_facade() {
        let plan = ProbeConfig {
            cluster_id: ClusterId::new(),
            max_messages: 1,
            max_messages_per_second: 1,
            max_payload_bytes: 1,
            max_duration_seconds: 1,
        }
        .plan(Uuid::nil())
        .expect("plan");
        let result = ProbeRunResult {
            probe_id: "probe".to_owned(),
            scenario: ProbeScenario::SendConsumeAck,
            status: ProbeRunStatus::Failed,
            started_at: Utc.timestamp_opt(2, 0).single().unwrap(),
            finished_at: Utc.timestamp_opt(1, 0).single().unwrap(),
            trace_id: "trace".to_owned(),
            stages: Vec::new(),
            sent_messages: 0,
            received_messages: 0,
            acknowledged_messages: 0,
            error_code: Some("private-error".to_owned()),
            cleanup: ProbeCleanupResult::default(),
        };

        let error = capture_probe_evidence(TenantId::new(), CorrelationId::new(), &plan, &result)
            .expect_err("invalid time range");
        let evidence = error.source().expect("evidence leaf");
        assert!(evidence.is::<ProbeEvidenceFailure>());
        assert!(
            evidence
                .source()
                .is_some_and(|source| source.is::<rocketmq_sre_contracts::SreContractError>())
        );
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("private-error"));
        }
    }
}
