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

use super::*;

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and real durable connector channel"]
async fn postgres_permanent_rejection_is_not_retried_and_transient_retries_share_the_run_limit() {
    for retryable in [false, true] {
        let (fixture, principal, session) = prepared().await;
        let created = fixture.create().await;
        let owner = RuntimeContext::from_current("d15-retry-regression").service_context("dispatch");
        let expected = if retryable { 5 } else { 3 };
        let drive = fixture
            .state
            .investigation_runs
            .dispatch_one(owner.task_group(), created.run.id);
        let wire = async {
            let mut after = 0;
            for _ in 0..expected {
                let envelope = next_query(&fixture, &principal, session, after).await;
                fixture
                    .state
                    .connector_channel
                    .submit_response(
                        &principal,
                        session,
                        &ConnectorResponseEnvelope {
                            schema: channel_schema(),
                            session_id: session,
                            correlation_id: envelope.correlation_id,
                            sequence: envelope.sequence,
                            evidence: None,
                            error_code: Some(
                                if retryable {
                                    "source_unavailable"
                                } else {
                                    "unauthorized_scope"
                                }
                                .to_owned(),
                            ),
                            retryable,
                        },
                    )
                    .await
                    .unwrap();
                after = envelope.sequence;
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(drive, wire) })
            .await
            .unwrap();
        result.unwrap();
        let run = fixture
            .state
            .investigation_runs
            .get(&fixture.auth, created.run.id)
            .await
            .unwrap();
        assert_eq!(run.status, InvestigationRunStatus::Inconclusive, "{run:?}");
        assert_eq!(charged_reads(&run), expected);
        assert_eq!(run.query_retry_count().unwrap(), if retryable { 2 } else { 0 });
        let HypothesisAssessmentConclusion::Inconclusive {
            missing_requirements, ..
        } = &run.assessments[0].conclusion
        else {
            panic!("permanent/missing source must remain inconclusive");
        };
        assert_eq!(missing_requirements.len(), 2);
        assert!(
            run.pack_results
                .iter()
                .all(|result| result.partial && result.required_evidence_quality == DiagnosticEvidenceQuality::Missing)
        );
    }
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and real durable connector channel"]
async fn postgres_large_bounded_evidence_is_materialized_and_null_fields_remain_missing() {
    let (fixture, principal, session) = prepared_range().await;
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("d15-large-partial-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        let mut after = 0;
        for _ in 0..4 {
            let envelope = next_query(&fixture, &principal, session, after).await;
            let mut snapshot = healthy_snapshot(&envelope.query);
            if snapshot.resource.starts_with("broker-health/") {
                let EvidenceContent::Inline(value) = &mut snapshot.content else {
                    unreachable!();
                };
                value["ready"] = serde_json::Value::Null;
                value["synthetic_padding"] = json!("x".repeat(80 * 1024));
                snapshot.content_hash = snapshot.compute_content_hash().unwrap();
            }
            respond(&fixture, &principal, &envelope, snapshot).await;
            after = envelope.sequence;
        }
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(drive, wire) })
        .await
        .unwrap();
    result.unwrap();
    let run = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, created.run.id)
        .await
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Inconclusive);
    assert_eq!(charged_reads(&run), 4);
    let pressure = run
        .pack_results
        .iter()
        .find(|result| result.pack.pack_id == "broker-health.v1")
        .unwrap();
    assert!(pressure.partial);
    let revision = fixture
        .repository
        .investigation_run_revision(&fixture.auth, run.id, pressure.run_revision_id)
        .await
        .unwrap();
    assert!(
        revision
            .requirements
            .iter()
            .any(|requirement| requirement.required && requirement.missing_fields.contains(&"ready".to_owned()))
    );
    let HypothesisAssessmentConclusion::Inconclusive {
        considered_evidence_ids,
        missing_requirements,
        ..
    } = &run.assessments[0].conclusion
    else {
        panic!("null evidence cannot become healthy");
    };
    assert_eq!(considered_evidence_ids.len(), 4);
    assert_eq!(missing_requirements.len(), 1);
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL"]
async fn postgres_insufficient_response_reservation_does_not_start_a_source_read() {
    let (mut fixture, _, _) = prepared().await;
    fixture.request.budget.limit.output_bytes = crate::connector_channel::MAX_RESPONSE_BYTES as u64 - 1;
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("d15-byte-budget-regression").service_context("dispatch");
    fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id)
        .await
        .unwrap();
    let run = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, created.run.id)
        .await
        .unwrap();
    assert_eq!(charged_reads(&run), 0);
    assert_eq!(run.stop_reason, Some(InvestigationStopReason::BudgetExhausted));
    assert!(
        !run.steps
            .iter()
            .any(|step| matches!(step.spec.work, InvestigationStepKind::CollectEvidence { .. }))
    );
}
