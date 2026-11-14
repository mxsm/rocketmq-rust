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
use sqlx::Row;

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and durable synthetic Connector frames"]
async fn postgres_store_pressure_context_retains_findings_in_real_saved_revision_without_strong_finish() {
    let (mut fixture, principal, session) = prepared().await;
    let now = Utc::now();
    fixture.request.scope.time_range = TimeRange::new(now - chrono::Duration::minutes(2), now).unwrap();
    fixture.request.packs = vec![InvestigationPackRef {
        pack_id: "store-pressure.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
    }];
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("c02-context-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        let envelope = next_query(&fixture, &principal, session, 0).await;
        assert_eq!(envelope.query.source, "admin-query");
        assert_eq!(envelope.query.resource, "store-pressure/broker-a");
        let point = envelope.query.time_range.end;
        let mut observation: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../../../tests/fixtures/diagnostics/store-pressure-observation.v1.json"
        ))
        .unwrap();
        observation["observed_at_millis"] = json!(point.timestamp_millis());
        let mut snapshot = healthy_snapshot(&envelope.query);
        snapshot.observed_at = point;
        snapshot.partial = true;
        snapshot.coverage = CoverageStatus::Partial;
        snapshot.content = EvidenceContent::Inline(json!({"schema_version":"rocketmq.sre-diagnostic-source.v1",
            "profile":"store_pressure","observed_brokers":1,"brokers":[{"broker_name":"broker-a","broker_id":0,"store_pressure":observation}]}));
        snapshot.content_hash = snapshot.compute_content_hash().unwrap();
        let id = snapshot.evidence_id;
        respond(&fixture, &principal, &envelope, snapshot).await;
        id
    };
    let (result, id) = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(drive, wire) })
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
    assert_eq!(charged_reads(&run), 1);
    assert!(matches!(
        &run.assessments[0].conclusion,
        HypothesisAssessmentConclusion::Inconclusive { .. }
    ));
    let saved = fixture
        .repository
        .investigation_run_revision(&fixture.auth, run.id, run.pack_results[0].run_revision_id)
        .await
        .unwrap();
    assert!(saved.partial);
    assert_eq!(saved.status, InvestigationDiagnosisStatus::Inconclusive);
    assert_eq!(saved.requirements[0].quality, DiagnosticEvidenceQuality::Partial);
    assert!(
        saved
            .findings
            .iter()
            .any(|finding| finding.reason_code == "STORE_DISK_WARNING_RATIO_EXCEEDED"
                && finding.supporting_evidence_ids == vec![id])
    );
    assert!(
        saved.requirements[0]
            .missing_fields
            .iter()
            .any(|fact| fact.contains("monotonic_progress"))
    );
    owner.task_group().cancel();
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL with the current migration set"]
async fn postgres_store_pressure_legacy_template_is_explicit_authenticated_and_keeps_old_results() {
    let fixture = fixture().await;
    let cluster = fixture.request.scope.cluster_id;
    let at = Utc::now();
    let mut sample = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.auth.tenant_id,
            cluster_id: cluster,
            source: "admin-query".to_owned(),
            resource: "store-pressure/broker-a".to_owned(),
            time_range: TimeRange::new(at, at).unwrap(),
        },
        current_evidence_schema(),
        at,
        EvidenceContent::Inline(
            json!({"disk_pressure":false,"flush_or_dispatch_stalled":false,"tiered_wal_pressure":false}),
        ),
    )
    .unwrap();
    sample.exposure = EvidenceExposure::Synthetic;
    sample
        .warnings
        .push("synthetic compatibility regression, not production verification".to_owned());
    fixture
        .state
        .evidence
        .persist_cluster(&fixture.auth, sample)
        .await
        .unwrap();
    let router = || crate::phase1_api::public_routes().with_state(fixture.state.clone());
    let legacy_body = json!({"cluster_id":cluster,"template":"store_pressure_legacy_v1","schedule":null});
    let mut viewer = fixture.auth.clone();
    viewer.roles = std::collections::BTreeSet::from(["viewer".to_owned()]);
    assert_eq!(
        call(
            router(),
            "POST",
            "/v1/inspections",
            Some(&viewer),
            Some(legacy_body.clone())
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            router(),
            "POST",
            "/v1/inspections",
            Some(&viewer),
            Some(json!({"cluster_id":cluster,"template":"broker","schedule":"every 37m"}))
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for statement in [
        "SELECT COUNT(*) FROM inspection_runs WHERE tenant_id=$1 AND cluster_id=$2",
        "SELECT COUNT(*) FROM workflow_events WHERE tenant_id=$1 AND cluster_id=$2 AND aggregate_type='inspection'",
    ] {
        let count: i64 = sqlx::query_scalar(statement)
            .bind(fixture.auth.tenant_id.as_uuid())
            .bind(cluster.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "rejected callers must leave no scheduled work or event");
    }
    let (status, body) = call(
        router(),
        "POST",
        "/v1/inspections",
        Some(&fixture.auth),
        Some(legacy_body),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["run"]["template"], "store_pressure_legacy_v1");
    let legacy_id: InspectionRunId = serde_json::from_value(body["run"]["id"].clone()).unwrap();
    let original = sqlx::query("SELECT pack_id,output FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
        .bind(legacy_id.as_uuid())
        .fetch_all(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(original.len(), 1);
    assert_eq!(original[0].get::<String, _>("pack_id"), "store-pressure.v1");
    let original_output: serde_json::Value = original[0].get("output");
    assert_eq!(original_output["status"], "healthy");
    let (status, current) = call(
        router(),
        "POST",
        "/v1/inspections",
        Some(&fixture.auth),
        Some(json!({"cluster_id":cluster,"template":"broker","schedule":null})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    let current_id: InspectionRunId = serde_json::from_value(current["run"]["id"].clone()).unwrap();
    let current_packs: Vec<String> =
        sqlx::query_scalar("SELECT pack_id FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
            .bind(current_id.as_uuid())
            .fetch_all(&fixture.repository.pool)
            .await
            .unwrap();
    assert!(current_packs.contains(&"store-pressure.v2".to_owned()));
    assert!(!current_packs.contains(&"store-pressure.v1".to_owned()));
    let retained: serde_json::Value =
        sqlx::query_scalar("SELECT output FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
            .bind(legacy_id.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
    assert_eq!(retained, original_output);
}
