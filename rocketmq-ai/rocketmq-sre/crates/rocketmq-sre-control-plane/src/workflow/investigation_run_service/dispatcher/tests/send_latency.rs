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

fn numeric_snapshot(query: &EvidenceQuery) -> EvidenceSnapshot {
    numeric_snapshot_at(query, Utc::now())
}

fn numeric_snapshot_at(query: &EvidenceQuery, observed: chrono::DateTime<Utc>) -> EvidenceSnapshot {
    let mut body: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../../tests/fixtures/diagnostics/send-latency-window.v1.json"
    ))
    .unwrap();
    let old: chrono::DateTime<Utc> = body["window"]["query_start"].as_str().unwrap().parse().unwrap();
    let delta = query.time_range.start.timestamp_millis() - old.timestamp_millis();
    body["scope"]["tenant_id"] = json!(query.tenant_id);
    body["scope"]["cluster_id"] = json!(query.cluster_id);
    body["policy"]["scope"]["cluster_id"] = json!(query.cluster_id);
    body["window"]["requested_start"] = json!(query.time_range.start);
    body["window"]["requested_end"] = json!(query.time_range.end);
    for key in [
        "query_start",
        "query_end",
        "data_first_at",
        "data_last_at",
        "oldest_last_sample_at",
    ] {
        let at: chrono::DateTime<Utc> = body["window"][key].as_str().unwrap().parse().unwrap();
        body["window"][key] = json!(at + chrono::Duration::milliseconds(delta));
    }
    for histogram in body["raw_histograms"].as_array_mut().unwrap() {
        for bucket in histogram["buckets"].as_array_mut().unwrap() {
            for point in bucket["samples"].as_array_mut().unwrap() {
                point[0] = json!(point[0].as_i64().unwrap() + delta);
            }
        }
    }
    body["collection_started_at"] = json!(observed);
    body["collected_at"] = json!(observed);
    let last: chrono::DateTime<Utc> = body["window"]["oldest_last_sample_at"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let nanos = i128::from(observed.timestamp()) * 1_000_000_000 + i128::from(observed.timestamp_subsec_nanos())
        - i128::from(last.timestamp_millis()) * 1_000_000;
    let age = u64::try_from((nanos + 999_999_999) / 1_000_000_000).unwrap();
    body["source_age_seconds"] = json!(age);
    let mut snapshot = EvidenceSnapshot::capture(
        query.clone(),
        current_evidence_schema(),
        observed,
        EvidenceContent::Inline(body),
    )
    .unwrap();
    snapshot.freshness_seconds = age;
    snapshot.exposure = EvidenceExposure::Synthetic;
    snapshot
        .warnings
        .push("synthetic persisted contract regression, not production verification".to_owned());
    // Simulate an untrusted complete declaration; the compiled missing-facts
    // gate must keep the four unsupported populations and correlation partial.
    snapshot.partial = false;
    snapshot.coverage = CoverageStatus::Available;
    snapshot
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL with legitimate historical synthetic captures"]
async fn postgres_send_latency_default_inspection_uses_storage_age_at_execution() {
    for stored_seconds in [0, 301] {
        let fixture = fixture().await;
        let captured = Utc::now() - chrono::Duration::seconds(stored_seconds);
        let base = chrono::DateTime::<Utc>::from_timestamp_millis(captured.timestamp_millis() - 1000).unwrap();
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.auth.tenant_id,
            cluster_id: fixture.request.scope.cluster_id,
            source: "prometheus".to_owned(),
            resource: "send-latency/orders".to_owned(),
            time_range: TimeRange::new(
                base + chrono::Duration::nanoseconds(500),
                base + chrono::Duration::milliseconds(100) + chrono::Duration::nanoseconds(800),
            )
            .unwrap(),
        };
        let original = numeric_snapshot_at(&query, captured);
        assert_eq!(original.freshness_seconds, 1);
        let snapshot = fixture
            .state
            .evidence
            .persist_cluster(&fixture.auth, original)
            .await
            .unwrap();
        assert!(
            rocketmq_sre_core::diagnostics::packs::assess_send_latency_observation(
                &snapshot,
                snapshot.observed_at.timestamp_millis()
            )
            .is_ok()
        );
        assert_eq!(
            rocketmq_sre_core::diagnostics::packs::assess_send_latency_observation(
                &snapshot,
                Utc::now().timestamp_millis()
            )
            .is_ok(),
            stored_seconds == 0
        );
        let router = crate::phase1_api::public_routes().with_state(fixture.state.clone());
        let (status, view) = call(
            router,
            "POST",
            "/v1/inspections",
            Some(&fixture.auth),
            Some(json!({"cluster_id":fixture.request.scope.cluster_id,"template":"telemetry","schedule":null})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{view}");
        let id: InspectionRunId = serde_json::from_value(view["run"]["id"].clone()).unwrap();
        let report: serde_json::Value = sqlx::query_scalar(
            "SELECT output FROM diagnostic_pack_runs WHERE inspection_run_id=$1 AND pack_id='send-latency.v2'",
        )
        .bind(id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
        assert_eq!(report["status"], "inconclusive");
        let local = report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|finding| finding["reason_code"] == "BROKER_SEND_LATENCY_ESTIMATE_EXCEEDS_POLICY");
        assert_eq!(
            local,
            stored_seconds == 0,
            "a historical capture must not be replayed as a current local predicate"
        );
        let retained = fixture
            .state
            .evidence
            .get(&fixture.auth, snapshot.evidence_id)
            .await
            .unwrap();
        assert_eq!(
            retained, snapshot,
            "execution must leave immutable historical observations unchanged"
        );
    }
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and durable synthetic Connector frames"]
async fn postgres_send_latency_ns_window_survives_real_persistence_and_saves_partial_local_finding() {
    let (mut fixture, principal, session) = prepared_range().await;
    let start = chrono::DateTime::from_timestamp_millis(Utc::now().timestamp_millis() - 1000).unwrap()
        + chrono::Duration::nanoseconds(500);
    fixture.request.scope.time_range = TimeRange::new(
        start,
        start + chrono::Duration::milliseconds(100) + chrono::Duration::nanoseconds(300),
    )
    .unwrap();
    fixture.request.packs = vec![InvestigationPackRef {
        pack_id: "send-latency.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
    }];
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("c03-window-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        let envelope = next_query(&fixture, &principal, session, 0).await;
        assert_eq!(envelope.query.source, "prometheus");
        assert_eq!(envelope.query.resource, "send-latency/orders");
        assert_ne!(envelope.query.time_range.start.timestamp_subsec_nanos() % 1000, 0);
        let snapshot = numeric_snapshot(&envelope.query);
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
    let stored = fixture.state.evidence.get(&fixture.auth, id).await.unwrap();
    assert_eq!(stored.time_range.start.timestamp_subsec_nanos() % 1000, 0);
    assert_eq!(stored.time_range.start.timestamp_micros(), start.timestamp_micros());
    let EvidenceContent::Inline(content) = &stored.content else {
        panic!("bounded fixture stays inline");
    };
    let original: chrono::DateTime<Utc> = content["window"]["requested_start"].as_str().unwrap().parse().unwrap();
    assert_eq!(original, start);
    let observed =
        rocketmq_sre_core::diagnostics::packs::assess_send_latency_observation(&stored, Utc::now().timestamp_millis())
            .unwrap();
    assert_eq!(
        observed.policy,
        rocketmq_sre_core::diagnostics::packs::LatencyPolicyState::Exceeded
    );
    let saved = fixture
        .repository
        .investigation_run_revision(&fixture.auth, run.id, run.pack_results[0].run_revision_id)
        .await
        .unwrap();
    assert!(saved.partial);
    assert_eq!(saved.status, InvestigationDiagnosisStatus::Inconclusive);
    assert_eq!(saved.requirements[0].quality, DiagnosticEvidenceQuality::Partial);
    assert!(
        saved.requirements[0]
            .missing_fields
            .iter()
            .any(|fact| fact.contains("correlated_segment_population"))
    );
    assert!(saved.findings.iter().any(
        |finding| finding.reason_code == "BROKER_SEND_LATENCY_ESTIMATE_EXCEEDS_POLICY"
            && finding.supporting_evidence_ids == vec![id]
    ));
    assert!(matches!(
        run.assessments[0].conclusion,
        HypothesisAssessmentConclusion::Inconclusive { .. }
    ));
    owner.task_group().cancel();
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL with the current migration set"]
async fn postgres_send_latency_legacy_template_keeps_saved_boolean_rules_and_rejects_viewer_writes() {
    let fixture = fixture().await;
    let cluster = fixture.request.scope.cluster_id;
    let at = Utc::now();
    let mut evidence = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.auth.tenant_id,
            cluster_id: cluster,
            source: "prometheus".to_owned(),
            resource: "send-latency/orders".to_owned(),
            time_range: TimeRange::new(at, at).unwrap(),
        },
        current_evidence_schema(),
        at,
        EvidenceContent::Inline(
            json!({"client_or_proxy_slow":false,"remoting_or_broker_slow":false,"store_slow":false}),
        ),
    )
    .unwrap();
    evidence.exposure = EvidenceExposure::Synthetic;
    fixture
        .state
        .evidence
        .persist_cluster(&fixture.auth, evidence)
        .await
        .unwrap();
    let router = || crate::phase1_api::public_routes().with_state(fixture.state.clone());
    let body = json!({"cluster_id":cluster,"template":"send_latency_legacy_v1","schedule":null});
    let mut viewer = fixture.auth.clone();
    viewer.roles = std::collections::BTreeSet::from(["viewer".to_owned()]);
    for schedule in [serde_json::Value::Null, json!("every 37m")] {
        let mut request = body.clone();
        request["schedule"] = schedule;
        assert_eq!(
            call(router(), "POST", "/v1/inspections", Some(&viewer), Some(request))
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inspection_runs WHERE tenant_id=$1 AND cluster_id=$2")
        .bind(fixture.auth.tenant_id.as_uuid())
        .bind(cluster.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let (status, body) = call(router(), "POST", "/v1/inspections", Some(&fixture.auth), Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let legacy: InspectionRunId = serde_json::from_value(body["run"]["id"].clone()).unwrap();
    let rows = sqlx::query("SELECT pack_id,output FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
        .bind(legacy.as_uuid())
        .fetch_all(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("pack_id"), "send-latency.v1");
    let old: serde_json::Value = rows[0].get("output");
    assert_eq!(old["status"], "healthy");
    let (status, current) = call(
        router(),
        "POST",
        "/v1/inspections",
        Some(&fixture.auth),
        Some(json!({"cluster_id":cluster,"template":"telemetry","schedule":null})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    let current: InspectionRunId = serde_json::from_value(current["run"]["id"].clone()).unwrap();
    let packs: Vec<String> = sqlx::query_scalar("SELECT pack_id FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
        .bind(current.as_uuid())
        .fetch_all(&fixture.repository.pool)
        .await
        .unwrap();
    assert!(packs.contains(&"send-latency.v2".to_owned()));
    assert!(!packs.contains(&"send-latency.v1".to_owned()));
    let retained: serde_json::Value =
        sqlx::query_scalar("SELECT output FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
            .bind(legacy.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
    assert_eq!(retained, old);
}
