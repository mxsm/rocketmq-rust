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

// Synthetic protocol transport fixture. AdminRpc records the contract under
// test, not a production-source or independently adjudicated qualification.
fn current_snapshot(query: &EvidenceQuery, captured: chrono::DateTime<Utc>) -> EvidenceSnapshot {
    let mut content: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../../../tests/fixtures/diagnostics/namesrv-route-observation.v1.json"
    ))
    .unwrap();
    let begin = captured - chrono::Duration::milliseconds(100);
    content["tenant_id"] = json!(query.tenant_id);
    content["cluster_id"] = json!(query.cluster_id);
    content["topic"] = json!(query.resource.strip_prefix("namesrv-route/").unwrap());
    content["requested_window"] = json!({"start":query.time_range.start,"end":query.time_range.end});
    content["collection_started_at"] = json!(begin);
    content["collected_at"] = json!(captured);
    for (index, round) in content["rounds"].as_array_mut().unwrap().iter_mut().enumerate() {
        round["started_at"] = json!(begin);
        round["finished_at"] = json!(if index == 0 { begin } else { captured });
        for peer in round["peers"].as_array_mut().unwrap() {
            for read in ["configuration", "route"] {
                peer[read]["started_at"] = json!(begin);
                peer[read]["finished_at"] = json!(begin);
            }
        }
    }
    content["comparison"] = rocketmq_sre_core::diagnostics::packs::compare_namesrv_route_content(&content)
        .unwrap()
        .comparison_json();
    let mut snapshot = EvidenceSnapshot::capture(
        query.clone(),
        current_evidence_schema(),
        captured,
        EvidenceContent::Inline(content),
    )
    .unwrap();
    snapshot.exposure = EvidenceExposure::AdminRpc;
    snapshot.freshness_seconds = 1;
    snapshot.partial = false;
    snapshot.coverage = CoverageStatus::Available;
    snapshot
        .warnings
        .push("synthetic stored-protocol regression; no production verification".to_owned());
    snapshot
}

#[tokio::test]
#[ignore = "requires task-owned Docker PostgreSQL and ordinary ended-window transport fixtures"]
async fn postgres_namesrv_current_context_survives_ns_to_us_persistence_and_real_saved_revision() {
    let (mut fixture, principal, session) = prepared().await;
    fixture.request.scope.resource = Some(InvestigationResource {
        kind: ResourceKind::Topic,
        key: "orders".to_owned(),
    });
    fixture.request.packs = vec![InvestigationPackRef {
        pack_id: "namesrv-route.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
    }];
    let end = chrono::DateTime::<Utc>::from_timestamp_micros(Utc::now().timestamp_micros() - 1_000_000).unwrap()
        + chrono::Duration::nanoseconds(123);
    fixture.request.scope.time_range = TimeRange::new(end - chrono::Duration::minutes(10), end).unwrap();
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("c04-current-context-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        let envelope = next_query(&fixture, &principal, session, 0).await;
        assert_eq!(envelope.query.source, "admin-query");
        assert_eq!(envelope.query.resource, "namesrv-route/orders");
        let captured = Utc::now();
        assert!(captured > envelope.query.time_range.end);
        let snapshot = current_snapshot(&envelope.query, captured);
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
    assert_eq!(run.pack_results.len(), 1);
    let stored = fixture.state.evidence.get(&fixture.auth, id).await.unwrap();
    assert_eq!(stored.time_range.end.timestamp_subsec_nanos() % 1000, 0);
    let EvidenceContent::Inline(content) = &stored.content else {
        panic!("small safe proof stays inline")
    };
    assert_eq!(content["requested_window"]["end"], json!(end));
    assert!(
        rocketmq_sre_core::diagnostics::packs::assess_namesrv_route_observation(&stored, Utc::now().timestamp_millis())
            .is_ok()
    );
    let saved = fixture
        .repository
        .investigation_run_revision(&fixture.auth, run.id, run.pack_results[0].run_revision_id)
        .await
        .unwrap();
    assert!(saved.partial);
    assert_eq!(saved.status, InvestigationDiagnosisStatus::Inconclusive);
    assert_eq!(saved.evidence_ids, vec![id]);
    assert_eq!(saved.requirements[0].quality, DiagnosticEvidenceQuality::Partial);
    assert!(
        saved.requirements[0]
            .missing_fields
            .iter()
            .any(|field| field == "historical_window_coverage")
    );
    assert!(saved.findings.iter().any(
        |finding| finding.reason_code == "NAMESRV_CURRENT_ROUTE_DIFFERENCE_OBSERVED"
            && finding.supporting_evidence_ids == vec![id]
    ));
    assert!(matches!(
        run.assessments[0].conclusion,
        HypothesisAssessmentConclusion::Inconclusive { .. }
    ));
    owner.task_group().cancel();
}

#[tokio::test]
#[ignore = "requires task-owned Docker PostgreSQL with actual current migrations"]
async fn postgres_namesrv_legacy_template_remains_exact_and_viewer_cannot_create_or_schedule() {
    let fixture = fixture().await;
    let cluster = fixture.request.scope.cluster_id;
    let at = Utc::now();
    let mut snapshot = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.auth.tenant_id,
            cluster_id: cluster,
            source: "rocketmq-mcp".to_owned(),
            resource: "namesrv-route/orders".to_owned(),
            time_range: TimeRange::new(at, at).unwrap(),
        },
        current_evidence_schema(),
        at,
        EvidenceContent::Inline(json!({"route_divergent":false,"registration_stale":false,"broker_unreachable":false})),
    )
    .unwrap();
    snapshot.exposure = EvidenceExposure::Synthetic;
    fixture
        .state
        .evidence
        .persist_cluster(&fixture.auth, snapshot)
        .await
        .unwrap();
    let router = || crate::phase1_api::public_routes().with_state(fixture.state.clone());
    let mut viewer = fixture.auth.clone();
    viewer.roles = std::collections::BTreeSet::from(["viewer".to_owned()]);
    for schedule in [serde_json::Value::Null, json!("every 37m")] {
        let body = json!({"cluster_id":cluster,"template":"namesrv_route_legacy_v1","schedule":schedule});
        assert_eq!(
            call(router(), "POST", "/v1/inspections", Some(&viewer), Some(body))
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
    let (status, body) = call(
        router(),
        "POST",
        "/v1/inspections",
        Some(&fixture.auth),
        Some(json!({"cluster_id":cluster,"template":"namesrv_route_legacy_v1","schedule":null})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let legacy: InspectionRunId = serde_json::from_value(body["run"]["id"].clone()).unwrap();
    let rows = sqlx::query("SELECT pack_id,output FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
        .bind(legacy.as_uuid())
        .fetch_all(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("pack_id"), "namesrv-route.v1");
    let old: serde_json::Value = rows[0].get("output");
    assert_eq!(old["status"], "healthy");
    let (status, body) = call(
        router(),
        "POST",
        "/v1/inspections",
        Some(&fixture.auth),
        Some(json!({"cluster_id":cluster,"template":"routing_proxy","schedule":null})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let current: InspectionRunId = serde_json::from_value(body["run"]["id"].clone()).unwrap();
    let packs: Vec<String> = sqlx::query_scalar("SELECT pack_id FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
        .bind(current.as_uuid())
        .fetch_all(&fixture.repository.pool)
        .await
        .unwrap();
    assert!(packs.contains(&"namesrv-route.v2".to_owned()));
    assert!(!packs.contains(&"namesrv-route.v1".to_owned()));
    let retained: serde_json::Value =
        sqlx::query_scalar("SELECT output FROM diagnostic_pack_runs WHERE inspection_run_id=$1")
            .bind(legacy.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
    assert_eq!(retained, old);
}

#[tokio::test]
#[ignore = "requires task-owned Docker PostgreSQL with legitimate stored protocol fixtures"]
async fn postgres_namesrv_inspection_does_not_refresh_stale_current_advisory() {
    for stored_seconds in [0, 301] {
        let fixture = fixture().await;
        let captured = Utc::now() - chrono::Duration::seconds(stored_seconds);
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.auth.tenant_id,
            cluster_id: fixture.request.scope.cluster_id,
            source: "admin-query".to_owned(),
            resource: "namesrv-route/orders".to_owned(),
            time_range: TimeRange::new(
                captured - chrono::Duration::minutes(5),
                captured - chrono::Duration::seconds(1),
            )
            .unwrap(),
        };
        let stored = fixture
            .state
            .evidence
            .persist_cluster(&fixture.auth, current_snapshot(&query, captured))
            .await
            .unwrap();
        let router = crate::phase1_api::public_routes().with_state(fixture.state.clone());
        let (status, body) = call(
            router,
            "POST",
            "/v1/inspections",
            Some(&fixture.auth),
            Some(json!({"cluster_id":query.cluster_id,"template":"routing_proxy","schedule":null})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let id: InspectionRunId = serde_json::from_value(body["run"]["id"].clone()).unwrap();
        let report: serde_json::Value = sqlx::query_scalar(
            "SELECT output FROM diagnostic_pack_runs WHERE inspection_run_id=$1 AND pack_id='namesrv-route.v2'",
        )
        .bind(id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
        assert_eq!(report["status"], "inconclusive");
        assert_eq!(
            report["findings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|finding| finding["reason_code"] == "NAMESRV_CURRENT_ROUTE_DIFFERENCE_OBSERVED"),
            stored_seconds == 0
        );
        assert_eq!(
            fixture
                .state
                .evidence
                .get(&fixture.auth, stored.evidence_id)
                .await
                .unwrap(),
            stored
        );
    }
}

#[tokio::test]
#[ignore = "requires task-owned Docker PostgreSQL with original admitted query proofs"]
async fn postgres_namesrv_planning_reads_original_stale_domain_metadata_after_narrowing_without_rule_reuse() {
    let (mut fixture, principal, session) = prepared().await;
    fixture.request.scope.resource = Some(InvestigationResource {
        kind: ResourceKind::Topic,
        key: "orders".to_owned(),
    });
    fixture.request.packs = vec![InvestigationPackRef {
        pack_id: "namesrv-route.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
    }];
    let created = fixture.create().await;
    let service = &fixture.state.investigation_runs;
    let runtime = service.dispatcher.as_ref().unwrap();
    let delegation = fixture
        .repository
        .investigation_run_delegation(created.run.id)
        .await
        .unwrap();
    let lease = fixture
        .repository
        .investigation_run_claim(&fixture.auth, created.run.id, uuid::Uuid::new_v4(), 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let context = context::catalog_context(service, &fixture.auth, &created.run);
    let worker = RunWorkerContext {
        auth: &fixture.auth,
        context: &context,
        fence: &lease.fence,
    };
    let running = fixture
        .repository
        .investigation_run_start(&worker, created.run.id, created.run.revision, Utc::now())
        .await
        .unwrap();
    let owner = RuntimeContext::from_current("c04-domain-metadata-regression").service_context("one-admitted-read");
    let requirement = InvestigationRequirementRef {
        pack: running.effective_packs[0].clone(),
        requirement_id: "namesrv-route".to_owned(),
    };
    let plan = InvestigationPlannedCollection {
        spec: InvestigationStepSpec {
            scope: running.effective_scope.clone(),
            pack: requirement.pack.clone(),
            work: InvestigationStepKind::CollectEvidence {
                requirement_id: requirement.requirement_id.clone(),
            },
        },
        source: "admin-query".to_owned(),
        resource: "namesrv-route/orders".to_owned(),
        consumers: vec![requirement],
        retry_step_id: None,
        reservation: InvestigationBudgetCost {
            read_queries: 1,
            model_calls: 0,
            evidence_items: 1,
            output_bytes: crate::connector_channel::MAX_RESPONSE_BYTES as u64,
        },
    };
    let execution = RunExecution {
        service,
        group: owner.task_group(),
        delegation: &delegation,
        lease: &lease,
    };
    let read = runtime.collect(&execution, &running, plan);
    let wire = async {
        let envelope = next_query(&fixture, &principal, session, 0).await;
        let snapshot = current_snapshot(&envelope.query, Utc::now() - chrono::Duration::seconds(400));
        let id = snapshot.evidence_id;
        respond(&fixture, &principal, &envelope, snapshot).await;
        id
    };
    let (result, id) = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(read, wire) })
        .await
        .unwrap();
    result.unwrap();
    let collected = service.get(&fixture.auth, created.run.id).await.unwrap();
    assert_eq!(collected.status, InvestigationRunStatus::Running);
    let mut narrower = collected.effective_scope.clone();
    narrower.time_range.start += chrono::Duration::seconds(1);
    narrower.time_range.end -= chrono::Duration::seconds(1);
    let request = command(
        &collected,
        InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: narrower,
                packs: collected.effective_packs.clone(),
                evidence_ids: vec![],
            },
        },
        "narrow-clear-business-inputs",
    );
    service.command(&fixture.auth, collected.id, &request).await.unwrap();
    let run = service.get(&fixture.auth, collected.id).await.unwrap();
    assert!(run.evidence_ids.is_empty());
    let mut connection = fixture.repository.pool.acquire().await.unwrap();
    let mut bindings = super::super::super::bindings::resolve(&mut connection, &service.catalog, &run)
        .await
        .unwrap();
    drop(connection);
    for proof in fixture
        .repository
        .investigation_run_read_proofs(&fixture.auth, &run)
        .await
        .unwrap()
    {
        let binding = proof.binding();
        if !bindings.contains(&binding) {
            bindings.push(binding);
        }
    }
    let pool = runtime
        .planning_snapshots(service, &fixture.auth, &run, &bindings, &[])
        .await
        .unwrap();
    assert_eq!(pool.len(), 1);
    assert_eq!(pool[0].evidence_id, id);
    assert!(
        rocketmq_sre_core::diagnostics::packs::assess_namesrv_route_observation(
            &pool[0],
            Utc::now().timestamp_millis()
        )
        .is_err()
    );
    let input = runtime.planning_input(&run, &bindings, &pool);
    assert_eq!(
        investigation_namesrv_domain_continuity(&input),
        NamesrvDomainContinuity::Matched
    );
    assert!(
        investigation_rule_inputs(&service.catalog, &run.effective_packs[0], &input)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        plan_investigation(&service.catalog, input).unwrap(),
        InvestigationPlanDecision::Collect(_)
    ));
    assert_eq!(charged_reads(&run), 1);
    assert!(run.pack_results.is_empty());
    let mut revoked = fixture.auth.clone();
    revoked.clusters.clear();
    assert!(
        runtime
            .planning_snapshots(service, &revoked, &run, &bindings, &[])
            .await
            .is_err()
    );
    fixture
        .repository
        .investigation_run_release(&fixture.auth, run.id, &lease.fence, Utc::now())
        .await
        .unwrap();
    owner.task_group().cancel();
}
