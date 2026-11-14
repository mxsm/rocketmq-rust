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

use std::collections::BTreeSet;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{Duration, Utc};
use rocketmq_sre_contracts::{
    ClusterId, CorrelationId, EvidenceContent, EvidenceExposure, EvidenceQuery, EvidenceSnapshot, QueryId, TenantId,
    TimeRange, current_evidence_schema,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    PostgresRepository,
    api::{CapabilityDocuments, build_router},
    auth::AuthContext,
    evidence::{EvidenceBlobStore, EvidenceService},
};

#[tokio::test]
#[ignore = "requires an isolated ROCKETMQ_SRE_TEST_DATABASE_URL and ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH"]
async fn coverage_quality_preserves_tenant_cluster_observation_order_and_legacy_shape() {
    let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit isolated test database");
    let repository = PostgresRepository::connect(&url, 4).await.unwrap();
    let tenant = TenantId::new();
    let foreign_tenant = TenantId::new();
    let a = ClusterId::new();
    let b = ClusterId::new();
    let forbidden = ClusterId::new();
    for cluster in [a, b, forbidden] {
        sqlx::query("INSERT INTO clusters (id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,requested_access_profile,effective_access_profile,onboarding_state)
            VALUES ($1,$2,$3,'test','local','5.3.2','test','coverage-quality','read_only','read_only','ready_read_only')")
            .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("coverage-{cluster}"))
            .execute(&repository.pool).await.unwrap();
    }
    let auth = AuthContext {
        tenant_id: tenant,
        subject: "coverage-quality".to_owned(),
        clusters: BTreeSet::from([a, b]),
        roles: BTreeSet::from(["rocketmq:read".to_owned()]),
    };
    let service = EvidenceService::new(repository.clone(), EvidenceBlobStore::in_memory(65_536));
    let now = Utc::now();
    let complete = json!({"total_lag":10,"lag_slope_per_min":1,"queue_skew_ratio":1,"consume_rate_per_sec":5,"produce_rate_per_sec":6});
    let make = |cluster, at, content| {
        let mut evidence = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: tenant,
                cluster_id: cluster,
                source: "rocketmq-mcp".to_owned(),
                resource: "consumer-lag/group/orders".to_owned(),
                time_range: TimeRange::new(at, at).unwrap(),
            },
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(content),
        )
        .unwrap();
        evidence.exposure = EvidenceExposure::McpTool;
        evidence
    };
    let newest_a = service
        .persist_cluster(&auth, make(a, now - Duration::seconds(1), json!({"total_lag":0})))
        .await
        .unwrap();
    // An older observation arriving later must not replace the newer incomplete sample.
    let older_a = service
        .persist_cluster(&auth, make(a, now - Duration::seconds(20), complete.clone()))
        .await
        .unwrap();
    let newest_b = service.persist_cluster(&auth, make(b, now, complete)).await.unwrap();
    for (source_tenant, cluster, status) in [
        (tenant, a, "missing"),
        (tenant, b, "queryable"),
        (foreign_tenant, a, "queryable"),
    ] {
        sqlx::query("INSERT INTO source_capability_history (tenant_id,cluster_id,source,schema_major,status,limits,observed_at) VALUES ($1,$2,'rocketmq-mcp',1,$3,'{}'::jsonb,$4)")
            .bind(source_tenant.as_uuid()).bind(cluster.as_uuid()).bind(status).bind(now).execute(&repository.pool).await.unwrap();
    }
    let router = build_router(
        repository.clone(),
        CapabilityDocuments::embedded().unwrap(),
        "coverage-quality-test-token-with-signing-entropy",
    )
    .unwrap();
    let request_uri = |uri: String| {
        Request::builder()
            .uri(uri)
            .header(
                "authorization",
                "Bearer coverage-quality-test-token-with-signing-entropy",
            )
            .header("x-rocketmq-tenant", tenant.to_string())
            .header("x-rocketmq-clusters", format!("{a},{b}"))
            .header("x-rocketmq-subject", "coverage-quality")
            .body(Body::empty())
            .unwrap()
    };
    let request = |cluster: Option<ClusterId>| {
        request_uri(cluster.map_or_else(
            || "/v1/capabilities/coverage".to_owned(),
            |cluster| format!("/v1/capabilities/coverage?cluster_id={cluster}"),
        ))
    };
    for (cluster, expected, id) in [
        (a, "partial", newest_a.evidence_id),
        (b, "available", newest_b.evidence_id),
    ] {
        let response = router.clone().oneshot(request(Some(cluster))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap()).unwrap();
        assert!(value.get("rows").is_some() && value.get("packs").is_some() && value.get("selected").is_some());
        assert_eq!(value["quality"]["cluster_id"], cluster.to_string());
        assert_eq!(value["quality"]["execution_eligible"], false);
        let pack = value["quality"]["packs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|pack| pack["pack_id"] == "consumer-lag.v2")
            .unwrap();
        let required = &pack["requirements"][0];
        assert_eq!(required["quality"], expected);
        assert_eq!(required["evidence_ids"], json!([id]));
        if cluster == a {
            assert_eq!(pack["layers"]["configured"]["status"], "absent");
        }
        assert!(
            value["sourceCapabilities"]
                .as_array()
                .unwrap()
                .iter()
                .all(|source| source["clusterId"] == cluster.to_string())
        );
    }
    let response = router.clone().oneshot(request(None)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap()).unwrap();
    assert_eq!(
        value["quality"]["cluster_id"],
        auth.clusters.first().unwrap().to_string()
    );
    let response = router.clone().oneshot(request(Some(forbidden))).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let (investigation, revision, conversation) = seed_revision(&repository, &auth, a, &[older_a.evidence_id]).await;
    let (other_investigation, other_revision, other_conversation) =
        seed_revision(&repository, &auth, b, &[newest_b.evidence_id]).await;
    for _ in 0..2 {
        let uri =
            format!("/v1/capabilities/coverage?cluster_id={a}&investigation_id={investigation}&revision_id={revision}");
        let response = router.clone().oneshot(request_uri(uri)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap()).unwrap();
        assert_eq!(value["quality"]["investigation_id"], investigation.to_string());
        assert_eq!(value["quality"]["revision_id"], revision.to_string());
        let pack = value["quality"]["packs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|pack| pack["pack_id"] == "consumer-lag.v2")
            .unwrap();
        assert_eq!(pack["requirements"][0]["quality"], "available");
        assert_eq!(pack["requirements"][0]["evidence_ids"], json!([older_a.evidence_id]));
    }
    for (query, status) in [
        (format!("investigation_id={investigation}"), StatusCode::BAD_REQUEST),
        (format!("revision_id={revision}"), StatusCode::BAD_REQUEST),
        (
            format!("investigation_id=invalid&revision_id={revision}"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("investigation_id={investigation}&revision_id={other_revision}"),
            StatusCode::NOT_FOUND,
        ),
        (
            format!("cluster_id={a}&investigation_id={other_investigation}&revision_id={other_revision}"),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let response = router
            .clone()
            .oneshot(request_uri(format!("/v1/capabilities/coverage?{query}")))
            .await
            .unwrap();
        assert_eq!(response.status(), status);
    }
    let bound_uri =
        format!("/v1/capabilities/coverage?cluster_id={a}&investigation_id={investigation}&revision_id={revision}");
    sqlx::query("UPDATE investigation_diagnosis_revisions SET cluster_id=$1 WHERE id=$2")
        .bind(b.as_uuid())
        .bind(revision.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    let response = router.clone().oneshot(request_uri(bound_uri.clone())).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    sqlx::query("UPDATE investigation_diagnosis_revisions SET cluster_id=$1 WHERE id=$2")
        .bind(a.as_uuid())
        .bind(revision.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE investigations SET tenant_id=$1 WHERE id=$2")
        .bind(foreign_tenant.as_uuid())
        .bind(investigation.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    let response = router.clone().oneshot(request_uri(bound_uri.clone())).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    sqlx::query("UPDATE investigations SET tenant_id=$1 WHERE id=$2")
        .bind(tenant.as_uuid())
        .bind(investigation.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE investigation_diagnosis_revisions SET evidence_ids=$1 WHERE id=$2")
        .bind(vec![newest_b.evidence_id.as_uuid()])
        .bind(revision.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    let response = router.clone().oneshot(request_uri(bound_uri.clone())).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let reference_service = EvidenceService::new(repository.clone(), EvidenceBlobStore::in_memory(1024));
    let mut external_content = json!({"total_lag":10,"lag_slope_per_min":1,"queue_skew_ratio":1,"consume_rate_per_sec":5,"produce_rate_per_sec":6});
    external_content["padding"] = Value::String("x".repeat(2048));
    let external = reference_service
        .persist_cluster(&auth, make(a, now, external_content))
        .await
        .unwrap();
    sqlx::query("UPDATE investigation_diagnosis_revisions SET evidence_ids=$1 WHERE id=$2")
        .bind(vec![external.evidence_id.as_uuid()])
        .bind(revision.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    let response = router.clone().oneshot(request_uri(bound_uri.clone())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let encoded = to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap();
    let value: Value = serde_json::from_slice(&encoded).unwrap();
    let pack = value["quality"]["packs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pack| pack["pack_id"] == "consumer-lag.v2")
        .unwrap();
    assert_eq!(pack["requirements"][0]["quality"], "unknown");
    assert_eq!(pack["requirements"][0]["reason_code"], "content_not_inline");
    assert!(!String::from_utf8_lossy(&encoded).contains("padding"));
    sqlx::query("UPDATE investigation_diagnosis_revisions SET evidence_ids=$1 WHERE id=$2")
        .bind(vec![older_a.evidence_id.as_uuid()])
        .bind(revision.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    // Expired references remain missing in the saved revision, never replaced by a newer sample.
    sqlx::query("UPDATE evidence_snapshots SET expires_at=NOW()-INTERVAL '1 second' WHERE id=$1")
        .bind(older_a.evidence_id.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    let response = router
        .clone()
        .oneshot(request_uri(format!(
            "/v1/capabilities/coverage?cluster_id={a}&investigation_id={investigation}&revision_id={revision}"
        )))
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap()).unwrap();
    let pack = value["quality"]["packs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pack| pack["pack_id"] == "consumer-lag.v2")
        .unwrap();
    assert_eq!(pack["requirements"][0]["quality"], "missing");
    assert_eq!(pack["requirements"][0]["evidence_ids"], json!([]));
    // The latest-per-resource scan is bounded. Excluded signals are unknown,
    // rather than reported absent or filled by an older retained sample.
    let target_resource = "consumer-lag/group/targeted";
    let target_at = Utc::now();
    let mut target=EvidenceSnapshot::capture(EvidenceQuery {
        query_id:QueryId::new(),correlation_id:CorrelationId::new(),tenant_id:tenant,cluster_id:a,
        source:"rocketmq-mcp".to_owned(),resource:target_resource.to_owned(),time_range:TimeRange::new(target_at,target_at).unwrap(),
    },current_evidence_schema(),target_at,EvidenceContent::Inline(json!({"total_lag":10,"lag_slope_per_min":1,"queue_skew_ratio":1,"consume_rate_per_sec":5,"produce_rate_per_sec":6}))).unwrap();
    target.exposure = EvidenceExposure::McpTool;
    let target = service.persist_cluster(&auth, target).await.unwrap();
    for index in 0..65 {
        let observed = Utc::now();
        let sample = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: tenant,
                cluster_id: a,
                source: "prometheus".to_owned(),
                resource: format!("unrelated/{index}"),
                time_range: TimeRange::new(observed, observed).unwrap(),
            },
            current_evidence_schema(),
            observed,
            EvidenceContent::Inline(json!({"sample":index})),
        )
        .unwrap();
        service.persist_cluster(&auth, sample).await.unwrap();
    }
    let response = router.clone().oneshot(request(Some(a))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap()).unwrap();
    assert_eq!(value["quality"]["partial"], true);
    let pack = value["quality"]["packs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pack| pack["pack_id"] == "consumer-lag.v2")
        .unwrap();
    assert_eq!(pack["requirements"][0]["quality"], "unknown");
    assert!(pack["requirements"][0]["evidence_ids"].as_array().unwrap().is_empty());
    let response = router
        .clone()
        .oneshot(request_uri(format!(
            "/v1/capabilities/coverage?cluster_id={a}&resource={target_resource}"
        )))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap()).unwrap();
    assert_eq!(value["quality"]["partial"], false);
    let pack = value["quality"]["packs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pack| pack["pack_id"] == "consumer-lag.v2")
        .unwrap();
    assert_eq!(pack["requirements"][0]["quality"], "available");
    assert_eq!(pack["requirements"][0]["evidence_ids"], json!([target.evidence_id]));
    let investigations = vec![investigation.as_uuid(), other_investigation.as_uuid()];
    let conversations = vec![conversation.as_uuid(), other_conversation.as_uuid()];
    for statement in [
        "DELETE FROM investigation_diagnosis_revisions WHERE investigation_id=ANY($1)",
        "DELETE FROM incident_timeline WHERE investigation_id=ANY($1)",
    ] {
        sqlx::query(statement)
            .bind(&investigations)
            .execute(&repository.pool)
            .await
            .unwrap();
    }
    for statement in [
        "DELETE FROM conversation_answer_revisions WHERE conversation_id=ANY($1)",
        "DELETE FROM conversation_turns WHERE conversation_id=ANY($1)",
        "UPDATE conversations SET investigation_id=NULL WHERE id=ANY($1)",
    ] {
        sqlx::query(statement)
            .bind(&conversations)
            .execute(&repository.pool)
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM investigations WHERE id=ANY($1)")
        .bind(&investigations)
        .execute(&repository.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM conversations WHERE id=ANY($1)")
        .bind(&conversations)
        .execute(&repository.pool)
        .await
        .unwrap();
    for cluster in [a, b, forbidden] {
        sqlx::query("DELETE FROM workflow_events WHERE cluster_id=$1")
            .bind(cluster.as_uuid())
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM source_capability_history WHERE cluster_id=$1")
            .bind(cluster.as_uuid())
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM evidence_snapshots WHERE cluster_id=$1")
            .bind(cluster.as_uuid())
            .execute(&repository.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM clusters WHERE id=$1")
            .bind(cluster.as_uuid())
            .execute(&repository.pool)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires disposable ROCKETMQ_SRE_TEST_DATABASE_URL and ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH"]
async fn postgres_store_pressure_scenario_pins_only_an_authorized_exact_primary_revision() {
    let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit disposable test database");
    let repository = PostgresRepository::connect(&url, 4).await.unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    sqlx::query("INSERT INTO clusters (id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,requested_access_profile,effective_access_profile,onboarding_state)
        VALUES ($1,$2,$3,'test','local','test','test','synthetic-scenario-compatibility','read_only','read_only','ready_read_only')")
        .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("c02-{cluster}"))
        .execute(&repository.pool).await.unwrap();
    let auth = AuthContext {
        tenant_id: tenant,
        subject: "c02-synthetic-scenario-compatibility".to_owned(),
        clusters: BTreeSet::from([cluster]),
        roles: BTreeSet::from(["rocketmq:read".to_owned()]),
    };
    let (legacy_i, legacy_r, _) =
        seed_revision_with_pack(&repository, &auth, cluster, &[], ("store-pressure.v1", "1.0.0")).await;
    let (current_i, current_r, _) =
        seed_revision_with_pack(&repository, &auth, cluster, &[], ("store-pressure.v2", "2.0.0")).await;
    let router = build_router(
        repository.clone(),
        CapabilityDocuments::embedded().unwrap(),
        "c02-synthetic-scenario-token-with-signing-entropy",
    )
    .unwrap();
    for (binding, expected) in [
        (String::new(), "store-pressure.v2"),
        (
            format!("&investigation_id={legacy_i}&revision_id={legacy_r}"),
            "store-pressure.v1",
        ),
        (
            format!("&investigation_id={current_i}&revision_id={current_r}"),
            "store-pressure.v2",
        ),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/capabilities/coverage?cluster_id={cluster}{binding}"))
                    .header(
                        "authorization",
                        "Bearer c02-synthetic-scenario-token-with-signing-entropy",
                    )
                    .header("x-rocketmq-tenant", tenant.to_string())
                    .header("x-rocketmq-clusters", cluster.to_string())
                    .header("x-rocketmq-subject", &auth.subject)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 2 * 1024 * 1024).await.unwrap()).unwrap();
        let scenario = value["quality"]["scenarios"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == "backlog_store")
            .unwrap();
        assert_eq!(scenario["pack_ids"], json!(["consumer-lag.v2", expected]));
        assert!(
            scenario["required_signal_ids"]
                .as_array()
                .unwrap()
                .iter()
                .any(|id| id.as_str().unwrap().starts_with(&format!("{expected}:")))
        );
        assert_eq!(value["quality"]["execution_eligible"], false);
    }
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/capabilities/coverage?cluster_id={cluster}&investigation_id={legacy_i}&revision_id={legacy_r}"
                ))
                .header(
                    "authorization",
                    "Bearer c02-synthetic-scenario-token-with-signing-entropy",
                )
                .header("x-rocketmq-tenant", tenant.to_string())
                .header("x-rocketmq-clusters", ClusterId::new().to_string())
                .header("x-rocketmq-subject", &auth.subject)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let retained: String = sqlx::query_scalar("SELECT pack_id FROM investigation_diagnosis_revisions WHERE id=$1")
        .bind(legacy_r.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
    assert_eq!(retained, "store-pressure.v1");
}

pub(super) async fn seed_revision(
    repository: &PostgresRepository,
    auth: &AuthContext,
    cluster: ClusterId,
    ids: &[rocketmq_sre_contracts::EvidenceId],
) -> (
    rocketmq_sre_contracts::InvestigationId,
    rocketmq_sre_contracts::DiagnosisRevisionId,
    rocketmq_sre_contracts::ConversationId,
) {
    seed_revision_with_pack(repository, auth, cluster, ids, ("consumer-lag.v2", "2.0.0")).await
}

async fn seed_revision_with_pack(
    repository: &PostgresRepository,
    auth: &AuthContext,
    cluster: ClusterId,
    ids: &[rocketmq_sre_contracts::EvidenceId],
    pack: (&str, &str),
) -> (
    rocketmq_sre_contracts::InvestigationId,
    rocketmq_sre_contracts::DiagnosisRevisionId,
    rocketmq_sre_contracts::ConversationId,
) {
    use crate::workflow::ConversationCreateRequest;
    use rocketmq_sre_contracts::{ConversationAnswerRevisionId, ConversationTurnId, DiagnosisRevisionId};
    let correlation = CorrelationId::new();
    let view = repository
        .create_conversation(
            auth,
            &ConversationCreateRequest {
                cluster_id: cluster,
                question: "Inspect saved evidence quality".to_owned(),
                resource: None,
                persist_investigation: true,
            },
            correlation,
        )
        .await
        .unwrap();
    let investigation = view.investigation.unwrap().id;
    let turn = ConversationTurnId::new();
    sqlx::query("INSERT INTO conversation_turns (id,conversation_id,tenant_id,cluster_id,sequence,question,status,correlation_id,created_at,completed_at)
        VALUES ($1,$2,$3,$4,1,'Inspect saved evidence quality','answered',$5,NOW(),NOW())")
        .bind(turn.as_uuid()).bind(view.conversation.id.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(cluster.as_uuid())
        .bind(correlation.as_uuid()).execute(&repository.pool).await.unwrap();
    let answer = ConversationAnswerRevisionId::new();
    let revision = DiagnosisRevisionId::new();
    let evidence = ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>();
    sqlx::query("INSERT INTO conversation_answer_revisions (id,conversation_id,turn_id,revision,answer,mode,evidence_ids,partial,created_at) VALUES ($1,$2,$3,1,'Bounded saved result','rules_only',$4,true,NOW())")
        .bind(answer.as_uuid()).bind(view.conversation.id.as_uuid()).bind(turn.as_uuid()).bind(&evidence).execute(&repository.pool).await.unwrap();
    sqlx::query("INSERT INTO investigation_diagnosis_revisions (id,tenant_id,cluster_id,investigation_id,conversation_id,turn_id,answer_revision_id,revision,pack_id,pack_version,status,rule_result,hypotheses,evidence_ids,execution_eligible,partial,correlation_id,created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,1,$10,$11,'inconclusive','{}'::jsonb,'[]'::jsonb,$8,false,true,$9,NOW())")
        .bind(revision.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(cluster.as_uuid()).bind(investigation.as_uuid())
        .bind(view.conversation.id.as_uuid()).bind(turn.as_uuid()).bind(answer.as_uuid()).bind(&evidence).bind(correlation.as_uuid())
        .bind(pack.0).bind(pack.1).execute(&repository.pool).await.unwrap();
    (investigation, revision, view.conversation.id)
}
