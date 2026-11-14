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

use chrono::{Duration, TimeZone};
use rocketmq_sre_contracts::{
    CAPABILITY_VERIFICATION_SCHEMA_VERSION, CapabilityVerificationLevel as Level,
    CapabilityVerificationMethod as Method, CapabilityVerificationOutcome as Outcome, ClusterId, EvidenceId, TenantId,
    TimeRange,
};

use super::*;

fn request() -> CapabilityVerificationRequest {
    let now = Utc.with_ymd_and_hms(2026, 11, 11, 10, 0, 0).single().unwrap();
    CapabilityVerificationRequest {
        schema_version: CAPABILITY_VERIFICATION_SCHEMA_VERSION.to_owned(),
        tenant_id: TenantId::new(),
        cluster_id: ClusterId::new(),
        pack_id: "consumer-lag.v2".to_owned(),
        pack_version: "2.0.0".to_owned(),
        level: Level::Production,
        method: Method::LiveReadOnly,
        outcome: Outcome::Passed,
        resource: Some("consumer-lag/group/orders".to_owned()),
        investigation_id: None,
        revision_id: None,
        time_range: TimeRange::new(now - Duration::minutes(5), now).unwrap(),
        observed_at: now,
        valid_until: now + Duration::hours(1),
        source_ref: "urn:rocketmq:validation:test-report".to_owned(),
        report_digest: format!("sha256:{}", "a".repeat(64)),
        evidence_ids: vec![EvidenceId::new()],
    }
}

#[test]
fn ordinary_connector_and_model_identities_cannot_issue_verification_records() {
    let request = request();
    let mut auth = AuthContext {
        tenant_id: request.tenant_id,
        subject: "verifier-workload".to_owned(),
        clusters: BTreeSet::from([request.cluster_id]),
        roles: BTreeSet::new(),
    };
    for roles in [
        vec!["operator"],
        vec!["connector_service"],
        vec!["model_service"],
        vec!["capability_verifier_service", "model_service"],
        vec!["capability_verifier_service", "connector_service"],
    ] {
        auth.roles = roles.into_iter().map(str::to_owned).collect();
        assert!(validation::authorize(&auth, VerifierIdentity::Oidc, &request).is_err());
    }
    auth.roles = BTreeSet::from(["capability_verifier_service".to_owned()]);
    assert!(validation::authorize(&auth, VerifierIdentity::Oidc, &request).is_ok());
    assert!(validation::authorize(&auth, VerifierIdentity::Development, &request).is_err());
    auth.tenant_id = TenantId::new();
    assert!(validation::authorize(&auth, VerifierIdentity::Oidc, &request).is_err());
}

#[test]
fn reports_need_exact_pack_time_provenance_and_an_explicit_production_target() {
    let valid = request();
    let now = valid.observed_at;
    assert!(validation::validate(&valid, now).is_ok());
    let mut invalid = valid.clone();
    invalid.pack_version = "2.0.1".to_owned();
    assert!(validation::validate(&invalid, now).is_err());
    invalid = valid.clone();
    invalid.method = Method::DeterministicFixture;
    assert!(validation::validate(&invalid, now).is_err());
    invalid = valid.clone();
    invalid.resource = None;
    assert!(validation::validate(&invalid, now).is_err());
    invalid = valid.clone();
    invalid.source_ref = "https://user:secret@example.invalid/report".to_owned();
    assert!(validation::validate(&invalid, now).is_err());
    invalid = valid.clone();
    invalid.observed_at = now + Duration::seconds(1);
    assert!(validation::validate(&invalid, now).is_err());
    invalid = valid.clone();
    invalid.valid_until = now + Duration::days(31);
    assert!(validation::validate(&invalid, now).is_err());
    assert!(validation::validate_resource("consumer/group").is_err());
    assert!(validation::validate_resource("https://example.invalid/raw").is_err());
}

#[tokio::test]
#[ignore = "requires an explicitly disposable ROCKETMQ_SRE_TEST_DATABASE_URL; append-only reports are retained"]
async fn postgres_verification_records_enforce_proof_scope_identity_expiry_and_source_order() {
    use crate::{
        PostgresRepository,
        api::capability_verification_test_state,
        coverage::{self, CoverageQuery},
    };
    use rocketmq_sre_contracts::{
        CorrelationId, EvidenceContent, EvidenceExposure, EvidenceQuery, EvidenceSnapshot, QueryId,
        current_evidence_schema,
    };
    use serde_json::{Value, json};
    let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit disposable database");
    let repository = PostgresRepository::connect(&url, 4).await.unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    sqlx::query("INSERT INTO clusters (id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,requested_access_profile,effective_access_profile,onboarding_state)
        VALUES ($1,$2,$3,'test','local','5.3.2','test','capability-verification','read_only','read_only','ready_read_only')")
        .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("verification-{cluster}")).execute(&repository.pool).await.unwrap();
    let state = capability_verification_test_state(repository.clone()).unwrap();
    let auth = AuthContext {
        tenant_id: tenant,
        subject: "dedicated-verifier-fixture".to_owned(),
        clusters: BTreeSet::from([cluster]),
        roles: BTreeSet::from(["capability_verifier_service".to_owned(), "rocketmq:read".to_owned()]),
    };
    let mut foreign_auth = auth.clone();
    foreign_auth.tenant_id = TenantId::new();
    let mut foreign_request = request();
    foreign_request.tenant_id = foreign_auth.tenant_id;
    foreign_request.cluster_id = cluster;
    foreign_request.level = Level::Test;
    foreign_request.method = Method::DeterministicFixture;
    foreign_request.resource = None;
    foreign_request.evidence_ids.clear();
    foreign_request.observed_at = Utc::now();
    foreign_request.time_range = TimeRange::new(foreign_request.observed_at, foreign_request.observed_at).unwrap();
    foreign_request.valid_until = foreign_request.observed_at + Duration::hours(1);
    assert!(
        record(&state, &foreign_auth, VerifierIdentity::Oidc, foreign_request)
            .await
            .is_err()
    );
    assert!(
        coverage::matrix(
            &state,
            &foreign_auth,
            CoverageQuery {
                cluster_id: Some(cluster.to_string()),
                ..CoverageQuery::default()
            }
        )
        .await
        .is_err()
    );
    let at = Utc::now() - Duration::seconds(2);
    let make = |resource: &str, complete: bool| {
        let content = if complete {
            json!({"total_lag":10,"lag_slope_per_min":1,"queue_skew_ratio":1,"consume_rate_per_sec":5,"produce_rate_per_sec":6})
        } else {
            json!({"total_lag":0})
        };
        let mut snapshot = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: tenant,
                cluster_id: cluster,
                source: "rocketmq-mcp".to_owned(),
                resource: resource.to_owned(),
                time_range: TimeRange::new(at, at).unwrap(),
            },
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(content),
        )
        .unwrap();
        snapshot.exposure = EvidenceExposure::McpTool;
        snapshot
    };
    let target = "consumer-lag/group/orders";
    let sample = state.evidence.persist_cluster(&auth, make(target, true)).await.unwrap();
    let mut base = request();
    base.tenant_id = tenant;
    base.cluster_id = cluster;
    base.evidence_ids = vec![sample.evidence_id];
    base.observed_at = Utc::now();
    base.time_range = TimeRange::new(at - Duration::seconds(1), base.observed_at).unwrap();
    base.valid_until = base.observed_at + Duration::hours(1);
    let saved = record(&state, &auth, VerifierIdentity::Oidc, base.clone())
        .await
        .unwrap();
    assert_eq!(saved.verified_by, auth.subject);
    let retry = record(&state, &auth, VerifierIdentity::Oidc, base.clone())
        .await
        .unwrap();
    assert_eq!(saved.record_id, retry.record_id);
    assert_eq!(saved.recorded_at, retry.recorded_at);
    let mut uppercase = base.clone();
    uppercase.report_digest = format!("sha256:{}", "A".repeat(64));
    assert_eq!(
        record(&state, &auth, VerifierIdentity::Oidc, uppercase)
            .await
            .unwrap()
            .record_id,
        saved.record_id
    );
    {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let request = Request::builder()
            .method("POST")
            .uri("/internal/v1/capability-verifications")
            .header(
                "authorization",
                "Bearer capability-verification-test-signing-key-at-least-32-bytes",
            )
            .header("x-rocketmq-tenant", tenant.to_string())
            .header("x-rocketmq-clusters", cluster.to_string())
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&base).unwrap()))
            .unwrap();
        let response = routes().with_state(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    let mut conflicting = base.clone();
    conflicting.outcome = Outcome::Failed;
    assert!(
        record(&state, &auth, VerifierIdentity::Oidc, conflicting)
            .await
            .is_err()
    );
    for outcome in [
        Outcome::Passed,
        Outcome::Failed,
        Outcome::Inconclusive,
        Outcome::Revoked,
    ] {
        let mut request = base.clone();
        request.outcome = outcome;
        assert!(
            record(&state, &auth, VerifierIdentity::Development, request)
                .await
                .is_err()
        );
    }
    let get = |resource: Option<&str>| CoverageQuery {
        cluster_id: Some(cluster.to_string()),
        resource: resource.map(str::to_owned),
        ..CoverageQuery::default()
    };
    let production = |value: &Value| {
        value["quality"]["packs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|pack| pack["pack_id"] == "consumer-lag.v2")
            .unwrap()["layers"]["production_verified"]
            .clone()
    };
    let mut test_report = base.clone();
    test_report.level = Level::Test;
    test_report.method = Method::DeterministicFixture;
    test_report.resource = None;
    test_report.evidence_ids.clear();
    record(&state, &auth, VerifierIdentity::Oidc, test_report)
        .await
        .unwrap();
    let global = coverage::matrix(&state, &auth, get(None)).await.unwrap();
    assert_eq!(production(&global)["status"], "unknown");
    let test_fact = &global["quality"]["packs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pack| pack["pack_id"] == "consumer-lag.v2")
        .unwrap()["layers"]["test_verified"];
    assert_eq!(test_fact["status"], "present");
    assert_eq!(
        test_fact["verification"]["assessment"]["method"],
        "deterministic_fixture"
    );
    let scoped = coverage::matrix(&state, &auth, get(Some(target))).await.unwrap();
    assert_eq!(production(&scoped)["status"], "present");
    assert_eq!(
        production(&scoped)["verification"]["record_id"],
        saved.record_id.to_string()
    );
    assert_eq!(scoped["quality"]["execution_eligible"], false);
    let foreign = coverage::matrix(&state, &auth, get(Some("consumer-lag/group/other")))
        .await
        .unwrap();
    assert_eq!(production(&foreign)["status"], "unknown");

    let bad = state
        .evidence
        .persist_cluster(&auth, make("consumer-lag/group/partial", false))
        .await
        .unwrap();
    let mut rejected = base.clone();
    rejected.resource = Some(bad.resource.clone());
    rejected.evidence_ids = vec![bad.evidence_id];
    assert!(record(&state, &auth, VerifierIdentity::Oidc, rejected).await.is_err());
    let mut synthetic = make("consumer-lag/group/synthetic", true);
    synthetic.exposure = EvidenceExposure::Synthetic;
    let synthetic = state.evidence.persist_cluster(&auth, synthetic).await.unwrap();
    let mut rejected = base.clone();
    rejected.resource = Some(synthetic.resource.clone());
    rejected.evidence_ids = vec![synthetic.evidence_id];
    assert!(record(&state, &auth, VerifierIdentity::Oidc, rejected).await.is_err());

    // A later failed source observation wins over a late, older pass. Replaying
    // the original report returns its old identity and cannot reorder it.
    let mut failed = base.clone();
    failed.outcome = Outcome::Failed;
    failed.observed_at = Utc::now();
    failed.report_digest = format!("sha256:{:064x}", 2);
    failed.time_range.end = failed.observed_at;
    record(&state, &auth, VerifierIdentity::Oidc, failed.clone())
        .await
        .unwrap();
    let mut late = base.clone();
    late.report_digest = format!("sha256:{:064x}", 3);
    record(&state, &auth, VerifierIdentity::Oidc, late).await.unwrap();
    record(&state, &auth, VerifierIdentity::Oidc, base.clone())
        .await
        .unwrap();
    assert_eq!(
        production(&coverage::matrix(&state, &auth, get(Some(target))).await.unwrap())["status"],
        "absent"
    );
    let mut same_time = failed.clone();
    same_time.outcome = Outcome::Passed;
    same_time.report_digest = format!("sha256:{:064x}", 4);
    record(&state, &auth, VerifierIdentity::Oidc, same_time).await.unwrap();
    assert_eq!(
        production(&coverage::matrix(&state, &auth, get(Some(target))).await.unwrap())["status"],
        "absent"
    );
    let mut revoked = failed;
    revoked.outcome = Outcome::Revoked;
    revoked.observed_at = Utc::now();
    revoked.time_range.end = revoked.observed_at;
    revoked.report_digest = format!("sha256:{:064x}", 5);
    record(&state, &auth, VerifierIdentity::Oidc, revoked.clone())
        .await
        .unwrap();
    assert_eq!(
        production(&coverage::matrix(&state, &auth, get(Some(target))).await.unwrap())["reason_code"],
        "verification_record_revoked"
    );
    let mut expired = revoked;
    expired.outcome = Outcome::Passed;
    expired.report_digest = format!("sha256:{:064x}", 6);
    expired.observed_at = Utc::now();
    expired.time_range.end = expired.observed_at;
    expired.valid_until = expired.observed_at + Duration::milliseconds(1);
    // Use a deterministic evaluation instant below rather than sleeping for expiry.
    let expired = record(&state, &auth, VerifierIdentity::Oidc, expired).await.unwrap();
    let mut context: rocketmq_sre_contracts::CapabilityQualityProjection =
        serde_json::from_value(coverage::matrix(&state, &auth, get(Some(target))).await.unwrap()["quality"].clone())
            .unwrap();
    context.observed_at = expired.assessment.valid_until + Duration::seconds(1);
    let pack = context
        .packs
        .iter()
        .find(|pack| pack.pack_id == "consumer-lag.v2")
        .unwrap();
    assert_eq!(
        projection::evaluate(&expired, &context, pack, std::slice::from_ref(&sample))
            .unwrap()
            .reason_code,
        "verification_record_expired"
    );

    let bound_resource = "consumer-lag/group/revision";
    let bound_sample = state
        .evidence
        .persist_cluster(&auth, make(bound_resource, true))
        .await
        .unwrap();
    let (investigation, revision, _) =
        crate::coverage::postgres_tests::seed_revision(&repository, &auth, cluster, &[bound_sample.evidence_id]).await;
    let (other_investigation, other_revision, _) =
        crate::coverage::postgres_tests::seed_revision(&repository, &auth, cluster, &[bound_sample.evidence_id]).await;
    let mut bound = base.clone();
    bound.resource = Some(bound_resource.to_owned());
    bound.evidence_ids = vec![bound_sample.evidence_id];
    bound.investigation_id = Some(investigation);
    bound.revision_id = Some(revision);
    bound.report_digest = format!("sha256:{:064x}", 7);
    record(&state, &auth, VerifierIdentity::Oidc, bound).await.unwrap();
    let query = |investigation, revision, resource: Option<&str>| CoverageQuery {
        cluster_id: Some(cluster.to_string()),
        investigation_id: Some(format!("{investigation}")),
        revision_id: Some(format!("{revision}")),
        resource: resource.map(str::to_owned),
    };
    assert_eq!(
        production(
            &coverage::matrix(&state, &auth, query(investigation, revision, None))
                .await
                .unwrap()
        )["status"],
        "present"
    );
    assert_eq!(
        production(
            &coverage::matrix(&state, &auth, query(other_investigation, other_revision, None))
                .await
                .unwrap()
        )["status"],
        "unknown"
    );
    assert!(
        coverage::matrix(&state, &auth, query(investigation, revision, Some(target)))
            .await
            .is_err()
    );
    sqlx::query("UPDATE evidence_snapshots SET expires_at=NOW()-INTERVAL '1 second' WHERE id=$1")
        .bind(bound_sample.evidence_id.as_uuid())
        .execute(&repository.pool)
        .await
        .unwrap();
    assert_eq!(
        production(
            &coverage::matrix(&state, &auth, query(investigation, revision, None))
                .await
                .unwrap()
        )["status"],
        "unknown"
    );
    assert!(
        sqlx::query("DELETE FROM capability_verification_records WHERE id=$1")
            .bind(saved.record_id.as_uuid())
            .execute(&repository.pool)
            .await
            .is_err()
    );
    // This test explicitly requires a disposable database: immutable attestation
    // fixtures are retained until its owner tears down the test database.
}

#[tokio::test]
#[ignore = "requires an explicitly disposable ROCKETMQ_SRE_TEST_DATABASE_URL; append-only reports are retained"]
async fn postgres_production_proof_ages_without_rewriting_history_or_borrowing_new_evidence() {
    use crate::{
        PostgresRepository,
        api::capability_verification_test_state,
        coverage::{self, CoverageQuery, quality},
    };
    use rocketmq_sre_contracts::{
        CapabilityFactStatus, CorrelationId, DiagnosticEvidenceQuality, EvidenceContent, EvidenceExposure,
        EvidenceQuery, EvidenceSnapshot, QueryId, current_evidence_schema,
    };
    use rocketmq_sre_core::diagnostics::full_registry;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit disposable database");
    let repository = PostgresRepository::connect(&url, 4).await.unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    sqlx::query("INSERT INTO clusters (id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,requested_access_profile,effective_access_profile,onboarding_state)
        VALUES ($1,$2,$3,'test','local','5.3.2','test','capability-verification-age','read_only','read_only','ready_read_only')")
        .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("verification-age-{cluster}"))
        .execute(&repository.pool).await.unwrap();
    let state = capability_verification_test_state(repository.clone()).unwrap();
    let auth = AuthContext {
        tenant_id: tenant,
        subject: "dedicated-verifier-age-fixture".to_owned(),
        clusters: BTreeSet::from([cluster]),
        roles: BTreeSet::from(["capability_verifier_service".to_owned(), "rocketmq:read".to_owned()]),
    };
    let now = Utc::now();
    let target = "consumer-lag/group/orders";
    let make = |observed, freshness| {
        let mut snapshot = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: tenant,
                cluster_id: cluster,
                source: "rocketmq-mcp".to_owned(),
                resource: target.to_owned(),
                time_range: TimeRange::new(observed, observed).unwrap(),
            },
            current_evidence_schema(),
            observed,
            EvidenceContent::Inline(json!({
                "total_lag":10,"lag_slope_per_min":1,"queue_skew_ratio":1,
                "consume_rate_per_sec":5,"produce_rate_per_sec":6
            })),
        )
        .unwrap();
        snapshot.exposure = EvidenceExposure::McpTool;
        snapshot.freshness_seconds = freshness;
        snapshot
    };
    let stale = state
        .evidence
        .persist_cluster(&auth, make(now - Duration::seconds(250), 250))
        .await
        .unwrap();
    let mut report = request();
    report.tenant_id = tenant;
    report.cluster_id = cluster;
    report.observed_at = now;
    report.valid_until = now + Duration::hours(1);
    report.time_range = TimeRange::new(now - Duration::minutes(5), now).unwrap();
    report.evidence_ids = vec![stale.evidence_id];
    let rejected = record(&state, &auth, VerifierIdentity::Oidc, report.clone())
        .await
        .unwrap_err();
    assert!(matches!(rejected, ControlPlaneRequestFailure::Rejected(reason)
        if reason.code() == "verification_evidence_not_qualified"));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM capability_verification_records WHERE tenant_id=$1")
            .bind(tenant.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .unwrap(),
        0
    );

    let proof = state
        .evidence
        .persist_cluster(&auth, make(now - Duration::seconds(20), 250))
        .await
        .unwrap();
    let (investigation, revision, _) =
        crate::coverage::postgres_tests::seed_revision(&repository, &auth, cluster, &[proof.evidence_id]).await;
    report.evidence_ids = vec![proof.evidence_id];
    report.investigation_id = Some(investigation);
    report.revision_id = Some(revision);
    let saved = record(&state, &auth, VerifierIdentity::Oidc, report).await.unwrap();
    let scoped = coverage::matrix(
        &state,
        &auth,
        CoverageQuery {
            cluster_id: Some(cluster.to_string()),
            investigation_id: Some(investigation.to_string()),
            revision_id: Some(revision.to_string()),
            resource: Some(target.to_owned()),
        },
    )
    .await
    .unwrap();
    let current = scoped["quality"]["packs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|pack| pack["pack_id"] == "consumer-lag.v2")
        .unwrap();
    assert_eq!(current["layers"]["production_verified"]["status"], "present");
    assert_eq!(current["requirements"][0]["evidence_ids"], json!([proof.evidence_id]));

    // Advance the assessment clock, not the append-only report or Evidence.
    let registry = full_registry().unwrap();
    let compiled = registry.resolve("consumer-lag.v2").unwrap();
    let later = Utc::now() + Duration::seconds(40);
    let project = |evidence: &[EvidenceSnapshot]| {
        let mut projected = quality::project(
            tenant,
            Some(cluster),
            later,
            &[compiled],
            &BTreeMap::new(),
            evidence,
            false,
        );
        projected.resource = Some(target.to_owned());
        projected.investigation_id = Some(investigation);
        projected.revision_id = Some(revision);
        projected
    };
    let mut aged = project(std::slice::from_ref(&proof));
    projection::apply(&state, &auth, &mut aged, std::slice::from_ref(&proof))
        .await
        .unwrap();
    assert_eq!(aged.packs[0].requirements[0].quality, DiagnosticEvidenceQuality::Stale);
    assert_eq!(
        aged.packs[0].layers.production_verified.status,
        CapabilityFactStatus::Unknown
    );
    assert_eq!(
        aged.packs[0].layers.production_verified.reason_code,
        "verification_evidence_no_longer_qualified"
    );
    assert_eq!(
        aged.packs[0].layers.production_verified.verification.as_ref(),
        Some(&saved)
    );

    // Even an otherwise qualified current sample of the same resource cannot
    // replace the exact Evidence IDs bound to the historical report/revision.
    let replacement = state.evidence.persist_cluster(&auth, make(now, 0)).await.unwrap();
    let samples = [proof, replacement.clone()];
    let mut borrowed = project(&samples);
    projection::apply(&state, &auth, &mut borrowed, &samples).await.unwrap();
    assert_eq!(
        borrowed.packs[0].requirements[0].quality,
        DiagnosticEvidenceQuality::Available
    );
    assert_eq!(
        borrowed.packs[0].requirements[0].evidence_ids,
        vec![replacement.evidence_id]
    );
    assert_eq!(
        borrowed.packs[0].layers.production_verified.status,
        CapabilityFactStatus::Unknown
    );
    let historical =
        sqlx::query_scalar::<_, Value>("SELECT record_snapshot FROM capability_verification_records WHERE id=$1")
            .bind(saved.record_id.as_uuid())
            .fetch_one(&repository.pool)
            .await
            .unwrap();
    assert_eq!(historical, serde_json::to_value(&saved).unwrap());
    assert_eq!(saved.assessment.outcome, Outcome::Passed);
}
