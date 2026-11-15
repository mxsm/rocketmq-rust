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

use axum::http::StatusCode;
use chrono::Duration;
use rocketmq_sre_contracts::*;
use serde_json::json;
use std::sync::Arc;

use super::super::target_scope::ActionTargetScope;
use super::support::*;

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL and actual confirmed-source model lineage"]
async fn postgres_readiness_critic_consumes_exact_confirmed_lineage_and_checks_promotion_deadline() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.request.action_id = "broker.config.patch_allowlisted.v1".to_owned();
    fixture.request.parameters =
        json!({"broker":"127.0.0.1:10911","expected_generation":1,"patch":{"max_client_event_count":100}});
    fixture.scope = Arc::new(ActionTargetScope::parse(Some(&json!({"schema_version":"rocketmq-sre.action-target-scope.v1","rules":[{
        "tenant_id":fixture.auth.tenant_id,"cluster_id":fixture.request.cluster_id,"action_id":fixture.request.action_id,
        "descriptor_version":"1.0.0","incident_resource":"broker/broker-a","target":TARGET,"security_revision":"1",
        "lookup":{"kind":"broker","broker":"127.0.0.1:10911"}}]}).to_string())).unwrap());
    fixture.state.supervised_execution = fixture
        .state
        .supervised_execution
        .clone()
        .with_target_scope(fixture.scope.clone());
    fixture.prepare().await;
    let plan = fixture.draft().await;
    assert_eq!(plan.status, PlanStatus::NeedsCritic);
    let primary = fixture
        .repository
        .exact_primary_model_invocation(&fixture.auth, &plan)
        .await
        .unwrap();
    assert_eq!(primary.diagnosis_revision_id, Some(fixture.source_revision));
    assert_ne!(primary.diagnosis_revision_id, Some(plan.diagnosis_revision));
    let (status, body) = fixture
        .call(
            &format!("/v1/plans/{}/critic", plan.id),
            &fixture.auth,
            json!({"plan_hash":plan.plan_hash}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["review"]["status"], "unavailable");
    assert_eq!(body["plan"]["status"], "needs_critic");
    let mut review: CriticReview = serde_json::from_value(body["review"].clone()).unwrap();
    // A failed invocation is not a valid current primary, including through an
    // otherwise exact confirmed->source link.
    sqlx::query("UPDATE model_invocations SET error_code='SyntheticFixtureFailure' WHERE id=$1")
        .bind(primary.id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let view = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert!(!view.diagnosis.primary_model_lineage_valid);
    let (status, body) = fixture
        .call(
            &format!("/v1/plans/{}/critic", plan.id),
            &fixture.auth,
            json!({"plan_hash":plan.plan_hash}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "diagnosis_not_execution_ready");
    sqlx::query("UPDATE model_invocations SET error_code=NULL WHERE id=$1")
        .bind(primary.id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let admission = fixture
        .state
        .supervised_execution
        .admission_for_plan(&fixture.auth, &plan)
        .await
        .unwrap();
    let mut lock = fixture.repository.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM sre_incidents WHERE id=$1 FOR UPDATE")
        .bind(fixture.incident_id.as_uuid())
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    review.id = CriticReviewId::new();
    let audit = AuditEvent {
        id: AuditEventId::new(),
        tenant_id: plan.tenant_id,
        cluster_id: plan.cluster_id,
        correlation_id: CorrelationId::new(),
        event_kind: AuditEventKind::CriticReviewed,
        actor_subject: fixture.auth.subject.clone(),
        actor_role: "operator".to_owned(),
        resource_kind: "action_plan".to_owned(),
        resource_id: plan.id.to_string(),
        reason_code: "SyntheticFixturePromotionRace".to_owned(),
        details: json!({}),
        occurred_at: *fixture.clock.lock().unwrap(),
    };
    let promotion = fixture
        .repository
        .persist_critic_review(&plan, &review, &audit, true, &admission);
    let expire = async {
        super::races::wait_for_incident_locks(&fixture.repository, 1).await;
        *fixture.clock.lock().unwrap() += Duration::seconds(121);
        lock.commit().await.unwrap();
    };
    let (result, ()) = tokio::join!(promotion, expire);
    let crate::ControlPlaneRequestFailure::Rejected(error) = result.unwrap_err() else {
        panic!("expected typed expiry");
    };
    assert_eq!(error.code(), "action_readiness_expired");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM critic_reviews WHERE plan_id=$1")
        .bind(plan.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        fixture
            .repository
            .supervised_plan(&fixture.auth, plan.id)
            .await
            .unwrap()
            .plan
            .status,
        PlanStatus::NeedsCritic
    );
    fixture.close().await;
}
