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

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::GovernanceAccessPath;
use rocketmq_sre_contracts::GovernanceImpactKind;
use rocketmq_sre_contracts::GovernanceLifecycleState;
use rocketmq_sre_contracts::GovernanceObjectKind;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::TenantId;

use super::GovernanceAdmissionGuard;
use super::GovernanceRequirement;
use super::GovernanceService;
use super::model::CreateGovernanceArtifactRequest;
use super::model::CreateGovernanceVersionRequest;
use super::model::EvaluateGovernanceAdmissionRequest;
use super::model::GovernanceAuditQuery;
use super::model::GovernanceImpactQuery;
use super::model::RecordGovernanceImpactRequest;
use super::model::TransitionGovernanceVersionRequest;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const SIGNING_KEY: &[u8] = b"governance-test-signing-key-with-sufficient-entropy";

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_governance_enforces_human_lifecycle_and_fail_closed_admission() {
    let Some(database_url) = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").ok() else {
        return;
    };
    let repository = PostgresRepository::connect(&database_url, 5)
        .await
        .expect("repository with governance migrations");
    let fixture = seed_fixture(&repository).await;
    let service = GovernanceService::new(repository.clone(), SIGNING_KEY).expect("governance service");
    let owner = auth(
        fixture.tenant_id,
        fixture.cluster_id,
        "governance-owner",
        "model-governance",
    );
    let reviewer = auth(
        fixture.tenant_id,
        fixture.cluster_id,
        "governance-reviewer",
        "model-governance",
    );
    let model = auth(
        fixture.tenant_id,
        fixture.cluster_id,
        "model:diagnostic",
        "model_service",
    );

    let policy_artifact = service
        .create_artifact(
            &owner,
            &CreateGovernanceArtifactRequest {
                kind: GovernanceObjectKind::PolicyBundle,
                logical_key: "supervised-execution".to_owned(),
                owner: owner.subject.clone(),
                reviewer: reviewer.subject.clone(),
            },
        )
        .await
        .expect("governed policy artifact");
    let policy_version = service
        .create_version(&model, policy_artifact.id, &version_request("3.0.0", 'a'))
        .await
        .expect("model-authored draft candidate");
    assert_eq!(policy_version.state, GovernanceLifecycleState::Draft);
    assert!(
        service
            .transition_version(&model, policy_version.id, &transition(GovernanceLifecycleState::Review),)
            .await
            .is_err(),
        "models must never promote governed candidates"
    );

    let review = service
        .transition_version(&owner, policy_version.id, &transition(GovernanceLifecycleState::Review))
        .await
        .expect("owner submits candidate for review");
    let active = service
        .transition_version(&reviewer, review.id, &transition(GovernanceLifecycleState::Active))
        .await
        .expect("independent reviewer signs and activates");
    assert!(active.signature.is_some());

    let admitted = service
        .evaluate_admission(
            &reviewer,
            &EvaluateGovernanceAdmissionRequest {
                cluster_id: Some(fixture.cluster_id),
                access_path: GovernanceAccessPath::HighPrivilege,
                required_version_ids: vec![active.id],
            },
        )
        .await
        .expect("active signed policy admission");
    assert!(admitted.decision.allowed);
    assert!(!admitted.decision.degraded);

    for (kind, reference_id) in [
        (GovernanceImpactKind::Cluster, fixture.cluster_id.to_string()),
        (
            GovernanceImpactKind::DiagnosticPack,
            "consumer-lag-diagnosis".to_owned(),
        ),
        (GovernanceImpactKind::ActionPlan, "plan:governance-test".to_owned()),
        (GovernanceImpactKind::Action, "proxy.restart_one.v1".to_owned()),
        (GovernanceImpactKind::Incident, "incident:governance-test".to_owned()),
        (GovernanceImpactKind::ModelRoute, "route:primary-diagnosis".to_owned()),
    ] {
        service
            .record_impact(
                &owner,
                active.id,
                &RecordGovernanceImpactRequest {
                    cluster_id: Some(fixture.cluster_id),
                    kind,
                    reference_id,
                    label: "Governance integration test impact".to_owned(),
                },
            )
            .await
            .expect("governance impact");
    }
    let impacts = service
        .impacts(
            &owner,
            active.id,
            &GovernanceImpactQuery {
                cluster_id: Some(fixture.cluster_id),
                kind: None,
                limit: 20,
            },
        )
        .await
        .expect("bounded impact view");
    assert_eq!(impacts.items.len(), 6);

    let guard = GovernanceAdmissionGuard::new(repository.clone(), SIGNING_KEY).expect("admission guard");
    let expired_high_privilege = guard
        .evaluate(
            fixture.tenant_id,
            Some(fixture.cluster_id),
            GovernanceAccessPath::HighPrivilege,
            &[active.id],
            Utc::now() + Duration::days(40),
        )
        .await
        .expect("future high-privilege decision");
    assert!(!expired_high_privilege.allowed);
    assert!(
        expired_high_privilege
            .reason_codes
            .iter()
            .any(|reason| reason == "governance_version_expired")
    );
    let expired_read_only = guard
        .evaluate(
            fixture.tenant_id,
            Some(fixture.cluster_id),
            GovernanceAccessPath::ReadOnly,
            &[active.id],
            Utc::now() + Duration::days(40),
        )
        .await
        .expect("future read-only decision");
    assert!(expired_read_only.allowed);
    assert!(expired_read_only.degraded);

    let quarantined = service
        .transition_version(&reviewer, active.id, &transition(GovernanceLifecycleState::Quarantined))
        .await
        .expect("reviewer quarantines active policy");
    let quarantined_decision = guard
        .evaluate(
            fixture.tenant_id,
            Some(fixture.cluster_id),
            GovernanceAccessPath::HighPrivilege,
            &[quarantined.id],
            Utc::now(),
        )
        .await
        .expect("quarantined admission");
    assert!(!quarantined_decision.allowed);

    let action_artifact = service
        .create_artifact(
            &owner,
            &CreateGovernanceArtifactRequest {
                kind: GovernanceObjectKind::ActionDescriptor,
                logical_key: "proxy.restart_one.v1".to_owned(),
                owner: owner.subject.clone(),
                reviewer: reviewer.subject.clone(),
            },
        )
        .await
        .expect("governed action descriptor");
    let unknown_exact_version = guard
        .ensure_high_privilege_overrides(
            fixture.tenant_id,
            fixture.cluster_id,
            &[GovernanceRequirement {
                kind: GovernanceObjectKind::ActionDescriptor,
                logical_key: "proxy.restart_one.v1",
                version: "1.0.0",
            }],
            Utc::now(),
        )
        .await;
    assert!(unknown_exact_version.is_err());
    let action_draft = service
        .create_version(&owner, action_artifact.id, &version_request("1.0.0", 'b'))
        .await
        .expect("action draft");
    let unsigned = guard
        .ensure_high_privilege_overrides(
            fixture.tenant_id,
            fixture.cluster_id,
            &[GovernanceRequirement {
                kind: GovernanceObjectKind::ActionDescriptor,
                logical_key: "proxy.restart_one.v1",
                version: "1.0.0",
            }],
            Utc::now(),
        )
        .await;
    assert!(unsigned.is_err());
    let action_review = service
        .transition_version(&owner, action_draft.id, &transition(GovernanceLifecycleState::Review))
        .await
        .expect("action review");
    service
        .transition_version(
            &reviewer,
            action_review.id,
            &transition(GovernanceLifecycleState::Active),
        )
        .await
        .expect("active action descriptor");
    guard
        .ensure_high_privilege_overrides(
            fixture.tenant_id,
            fixture.cluster_id,
            &[GovernanceRequirement {
                kind: GovernanceObjectKind::ActionDescriptor,
                logical_key: "proxy.restart_one.v1",
                version: "1.0.0",
            }],
            Utc::now(),
        )
        .await
        .expect("active exact action version");

    register_remaining_object_kinds(&service, &owner, &reviewer).await;
    let audit = service
        .audit_export(
            &owner,
            &GovernanceAuditQuery {
                artifact_id: Some(policy_artifact.id),
                version_id: Some(policy_version.id),
                from: None,
                to: None,
                limit: 50,
            },
        )
        .await
        .expect("governance audit export");
    assert_eq!(audit.items.len(), 4);
    let compliance = service.compliance(&owner).await.expect("governance compliance");
    assert_eq!(compliance.quarantined, 1);
    assert_eq!(compliance.unsigned_active, 0);
}

async fn register_remaining_object_kinds(service: &GovernanceService, owner: &AuthContext, reviewer: &AuthContext) {
    for kind in [
        GovernanceObjectKind::DataPolicy,
        GovernanceObjectKind::EvidencePolicy,
        GovernanceObjectKind::Prompt,
        GovernanceObjectKind::Knowledge,
        GovernanceObjectKind::ModelProfile,
        GovernanceObjectKind::ProviderProfile,
        GovernanceObjectKind::DiagnosticPack,
        GovernanceObjectKind::Runbook,
        GovernanceObjectKind::IntegrationAdapter,
    ] {
        service
            .create_artifact(
                owner,
                &CreateGovernanceArtifactRequest {
                    kind,
                    logical_key: format!("fixture-{kind:?}-{}", uuid::Uuid::new_v4()),
                    owner: owner.subject.clone(),
                    reviewer: reviewer.subject.clone(),
                },
            )
            .await
            .expect("supported governed object kind");
    }
}

fn version_request(version: &str, digest_fill: char) -> CreateGovernanceVersionRequest {
    CreateGovernanceVersionRequest {
        version: version.to_owned(),
        content_digest: format!("sha256:{}", digest_fill.to_string().repeat(64)),
        applicable_components: BTreeSet::from(["control-plane".to_owned()]),
        applicable_version_range: ">=5.3.0,<6.0.0".to_owned(),
        dependencies: BTreeSet::new(),
        review_due_at: Utc::now() + Duration::days(14),
        expires_at: Some(Utc::now() + Duration::days(30)),
        rollback_version_id: None,
    }
}

fn transition(state: GovernanceLifecycleState) -> TransitionGovernanceVersionRequest {
    TransitionGovernanceVersionRequest {
        state,
        reason: format!("Governance integration test transition to {state:?}"),
        replacement_version_id: None,
        rollback_version_id: None,
    }
}

#[derive(Clone, Copy)]
struct GovernanceFixture {
    tenant_id: TenantId,
    cluster_id: ClusterId,
}

async fn seed_fixture(repository: &PostgresRepository) -> GovernanceFixture {
    let fleet_id = FleetId::new();
    let tenant_id = TenantId::new();
    let region_id = RegionId::new();
    let cluster_id = ClusterId::new();
    sqlx::query("INSERT INTO fleets (id, name, owner_name) VALUES ($1, $2, 'governance-test')")
        .bind(fleet_id.as_uuid())
        .bind(format!("governance-fleet-{fleet_id}"))
        .execute(&repository.pool)
        .await
        .expect("governance fleet fixture");
    sqlx::query(
        "INSERT INTO fleet_tenants (id, fleet_id, name, owner_name)
         VALUES ($1, $2, $3, 'governance-test')",
    )
    .bind(tenant_id.as_uuid())
    .bind(fleet_id.as_uuid())
    .bind(format!("governance-tenant-{tenant_id}"))
    .execute(&repository.pool)
    .await
    .expect("governance tenant fixture");
    sqlx::query(
        "INSERT INTO fleet_regions (
            id, fleet_id, region_key, display_name, owner_name, residency_tags
         ) VALUES ($1, $2, $3, 'Governance test region', 'governance-test', $4)",
    )
    .bind(region_id.as_uuid())
    .bind(fleet_id.as_uuid())
    .bind(format!("governance-region-{region_id}"))
    .bind(serde_json::json!(["test-residency"]))
    .execute(&repository.pool)
    .await
    .expect("governance region fixture");
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', $4, '5.3.2', 'docker', 'governance-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(format!("governance-test-{cluster_id}"))
    .bind(region_id.to_string())
    .execute(&repository.pool)
    .await
    .expect("governance cluster fixture");
    sqlx::query(
        "INSERT INTO fleet_cluster_registrations (
            cluster_id, fleet_id, tenant_id, region_id, environment,
            owner_name, lifecycle_state, residency_tags
         ) VALUES ($1, $2, $3, $4, 'test', 'governance-test', 'active', $5)",
    )
    .bind(cluster_id.as_uuid())
    .bind(fleet_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(region_id.as_uuid())
    .bind(serde_json::json!(["test-residency"]))
    .execute(&repository.pool)
    .await
    .expect("governance cluster registration fixture");
    GovernanceFixture { tenant_id, cluster_id }
}

fn auth(tenant_id: TenantId, cluster_id: ClusterId, subject: &str, role: &str) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: subject.to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from([role.to_owned()]),
    }
}
