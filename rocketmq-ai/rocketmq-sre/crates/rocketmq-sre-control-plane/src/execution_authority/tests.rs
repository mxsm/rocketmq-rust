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

use super::LeaseAuthorityService;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;
use crate::supervised_execution::signing::GrantSigner;
use chrono::Utc;
use rocketmq_sre_contracts::ActivateLeaseRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverRequest;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseState;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerifyReconcileGrantRequest;

const AUTHORITY_KEY: &str = "lease-authority-test-key-at-least-32-bytes";
const AGENT_ACK_KEY: &str = "agent-ack-test-key-at-least-32-bytes";

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_authority_requires_agent_ack_before_activation_and_rejects_forgery() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let repository = PostgresRepository::connect(&database_url, 8).await.expect("repository");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    seed_cluster(&repository, tenant_id, cluster_id).await;
    let authority =
        LeaseAuthorityService::new(repository.pool.clone(), AUTHORITY_KEY, AGENT_ACK_KEY).expect("Lease Authority");
    let executor = auth(tenant_id, cluster_id, "executor-authority-test", "executor_service");
    let agent = auth(
        tenant_id,
        cluster_id,
        "execution-agent-authority-test",
        "execution_agent",
    );
    let pending = authority
        .begin_takeover(
            &executor,
            &BeginLeaseTakeoverRequest {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                tenant_id,
                cluster_id,
                requested_ttl_seconds: 60,
            },
        )
        .await
        .expect("pending takeover");
    assert_eq!(pending.lease.state, LeaseState::PendingFence);
    let verification = authority
        .verify_reconcile_grant(
            &agent,
            &VerifyReconcileGrantRequest {
                schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
                tenant_id,
                grant: pending.reconcile_grant.clone(),
            },
        )
        .await
        .expect("signed reconcile grant");
    assert!(verification.valid);
    assert_eq!(verification.epoch, pending.lease.epoch);

    let mut ack = FenceAck {
        cluster_id,
        epoch: pending.lease.epoch,
        pending_nonce: pending.lease.pending_nonce.clone(),
        agent_subject: agent.subject.clone(),
        acknowledged_at: Utc::now(),
        signature: "forged".to_owned(),
    };
    let activation = ActivateLeaseRequest {
        schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
        tenant_id,
        lease_id: pending.lease.id,
        fence_ack: ack.clone(),
    };
    assert!(matches!(
        authority.activate(&executor, &activation).await,
        Err(ControlPlaneError::Forbidden {
            code: "invalid_grant_signature",
            ..
        })
    ));

    GrantSigner::new(AGENT_ACK_KEY)
        .expect("Agent acknowledgement signer")
        .sign_fence_ack(&mut ack)
        .expect("signed FenceAck");
    let activation = ActivateLeaseRequest {
        fence_ack: ack.clone(),
        ..activation
    };
    assert!(matches!(
        authority.activate(&executor, &activation).await,
        Err(ControlPlaneError::Conflict {
            code: "fence_ack_rejected",
            ..
        })
    ));

    sqlx::query(
        "INSERT INTO execution_agent_fences (
            cluster_id, tenant_id, highest_epoch, lease_id, agent_subject,
            fence_ack_snapshot, acknowledged_at, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.as_uuid())
    .bind(i64::try_from(pending.lease.epoch.0).expect("test epoch"))
    .bind(pending.lease.id.as_uuid())
    .bind(&ack.agent_subject)
    .bind(serde_json::to_value(&ack).expect("FenceAck snapshot"))
    .bind(ack.acknowledged_at)
    .execute(&repository.pool)
    .await
    .expect("durable Agent fence");
    let active = authority
        .activate(&executor, &activation)
        .await
        .expect("activation after durable Agent acknowledgement");
    assert_eq!(active.state, LeaseState::Active);

    cleanup_cluster(&repository, cluster_id).await;
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn lease_takeover_requires_executor_workload_role_and_exact_cluster_scope() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let repository = PostgresRepository::connect(&database_url, 4).await.expect("repository");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    seed_cluster(&repository, tenant_id, cluster_id).await;
    let authority =
        LeaseAuthorityService::new(repository.pool.clone(), AUTHORITY_KEY, AGENT_ACK_KEY).expect("Lease Authority");
    let request = BeginLeaseTakeoverRequest {
        schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
        tenant_id,
        cluster_id,
        requested_ttl_seconds: 60,
    };
    let operator = auth(tenant_id, cluster_id, "ordinary-operator", "operator");
    assert!(matches!(
        authority.begin_takeover(&operator, &request).await,
        Err(ControlPlaneError::Forbidden {
            code: "unauthorized_workload_identity",
            ..
        })
    ));
    let wrong_cluster = auth(tenant_id, ClusterId::new(), "executor-wrong-scope", "executor_service");
    assert!(matches!(
        authority.begin_takeover(&wrong_cluster, &request).await,
        Err(ControlPlaneError::Forbidden {
            code: "cluster_not_allowed",
            ..
        })
    ));

    cleanup_cluster(&repository, cluster_id).await;
}

fn auth(tenant_id: TenantId, cluster_id: ClusterId, subject: &str, role: &str) -> AuthContext {
    AuthContext {
        tenant_id,
        subject: subject.to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from([role.to_owned()]),
    }
}

async fn seed_cluster(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local',
            '5.x', 'test', 'phase3-authority-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("phase3-authority-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("test cluster");
}

async fn cleanup_cluster(repository: &PostgresRepository, cluster_id: ClusterId) {
    sqlx::query("DELETE FROM execution_agent_fences WHERE cluster_id = $1")
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("delete test Agent fence");
    sqlx::query("DELETE FROM executor_leases WHERE cluster_id = $1")
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("delete test leases");
    sqlx::query("DELETE FROM clusters WHERE id = $1")
        .bind(cluster_id.as_uuid())
        .execute(&repository.pool)
        .await
        .expect("delete test cluster");
}
