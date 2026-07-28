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

mod api;
mod repository;

use std::fmt::Debug;
use std::fmt::Formatter;

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ActivateLeaseRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverRequest;
use rocketmq_sre_contracts::BeginLeaseTakeoverResponse;
use rocketmq_sre_contracts::EXECUTION_AGENT_AUDIENCE;
use rocketmq_sre_contracts::EXECUTION_AGENT_RECONCILE_AUDIENCE;
use rocketmq_sre_contracts::ExecutorLease;
use rocketmq_sre_contracts::GrantVerification;
use rocketmq_sre_contracts::IssueFenceGrantRequest;
use rocketmq_sre_contracts::LEASE_AUTHORITY_SCHEMA_VERSION;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::LeaseState;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::VerifyExecutionRequest;
use rocketmq_sre_contracts::VerifyFenceGrantRequest;
use rocketmq_sre_contracts::VerifyReconcileGrantRequest;
use sqlx::PgPool;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::supervised_execution::signing::GrantSigner;

pub(crate) use api::routes;
pub(crate) use repository::LeaseAuthorityRepository;

const EXECUTOR_AUDIENCE: &str = "rocketmq-sre-executor";
const CONTROL_PLANE_ISSUER: &str = "rocketmq-sre-control-plane";
const FENCE_GRANT_TTL_SECONDS: i64 = 20;

#[derive(Clone)]
pub(crate) struct LeaseAuthorityService {
    repository: LeaseAuthorityRepository,
    grant_signer: GrantSigner,
    agent_ack_verifier: GrantSigner,
}

impl LeaseAuthorityService {
    pub(crate) fn new(
        pool: PgPool,
        grant_signing_key: impl AsRef<[u8]>,
        agent_ack_verification_key: impl AsRef<[u8]>,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            repository: LeaseAuthorityRepository::new(pool),
            grant_signer: GrantSigner::new(grant_signing_key)?,
            agent_ack_verifier: GrantSigner::new(agent_ack_verification_key)?,
        })
    }

    pub(crate) async fn begin_takeover(
        &self,
        auth: &AuthContext,
        request: &BeginLeaseTakeoverRequest,
    ) -> Result<BeginLeaseTakeoverResponse, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_lease_request", error.to_string()))?;
        require_scope(auth, request.tenant_id, request.cluster_id)?;
        let acquired_at = Utc::now();
        let expires_at = acquired_at + TimeDelta::seconds(i64::from(request.requested_ttl_seconds));
        let pending_nonce = Uuid::new_v4().to_string();
        let lease = self
            .repository
            .begin_takeover(
                request.tenant_id,
                request.cluster_id,
                &auth.subject,
                &pending_nonce,
                acquired_at,
                expires_at,
            )
            .await?;
        let mut reconcile_grant = ReconcileGrant {
            lease_id: lease.id,
            owner: lease.owner.clone(),
            cluster_id: lease.cluster_id,
            pending_epoch: lease.epoch,
            audience: EXECUTION_AGENT_RECONCILE_AUDIENCE.to_owned(),
            issued_at: acquired_at,
            expires_at,
            nonce: pending_nonce,
            signature: String::new(),
        };
        self.grant_signer.sign_reconcile_grant(&mut reconcile_grant)?;
        Ok(BeginLeaseTakeoverResponse {
            schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
            lease,
            reconcile_grant,
        })
    }

    pub(crate) async fn activate(
        &self,
        auth: &AuthContext,
        request: &ActivateLeaseRequest,
    ) -> Result<ExecutorLease, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_fence_ack", error.to_string()))?;
        let lease = self.repository.lease(request.lease_id).await?;
        require_scope(auth, request.tenant_id, lease.cluster_id)?;
        if lease.tenant_id != request.tenant_id
            || lease.state != LeaseState::PendingFence
            || lease.owner != auth.subject
            || request.fence_ack.cluster_id != lease.cluster_id
            || request.fence_ack.epoch != lease.epoch
            || request.fence_ack.pending_nonce != lease.pending_nonce
        {
            return Err(ControlPlaneError::forbidden(
                "fence_ack_rejected",
                "FenceAck does not bind the current pending owner and epoch",
            ));
        }
        self.agent_ack_verifier.verify_fence_ack(&request.fence_ack)?;
        if self
            .repository
            .unresolved_old_effect_count(lease.cluster_id, lease.epoch)
            .await?
            > 0
        {
            return Err(ControlPlaneError::conflict_code(
                "unresolved_old_effects",
                "old epoch effects must be reconciled before lease activation",
            ));
        }
        self.repository.activate(&lease, &request.fence_ack).await
    }

    pub(crate) async fn issue_fence_grant(
        &self,
        auth: &AuthContext,
        request: &IssueFenceGrantRequest,
    ) -> Result<LeaseFenceGrant, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        request
            .validate()
            .map_err(|error| ControlPlaneError::validation("invalid_fence_grant_request", error.to_string()))?;
        require_scope(auth, request.tenant_id, request.cluster_id)?;
        let lease = self.repository.lease(request.lease_id).await?;
        let issued_at = Utc::now();
        if lease.tenant_id != request.tenant_id
            || lease.cluster_id != request.cluster_id
            || lease.owner != auth.subject
            || lease.epoch != request.epoch
            || lease.state != LeaseState::Active
            || lease.expires_at <= issued_at
        {
            return Err(ControlPlaneError::forbidden(
                "stale_lease_epoch",
                "only the current active owner can request a dispatch grant",
            ));
        }
        let (action, resource) = self.execution_step(request).await?;
        let expires_at = std::cmp::min(
            lease.expires_at,
            issued_at + TimeDelta::seconds(FENCE_GRANT_TTL_SECONDS),
        );
        let mut grant = LeaseFenceGrant {
            lease_id: lease.id,
            owner: lease.owner,
            cluster_id: lease.cluster_id,
            epoch: lease.epoch,
            execution_id: request.execution_id,
            step_id: request.step_id,
            plan_step_id: request.plan_step_id,
            action,
            resource,
            compensation: request.compensation,
            audience: EXECUTION_AGENT_AUDIENCE.to_owned(),
            issued_at,
            expires_at,
            nonce: Uuid::new_v4().to_string(),
            signature: String::new(),
        };
        self.grant_signer.sign_fence_grant(&mut grant)?;
        Ok(grant)
    }

    pub(crate) async fn verify_execution(
        &self,
        auth: &AuthContext,
        request: &VerifyExecutionRequest,
    ) -> Result<GrantVerification, ControlPlaneError> {
        require_role(auth, "executor_service")?;
        require_schema(&request.schema_version)?;
        require_scope(auth, request.execution.tenant_id, request.execution.cluster_id)?;
        let now = Utc::now();
        request
            .execution
            .validate_at(now, EXECUTOR_AUDIENCE)
            .map_err(|error| ControlPlaneError::validation("invalid_execution_request", error.to_string()))?;
        if request.execution.issuer != CONTROL_PLANE_ISSUER {
            return Err(ControlPlaneError::forbidden(
                "invalid_execution_issuer",
                "execution request was not issued by the Control Plane",
            ));
        }
        self.grant_signer.verify_execution(&request.execution)?;
        if let Some(grant) = &request.execution.autonomy_grant {
            if grant.issuer != CONTROL_PLANE_ISSUER {
                return Err(ControlPlaneError::forbidden(
                    "invalid_autonomy_grant_issuer",
                    "autonomy grant was not issued by the Control Plane",
                ));
            }
            self.grant_signer.verify_autonomy(grant)?;
            self.repository.autonomy_grant_is_current(grant).await?;
        } else {
            for approval in &request.execution.approvals {
                if approval.issuer != CONTROL_PLANE_ISSUER {
                    return Err(ControlPlaneError::forbidden(
                        "invalid_approval_issuer",
                        "approval grant was not issued by the Control Plane",
                    ));
                }
                self.grant_signer.verify_approval(approval)?;
            }
        }
        self.repository.execution_is_current(&request.execution, now).await?;
        Ok(GrantVerification {
            schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
            valid: true,
            cluster_id: request.execution.cluster_id,
            epoch: rocketmq_sre_contracts::LeaseEpoch(0),
            expires_at: request.execution.expires_at,
        })
    }

    pub(crate) async fn verify_fence_grant(
        &self,
        auth: &AuthContext,
        request: &VerifyFenceGrantRequest,
    ) -> Result<GrantVerification, ControlPlaneError> {
        require_role(auth, "execution_agent")?;
        require_schema(&request.schema_version)?;
        require_scope(auth, request.tenant_id, request.grant.cluster_id)?;
        let now = Utc::now();
        if request.grant.audience != EXECUTION_AGENT_AUDIENCE
            || request.grant.owner.trim().is_empty()
            || request.grant.resource.trim().is_empty()
            || request.grant.nonce.trim().is_empty()
            || request.grant.issued_at > now
            || request.grant.expires_at <= now
        {
            return Err(ControlPlaneError::forbidden(
                "invalid_fence_grant",
                "dispatch grant identity, audience, or validity window is invalid",
            ));
        }
        self.grant_signer.verify_fence_grant(&request.grant)?;
        self.repository
            .assert_active(request.tenant_id, &request.grant, now)
            .await?;
        self.repository.validate_fence_grant_binding(&request.grant).await?;
        Ok(GrantVerification {
            schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
            valid: true,
            cluster_id: request.grant.cluster_id,
            epoch: request.grant.epoch,
            expires_at: request.grant.expires_at,
        })
    }

    pub(crate) async fn verify_reconcile_grant(
        &self,
        auth: &AuthContext,
        request: &VerifyReconcileGrantRequest,
    ) -> Result<GrantVerification, ControlPlaneError> {
        require_role(auth, "execution_agent")?;
        require_schema(&request.schema_version)?;
        require_scope(auth, request.tenant_id, request.grant.cluster_id)?;
        let now = Utc::now();
        if request.grant.audience != EXECUTION_AGENT_RECONCILE_AUDIENCE
            || request.grant.owner.trim().is_empty()
            || request.grant.nonce.trim().is_empty()
            || request.grant.issued_at > now
            || request.grant.expires_at <= now
        {
            return Err(ControlPlaneError::forbidden(
                "invalid_reconcile_grant",
                "reconcile grant identity, audience, or validity window is invalid",
            ));
        }
        self.grant_signer.verify_reconcile_grant(&request.grant)?;
        self.repository
            .assert_pending(request.tenant_id, &request.grant, now)
            .await?;
        Ok(GrantVerification {
            schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
            valid: true,
            cluster_id: request.grant.cluster_id,
            epoch: request.grant.pending_epoch,
            expires_at: request.grant.expires_at,
        })
    }

    async fn execution_step(
        &self,
        request: &IssueFenceGrantRequest,
    ) -> Result<(rocketmq_sre_contracts::ExecutionAction, String), ControlPlaneError> {
        self.repository
            .execution_step(
                request.tenant_id,
                request.cluster_id,
                request.execution_id,
                request.plan_step_id,
                request.compensation,
            )
            .await
    }
}

impl Debug for LeaseAuthorityService {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseAuthorityService")
            .field("repository", &self.repository)
            .field("grant_signer", &"[REDACTED]")
            .field("agent_ack_verifier", &"[REDACTED]")
            .finish()
    }
}

fn require_role(auth: &AuthContext, role: &'static str) -> Result<(), ControlPlaneError> {
    if auth.roles.contains(role) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_workload_identity",
            "the authenticated workload role is not permitted for this operation",
        ))
    }
}

fn require_scope(
    auth: &AuthContext,
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: rocketmq_sre_contracts::ClusterId,
) -> Result<(), ControlPlaneError> {
    if auth.tenant_id == tenant_id && auth.clusters.contains(&cluster_id) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "workload identity does not own the requested tenant and cluster scope",
        ))
    }
}

fn require_schema(schema: &str) -> Result<(), ControlPlaneError> {
    if schema == LEASE_AUTHORITY_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(ControlPlaneError::validation(
            "unsupported_schema_major",
            "lease authority schema version is unsupported",
        ))
    }
}

#[cfg(test)]
mod tests;
