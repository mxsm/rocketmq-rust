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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::Hmac;
use hmac::Mac;
use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ApprovalGrant;
use rocketmq_sre_contracts::ApprovalId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::ExecutionRequest;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::LeaseFenceGrant;
use rocketmq_sre_contracts::ReconcileGrant;
use rocketmq_sre_contracts::TenantId;
use serde::Serialize;
use sha2::Sha256;

use crate::ControlPlaneError;

const SIGNATURE_PREFIX: &str = "hmac-sha256:";

#[derive(Clone)]
pub(crate) struct GrantSigner {
    key: Arc<[u8]>,
}

#[derive(Serialize)]
struct UnsignedApprovalGrant<'a> {
    issuer: &'a str,
    audience: &'a str,
    approval_id: ApprovalId,
    plan_id: rocketmq_sre_contracts::ActionPlanId,
    plan_hash: &'a str,
    precondition_hash: &'a str,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    approver_subject: &'a str,
    issued_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    nonce: &'a str,
}

#[derive(Serialize)]
struct UnsignedExecutionRequest<'a> {
    schema_version: &'a str,
    id: ExecutionId,
    tenant_id: TenantId,
    cluster_id: ClusterId,
    correlation_id: CorrelationId,
    plan: &'a ActionPlan,
    approvals: &'a [ApprovalGrant],
    requested_by: &'a str,
    idempotency_key: &'a str,
    issuer: &'a str,
    audience: &'a str,
    issued_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    nonce: &'a str,
}

#[derive(Serialize)]
struct UnsignedLeaseFenceGrant<'a> {
    lease_id: rocketmq_sre_contracts::LeaseId,
    owner: &'a str,
    cluster_id: ClusterId,
    epoch: rocketmq_sre_contracts::LeaseEpoch,
    execution_id: ExecutionId,
    step_id: rocketmq_sre_contracts::ExecutionStepId,
    plan_step_id: rocketmq_sre_contracts::PlanStepId,
    action: rocketmq_sre_contracts::ExecutionAction,
    resource: &'a str,
    audience: &'a str,
    issued_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    nonce: &'a str,
}

#[derive(Serialize)]
struct UnsignedReconcileGrant<'a> {
    lease_id: rocketmq_sre_contracts::LeaseId,
    owner: &'a str,
    cluster_id: ClusterId,
    pending_epoch: rocketmq_sre_contracts::LeaseEpoch,
    audience: &'a str,
    issued_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    nonce: &'a str,
}

#[derive(Serialize)]
struct UnsignedFenceAck<'a> {
    cluster_id: ClusterId,
    epoch: rocketmq_sre_contracts::LeaseEpoch,
    pending_nonce: &'a str,
    agent_subject: &'a str,
    acknowledged_at: chrono::DateTime<chrono::Utc>,
}

impl GrantSigner {
    pub(crate) fn new(key: impl AsRef<[u8]>) -> Result<Self, ControlPlaneError> {
        let key = key.as_ref();
        if key.is_empty() {
            return Err(ControlPlaneError::configuration("grant signing key must not be empty"));
        }
        Ok(Self { key: Arc::from(key) })
    }

    pub(super) fn sign_approval(&self, grant: &mut ApprovalGrant) -> Result<(), ControlPlaneError> {
        let payload = approval_payload(grant)?;
        grant.signature = self.sign(&payload)?;
        Ok(())
    }

    pub(super) fn verify_approval(&self, grant: &ApprovalGrant) -> Result<(), ControlPlaneError> {
        self.verify(&approval_payload(grant)?, &grant.signature)
    }

    pub(super) fn sign_execution(&self, request: &mut ExecutionRequest) -> Result<(), ControlPlaneError> {
        let payload = execution_payload(request)?;
        request.signature = self.sign(&payload)?;
        Ok(())
    }

    pub(crate) fn verify_execution(&self, request: &ExecutionRequest) -> Result<(), ControlPlaneError> {
        self.verify(&execution_payload(request)?, &request.signature)
    }

    pub(crate) fn sign_fence_grant(&self, grant: &mut LeaseFenceGrant) -> Result<(), ControlPlaneError> {
        grant.signature = self.sign(&lease_fence_payload(grant)?)?;
        Ok(())
    }

    pub(crate) fn verify_fence_grant(&self, grant: &LeaseFenceGrant) -> Result<(), ControlPlaneError> {
        self.verify(&lease_fence_payload(grant)?, &grant.signature)
    }

    pub(crate) fn sign_reconcile_grant(&self, grant: &mut ReconcileGrant) -> Result<(), ControlPlaneError> {
        grant.signature = self.sign(&reconcile_payload(grant)?)?;
        Ok(())
    }

    pub(crate) fn verify_reconcile_grant(&self, grant: &ReconcileGrant) -> Result<(), ControlPlaneError> {
        self.verify(&reconcile_payload(grant)?, &grant.signature)
    }

    pub(crate) fn verify_fence_ack(&self, ack: &FenceAck) -> Result<(), ControlPlaneError> {
        self.verify(&fence_ack_payload(ack)?, &ack.signature)
    }

    #[cfg(test)]
    pub(crate) fn sign_fence_ack(&self, ack: &mut FenceAck) -> Result<(), ControlPlaneError> {
        ack.signature = self.sign(&fence_ack_payload(ack)?)?;
        Ok(())
    }

    fn sign(&self, payload: &[u8]) -> Result<String, ControlPlaneError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| ControlPlaneError::configuration("grant signing key is invalid"))?;
        mac.update(payload);
        Ok(format!(
            "{SIGNATURE_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        ))
    }

    fn verify(&self, payload: &[u8], signature: &str) -> Result<(), ControlPlaneError> {
        let encoded = signature.strip_prefix(SIGNATURE_PREFIX).ok_or_else(|| {
            ControlPlaneError::forbidden("invalid_grant_signature", "grant signature format is invalid")
        })?;
        let signature = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| {
            ControlPlaneError::forbidden("invalid_grant_signature", "grant signature encoding is invalid")
        })?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| ControlPlaneError::configuration("grant signing key is invalid"))?;
        mac.update(payload);
        mac.verify_slice(&signature)
            .map_err(|_| ControlPlaneError::forbidden("invalid_grant_signature", "grant signature verification failed"))
    }
}

impl Debug for GrantSigner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GrantSigner")
            .field("key", &"[REDACTED]")
            .finish()
    }
}

fn approval_payload(grant: &ApprovalGrant) -> Result<Vec<u8>, ControlPlaneError> {
    serde_jcs::to_vec(&UnsignedApprovalGrant {
        issuer: &grant.issuer,
        audience: &grant.audience,
        approval_id: grant.approval_id,
        plan_id: grant.plan_id,
        plan_hash: &grant.plan_hash,
        precondition_hash: &grant.precondition_hash,
        tenant_id: grant.tenant_id,
        cluster_id: grant.cluster_id,
        approver_subject: &grant.approver_subject,
        issued_at: grant.issued_at,
        expires_at: grant.expires_at,
        nonce: &grant.nonce,
    })
    .map_err(|error| ControlPlaneError::configuration(format!("approval grant cannot be canonicalized: {error}")))
}

fn execution_payload(request: &ExecutionRequest) -> Result<Vec<u8>, ControlPlaneError> {
    serde_jcs::to_vec(&UnsignedExecutionRequest {
        schema_version: &request.schema_version,
        id: request.id,
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        correlation_id: request.correlation_id,
        plan: &request.plan,
        approvals: &request.approvals,
        requested_by: &request.requested_by,
        idempotency_key: &request.idempotency_key,
        issuer: &request.issuer,
        audience: &request.audience,
        issued_at: request.issued_at,
        expires_at: request.expires_at,
        nonce: &request.nonce,
    })
    .map_err(|error| ControlPlaneError::configuration(format!("execution request cannot be canonicalized: {error}")))
}

fn lease_fence_payload(grant: &LeaseFenceGrant) -> Result<Vec<u8>, ControlPlaneError> {
    serde_jcs::to_vec(&UnsignedLeaseFenceGrant {
        lease_id: grant.lease_id,
        owner: &grant.owner,
        cluster_id: grant.cluster_id,
        epoch: grant.epoch,
        execution_id: grant.execution_id,
        step_id: grant.step_id,
        plan_step_id: grant.plan_step_id,
        action: grant.action,
        resource: &grant.resource,
        audience: &grant.audience,
        issued_at: grant.issued_at,
        expires_at: grant.expires_at,
        nonce: &grant.nonce,
    })
    .map_err(|error| ControlPlaneError::configuration(format!("lease fence grant cannot be canonicalized: {error}")))
}

fn reconcile_payload(grant: &ReconcileGrant) -> Result<Vec<u8>, ControlPlaneError> {
    serde_jcs::to_vec(&UnsignedReconcileGrant {
        lease_id: grant.lease_id,
        owner: &grant.owner,
        cluster_id: grant.cluster_id,
        pending_epoch: grant.pending_epoch,
        audience: &grant.audience,
        issued_at: grant.issued_at,
        expires_at: grant.expires_at,
        nonce: &grant.nonce,
    })
    .map_err(|error| ControlPlaneError::configuration(format!("reconcile grant cannot be canonicalized: {error}")))
}

fn fence_ack_payload(ack: &FenceAck) -> Result<Vec<u8>, ControlPlaneError> {
    serde_jcs::to_vec(&UnsignedFenceAck {
        cluster_id: ack.cluster_id,
        epoch: ack.epoch,
        pending_nonce: &ack.pending_nonce,
        agent_subject: &ack.agent_subject,
        acknowledged_at: ack.acknowledged_at,
    })
    .map_err(|error| {
        ControlPlaneError::configuration(format!("fence acknowledgement cannot be canonicalized: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use chrono::Utc;
    use rocketmq_sre_contracts::ActionPlanId;
    use rocketmq_sre_contracts::ApprovalId;

    use super::*;

    #[test]
    fn signed_grant_is_bound_to_exact_plan_hash_and_audience() {
        let signer = GrantSigner::new("test-signing-key-that-is-not-exported").expect("signer");
        let now = Utc::now();
        let mut grant = ApprovalGrant {
            issuer: "rocketmq-sre-control-plane".to_owned(),
            audience: "rocketmq-sre-executor".to_owned(),
            approval_id: ApprovalId::new(),
            plan_id: ActionPlanId::new(),
            plan_hash: format!("sha256:{}", "a".repeat(64)),
            precondition_hash: format!("sha256:{}", "b".repeat(64)),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            approver_subject: "approver-a".to_owned(),
            issued_at: now,
            expires_at: now + Duration::minutes(15),
            nonce: uuid::Uuid::new_v4().to_string(),
            signature: String::new(),
        };
        signer.sign_approval(&mut grant).expect("signature");
        signer.verify_approval(&grant).expect("valid signature");

        grant.audience = "ordinary-user".to_owned();
        assert!(signer.verify_approval(&grant).is_err());
    }

    #[test]
    fn signer_debug_never_contains_key_material() {
        let signer = GrantSigner::new("highly-sensitive-signing-key").expect("signer");
        let debug = format!("{signer:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("highly-sensitive"));
    }
}
