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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::ContractError;
use crate::ExecutionAction;
use crate::ExecutionId;
use crate::ExecutionRequest;
use crate::ExecutionStepId;
use crate::LeaseId;
use crate::PlanStepId;
use crate::TenantId;

/// Wire schema shared by every Lease Authority RPC.
pub const LEASE_AUTHORITY_SCHEMA_VERSION: &str = "rocketmq-sre.lease-authority.v1";

/// Monotonically increasing cluster fencing epoch.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseEpoch(pub u64);

/// Two-phase executor lease state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    PendingFence,
    Active,
    Expired,
}

/// Short-lived grant authorizing one active epoch to dispatch.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseFenceGrant {
    pub lease_id: LeaseId,
    pub owner: String,
    pub cluster_id: ClusterId,
    pub epoch: LeaseEpoch,
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub plan_step_id: PlanStepId,
    pub action: ExecutionAction,
    pub resource: String,
    #[serde(default)]
    pub compensation: bool,
    pub audience: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}

/// Read-only grant used while a pending owner reconciles old effects.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileGrant {
    pub lease_id: LeaseId,
    pub owner: String,
    pub cluster_id: ClusterId,
    pub pending_epoch: LeaseEpoch,
    pub audience: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}

/// Agent acknowledgement that the durable highest epoch has advanced.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FenceAck {
    pub cluster_id: ClusterId,
    pub epoch: LeaseEpoch,
    pub pending_nonce: String,
    pub agent_subject: String,
    pub acknowledged_at: DateTime<Utc>,
    pub signature: String,
}

/// Durable lease projection returned by the PostgreSQL Lease Authority.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorLease {
    pub id: LeaseId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub epoch: LeaseEpoch,
    pub owner: String,
    pub state: LeaseState,
    pub pending_nonce: String,
    pub acquired_at: DateTime<Utc>,
    pub activated_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
}

/// Bounded request to begin a two-phase Executor takeover.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeginLeaseTakeoverRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub requested_ttl_seconds: u32,
}

impl BeginLeaseTakeoverRequest {
    /// Validates the closed schema and bounded lease duration.
    ///
    /// # Errors
    ///
    /// Rejects unknown schemas, nil scope identifiers, and durations outside
    /// the 5–300 second takeover window.
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_schema(&self.schema_version)?;
        if self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || !(5..=300).contains(&self.requested_ttl_seconds)
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "lease takeover requires non-nil scope and a 5-300 second TTL".to_owned(),
            });
        }
        Ok(())
    }
}

/// Pending lease and read-only reconciliation capability.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeginLeaseTakeoverResponse {
    pub schema_version: String,
    pub lease: ExecutorLease,
    pub reconcile_grant: ReconcileGrant,
}

/// Request for a fresh, step-scoped dispatch grant.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueFenceGrantRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub lease_id: LeaseId,
    pub epoch: LeaseEpoch,
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub plan_step_id: PlanStepId,
    #[serde(default)]
    pub compensation: bool,
}

impl IssueFenceGrantRequest {
    /// Validates the exact lease, execution, and step binding.
    ///
    /// # Errors
    ///
    /// Rejects unknown schemas, nil identifiers, and epoch zero.
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_schema(&self.schema_version)?;
        if self.tenant_id.as_uuid().is_nil()
            || self.cluster_id.as_uuid().is_nil()
            || self.lease_id.as_uuid().is_nil()
            || self.execution_id.as_uuid().is_nil()
            || self.step_id.as_uuid().is_nil()
            || self.plan_step_id.as_uuid().is_nil()
            || self.epoch.0 == 0
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "fence grant request contains an invalid identity binding".to_owned(),
            });
        }
        Ok(())
    }
}

/// Signed Agent acknowledgement submitted to activate a pending lease.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivateLeaseRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub lease_id: LeaseId,
    pub fence_ack: FenceAck,
}

impl ActivateLeaseRequest {
    /// Validates the activation envelope before Agent signature verification.
    ///
    /// # Errors
    ///
    /// Rejects unknown schemas, nil scope, or incomplete acknowledgements.
    pub fn validate(&self) -> Result<(), ContractError> {
        validate_schema(&self.schema_version)?;
        if self.tenant_id.as_uuid().is_nil()
            || self.lease_id.as_uuid().is_nil()
            || self.fence_ack.cluster_id.as_uuid().is_nil()
            || self.fence_ack.epoch.0 == 0
            || self.fence_ack.pending_nonce.trim().is_empty()
            || self.fence_ack.agent_subject.trim().is_empty()
            || self.fence_ack.signature.trim().is_empty()
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "lease activation acknowledgement is incomplete".to_owned(),
            });
        }
        Ok(())
    }
}

/// Signed execution envelope submitted for Control Plane introspection.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyExecutionRequest {
    pub schema_version: String,
    pub execution: ExecutionRequest,
}

/// Agent-side introspection request for one short-lived dispatch grant.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyFenceGrantRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub grant: LeaseFenceGrant,
}

/// Agent-side introspection request for a read-only reconciliation grant.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyReconcileGrantRequest {
    pub schema_version: String,
    pub tenant_id: TenantId,
    pub grant: ReconcileGrant,
}

/// Minimal positive introspection result.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantVerification {
    pub schema_version: String,
    pub valid: bool,
    pub cluster_id: ClusterId,
    pub epoch: LeaseEpoch,
    pub expires_at: DateTime<Utc>,
}

fn validate_schema(actual: &str) -> Result<(), ContractError> {
    if actual == LEASE_AUTHORITY_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(ContractError::UnsupportedSchemaFamily {
            actual: actual.to_owned(),
            supported: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn takeover_ttl_is_bounded_and_unknown_schema_fails_closed() {
        let mut request = BeginLeaseTakeoverRequest {
            schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            requested_ttl_seconds: 60,
        };
        assert!(request.validate().is_ok());
        request.requested_ttl_seconds = 301;
        assert!(request.validate().is_err());
        request.requested_ttl_seconds = 60;
        request.schema_version = "rocketmq-sre.lease-authority.v2".to_owned();
        assert!(request.validate().is_err());
    }

    #[test]
    fn fence_grant_request_is_bound_to_execution_and_step() {
        let mut request = IssueFenceGrantRequest {
            schema_version: LEASE_AUTHORITY_SCHEMA_VERSION.to_owned(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            lease_id: LeaseId::new(),
            epoch: LeaseEpoch(1),
            execution_id: ExecutionId::new(),
            step_id: ExecutionStepId::new(),
            plan_step_id: PlanStepId::new(),
        };
        assert!(request.validate().is_ok());
        request.epoch = LeaseEpoch(0);
        assert!(request.validate().is_err());
    }
}
