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
use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ActionPlanId;
use rocketmq_sre_contracts::EXECUTION_OBSERVATION_SCHEMA_VERSION;
use rocketmq_sre_contracts::ExecutionLookupRequest;
use rocketmq_sre_contracts::ExecutionLookupStatus;
use rocketmq_sre_contracts::ExecutionLookupView;
use rocketmq_sre_contracts::ExecutionReceiptReference;
use rocketmq_sre_contracts::PlanExecutionPage;
use rocketmq_sre_contracts::is_sha256_digest;

use super::SupervisedExecutionService;
use crate::ControlPlaneError;
use crate::ControlPlaneRequestFailure;
use crate::auth::AuthContext;
use crate::supervised_execution::model::StoredExecutionProjection;

mod projection;

const RECEIPT_LIMIT: usize = 20;
const SNAPSHOT_VALIDITY_SECONDS: i64 = 5;

impl SupervisedExecutionService {
    pub(crate) async fn lookup_execution(
        &self,
        auth: &AuthContext,
        request: &ExecutionLookupRequest,
    ) -> Result<ExecutionLookupView, ControlPlaneRequestFailure> {
        validate_lookup(request)?;
        let plan = self.repository.supervised_plan(auth, request.plan_id).await?.plan;
        verify_plan(&plan)?;
        verify_plan_scope(auth, &plan)?;
        if plan.plan_hash != request.plan_hash
            || plan.compute_precondition_hash().map_err(invalid_stored_plan)? != request.precondition_hash
        {
            return Err(intent_mismatch());
        }
        let stored = self
            .repository
            .lookup_execution_reference(auth, &request.idempotency_key)
            .await?;
        // A different requester is not a receipt-lookup oracle, even where the
        // caller can read the same Plan through the ordinary scoped read API.
        let receipt = match stored {
            Some(stored) if stored.request.requested_by == auth.subject => {
                let receipt = receipt_reference(auth, &stored)?;
                if receipt.plan_id != request.plan_id
                    || receipt.plan_hash != request.plan_hash
                    || receipt.precondition_hash != request.precondition_hash
                {
                    return Err(intent_mismatch());
                }
                Some(receipt)
            }
            Some(_) | None => None,
        };
        let observed_at = self.now();
        Ok(ExecutionLookupView {
            schema_version: EXECUTION_OBSERVATION_SCHEMA_VERSION.to_owned(),
            tenant_id: auth.tenant_id,
            actor_subject: auth.subject.clone(),
            request: request.clone(),
            status: if receipt.is_some() {
                ExecutionLookupStatus::Accepted
            } else {
                ExecutionLookupStatus::NotObserved
            },
            receipt,
            observed_at,
            valid_until: snapshot_deadline(observed_at)?,
        })
    }

    pub(crate) async fn plan_executions(
        &self,
        auth: &AuthContext,
        id: ActionPlanId,
        plan_hash: &str,
    ) -> Result<PlanExecutionPage, ControlPlaneRequestFailure> {
        if !is_sha256_digest(plan_hash) {
            return Err(invalid_selector());
        }
        let plan = self.repository.supervised_plan(auth, id).await?.plan;
        verify_plan(&plan)?;
        verify_plan_scope(auth, &plan)?;
        if plan.plan_hash != plan_hash {
            return Err(intent_mismatch());
        }
        let rows = self
            .repository
            .plan_execution_references(auth, &plan, (RECEIPT_LIMIT + 1) as i64)
            .await?;
        let partial = rows.len() > RECEIPT_LIMIT;
        let mut items = Vec::with_capacity(rows.len().min(RECEIPT_LIMIT));
        for row in rows.into_iter().take(RECEIPT_LIMIT) {
            let receipt = receipt_reference(auth, &row)?;
            if receipt.plan_id != plan.id || receipt.plan_hash != plan.plan_hash {
                return Err(invalid_stored_plan(rocketmq_sre_contracts::SreContractError::new(
                    rocketmq_sre_contracts::PublicErrorCode::InvalidDescriptor,
                )));
            }
            items.push(receipt);
        }
        let observed_at = self.now();
        Ok(PlanExecutionPage {
            schema_version: EXECUTION_OBSERVATION_SCHEMA_VERSION.to_owned(),
            tenant_id: auth.tenant_id,
            cluster_id: plan.cluster_id,
            plan_id: plan.id,
            plan_hash: plan.plan_hash,
            items,
            partial,
            observed_at,
            valid_until: snapshot_deadline(observed_at)?,
        })
    }
}

fn validate_lookup(request: &ExecutionLookupRequest) -> Result<(), ControlPlaneRequestFailure> {
    if !is_sha256_digest(&request.plan_hash)
        || !is_sha256_digest(&request.precondition_hash)
        || request.idempotency_key.is_empty()
        || request.idempotency_key.len() > 256
        || request.idempotency_key.chars().any(char::is_control)
        || request.idempotency_key.trim() != request.idempotency_key
    {
        return Err(invalid_selector());
    }
    Ok(())
}

pub(super) fn receipt_reference(
    auth: &AuthContext,
    stored: &StoredExecutionProjection,
) -> Result<ExecutionReceiptReference, ControlPlaneRequestFailure> {
    let request = &stored.request;
    if request.tenant_id != auth.tenant_id || !auth.clusters.contains(&request.cluster_id) {
        return Err(ControlPlaneRequestFailure::forbidden(
            "cluster_not_allowed",
            "execution receipt is outside the current authenticated scope",
        ));
    }
    verify_plan(&request.plan)?;
    if request.plan.tenant_id != request.tenant_id || request.plan.cluster_id != request.cluster_id {
        return Err(intent_mismatch());
    }
    Ok(ExecutionReceiptReference {
        execution_id: request.id,
        tenant_id: request.tenant_id,
        cluster_id: request.cluster_id,
        plan_id: request.plan.id,
        plan_hash: request.plan.plan_hash.clone(),
        precondition_hash: request.plan.compute_precondition_hash().map_err(invalid_stored_plan)?,
        incident_id: request.plan.incident_id,
        diagnosis_revision_id: request.plan.diagnosis_revision,
        correlation_id: request.correlation_id,
        requested_by: request.requested_by.clone(),
        accepted_at: stored.submitted_at,
        state: stored.state,
    })
}

pub(super) fn snapshot_deadline(observed_at: DateTime<Utc>) -> Result<DateTime<Utc>, ControlPlaneRequestFailure> {
    observed_at
        .checked_add_signed(Duration::seconds(SNAPSHOT_VALIDITY_SECONDS))
        .ok_or_else(|| ControlPlaneError::configuration("execution observation clock is unavailable").into())
}

fn verify_plan(plan: &rocketmq_sre_contracts::ActionPlan) -> Result<(), ControlPlaneRequestFailure> {
    plan.verify_plan_hash().map_err(invalid_stored_plan)
}

fn verify_plan_scope(
    auth: &AuthContext,
    plan: &rocketmq_sre_contracts::ActionPlan,
) -> Result<(), ControlPlaneRequestFailure> {
    if plan.tenant_id != auth.tenant_id || !auth.clusters.contains(&plan.cluster_id) {
        return Err(ControlPlaneRequestFailure::forbidden(
            "cluster_not_allowed",
            "plan receipt is outside the current authenticated scope",
        ));
    }
    Ok(())
}

fn invalid_stored_plan(error: rocketmq_sre_contracts::SreContractError) -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::contract(crate::ControlPlaneFailure::Validation, "invalid_plan_hash", error)
}

fn invalid_selector() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::validation(
        "invalid_execution_lookup",
        "an exact bounded execution selector is required",
    )
}

fn intent_mismatch() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::conflict_code(
        "execution_intent_mismatch",
        "the immutable execution selector does not match this receipt",
    )
}
