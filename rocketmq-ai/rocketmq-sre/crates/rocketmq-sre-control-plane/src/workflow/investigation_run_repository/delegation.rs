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

use rocketmq_sre_contracts::{
    CreateInvestigationRunRequest, CreateInvestigationRunResponse, InvestigationRun, InvestigationRunId,
    InvestigationRunStatus, canonical_sha256,
};
use sqlx::Row;

use crate::{ControlPlaneRequestFailure, PostgresRepository, auth::AuthContext};

/// A bounded read-job delegation proven by its immutable creation receipt.
/// Never deserialized from a caller and never constructed from a DB subject alone.
pub(crate) struct RunDelegation {
    accepted: CreateInvestigationRunResponse,
    auth: AuthContext,
}

impl RunDelegation {
    pub(crate) fn auth(&self) -> &AuthContext {
        &self.auth
    }
    fn from_receipt(
        accepted: CreateInvestigationRunResponse,
        current: &InvestigationRun,
    ) -> Result<Self, ControlPlaneRequestFailure> {
        accepted
            .run
            .validate()
            .map_err(|error| ControlPlaneRequestFailure::state_source("invalid_run_delegation", error))?;
        let initial = &accepted.run;
        if initial.status != InvestigationRunStatus::Queued
            || initial.created_by_subject != accepted.actor_subject
            || accepted.replayed
        {
            return Err(invalid_delegation());
        }
        let request = CreateInvestigationRunRequest {
            schema_version: initial.schema_version.clone(),
            run_id: initial.id,
            scope: initial.initial_scope.clone(),
            packs: initial.initial_packs.clone(),
            budget: initial.budget.clone(),
            deadline: initial.deadline,
            idempotency_key: accepted.idempotency_key.clone(),
        };
        if canonical_sha256(&request)
            .map_err(|error| ControlPlaneRequestFailure::state_source("invalid_run_delegation", error))?
            != accepted.request_digest
        {
            return Err(invalid_delegation());
        }
        let delegation = Self {
            auth: AuthContext {
                tenant_id: initial.initial_scope.tenant_id,
                subject: accepted.actor_subject.clone(),
                clusters: BTreeSet::from([initial.initial_scope.cluster_id]),
                // Compatibility input for the existing scoped read services.
                // Callers must first validate this delegation for the exact run.
                roles: BTreeSet::from(["diagnose".to_owned()]),
            },
            accepted,
        };
        delegation.validate_for(current)?;
        Ok(delegation)
    }

    pub(crate) fn validate_for(&self, current: &InvestigationRun) -> Result<(), ControlPlaneRequestFailure> {
        current
            .validate()
            .map_err(|error| ControlPlaneRequestFailure::state_source("invalid_run_delegation", error))?;
        let original = &self.accepted.run;
        if current.id != original.id
            || current.schema_version != original.schema_version
            || current.initial_scope != original.initial_scope
            || current.initial_packs != original.initial_packs
            || current.budget != original.budget
            || current.deadline != original.deadline
            || current.created_by_subject != original.created_by_subject
            || current.created_at != original.created_at
            || !current.effective_scope.narrows(&original.initial_scope)
            || current
                .effective_packs
                .iter()
                .any(|pack| !original.initial_packs.contains(pack))
        {
            return Err(invalid_delegation());
        }
        Ok(())
    }

    /// Restricted compatibility context for the exact delegated run's fixed reads.
    pub(crate) fn read_auth(&self, current: &InvestigationRun) -> Result<&AuthContext, ControlPlaneRequestFailure> {
        self.validate_for(current)?;
        Ok(&self.auth)
    }
}

impl PostgresRepository {
    /// Internal dispatcher lookup. Public handlers authenticate independently.
    pub(crate) async fn investigation_run_delegation(
        &self,
        id: InvestigationRunId,
    ) -> Result<RunDelegation, ControlPlaneRequestFailure> {
        let row = sqlx::query("SELECT run_snapshot FROM investigation_runs WHERE id=$1")
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(ControlPlaneRequestFailure::not_found)?;
        let current: InvestigationRun = super::decode(row.try_get("run_snapshot")?)?;
        if current.id != id {
            return Err(invalid_delegation());
        }
        let receipts = sqlx::query("SELECT actor_subject,idempotency_key,response_snapshot FROM investigation_run_creations WHERE tenant_id=$1 AND run_id=$2 LIMIT 2")
            .bind(current.initial_scope.tenant_id.as_uuid()).bind(id.as_uuid()).fetch_all(&self.pool).await?;
        if receipts.len() != 1 {
            return Err(invalid_delegation());
        }
        let receipt = &receipts[0];
        let accepted: CreateInvestigationRunResponse = super::decode(receipt.try_get("response_snapshot")?)?;
        if accepted.actor_subject != receipt.try_get::<String, _>("actor_subject")?
            || accepted.idempotency_key != receipt.try_get::<String, _>("idempotency_key")?
        {
            return Err(invalid_delegation());
        }
        RunDelegation::from_receipt(accepted, &current)
    }
}

fn invalid_delegation() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::state(
        "invalid_run_delegation",
        "persisted run is not an accepted bounded read job",
    )
}
