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
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FinOpsAllocationPolicy;
use rocketmq_sre_contracts::FinOpsBudget;
use rocketmq_sre_contracts::FinOpsBudgetDecision;
use rocketmq_sre_contracts::FinOpsBudgetId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::FinOpsRepository;
use super::support::allocation_from_row;
use super::support::allocation_mode_name;
use super::support::budget_from_row;
use super::support::budget_period_name;
use super::support::budget_scope_name;
use super::support::decision_from_row;
use super::support::degradation_name;
use super::support::work_class_name;
use crate::ControlPlaneError;
use crate::finops::model::FinOpsBudgetQuery;
use crate::finops::model::bounded_limit;

impl FinOpsRepository {
    pub(in crate::finops) async fn create_budget(
        &self,
        budget: &FinOpsBudget,
    ) -> Result<FinOpsBudget, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE finops_budgets
             SET active = FALSE
             WHERE tenant_id = $1 AND scope_kind = $2 AND scope_key = $3 AND active",
        )
        .bind(budget.tenant_id.as_uuid())
        .bind(budget_scope_name(budget.scope_kind))
        .bind(&budget.scope_key)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "INSERT INTO finops_budgets (
                id, tenant_id, scope_kind, scope_key, budget_version,
                period_kind, soft_limit_micros, hard_limit_micros,
                owner_name, active, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE, $10)
             RETURNING *",
        )
        .bind(budget.id.as_uuid())
        .bind(budget.tenant_id.as_uuid())
        .bind(budget_scope_name(budget.scope_kind))
        .bind(&budget.scope_key)
        .bind(stored(budget.version, "budget version")?)
        .bind(budget_period_name(budget.period))
        .bind(stored(budget.soft_limit_micros, "soft limit")?)
        .bind(stored(budget.hard_limit_micros, "hard limit")?)
        .bind(&budget.owner)
        .bind(budget.created_at)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        budget_from_row(&row)
    }

    pub(in crate::finops) async fn next_budget_version(
        &self,
        tenant_id: TenantId,
        scope_kind: rocketmq_sre_contracts::FinOpsBudgetScopeKind,
        scope_key: &str,
    ) -> Result<u64, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(budget_version), 0) AS version
             FROM finops_budgets
             WHERE tenant_id = $1 AND scope_kind = $2 AND scope_key = $3",
        )
        .bind(tenant_id.as_uuid())
        .bind(budget_scope_name(scope_kind))
        .bind(scope_key)
        .fetch_one(&self.pool)
        .await?;
        let version = row.try_get::<i64, _>("version")?;
        u64::try_from(version)
            .map_err(|_| invalid_persisted("budget version"))
            .and_then(|version| {
                version.checked_add(1).ok_or_else(|| {
                    ControlPlaneError::validation(
                        "invalid_finops_budget",
                        "FinOps budget version exhausted the supported range",
                    )
                })
            })
    }

    pub(in crate::finops) async fn budget(
        &self,
        tenant_id: TenantId,
        budget_id: FinOpsBudgetId,
    ) -> Result<FinOpsBudget, ControlPlaneError> {
        let row = sqlx::query("SELECT * FROM finops_budgets WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(budget_id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        budget_from_row(&row)
    }

    pub(in crate::finops) async fn budgets(
        &self,
        tenant_id: TenantId,
        query: &FinOpsBudgetQuery,
    ) -> Result<(Vec<FinOpsBudget>, bool), ControlPlaneError> {
        let limit = bounded_limit(query.limit);
        let scope = query.scope_kind.map(budget_scope_name);
        let rows = sqlx::query(
            "SELECT *
             FROM finops_budgets
             WHERE tenant_id = $1
               AND ($2::TEXT IS NULL OR scope_kind = $2)
               AND ($3::BOOLEAN IS NULL OR active = $3)
             ORDER BY active DESC, scope_kind, scope_key, budget_version DESC
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(scope)
        .bind(query.active)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| budget_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }

    pub(in crate::finops) async fn budget_cost(
        &self,
        budget: &FinOpsBudget,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<(u64, u64), ControlPlaneError> {
        let scope = budget_scope_name(budget.scope_kind);
        let row = sqlx::query(
            "WITH ledger AS (
                SELECT cost_micros, 1::BIGINT AS samples
                FROM finops_cost_ledger
                WHERE tenant_id = $1 AND occurred_at >= $4 AND occurred_at < $5
                  AND (
                    $2 = 'tenant'
                    OR ($2 = 'provider' AND provider_profile = $3)
                    OR ($2 = 'model' AND model_family = $3)
                    OR ($2 = 'region' AND region_id::TEXT = $3)
                    OR ($2 = 'cluster' AND cluster_id::TEXT = $3)
                    OR ($2 = 'incident' AND incident_id::TEXT = $3)
                    OR ($2 = 'diagnostic_pack' AND pack_id = $3)
                    OR ($2 = 'workflow' AND workflow_id = $3)
                  )
             ), model AS (
                SELECT invocation.cost_micros, 1::BIGINT AS samples
                FROM model_invocations invocation
                JOIN fleet_cluster_registrations registration
                  ON registration.cluster_id = invocation.cluster_id
                 AND registration.tenant_id = invocation.tenant_id
                WHERE invocation.tenant_id = $1
                  AND invocation.started_at >= $4 AND invocation.started_at < $5
                  AND (
                    $2 = 'tenant'
                    OR (
                        $2 = 'provider'
                        AND (
                            invocation.actual_profile_id::TEXT = $3
                            OR invocation.provider_family = $3
                        )
                    )
                    OR ($2 = 'model' AND invocation.model_family = $3)
                    OR ($2 = 'region' AND registration.region_id::TEXT = $3)
                    OR ($2 = 'cluster' AND invocation.cluster_id::TEXT = $3)
                    OR ($2 = 'incident' AND invocation.incident_id::TEXT = $3)
                  )
             ), combined AS (
                SELECT * FROM ledger
                UNION ALL
                SELECT * FROM model
             )
             SELECT
                COALESCE(SUM(cost_micros), 0)::BIGINT AS cost_micros,
                COALESCE(SUM(samples), 0)::BIGINT AS samples
             FROM combined",
        )
        .bind(budget.tenant_id.as_uuid())
        .bind(scope)
        .bind(&budget.scope_key)
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        Ok((
            unsigned(row.try_get("cost_micros")?, "budget cost")?,
            unsigned(row.try_get("samples")?, "budget samples")?,
        ))
    }

    pub(in crate::finops) async fn record_decision(
        &self,
        decision: &FinOpsBudgetDecision,
    ) -> Result<FinOpsBudgetDecision, ControlPlaneError> {
        let controls = decision
            .protected_controls
            .iter()
            .copied()
            .map(work_class_name)
            .collect::<Vec<_>>();
        let row = sqlx::query(
            "INSERT INTO finops_budget_decisions (
                id, tenant_id, cluster_id, budget_id, work_class,
                requested_cost_micros, observed_cost_micros, projected_cost_micros,
                soft_limit_micros, hard_limit_micros, allowed, degradation,
                reason_code, protected_controls, evaluated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15
             )
             RETURNING *",
        )
        .bind(decision.id.as_uuid())
        .bind(decision.tenant_id.as_uuid())
        .bind(decision.cluster_id.map(ClusterId::as_uuid))
        .bind(decision.budget_id.as_uuid())
        .bind(work_class_name(decision.work_class))
        .bind(stored(decision.requested_cost_micros, "requested cost")?)
        .bind(stored(decision.observed_cost_micros, "observed cost")?)
        .bind(stored(decision.projected_cost_micros, "projected cost")?)
        .bind(stored(decision.soft_limit_micros, "soft limit")?)
        .bind(stored(decision.hard_limit_micros, "hard limit")?)
        .bind(decision.allowed)
        .bind(degradation_name(decision.degradation))
        .bind(&decision.reason_code)
        .bind(controls)
        .bind(decision.evaluated_at)
        .fetch_one(&self.pool)
        .await?;
        decision_from_row(&row)
    }

    pub(in crate::finops) async fn create_allocation_policy(
        &self,
        policy: &FinOpsAllocationPolicy,
    ) -> Result<FinOpsAllocationPolicy, ControlPlaneError> {
        let keys = serde_json::to_value(&policy.allocation_keys).map_err(|_| {
            ControlPlaneError::validation(
                "invalid_finops_allocation",
                "FinOps allocation keys cannot be encoded",
            )
        })?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query("UPDATE finops_allocation_policies SET active = FALSE WHERE tenant_id = $1 AND active")
            .bind(policy.tenant_id.as_uuid())
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "INSERT INTO finops_allocation_policies (
                id, tenant_id, policy_version, allocation_mode, allocation_keys,
                organization_confirmed, owner_name, active, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, $8)
             RETURNING *",
        )
        .bind(policy.id.as_uuid())
        .bind(policy.tenant_id.as_uuid())
        .bind(stored(policy.version, "allocation policy version")?)
        .bind(allocation_mode_name(policy.mode))
        .bind(keys)
        .bind(policy.organization_confirmed)
        .bind(&policy.owner)
        .bind(policy.created_at)
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        allocation_from_row(&row)
    }

    pub(in crate::finops) async fn next_allocation_version(
        &self,
        tenant_id: TenantId,
    ) -> Result<u64, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(policy_version), 0) AS version
             FROM finops_allocation_policies
             WHERE tenant_id = $1",
        )
        .bind(tenant_id.as_uuid())
        .fetch_one(&self.pool)
        .await?;
        let version = unsigned(row.try_get("version")?, "allocation version")?;
        version.checked_add(1).ok_or_else(|| {
            ControlPlaneError::validation(
                "invalid_finops_allocation",
                "FinOps allocation version exhausted the supported range",
            )
        })
    }

    pub(in crate::finops) async fn allocation_policy(
        &self,
        tenant_id: TenantId,
    ) -> Result<Option<FinOpsAllocationPolicy>, ControlPlaneError> {
        sqlx::query(
            "SELECT *
             FROM finops_allocation_policies
             WHERE tenant_id = $1 AND active",
        )
        .bind(tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .map(|row| allocation_from_row(&row))
        .transpose()
    }
}

fn stored(value: u64, field: &str) -> Result<i64, ControlPlaneError> {
    i64::try_from(value).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_finops_budget",
            format!("FinOps {field} exceeds the supported storage range"),
        )
    })
}

fn unsigned(value: i64, field: &str) -> Result<u64, ControlPlaneError> {
    u64::try_from(value).map_err(|_| invalid_persisted(field))
}

fn invalid_persisted(field: &str) -> ControlPlaneError {
    ControlPlaneError::validation(
        "invalid_persisted_finops_state",
        format!("persisted FinOps {field} is invalid"),
    )
}
