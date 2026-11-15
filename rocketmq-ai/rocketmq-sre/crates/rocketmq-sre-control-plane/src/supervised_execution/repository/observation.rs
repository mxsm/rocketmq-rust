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

use rocketmq_sre_contracts::ActionPlan;
use sqlx::Row;
use sqlx::postgres::PgRow;

use super::PostgresRepository;
use super::StoredExecutionProjection;
use super::from_json;
use super::parse_execution_state;
use crate::ControlPlaneRequestFailure;
use crate::auth::AuthContext;

mod snapshot;

pub(in crate::supervised_execution) use snapshot::*;

impl PostgresRepository {
    pub(in crate::supervised_execution) async fn lookup_execution_reference(
        &self,
        auth: &AuthContext,
        key: &str,
    ) -> Result<Option<StoredExecutionProjection>, ControlPlaneRequestFailure> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, plan_id, plan_hash, correlation_id,idempotency_key,
                    requested_by, CASE WHEN octet_length(request_snapshot::text)<=524288 THEN request_snapshot END AS request_snapshot, state, started_at
             FROM executions WHERE tenant_id=$1 AND idempotency_key=$2",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode_receipt).transpose()
    }

    pub(in crate::supervised_execution) async fn plan_execution_references(
        &self,
        auth: &AuthContext,
        plan: &ActionPlan,
        limit: i64,
    ) -> Result<Vec<StoredExecutionProjection>, ControlPlaneRequestFailure> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, plan_id, plan_hash, correlation_id,idempotency_key,
                    requested_by, CASE WHEN octet_length(request_snapshot::text)<=524288 THEN request_snapshot END AS request_snapshot, state, started_at FROM executions
             WHERE tenant_id=$1 AND cluster_id=$2 AND plan_id=$3 AND plan_hash=$4
             ORDER BY started_at DESC, id DESC LIMIT $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(plan.cluster_id.as_uuid())
        .bind(plan.id.as_uuid())
        .bind(&plan.plan_hash)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_receipt).collect()
    }
}

fn decode_receipt(row: &PgRow) -> Result<StoredExecutionProjection, ControlPlaneRequestFailure> {
    let encoded: Option<serde_json::Value> = row.try_get("request_snapshot")?;
    let request: rocketmq_sre_contracts::ExecutionRequest = from_json(encoded.ok_or_else(|| {
        ControlPlaneRequestFailure::validation(
            "execution_history_unavailable",
            "retained execution identity exceeds the safe read bound",
        )
    })?)?;
    if row.try_get::<uuid::Uuid, _>("id")? != request.id.as_uuid()
        || row.try_get::<uuid::Uuid, _>("tenant_id")? != request.tenant_id.as_uuid()
        || row.try_get::<uuid::Uuid, _>("cluster_id")? != request.cluster_id.as_uuid()
        || row.try_get::<uuid::Uuid, _>("plan_id")? != request.plan.id.as_uuid()
        || row.try_get::<String, _>("plan_hash")? != request.plan.plan_hash
        || row.try_get::<uuid::Uuid, _>("correlation_id")? != request.correlation_id.as_uuid()
        || row.try_get::<String, _>("requested_by")? != request.requested_by
        || row.try_get::<String, _>("idempotency_key")? != request.idempotency_key
    {
        return Err(ControlPlaneRequestFailure::validation(
            "invalid_execution_receipt",
            "stored execution receipt bindings are inconsistent",
        ));
    }
    Ok(StoredExecutionProjection {
        request,
        state: parse_execution_state(row.try_get("state")?)?,
        submitted_at: row.try_get("started_at")?,
    })
}
