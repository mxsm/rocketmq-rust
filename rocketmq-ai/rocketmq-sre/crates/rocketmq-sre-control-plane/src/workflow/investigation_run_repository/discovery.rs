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

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rocketmq_sre_contracts::{InvestigationResource, InvestigationRunSummary, TimeRange};
use serde::Deserialize;

use super::*;

pub(crate) struct RunListFilter {
    pub window: Option<TimeRange>,
    pub resource: Option<InvestigationResource>,
    pub cursor: Option<String>,
    pub limit: u32,
}

pub(crate) struct RunDiscovery {
    pub cluster_id: ClusterId,
    pub saved_resource: Option<String>,
    pub items: Vec<InvestigationRunSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorScope {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    investigation_id: InvestigationId,
    window: Option<TimeRange>,
    resource: Option<InvestigationResource>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    scope: CursorScope,
    last_run_id: InvestigationRunId,
}

impl RunListFilter {
    pub(crate) fn validate(&mut self) -> Result<(), ControlPlaneRequestFailure> {
        if !(1..=50).contains(&self.limit)
            || self.cursor.as_ref().is_some_and(|cursor| cursor.len() > 2048)
            || self.resource.as_ref().is_some_and(|resource| {
                resource.key.trim().is_empty() || resource.key.len() > 512 || resource.key.chars().any(char::is_control)
            })
        {
            return Err(invalid_cursor());
        }
        if let Some(window) = &self.window
            && (window.start > window.end || window.end.signed_duration_since(window.start) > chrono::Duration::days(7))
        {
            return Err(invalid_cursor());
        }
        Ok(())
    }
}

impl PostgresRepository {
    /// Applies scope/window predicates before pagination. A cursor is only a
    /// position hint; its anchor is looked up again within the authorized filter.
    pub(crate) async fn investigation_run_list(
        &self,
        auth: &AuthContext,
        investigation_id: InvestigationId,
        filter: &mut RunListFilter,
    ) -> Result<RunDiscovery, ControlPlaneRequestFailure> {
        filter.validate()?;
        let mut tx = self.pool.begin().await?;
        let parent =
            sqlx::query("SELECT cluster_id,resource FROM investigations WHERE tenant_id=$1 AND id=$2 FOR SHARE")
                .bind(auth.tenant_id.as_uuid())
                .bind(investigation_id.as_uuid())
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(ControlPlaneRequestFailure::not_found)?;
        let cluster_id = ClusterId::from_uuid(parent.try_get("cluster_id")?);
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneRequestFailure::forbidden(
                "scope_mismatch",
                "investigation scope is not authorized",
            ));
        }
        ensure_cluster_scope(&mut tx, auth.tenant_id, cluster_id).await?;
        let saved_resource: Option<String> = parent.try_get("resource")?;
        let parent_subject = saved_resource
            .as_deref()
            .filter(|value| *value != "cluster/overview")
            .map(|value| super::super::investigation_run_service::scope::resource_subject(value, cluster_id));
        let cursor_scope = CursorScope {
            tenant_id: auth.tenant_id,
            cluster_id,
            investigation_id,
            window: filter.window.clone(),
            resource: filter.resource.clone(),
        };
        let cursor_id = filter
            .cursor
            .as_ref()
            .map(|encoded| {
                let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid_cursor())?;
                let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())?;
                if cursor.scope != cursor_scope
                    || cursor.last_run_id.as_uuid().is_nil()
                    || URL_SAFE_NO_PAD.encode(&bytes) != *encoded
                {
                    return Err(invalid_cursor());
                }
                Ok(cursor.last_run_id)
            })
            .transpose()?;
        let matches = |run: &InvestigationRun| {
            filter
                .window
                .as_ref()
                .is_none_or(|window| &run.effective_scope.time_range == window)
                && filter
                    .resource
                    .as_ref()
                    .is_none_or(|resource| run.effective_scope.resource.as_ref() == Some(resource))
        };
        let scope_matches = |run: &InvestigationRun, id: InvestigationRunId| {
            run.id == id
                && run.initial_scope.tenant_id == auth.tenant_id
                && run.initial_scope.cluster_id == cluster_id
                && run.initial_scope.investigation_id == investigation_id
                && parent_subject.as_ref().is_none_or(|subject| {
                    subject
                        .as_ref()
                        .is_some_and(|subject| run.initial_scope.resource.as_ref() == Some(subject))
                })
        };
        let anchor: Option<DateTime<Utc>> = if let Some(id) = cursor_id {
            let row=sqlx::query("SELECT run_snapshot,created_at FROM investigation_runs WHERE tenant_id=$1 AND cluster_id=$2 AND investigation_id=$3 AND id=$4")
                .bind(auth.tenant_id.as_uuid()).bind(cluster_id.as_uuid()).bind(investigation_id.as_uuid()).bind(id.as_uuid())
                .fetch_optional(&mut *tx).await?.ok_or_else(invalid_cursor)?;
            let run: InvestigationRun = decode(row.try_get("run_snapshot")?)?;
            run.validate().map_err(contract_failure)?;
            if !matches(&run) || !scope_matches(&run, id) {
                return Err(invalid_cursor());
            }
            Some(row.try_get("created_at")?)
        } else {
            None
        };
        let resource = filter.resource.as_ref().map(encode).transpose()?;
        let window = filter.window.as_ref().map(encode).transpose()?;
        // Compare canonical serialized windows without casting their JSON text
        // to PostgreSQL timestamps, which would round nanosecond Run scopes.
        let rows = sqlx::query(
            "SELECT id,tenant_id,cluster_id,investigation_id,run_snapshot FROM investigation_runs
            WHERE tenant_id=$1 AND cluster_id=$2 AND investigation_id=$3
              AND ($4::JSONB IS NULL OR run_snapshot #> '{effective_scope,time_range}'=$4)
              AND ($5::JSONB IS NULL OR run_snapshot #> '{effective_scope,resource}'=$5)
              AND ($6::TIMESTAMPTZ IS NULL OR (created_at,id)<($6,$7::UUID))
            ORDER BY created_at DESC,id DESC LIMIT $8",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(investigation_id.as_uuid())
        .bind(window)
        .bind(resource)
        .bind(anchor)
        .bind(cursor_id.map(InvestigationRunId::as_uuid))
        .bind(i64::from(filter.limit) + 1)
        .fetch_all(&mut *tx)
        .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let run: InvestigationRun = decode(row.try_get("run_snapshot")?)?;
            run.validate().map_err(contract_failure)?;
            if !scope_matches(&run, InvestigationRunId::from_uuid(row.try_get::<Uuid, _>("id")?)) || !matches(&run) {
                return Err(ControlPlaneRequestFailure::state(
                    "invalid_run",
                    "saved run scope is inconsistent",
                ));
            }
            items.push(InvestigationRunSummary::from(&run));
        }
        let more = items.len() > filter.limit as usize;
        items.truncate(filter.limit as usize);
        let next_cursor = if more {
            items
                .last()
                .map(|run| {
                    serde_json::to_vec(&Cursor {
                        scope: cursor_scope,
                        last_run_id: run.id,
                    })
                    .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                    .map_err(ControlPlaneRequestFailure::configuration_source)
                })
                .transpose()?
        } else {
            None
        };
        tx.commit().await?;
        Ok(RunDiscovery {
            cluster_id,
            saved_resource,
            items,
            next_cursor,
        })
    }
}

fn invalid_cursor() -> ControlPlaneRequestFailure {
    ControlPlaneRequestFailure::validation("invalid_input", "invalid run list filter or cursor")
}
