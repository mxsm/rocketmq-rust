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

use rocketmq_sre_contracts::GovernanceActorKind;
use rocketmq_sre_contracts::GovernanceArtifact;
use rocketmq_sre_contracts::GovernanceArtifactId;
use rocketmq_sre_contracts::GovernanceEvent;
use rocketmq_sre_contracts::GovernanceEventId;
use rocketmq_sre_contracts::GovernanceLifecycleState;
use rocketmq_sre_contracts::GovernanceObjectKind;
use rocketmq_sre_contracts::GovernanceSignature;
use rocketmq_sre_contracts::GovernanceVersion;
use rocketmq_sre_contracts::GovernanceVersionId;
use rocketmq_sre_contracts::TenantId;
use sqlx::Row;

use super::GovernanceRepository;
use super::support::actor_kind_name;
use super::support::artifact_from_row;
use super::support::dependency_value;
use super::support::lifecycle_state_name;
use super::support::object_kind_name;
use super::support::version_from_row;
use crate::ControlPlaneError;
use crate::governance::model::GovernanceArtifactQuery;
use crate::governance::model::GovernanceVersionQuery;
use crate::governance::model::bounded_limit;

pub(in crate::governance) struct GovernanceOverride {
    pub(in crate::governance) artifact_present: bool,
    pub(in crate::governance) version: Option<GovernanceVersion>,
}

impl GovernanceRepository {
    pub(in crate::governance) async fn create_artifact(
        &self,
        artifact: &GovernanceArtifact,
    ) -> Result<GovernanceArtifact, ControlPlaneError> {
        let row = sqlx::query(
            "INSERT INTO governance_artifacts (
                id, tenant_id, object_kind, logical_key, owner_name,
                reviewer_name, current_version_id, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $7)
             RETURNING *",
        )
        .bind(artifact.id.as_uuid())
        .bind(artifact.tenant_id.as_uuid())
        .bind(object_kind_name(artifact.kind))
        .bind(&artifact.logical_key)
        .bind(&artifact.owner)
        .bind(&artifact.reviewer)
        .bind(artifact.created_at)
        .fetch_one(&self.pool)
        .await?;
        artifact_from_row(&row)
    }

    pub(in crate::governance) async fn get_artifact(
        &self,
        tenant_id: TenantId,
        id: GovernanceArtifactId,
    ) -> Result<GovernanceArtifact, ControlPlaneError> {
        let row = sqlx::query("SELECT * FROM governance_artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        artifact_from_row(&row)
    }

    pub(in crate::governance) async fn list_artifacts(
        &self,
        tenant_id: TenantId,
        query: &GovernanceArtifactQuery,
    ) -> Result<(Vec<GovernanceArtifact>, bool), ControlPlaneError> {
        let limit = bounded_limit(query.limit);
        let kind = query.kind.map(object_kind_name);
        let rows = sqlx::query(
            "SELECT *
             FROM governance_artifacts
             WHERE tenant_id = $1
               AND ($2::TEXT IS NULL OR object_kind = $2)
               AND ($3::TEXT IS NULL OR logical_key = $3)
             ORDER BY object_kind, logical_key, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(kind)
        .bind(query.logical_key.as_deref())
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| artifact_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }

    pub(in crate::governance) async fn create_version(
        &self,
        version: &GovernanceVersion,
        event: &GovernanceEvent,
    ) -> Result<GovernanceVersion, ControlPlaneError> {
        let components = serde_json::to_value(&version.applicable_components).map_err(|_| {
            ControlPlaneError::validation("invalid_governance_version", "applicable components cannot be encoded")
        })?;
        let dependencies = dependency_value(&version.dependencies)?;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "INSERT INTO governance_versions (
                id, artifact_id, tenant_id, version_name, content_digest,
                signature_algorithm, signing_key_id, signature_value,
                lifecycle_state, applicable_components, applicable_version_range,
                dependencies, review_due_at, expires_at, replacement_version_id,
                rollback_version_id, created_by, created_at, updated_at
             )
             SELECT $1, artifact.id, artifact.tenant_id, $4, $5,
                    NULL, NULL, NULL, 'draft', $6, $7, $8, $9, $10,
                    NULL, $11, $12, $13, $13
             FROM governance_artifacts artifact
             WHERE artifact.id = $2 AND artifact.tenant_id = $3
             RETURNING *",
        )
        .bind(version.id.as_uuid())
        .bind(version.artifact_id.as_uuid())
        .bind(version.tenant_id.as_uuid())
        .bind(&version.version)
        .bind(&version.content_digest)
        .bind(components)
        .bind(&version.applicable_version_range)
        .bind(dependencies)
        .bind(version.review_due_at)
        .bind(version.expires_at)
        .bind(version.rollback_version_id.map(GovernanceVersionId::as_uuid))
        .bind(&version.created_by)
        .bind(version.created_at)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        insert_event(&mut transaction, event).await?;
        transaction.commit().await?;
        version_from_row(&row)
    }

    pub(in crate::governance) async fn get_version(
        &self,
        tenant_id: TenantId,
        id: GovernanceVersionId,
    ) -> Result<GovernanceVersion, ControlPlaneError> {
        let row = sqlx::query("SELECT * FROM governance_versions WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(id.as_uuid())
            .fetch_optional(&self.pool)
            .await?
            .ok_or(ControlPlaneError::NotFound)?;
        version_from_row(&row)
    }

    pub(in crate::governance) async fn list_versions(
        &self,
        tenant_id: TenantId,
        artifact_id: GovernanceArtifactId,
        query: &GovernanceVersionQuery,
    ) -> Result<(Vec<GovernanceVersion>, bool), ControlPlaneError> {
        let limit = bounded_limit(query.limit);
        let state = query.state.map(lifecycle_state_name);
        let rows = sqlx::query(
            "SELECT *
             FROM governance_versions
             WHERE tenant_id = $1
               AND artifact_id = $2
               AND ($3::TEXT IS NULL OR lifecycle_state = $3)
             ORDER BY created_at DESC, id
             LIMIT $4",
        )
        .bind(tenant_id.as_uuid())
        .bind(artifact_id.as_uuid())
        .bind(state)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let truncated = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(200))
            .map(|row| version_from_row(&row))
            .collect::<Result<Vec<_>, _>>()
            .map(|items| (items, truncated))
    }

    pub(in crate::governance) async fn transition_version(
        &self,
        current: &GovernanceVersion,
        signature: Option<&GovernanceSignature>,
        replacement_version_id: Option<GovernanceVersionId>,
        rollback_version_id: Option<GovernanceVersionId>,
        event: &GovernanceEvent,
    ) -> Result<GovernanceVersion, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        if event.to_state == GovernanceLifecycleState::Active
            && let Some(previous) = sqlx::query(
                "SELECT *
                 FROM governance_versions
                 WHERE artifact_id = $1 AND lifecycle_state = 'active'
                 FOR UPDATE",
            )
            .bind(current.artifact_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await?
        {
            let previous = version_from_row(&previous)?;
            sqlx::query(
                "UPDATE governance_versions
                 SET lifecycle_state = 'deprecated',
                     replacement_version_id = $2,
                     updated_at = $3
                 WHERE id = $1 AND lifecycle_state = 'active'",
            )
            .bind(previous.id.as_uuid())
            .bind(current.id.as_uuid())
            .bind(event.occurred_at)
            .execute(&mut *transaction)
            .await?;
            insert_event(
                &mut transaction,
                &GovernanceEvent {
                    id: GovernanceEventId::new(),
                    tenant_id: previous.tenant_id,
                    artifact_id: previous.artifact_id,
                    version_id: previous.id,
                    from_state: Some(GovernanceLifecycleState::Active),
                    to_state: GovernanceLifecycleState::Deprecated,
                    actor: event.actor.clone(),
                    actor_kind: event.actor_kind,
                    reason: format!("Superseded by {}", current.id),
                    occurred_at: event.occurred_at,
                },
            )
            .await?;
        }
        let row = sqlx::query(
            "UPDATE governance_versions
             SET lifecycle_state = $4,
                 signature_algorithm = COALESCE($5, signature_algorithm),
                 signing_key_id = COALESCE($6, signing_key_id),
                 signature_value = COALESCE($7, signature_value),
                 replacement_version_id = $8,
                 rollback_version_id = $9,
                 updated_at = $10
             WHERE tenant_id = $1
               AND id = $2
               AND lifecycle_state = $3
             RETURNING *",
        )
        .bind(current.tenant_id.as_uuid())
        .bind(current.id.as_uuid())
        .bind(lifecycle_state_name(current.state))
        .bind(lifecycle_state_name(event.to_state))
        .bind(signature.map(|value| value.algorithm.as_str()))
        .bind(signature.map(|value| value.key_id.as_str()))
        .bind(signature.map(|value| value.value.as_str()))
        .bind(replacement_version_id.map(GovernanceVersionId::as_uuid))
        .bind(rollback_version_id.map(GovernanceVersionId::as_uuid))
        .bind(event.occurred_at)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::conflict_code(
                "governance_state_conflict",
                "governance version changed before the transition was persisted",
            )
        })?;
        if event.to_state == GovernanceLifecycleState::Active {
            sqlx::query(
                "UPDATE governance_artifacts
                 SET current_version_id = $3, updated_at = $4
                 WHERE tenant_id = $1 AND id = $2",
            )
            .bind(current.tenant_id.as_uuid())
            .bind(current.artifact_id.as_uuid())
            .bind(current.id.as_uuid())
            .bind(event.occurred_at)
            .execute(&mut *transaction)
            .await?;
        } else if matches!(
            event.to_state,
            GovernanceLifecycleState::Deprecated
                | GovernanceLifecycleState::Quarantined
                | GovernanceLifecycleState::Retired
        ) {
            sqlx::query(
                "UPDATE governance_artifacts
                 SET current_version_id = NULL, updated_at = $3
                 WHERE tenant_id = $1 AND id = $2 AND current_version_id = $4",
            )
            .bind(current.tenant_id.as_uuid())
            .bind(current.artifact_id.as_uuid())
            .bind(event.occurred_at)
            .bind(current.id.as_uuid())
            .execute(&mut *transaction)
            .await?;
        }
        insert_event(&mut transaction, event).await?;
        transaction.commit().await?;
        version_from_row(&row)
    }

    pub(in crate::governance) async fn governance_override(
        &self,
        tenant_id: TenantId,
        kind: GovernanceObjectKind,
        logical_key: &str,
        version: &str,
    ) -> Result<GovernanceOverride, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT artifact.id AS artifact_id, version.*
             FROM governance_artifacts artifact
             LEFT JOIN governance_versions version
               ON version.artifact_id = artifact.id
              AND version.version_name = $4
             WHERE artifact.tenant_id = $1
               AND artifact.object_kind = $2
               AND artifact.logical_key = $3",
        )
        .bind(tenant_id.as_uuid())
        .bind(object_kind_name(kind))
        .bind(logical_key)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(GovernanceOverride {
                artifact_present: false,
                version: None,
            });
        };
        let version_id = row.try_get::<Option<uuid::Uuid>, _>("id")?;
        Ok(GovernanceOverride {
            artifact_present: true,
            version: version_id.map(|_| version_from_row(&row)).transpose()?,
        })
    }
}

async fn insert_event(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event: &GovernanceEvent,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO governance_events (
            id, tenant_id, artifact_id, version_id, from_state, to_state,
            actor_name, actor_kind, reason, occurred_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(event.id.as_uuid())
    .bind(event.tenant_id.as_uuid())
    .bind(event.artifact_id.as_uuid())
    .bind(event.version_id.as_uuid())
    .bind(event.from_state.map(lifecycle_state_name))
    .bind(lifecycle_state_name(event.to_state))
    .bind(&event.actor)
    .bind(actor_kind_name(event.actor_kind))
    .bind(&event.reason)
    .bind(event.occurred_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(in crate::governance) fn human_event(
    version: &GovernanceVersion,
    to_state: GovernanceLifecycleState,
    actor: &str,
    reason: &str,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> GovernanceEvent {
    GovernanceEvent {
        id: GovernanceEventId::new(),
        tenant_id: version.tenant_id,
        artifact_id: version.artifact_id,
        version_id: version.id,
        from_state: Some(version.state),
        to_state,
        actor: actor.to_owned(),
        actor_kind: GovernanceActorKind::Human,
        reason: reason.to_owned(),
        occurred_at,
    }
}
