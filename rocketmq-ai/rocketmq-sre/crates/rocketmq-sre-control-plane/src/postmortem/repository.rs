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
use rocketmq_sre_contracts::ActionItem;
use rocketmq_sre_contracts::ActionItemId;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::KnowledgeChunkId;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::KnowledgeItemId;
use rocketmq_sre_contracts::KnowledgeReviewStatus;
use rocketmq_sre_contracts::PostmortemDraft;
use rocketmq_sre_contracts::PostmortemId;
use rocketmq_sre_contracts::PostmortemRevision;
use rocketmq_sre_contracts::PostmortemStatus;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::Transaction;
use uuid::Uuid;

use super::ActionItemListQuery;
use super::IncidentRecurrenceView;
use super::OperatorTodo;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

impl PostgresRepository {
    pub(super) async fn create_postmortem_bundle(
        &self,
        draft: &PostmortemDraft,
        revision: &PostmortemRevision,
        action_items: &[ActionItem],
        fingerprint: Option<&str>,
        root_cause_code: Option<&str>,
        affected_component: Option<&str>,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO postmortems (
                id, tenant_id, cluster_id, incident_id, status, current_revision,
                confirmed_by, confirmed_at, published_knowledge_item_id,
                created_by, created_at, updated_at, fingerprint,
                root_cause_code, affected_component
             ) VALUES (
                $1, $2, $3, $4, $5, 1, NULL, NULL, NULL,
                $6, $7, $7, $8, $9, $10
             )",
        )
        .bind(draft.id.as_uuid())
        .bind(draft.tenant_id.as_uuid())
        .bind(draft.cluster_id.as_uuid())
        .bind(draft.incident_id.as_uuid())
        .bind(enum_name(PostmortemStatus::Draft)?)
        .bind(&draft.created_by)
        .bind(draft.created_at)
        .bind(fingerprint)
        .bind(root_cause_code)
        .bind(affected_component)
        .execute(&mut *transaction)
        .await?;
        insert_revision(&mut transaction, revision).await?;
        for item in action_items {
            insert_action_item(&mut transaction, item).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn postmortem_by_incident(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
    ) -> Result<Option<PostmortemDraft>, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, incident_id, status, current_revision,
                    confirmed_by, confirmed_at, published_knowledge_item_id,
                    created_by, created_at, updated_at
             FROM postmortems
             WHERE incident_id = $1 AND tenant_id = $2",
        )
        .bind(incident_id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(postmortem_from_row).transpose()
    }

    pub(super) async fn scoped_postmortem(
        &self,
        auth: &AuthContext,
        id: PostmortemId,
    ) -> Result<PostmortemDraft, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, incident_id, status, current_revision,
                    confirmed_by, confirmed_at, published_knowledge_item_id,
                    created_by, created_at, updated_at
             FROM postmortems
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let draft = postmortem_from_row(&row)?;
        enforce_cluster(auth, draft.cluster_id)?;
        Ok(draft)
    }

    pub(super) async fn scoped_revisions(
        &self,
        auth: &AuthContext,
        id: PostmortemId,
    ) -> Result<Vec<PostmortemRevision>, ControlPlaneError> {
        self.scoped_postmortem(auth, id).await?;
        let rows = sqlx::query(
            "SELECT id, postmortem_id, revision, summary, impact, detection,
                    timeline, root_causes, contributing_factors, conclusions,
                    recovery, effective_actions, ineffective_actions, evidence_ids,
                    model_invocation_id, edited_by, human_confirmed, created_at
             FROM postmortem_revisions
             WHERE postmortem_id = $1
             ORDER BY revision ASC",
        )
        .bind(id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(revision_from_row).collect()
    }

    pub(super) async fn scoped_action_items(
        &self,
        auth: &AuthContext,
        id: PostmortemId,
    ) -> Result<Vec<ActionItem>, ControlPlaneError> {
        self.scoped_postmortem(auth, id).await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, postmortem_id, incident_id, title,
                    owner_name, due_at, status, verification, evidence_ids,
                    execution_journal, created_at, updated_at, completed_at
             FROM action_items
             WHERE postmortem_id = $1
             ORDER BY created_at ASC, id ASC",
        )
        .bind(id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(action_item_from_row).collect()
    }

    pub(super) async fn list_action_items_scoped(
        &self,
        auth: &AuthContext,
        query: &ActionItemListQuery,
    ) -> Result<Vec<ActionItem>, ControlPlaneError> {
        enforce_cluster(auth, query.cluster_id)?;
        let status = query.status.map(enum_name).transpose()?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, postmortem_id, incident_id, title,
                    owner_name, due_at, status, verification, evidence_ids,
                    execution_journal, created_at, updated_at, completed_at
             FROM action_items
             WHERE tenant_id = $1 AND cluster_id = $2
               AND ($3::TEXT IS NULL OR status = $3)
               AND ($4::TEXT IS NULL OR owner_name = $4)
             ORDER BY due_at ASC NULLS LAST, updated_at DESC, id
             LIMIT $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(status)
        .bind(query.owner.as_deref())
        .bind(i64::from(query.bounded_limit()))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(action_item_from_row).collect()
    }

    pub(super) async fn scoped_action_item(
        &self,
        auth: &AuthContext,
        id: ActionItemId,
    ) -> Result<ActionItem, ControlPlaneError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, postmortem_id, incident_id, title,
                    owner_name, due_at, status, verification, evidence_ids,
                    execution_journal, created_at, updated_at, completed_at
             FROM action_items
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneError::NotFound)?;
        let item = action_item_from_row(&row)?;
        enforce_cluster(auth, item.cluster_id)?;
        Ok(item)
    }

    pub(super) async fn update_action_item(
        &self,
        auth: &AuthContext,
        current: &ActionItem,
        next: &ActionItem,
    ) -> Result<(), ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE action_items
             SET owner_name = $1, due_at = $2, status = $3, verification = $4,
                 evidence_ids = $5, updated_at = $6, completed_at = $7
             WHERE id = $8 AND tenant_id = $9 AND status = $10",
        )
        .bind(&next.owner)
        .bind(next.due_at)
        .bind(enum_name(next.status)?)
        .bind(&next.verification)
        .bind(next.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(next.updated_at)
        .bind(next.completed_at)
        .bind(next.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(enum_name(current.status)?)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict("action item state changed concurrently"));
        }
        sqlx::query(
            "INSERT INTO action_item_events (
                action_item_id, tenant_id, previous_status, next_status, actor,
                verification, evidence_ids, occurred_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(next.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(enum_name(current.status)?)
        .bind(enum_name(next.status)?)
        .bind(&auth.subject)
        .bind(&next.verification)
        .bind(next.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(next.updated_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn recurrences(
        &self,
        auth: &AuthContext,
        id: PostmortemId,
    ) -> Result<Vec<IncidentRecurrenceView>, ControlPlaneError> {
        let draft = self.scoped_postmortem(auth, id).await?;
        let rows = sqlx::query(
            "SELECT incident_id, previous_incident_id, postmortem_id, fingerprint,
                    root_cause_code, affected_component, matched_at
             FROM incident_recurrences
             WHERE postmortem_id = $1 OR incident_id = $2
             ORDER BY matched_at DESC",
        )
        .bind(id.as_uuid())
        .bind(draft.incident_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(IncidentRecurrenceView {
                    incident_id: IncidentId::from_uuid(row.try_get("incident_id")?),
                    previous_incident_id: IncidentId::from_uuid(row.try_get("previous_incident_id")?),
                    postmortem_id: PostmortemId::from_uuid(row.try_get("postmortem_id")?),
                    fingerprint: row.try_get("fingerprint")?,
                    root_cause_code: row.try_get("root_cause_code")?,
                    affected_component: row.try_get("affected_component")?,
                    matched_at: row.try_get("matched_at")?,
                })
            })
            .collect()
    }

    pub(super) async fn discover_recurrences(
        &self,
        auth: &AuthContext,
        incident_id: IncidentId,
        cluster_id: ClusterId,
        fingerprint: Option<&str>,
        root_cause_code: Option<&str>,
        affected_component: Option<&str>,
        at: DateTime<Utc>,
    ) -> Result<(), ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT incident_id, id, fingerprint, root_cause_code, affected_component
             FROM postmortems
             WHERE tenant_id = $1 AND cluster_id = $2 AND status = 'published'
               AND incident_id <> $3
               AND (
                    ($4::TEXT IS NOT NULL AND fingerprint = $4)
                 OR ($5::TEXT IS NOT NULL AND root_cause_code = $5)
                 OR ($6::TEXT IS NOT NULL AND affected_component = $6)
               )
             ORDER BY updated_at DESC
             LIMIT 20",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(incident_id.as_uuid())
        .bind(fingerprint)
        .bind(root_cause_code)
        .bind(affected_component)
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let previous_incident_id: Uuid = row.try_get("incident_id")?;
            let postmortem_id: Uuid = row.try_get("id")?;
            sqlx::query(
                "INSERT INTO incident_recurrences (
                    incident_id, previous_incident_id, postmortem_id, fingerprint,
                    root_cause_code, affected_component, matched_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT DO NOTHING",
            )
            .bind(incident_id.as_uuid())
            .bind(previous_incident_id)
            .bind(postmortem_id)
            .bind(
                fingerprint
                    .or(row.try_get::<Option<&str>, _>("fingerprint")?)
                    .unwrap_or("unknown"),
            )
            .bind(
                root_cause_code
                    .or(row.try_get::<Option<&str>, _>("root_cause_code")?)
                    .unwrap_or("unknown"),
            )
            .bind(
                affected_component
                    .or(row.try_get::<Option<&str>, _>("affected_component")?)
                    .unwrap_or("unknown"),
            )
            .bind(at)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub(super) async fn publish_postmortem(
        &self,
        auth: &AuthContext,
        draft: &PostmortemDraft,
        revision: &PostmortemRevision,
        item: &KnowledgeItem,
        markdown: &str,
        root_cause_code: Option<&str>,
        affected_component: &str,
    ) -> Result<KnowledgeItem, ControlPlaneError> {
        let mut transaction = self.pool.begin().await?;
        insert_knowledge(&mut transaction, item, markdown).await?;
        let result = sqlx::query(
            "UPDATE postmortems
             SET status = 'published', published_knowledge_item_id = $1,
                 root_cause_code = $2, affected_component = $3, updated_at = $4
             WHERE id = $5 AND tenant_id = $6 AND status = 'confirmed'
               AND current_revision = $7",
        )
        .bind(item.id.as_uuid())
        .bind(root_cause_code)
        .bind(affected_component)
        .bind(item.updated_at)
        .bind(draft.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(i32::try_from(revision.revision).map_err(|_| {
            ControlPlaneError::validation("invalid_revision", "postmortem revision exceeds PostgreSQL INTEGER")
        })?)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(ControlPlaneError::conflict(
                "postmortem must have a current human-confirmed revision before publication",
            ));
        }
        transaction.commit().await?;
        Ok(item.clone())
    }

    pub(super) async fn knowledge_for_postmortem(
        &self,
        auth: &AuthContext,
        draft: &PostmortemDraft,
    ) -> Result<Option<KnowledgeItem>, ControlPlaneError> {
        let Some(id) = draft.published_knowledge_item_id else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, title, component, rocketmq_version_range,
                    source_uri, source_version, valid_from, valid_until, owner_name,
                    review_status, review_due_at, sensitivity, content_hash, conflict,
                    created_at, updated_at
             FROM knowledge_items WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(knowledge_from_row).transpose()
    }

    pub(super) async fn todos_for_postmortem(
        &self,
        auth: &AuthContext,
        draft: &PostmortemDraft,
    ) -> Result<Vec<OperatorTodo>, ControlPlaneError> {
        let action_ids = sqlx::query_scalar::<_, Uuid>("SELECT id FROM action_items WHERE postmortem_id = $1")
            .bind(draft.id.as_uuid())
            .fetch_all(&self.pool)
            .await?;
        let mut aggregate_ids = action_ids;
        if let Some(id) = draft.published_knowledge_item_id {
            aggregate_ids.push(id.as_uuid());
        }
        if aggregate_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, kind, aggregate_id, title,
                    due_at, status, created_at
             FROM operator_todos
             WHERE tenant_id = $1 AND aggregate_id = ANY($2)
             ORDER BY due_at ASC, id",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(aggregate_ids)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(todo_from_row).collect()
    }

    pub(super) async fn materialize_due_todos(&self, now: DateTime<Utc>) -> Result<u64, ControlPlaneError> {
        let due = sqlx::query(
            "SELECT id AS aggregate_id, tenant_id, cluster_id, incident_id,
                    title, due_at, 'action_item_due' AS kind
             FROM action_items
             WHERE due_at <= $1 AND status NOT IN ('completed', 'cancelled')
             UNION ALL
             SELECT k.id AS aggregate_id, k.tenant_id, k.cluster_id,
                    p.incident_id, k.title, k.review_due_at AS due_at,
                    'knowledge_review_due' AS kind
             FROM knowledge_items k
             JOIN postmortems p ON p.published_knowledge_item_id = k.id
             WHERE k.review_due_at <= $1
               AND k.review_status NOT IN ('deprecated', 'expired')",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        let mut inserted = 0_u64;
        for row in due {
            let kind: String = row.try_get("kind")?;
            let aggregate_id: Uuid = row.try_get("aggregate_id")?;
            let tenant_id: Uuid = row.try_get("tenant_id")?;
            let cluster_id: Option<Uuid> = row.try_get("cluster_id")?;
            let incident_id: Uuid = row.try_get("incident_id")?;
            let title: String = row.try_get("title")?;
            let due_at: DateTime<Utc> = row.try_get("due_at")?;
            let result = sqlx::query(
                "INSERT INTO operator_todos (
                    id, tenant_id, cluster_id, kind, aggregate_id, title,
                    due_at, status, created_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'open', $8)
                 ON CONFLICT (kind, aggregate_id) DO NOTHING",
            )
            .bind(Uuid::new_v4())
            .bind(tenant_id)
            .bind(cluster_id)
            .bind(&kind)
            .bind(aggregate_id)
            .bind(&title)
            .bind(due_at)
            .bind(now)
            .execute(&self.pool)
            .await?;
            inserted = inserted.saturating_add(result.rows_affected());
            if result.rows_affected() == 1
                && let Some(cluster_id) = cluster_id
            {
                enqueue_todo_notifications(
                    self,
                    tenant_id,
                    cluster_id,
                    incident_id,
                    &kind,
                    aggregate_id,
                    &title,
                    now,
                )
                .await?;
            }
        }
        Ok(inserted)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "notification delivery identity is an explicit immutable scope tuple"
)]
async fn enqueue_todo_notifications(
    repository: &PostgresRepository,
    tenant_id: Uuid,
    cluster_id: Uuid,
    incident_id: Uuid,
    kind: &str,
    aggregate_id: Uuid,
    title: &str,
    now: DateTime<Utc>,
) -> Result<(), ControlPlaneError> {
    let targets = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM notification_targets
         WHERE tenant_id = $1 AND enabled = TRUE
           AND (cluster_id IS NULL OR cluster_id = $2)
         ORDER BY id",
    )
    .bind(tenant_id)
    .bind(cluster_id)
    .fetch_all(&repository.pool)
    .await?;
    let summary = title.trim().chars().take(2_048).collect::<String>();
    let deep_link = if kind == "action_item_due" {
        format!("/action-items?focus={aggregate_id}")
    } else {
        format!("/knowledge?focus={aggregate_id}")
    };
    for target_id in targets {
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, target_id, tenant_id, cluster_id, incident_id,
                delivery_key, status, sanitized_summary, deep_link,
                attempt_count, next_attempt_at, created_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, 'pending', $7, $8, 0, $9, $9
             )
             ON CONFLICT (tenant_id, delivery_key) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(target_id)
        .bind(tenant_id)
        .bind(cluster_id)
        .bind(incident_id)
        .bind(format!("todo:{kind}:{aggregate_id}:{target_id}"))
        .bind(&summary)
        .bind(&deep_link)
        .bind(now)
        .execute(&repository.pool)
        .await?;
    }
    Ok(())
}

async fn insert_revision(
    transaction: &mut Transaction<'_, Postgres>,
    revision: &PostmortemRevision,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO postmortem_revisions (
            id, postmortem_id, revision, summary, impact, detection, timeline,
            root_causes, contributing_factors, conclusions, recovery,
            effective_actions, ineffective_actions, evidence_ids,
            model_invocation_id, edited_by, human_confirmed, created_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            $13, $14, $15, $16, $17, $18
         )",
    )
    .bind(revision.id.as_uuid())
    .bind(revision.postmortem_id.as_uuid())
    .bind(i32::try_from(revision.revision).map_err(|_| {
        ControlPlaneError::validation("invalid_revision", "postmortem revision exceeds PostgreSQL INTEGER")
    })?)
    .bind(&revision.summary)
    .bind(&revision.impact)
    .bind(&revision.detection)
    .bind(&revision.timeline)
    .bind(json_value(&revision.root_causes)?)
    .bind(json_value(&revision.contributing_factors)?)
    .bind(json_value(&revision.conclusions)?)
    .bind(&revision.recovery)
    .bind(json_value(&revision.effective_actions)?)
    .bind(json_value(&revision.ineffective_actions)?)
    .bind(revision.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
    .bind(revision.model_invocation_id.map(|id| id.as_uuid()))
    .bind(&revision.edited_by)
    .bind(revision.human_confirmed)
    .bind(revision.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_action_item(
    transaction: &mut Transaction<'_, Postgres>,
    item: &ActionItem,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO action_items (
            id, tenant_id, cluster_id, postmortem_id, incident_id, title,
            owner_name, due_at, status, verification, evidence_ids,
            execution_journal, created_at, updated_at, completed_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, $12, $12, NULL
         )",
    )
    .bind(item.id.as_uuid())
    .bind(item.tenant_id.as_uuid())
    .bind(item.cluster_id.as_uuid())
    .bind(item.postmortem_id.as_uuid())
    .bind(item.incident_id.as_uuid())
    .bind(&item.title)
    .bind(&item.owner)
    .bind(item.due_at)
    .bind(enum_name(item.status)?)
    .bind(&item.verification)
    .bind(item.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
    .bind(item.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_knowledge(
    transaction: &mut Transaction<'_, Postgres>,
    item: &KnowledgeItem,
    markdown: &str,
) -> Result<(), ControlPlaneError> {
    sqlx::query(
        "INSERT INTO knowledge_items (
            id, tenant_id, cluster_id, title, component, rocketmq_version_range,
            source_uri, source_version, valid_from, valid_until, owner_name,
            review_status, review_due_at, sensitivity, content_hash, conflict,
            created_at, updated_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            $13, $14, $15, FALSE, $16, $16
         )",
    )
    .bind(item.id.as_uuid())
    .bind(item.tenant_id.as_uuid())
    .bind(item.cluster_id.map(ClusterId::as_uuid))
    .bind(&item.title)
    .bind(&item.component)
    .bind(&item.rocketmq_version_range)
    .bind(&item.source_uri)
    .bind(&item.source_version)
    .bind(item.valid_from)
    .bind(item.valid_until)
    .bind(&item.owner)
    .bind(enum_name(KnowledgeReviewStatus::Validated)?)
    .bind(item.review_due_at)
    .bind(&item.sensitivity)
    .bind(&item.content_hash)
    .bind(item.created_at)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO knowledge_chunks (
            id, knowledge_item_id, ordinal, heading, content, content_hash
         ) VALUES ($1, $2, 0, $3, $4, $5)",
    )
    .bind(KnowledgeChunkId::new().as_uuid())
    .bind(item.id.as_uuid())
    .bind("Postmortem")
    .bind(markdown)
    .bind(format!(
        "sha256:{}",
        rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(markdown.as_bytes()))
    ))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn postmortem_from_row(row: &sqlx::postgres::PgRow) -> Result<PostmortemDraft, ControlPlaneError> {
    Ok(PostmortemDraft {
        id: PostmortemId::from_uuid(row.try_get("id")?),
        tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        incident_id: IncidentId::from_uuid(row.try_get("incident_id")?),
        status: enum_from_string(row.try_get("status")?, "postmortem status")?,
        current_revision: u32::try_from(row.try_get::<i32, _>("current_revision")?)
            .map_err(|_| ControlPlaneError::configuration("stored postmortem revision is negative"))?,
        confirmed_by: row.try_get("confirmed_by")?,
        confirmed_at: row.try_get("confirmed_at")?,
        published_knowledge_item_id: row
            .try_get::<Option<Uuid>, _>("published_knowledge_item_id")?
            .map(KnowledgeItemId::from_uuid),
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn revision_from_row(row: &sqlx::postgres::PgRow) -> Result<PostmortemRevision, ControlPlaneError> {
    Ok(PostmortemRevision {
        id: rocketmq_sre_contracts::PostmortemRevisionId::from_uuid(row.try_get("id")?),
        postmortem_id: PostmortemId::from_uuid(row.try_get("postmortem_id")?),
        revision: u32::try_from(row.try_get::<i32, _>("revision")?)
            .map_err(|_| ControlPlaneError::configuration("stored postmortem revision is negative"))?,
        summary: row.try_get("summary")?,
        impact: row.try_get("impact")?,
        detection: row.try_get("detection")?,
        timeline: row.try_get("timeline")?,
        root_causes: json_from_value(row.try_get("root_causes")?, "root causes")?,
        contributing_factors: json_from_value(row.try_get("contributing_factors")?, "contributing factors")?,
        conclusions: json_from_value(row.try_get("conclusions")?, "conclusions")?,
        recovery: row.try_get("recovery")?,
        effective_actions: json_from_value(row.try_get("effective_actions")?, "effective actions")?,
        ineffective_actions: json_from_value(row.try_get("ineffective_actions")?, "ineffective actions")?,
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        model_invocation_id: row
            .try_get::<Option<Uuid>, _>("model_invocation_id")?
            .map(rocketmq_sre_contracts::ModelInvocationId::from_uuid),
        edited_by: row.try_get("edited_by")?,
        human_confirmed: row.try_get("human_confirmed")?,
        created_at: row.try_get("created_at")?,
    })
}

fn action_item_from_row(row: &sqlx::postgres::PgRow) -> Result<ActionItem, ControlPlaneError> {
    Ok(ActionItem {
        id: ActionItemId::from_uuid(row.try_get("id")?),
        tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: ClusterId::from_uuid(row.try_get("cluster_id")?),
        postmortem_id: PostmortemId::from_uuid(row.try_get("postmortem_id")?),
        incident_id: IncidentId::from_uuid(row.try_get("incident_id")?),
        title: row.try_get("title")?,
        owner: row.try_get("owner_name")?,
        due_at: row.try_get("due_at")?,
        status: enum_from_string(row.try_get("status")?, "action item status")?,
        verification: row.try_get("verification")?,
        evidence_ids: row
            .try_get::<Vec<Uuid>, _>("evidence_ids")?
            .into_iter()
            .map(EvidenceId::from_uuid)
            .collect(),
        execution_journal: row.try_get("execution_journal")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn knowledge_from_row(row: &sqlx::postgres::PgRow) -> Result<KnowledgeItem, ControlPlaneError> {
    Ok(KnowledgeItem {
        id: KnowledgeItemId::from_uuid(row.try_get("id")?),
        tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        title: row.try_get("title")?,
        component: row.try_get("component")?,
        rocketmq_version_range: row.try_get("rocketmq_version_range")?,
        source_uri: row.try_get("source_uri")?,
        source_version: row.try_get("source_version")?,
        valid_from: row.try_get("valid_from")?,
        valid_until: row.try_get("valid_until")?,
        owner: row.try_get("owner_name")?,
        review_status: enum_from_string(row.try_get("review_status")?, "knowledge review status")?,
        review_due_at: row.try_get("review_due_at")?,
        sensitivity: row.try_get("sensitivity")?,
        content_hash: row.try_get("content_hash")?,
        conflict: row.try_get("conflict")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn todo_from_row(row: &sqlx::postgres::PgRow) -> Result<OperatorTodo, ControlPlaneError> {
    Ok(OperatorTodo {
        id: row.try_get("id")?,
        tenant_id: rocketmq_sre_contracts::TenantId::from_uuid(row.try_get("tenant_id")?),
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        kind: row.try_get("kind")?,
        aggregate_id: row.try_get("aggregate_id")?,
        title: row.try_get("title")?,
        due_at: row.try_get("due_at")?,
        status: row.try_get("status")?,
        created_at: row.try_get("created_at")?,
    })
}

fn enforce_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "cluster is outside the authenticated allowlist",
        ));
    }
    Ok(())
}

fn enum_name<T: Serialize>(value: T) -> Result<String, ControlPlaneError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| ControlPlaneError::configuration("workflow enum did not encode as text"))
}

fn enum_from_string<T: DeserializeOwned>(value: String, field: &str) -> Result<T, ControlPlaneError> {
    serde_json::from_value(Value::String(value))
        .map_err(|error| ControlPlaneError::configuration(format!("stored {field} is invalid: {error}")))
}

fn json_value<T: Serialize>(value: &T) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("postmortem JSON cannot be encoded: {error}")))
}

fn json_from_value<T: DeserializeOwned>(value: Value, field: &str) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|error| ControlPlaneError::configuration(format!("stored {field} is invalid: {error}")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::PostmortemConclusion;
    use rocketmq_sre_contracts::PostmortemRevisionId;
    use rocketmq_sre_contracts::TenantId;

    use super::*;
    use crate::phase2_repository::Phase2Repository;

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn postgres_postmortem_keeps_revisions_and_publishes_only_confirmed_content() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let incident_id = IncidentId::new();
        let evidence_id = EvidenceId::new();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'local', '5.3.0', 'test', 'postmortem-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(format!("postmortem-{cluster_id}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");
        sqlx::query(
            "INSERT INTO sre_incidents (
                id, tenant_id, cluster_id, title, resource, symptom_family,
                fingerprint, status, created_by_subject, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'consumer lag', 'consumer:orders', 'consumer_lag',
                $4, 'resolved', 'postmortem-test', $5, $5
             )",
        )
        .bind(incident_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(format!("fingerprint-{incident_id}"))
        .bind(now)
        .execute(&repository.pool)
        .await
        .expect("test incident");
        let auth = AuthContext {
            tenant_id,
            subject: "operator@example.test".to_owned(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::from(["rocketmq:diagnose".to_owned()]),
        };
        let postmortem_id = PostmortemId::new();
        let draft = PostmortemDraft {
            id: postmortem_id,
            tenant_id,
            cluster_id,
            incident_id,
            status: PostmortemStatus::Draft,
            current_revision: 1,
            confirmed_by: None,
            confirmed_at: None,
            published_knowledge_item_id: None,
            created_by: auth.subject.clone(),
            created_at: now,
            updated_at: now,
        };
        let conclusion = PostmortemConclusion {
            code: "consumer_lag_growth".to_owned(),
            statement: "arrival rate exceeded processing rate".to_owned(),
            evidence_ids: vec![evidence_id],
        };
        let revision = PostmortemRevision {
            id: PostmortemRevisionId::new(),
            postmortem_id,
            revision: 1,
            summary: "consumer lag incident".to_owned(),
            impact: "orders delayed".to_owned(),
            detection: "lag alert".to_owned(),
            timeline: serde_json::json!([]),
            root_causes: vec![conclusion.clone()],
            contributing_factors: Vec::new(),
            conclusions: vec![conclusion],
            recovery: "consumer recovered".to_owned(),
            effective_actions: Vec::new(),
            ineffective_actions: Vec::new(),
            evidence_ids: vec![evidence_id],
            model_invocation_id: None,
            edited_by: auth.subject.clone(),
            human_confirmed: false,
            created_at: now,
        };
        repository
            .create_postmortem_bundle(
                &draft,
                &revision,
                &[],
                Some("consumer-lag"),
                Some("consumer_lag_growth"),
                Some("consumer"),
            )
            .await
            .expect("draft bundle");
        let mut confirmed = revision.clone();
        confirmed.id = PostmortemRevisionId::new();
        confirmed.revision = 2;
        confirmed.human_confirmed = true;
        confirmed.created_at = now + chrono::Duration::seconds(1);
        repository
            .append_postmortem_revision(&confirmed)
            .await
            .expect("confirmed revision");
        let confirmed_draft = repository
            .scoped_postmortem(&auth, postmortem_id)
            .await
            .expect("confirmed draft");
        assert_eq!(confirmed_draft.status, PostmortemStatus::Confirmed);
        assert_eq!(
            repository
                .scoped_revisions(&auth, postmortem_id)
                .await
                .expect("revision history")
                .len(),
            2
        );
        let knowledge = KnowledgeItem {
            id: KnowledgeItemId::new(),
            tenant_id,
            cluster_id: Some(cluster_id),
            title: "consumer lag postmortem".to_owned(),
            component: "consumer".to_owned(),
            rocketmq_version_range: "*".to_owned(),
            source_uri: format!("rocketmq-sre://postmortems/{postmortem_id}"),
            source_version: "revision-2".to_owned(),
            valid_from: Some(now),
            valid_until: None,
            owner: auth.subject.clone(),
            review_status: KnowledgeReviewStatus::Validated,
            review_due_at: now + chrono::Duration::days(90),
            sensitivity: "internal".to_owned(),
            content_hash: format!(
                "sha256:{}",
                rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(b"postmortem"))
            ),
            conflict: false,
            created_at: now,
            updated_at: now,
        };
        repository
            .publish_postmortem(
                &auth,
                &confirmed_draft,
                &confirmed,
                &knowledge,
                "# Postmortem",
                Some("consumer_lag_growth"),
                "consumer",
            )
            .await
            .expect("human publication");
        let published = repository
            .scoped_postmortem(&auth, postmortem_id)
            .await
            .expect("published postmortem");
        assert_eq!(published.status, PostmortemStatus::Published);
        assert_eq!(published.published_knowledge_item_id, Some(knowledge.id));

        let recurring_incident_id = IncidentId::new();
        sqlx::query(
            "INSERT INTO sre_incidents (
                id, tenant_id, cluster_id, title, resource, symptom_family,
                fingerprint, status, created_by_subject, created_at, updated_at
             ) VALUES (
                $1, $2, $3, 'consumer lag recurrence', 'consumer:orders',
                'consumer_lag', $4, 'diagnosing', 'postmortem-test', $5, $5
             )",
        )
        .bind(recurring_incident_id.as_uuid())
        .bind(tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind("consumer-lag")
        .bind(now + chrono::Duration::days(1))
        .execute(&repository.pool)
        .await
        .expect("recurring incident");
        let recurring_postmortem_id = PostmortemId::new();
        let mut recurring_draft = draft.clone();
        recurring_draft.id = recurring_postmortem_id;
        recurring_draft.incident_id = recurring_incident_id;
        let mut recurring_revision = revision.clone();
        recurring_revision.id = PostmortemRevisionId::new();
        recurring_revision.postmortem_id = recurring_postmortem_id;
        repository
            .create_postmortem_bundle(
                &recurring_draft,
                &recurring_revision,
                &[],
                Some("consumer-lag"),
                Some("consumer_lag_growth"),
                Some("consumer"),
            )
            .await
            .expect("recurring draft");
        repository
            .discover_recurrences(
                &auth,
                recurring_incident_id,
                cluster_id,
                Some("consumer-lag"),
                Some("consumer_lag_growth"),
                Some("consumer"),
                now + chrono::Duration::days(1),
            )
            .await
            .expect("recurrence discovery");
        let recurrences = repository
            .recurrences(&auth, recurring_postmortem_id)
            .await
            .expect("recurrence links");
        assert_eq!(recurrences.len(), 1);
        assert_eq!(recurrences[0].postmortem_id, postmortem_id);
        assert_eq!(recurrences[0].previous_incident_id, incident_id);
    }
}
