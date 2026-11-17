// Copyright 2023 The RocketMQ Rust Authors
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

use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::KnowledgeFeedbackKind;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::KnowledgeItemId;
use rocketmq_sre_contracts::KnowledgeReviewStatus;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::model::KnowledgeFeedbackRequest;
use super::model::KnowledgeImport;
use super::model::KnowledgeImportResult;
use super::model::KnowledgeListQuery;
use super::model::KnowledgePage;
use crate::ControlPlaneError;
use crate::ControlPlaneRequestFailure;
use crate::PostgresRepository;
use crate::auth::AuthContext;

impl PostgresRepository {
    pub(super) async fn list_knowledge(
        &self,
        auth: &AuthContext,
        query: &KnowledgeListQuery,
    ) -> Result<KnowledgePage, ControlPlaneRequestFailure> {
        enforce_optional_cluster(auth, Some(query.cluster_id))?;
        let limit = query.bounded_limit()?;
        let cursor = query
            .cursor
            .as_deref()
            .map(|value| {
                value
                    .parse::<Uuid>()
                    .map_err(|_| ControlPlaneRequestFailure::validation("invalid_request", "cursor is invalid"))
            })
            .transpose()?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, cluster_id, title, component, rocketmq_version_range,
                    source_uri, source_version, valid_from, valid_until, owner_name,
                    review_status, review_due_at, sensitivity, content_hash, conflict,
                    created_at, updated_at
             FROM knowledge_items
             WHERE tenant_id = $1
               AND (cluster_id IS NULL OR cluster_id = $2)
               AND ($3::UUID IS NULL OR id < $3)
             ORDER BY id DESC
             LIMIT $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(cursor)
        .bind(i64::from(limit) + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > limit as usize;
        let items = rows
            .iter()
            .take(limit as usize)
            .map(knowledge_item_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = has_more.then(|| items.last().map(|item| item.id.to_string())).flatten();
        Ok(KnowledgePage {
            items,
            next_cursor,
            partial: false,
            warnings: Vec::new(),
            observed_at: Utc::now(),
        })
    }

    pub(super) async fn import_knowledge(
        &self,
        auth: &AuthContext,
        import: KnowledgeImport,
    ) -> Result<KnowledgeImportResult, ControlPlaneRequestFailure> {
        authorize_curation(auth, import.item.cluster_id)?;
        let mut transaction = self.pool.begin().await?;
        super::source::lock_knowledge_source(&mut transaction, &import.item).await?;
        let existing = sqlx::query(
            "SELECT id
             FROM knowledge_items
             WHERE tenant_id = $1 AND source_uri = $2 AND source_version = $3 AND content_hash = $4
               AND cluster_id IS NOT DISTINCT FROM $5",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(&import.item.source_uri)
        .bind(&import.item.source_version)
        .bind(&import.item.content_hash)
        .bind(import.item.cluster_id.map(ClusterId::as_uuid))
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let id = KnowledgeItemId::from_uuid(row.try_get("id")?);
            let chunk_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM knowledge_chunks WHERE knowledge_item_id=$1")
                    .bind(id.as_uuid())
                    .fetch_one(&mut *transaction)
                    .await?;
            transaction.rollback().await?;
            let item = self.knowledge_item(auth, id).await?;
            return Ok(KnowledgeImportResult {
                item,
                chunk_count: usize::try_from(chunk_count).map_err(ControlPlaneError::configuration_source)?,
                deduplicated: true,
            });
        }

        sqlx::query(
            "INSERT INTO knowledge_items (
                id, tenant_id, cluster_id, title, component, rocketmq_version_range,
                source_uri, source_version, valid_from, valid_until, owner_name,
                review_status, review_due_at, sensitivity, content_hash, conflict,
                created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                $14, $15, $16, $17, $18
             )",
        )
        .bind(import.item.id.as_uuid())
        .bind(import.item.tenant_id.as_uuid())
        .bind(import.item.cluster_id.map(ClusterId::as_uuid))
        .bind(&import.item.title)
        .bind(&import.item.component)
        .bind(&import.item.rocketmq_version_range)
        .bind(&import.item.source_uri)
        .bind(&import.item.source_version)
        .bind(import.item.valid_from)
        .bind(import.item.valid_until)
        .bind(&import.item.owner)
        .bind(review_status_name(import.item.review_status))
        .bind(import.item.review_due_at)
        .bind(&import.item.sensitivity)
        .bind(&import.item.content_hash)
        .bind(import.item.conflict)
        .bind(import.item.created_at)
        .bind(import.item.updated_at)
        .execute(&mut *transaction)
        .await?;

        for chunk in &import.chunks {
            sqlx::query(
                "INSERT INTO knowledge_chunks (
                    id, knowledge_item_id, ordinal, heading, content, content_hash
                 ) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(chunk.id.as_uuid())
            .bind(import.item.id.as_uuid())
            .bind(chunk.ordinal)
            .bind(chunk.heading.as_deref())
            .bind(&chunk.content)
            .bind(&chunk.content_hash)
            .execute(&mut *transaction)
            .await?;
        }

        sqlx::query(
            "INSERT INTO knowledge_review_tasks (id, knowledge_item_id, reason, status, created_at, completed_at,
                event_kind, actor_subject, to_review_status, item_content_hash, human_confirmed, ai_generated)
             VALUES ($1,$2,'knowledge_import','completed',$3,$3,'import',$4,$5,$6,$7,$8)",
        )
        .bind(Uuid::new_v4())
        .bind(import.item.id.as_uuid())
        .bind(import.item.created_at)
        .bind(&auth.subject)
        .bind(review_status_name(import.item.review_status))
        .bind(&import.item.content_hash)
        .bind(import.human_validated)
        .bind(import.ai_generated)
        .execute(&mut *transaction)
        .await?;

        let conflict = super::source::record_source_conflict(&mut transaction, &import.item).await?;
        transaction.commit().await?;
        let mut item = import.item;
        item.conflict = conflict;
        Ok(KnowledgeImportResult {
            item,
            chunk_count: import.chunks.len(),
            deduplicated: false,
        })
    }

    pub(super) async fn knowledge_item(
        &self,
        auth: &AuthContext,
        id: KnowledgeItemId,
    ) -> Result<KnowledgeItem, ControlPlaneRequestFailure> {
        let row = sqlx::query(
            "SELECT id, tenant_id, cluster_id, title, component, rocketmq_version_range,
                    source_uri, source_version, valid_from, valid_until, owner_name,
                    review_status, review_due_at, sensitivity, content_hash, conflict,
                    created_at, updated_at
             FROM knowledge_items
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(ControlPlaneRequestFailure::not_found())?;
        let item = knowledge_item_from_row(&row)?;
        enforce_optional_cluster(auth, item.cluster_id)?;
        Ok(item)
    }

    pub(super) async fn transition_knowledge(
        &self,
        auth: &AuthContext,
        mut item: KnowledgeItem,
        status: KnowledgeReviewStatus,
        reason: &str,
        human_confirmed: bool,
    ) -> Result<KnowledgeItem, ControlPlaneRequestFailure> {
        authorize_curation(auth, item.cluster_id)?;
        let now = Utc::now();
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE knowledge_items
             SET review_status = $1, updated_at = $2
             WHERE id = $3 AND tenant_id = $4 AND review_status = $5",
        )
        .bind(review_status_name(status))
        .bind(now)
        .bind(item.id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(review_status_name(item.review_status))
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(ControlPlaneRequestFailure::conflict_code(
                "knowledge_review_state_changed",
                "knowledge review state changed concurrently",
            ));
        }
        let task_status = if status == KnowledgeReviewStatus::InReview {
            "open"
        } else {
            "completed"
        };
        sqlx::query(
            "INSERT INTO knowledge_review_tasks (
                id, knowledge_item_id, reason, status, created_at, completed_at,
                event_kind, actor_subject, from_review_status, to_review_status, item_content_hash, human_confirmed
             ) VALUES ($1, $2, $3, $4, $5, $6, 'review', $7, $8, $9, $10, $11)",
        )
        .bind(Uuid::new_v4())
        .bind(item.id.as_uuid())
        .bind(reason)
        .bind(task_status)
        .bind(now)
        .bind((task_status == "completed").then_some(now))
        .bind(&auth.subject)
        .bind(review_status_name(item.review_status))
        .bind(review_status_name(status))
        .bind(&item.content_hash)
        .bind(human_confirmed)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        item.review_status = status;
        item.updated_at = now;
        Ok(item)
    }

    pub(super) async fn record_knowledge_feedback(
        &self,
        auth: &AuthContext,
        id: KnowledgeItemId,
        request: &KnowledgeFeedbackRequest,
    ) -> Result<KnowledgeItem, ControlPlaneRequestFailure> {
        let item = self.knowledge_item(auth, id).await?;
        let mut transaction = self.pool.begin().await?;
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO knowledge_feedback (
                id, knowledge_item_id, tenant_id, cluster_id, kind, comment,
                created_by_subject, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(Uuid::new_v4())
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(item.cluster_id.map(ClusterId::as_uuid))
        .bind(feedback_kind_name(request.kind))
        .bind(request.comment.as_deref())
        .bind(&auth.subject)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO knowledge_review_tasks (
                id, knowledge_item_id, reason, status, created_at,
                event_kind, actor_subject, item_content_hash
             ) VALUES ($1, $2, $3, 'open', $4, 'feedback', $5, $6)",
        )
        .bind(Uuid::new_v4())
        .bind(id.as_uuid())
        .bind(format!("operator_feedback:{}", feedback_kind_name(request.kind)))
        .bind(now)
        .bind(&auth.subject)
        .bind(&item.content_hash)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(item)
    }
}

pub(super) fn knowledge_item_from_row(row: &PgRow) -> Result<KnowledgeItem, ControlPlaneRequestFailure> {
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
        review_status: parse_review_status(row.try_get("review_status")?)?,
        review_due_at: row.try_get("review_due_at")?,
        sensitivity: row.try_get("sensitivity")?,
        content_hash: row.try_get("content_hash")?,
        conflict: row.try_get("conflict")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) fn parse_sensitivity(
    value: &str,
) -> Result<rocketmq_sre_contracts::Sensitivity, ControlPlaneRequestFailure> {
    match value {
        "public" => Ok(rocketmq_sre_contracts::Sensitivity::Public),
        "internal" => Ok(rocketmq_sre_contracts::Sensitivity::Internal),
        "confidential" => Ok(rocketmq_sre_contracts::Sensitivity::Confidential),
        "restricted" => Ok(rocketmq_sre_contracts::Sensitivity::Restricted),
        _ => Err(ControlPlaneError::configuration("stored knowledge sensitivity is invalid").into()),
    }
}

pub(super) fn enforce_optional_cluster(
    auth: &AuthContext,
    cluster_id: Option<ClusterId>,
) -> Result<(), ControlPlaneRequestFailure> {
    if cluster_id.is_some_and(|cluster_id| !auth.clusters.contains(&cluster_id)) {
        return Err(ControlPlaneRequestFailure::forbidden(
            "cluster_not_allowed",
            "knowledge cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

pub(super) fn authorize_curation(
    auth: &AuthContext,
    cluster_id: Option<ClusterId>,
) -> Result<(), ControlPlaneRequestFailure> {
    enforce_optional_cluster(auth, cluster_id)?;
    if !auth.roles.contains("operator") {
        return Err(ControlPlaneRequestFailure::forbidden(
            "operator_required",
            "knowledge curation requires the operator role",
        ));
    }
    if cluster_id.is_none() && !auth.roles.contains("sre-admin") {
        return Err(ControlPlaneRequestFailure::forbidden(
            "tenant_curation_required",
            "tenant-wide knowledge curation requires operator and sre-admin roles",
        ));
    }
    Ok(())
}

pub(super) fn review_status_name(status: KnowledgeReviewStatus) -> &'static str {
    match status {
        KnowledgeReviewStatus::Draft => "draft",
        KnowledgeReviewStatus::InReview => "in_review",
        KnowledgeReviewStatus::Validated => "validated",
        KnowledgeReviewStatus::Deprecated => "deprecated",
        KnowledgeReviewStatus::Expired => "expired",
    }
}

pub(super) fn parse_review_status(value: &str) -> Result<KnowledgeReviewStatus, ControlPlaneRequestFailure> {
    match value {
        "draft" => Ok(KnowledgeReviewStatus::Draft),
        "in_review" => Ok(KnowledgeReviewStatus::InReview),
        "validated" => Ok(KnowledgeReviewStatus::Validated),
        "deprecated" => Ok(KnowledgeReviewStatus::Deprecated),
        "expired" => Ok(KnowledgeReviewStatus::Expired),
        _ => Err(ControlPlaneRequestFailure::validation(
            "source_unavailable",
            "stored knowledge review status is invalid",
        )),
    }
}

fn feedback_kind_name(kind: KnowledgeFeedbackKind) -> &'static str {
    match kind {
        KnowledgeFeedbackKind::Useful => "useful",
        KnowledgeFeedbackKind::Incorrect => "incorrect",
        KnowledgeFeedbackKind::Outdated => "outdated",
    }
}
