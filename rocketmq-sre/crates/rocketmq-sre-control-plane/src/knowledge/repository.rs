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
use rocketmq_sre_contracts::KnowledgeChunkId;
use rocketmq_sre_contracts::KnowledgeFeedbackKind;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::KnowledgeItemId;
use rocketmq_sre_contracts::KnowledgeReviewStatus;
use semver::Version;
use semver::VersionReq;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::model::KnowledgeChunkView;
use super::model::KnowledgeFeedbackRequest;
use super::model::KnowledgeImport;
use super::model::KnowledgeImportResult;
use super::model::KnowledgeListQuery;
use super::model::KnowledgePage;
use super::model::KnowledgeSearchPage;
use super::model::KnowledgeSearchQuery;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

impl PostgresRepository {
    pub(super) async fn list_knowledge(
        &self,
        auth: &AuthContext,
        query: &KnowledgeListQuery,
    ) -> Result<KnowledgePage, ControlPlaneError> {
        enforce_optional_cluster(auth, Some(query.cluster_id))?;
        let limit = query.bounded_limit()?;
        let cursor = query
            .cursor
            .as_deref()
            .map(|value| {
                value
                    .parse::<Uuid>()
                    .map_err(|_| ControlPlaneError::validation("invalid_request", "knowledge cursor must be a UUID"))
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
    ) -> Result<KnowledgeImportResult, ControlPlaneError> {
        enforce_optional_cluster(auth, import.item.cluster_id)?;
        let mut transaction = self.pool.begin().await?;
        let existing = sqlx::query(
            "SELECT id
             FROM knowledge_items
             WHERE tenant_id = $1 AND source_uri = $2 AND source_version = $3 AND content_hash = $4",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(&import.item.source_uri)
        .bind(&import.item.source_version)
        .bind(&import.item.content_hash)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let id = KnowledgeItemId::from_uuid(row.try_get("id")?);
            transaction.rollback().await?;
            let item = self.knowledge_item(auth, id).await?;
            return Ok(KnowledgeImportResult {
                item,
                chunk_count: import.chunks.len(),
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

        let conflicting = sqlx::query(
            "SELECT id
             FROM knowledge_items
             WHERE tenant_id = $1 AND component = $2 AND rocketmq_version_range = $3
               AND cluster_id IS NOT DISTINCT FROM $4
               AND id <> $5 AND content_hash <> $6
               AND review_status NOT IN ('deprecated', 'expired')",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(&import.item.component)
        .bind(&import.item.rocketmq_version_range)
        .bind(import.item.cluster_id.map(ClusterId::as_uuid))
        .bind(import.item.id.as_uuid())
        .bind(&import.item.content_hash)
        .fetch_all(&mut *transaction)
        .await?;
        let conflict = !conflicting.is_empty();
        if conflict {
            let ids = conflicting
                .iter()
                .map(|row| row.try_get::<Uuid, _>("id"))
                .collect::<Result<Vec<_>, _>>()?;
            sqlx::query(
                "UPDATE knowledge_items
                 SET conflict = TRUE, updated_at = $1
                 WHERE tenant_id = $2 AND (id = $3 OR id = ANY($4))",
            )
            .bind(Utc::now())
            .bind(auth.tenant_id.as_uuid())
            .bind(import.item.id.as_uuid())
            .bind(ids)
            .execute(&mut *transaction)
            .await?;
        }
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
    ) -> Result<KnowledgeItem, ControlPlaneError> {
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
        .ok_or(ControlPlaneError::NotFound)?;
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
    ) -> Result<KnowledgeItem, ControlPlaneError> {
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
            return Err(ControlPlaneError::conflict(
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
                id, knowledge_item_id, reason, status, created_at, completed_at
             ) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(Uuid::new_v4())
        .bind(item.id.as_uuid())
        .bind(format!("{}: {reason}", auth.subject))
        .bind(task_status)
        .bind(now)
        .bind((task_status == "completed").then_some(now))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        item.review_status = status;
        item.updated_at = now;
        Ok(item)
    }

    pub(super) async fn search_knowledge(
        &self,
        auth: &AuthContext,
        query: &KnowledgeSearchQuery,
    ) -> Result<KnowledgeSearchPage, ControlPlaneError> {
        enforce_optional_cluster(auth, Some(query.cluster_id))?;
        let version = Version::parse(&query.rocketmq_version)
            .map_err(|_| ControlPlaneError::validation("invalid_request", "RocketMQ version must be semantic"))?;
        let candidate_limit = i64::from(query.bounded_limit()) * 4;
        let rows = sqlx::query(
            "SELECT c.id AS chunk_id, c.heading, c.content, c.content_hash AS chunk_hash,
                    k.id AS item_id, k.title, k.component, k.rocketmq_version_range,
                    k.source_uri, k.source_version, k.sensitivity, k.review_status, k.review_due_at,
                    k.valid_from, k.valid_until, k.content_hash AS item_hash, k.conflict
             FROM knowledge_chunks c
             JOIN knowledge_items k ON k.id = c.knowledge_item_id
             WHERE k.tenant_id = $1
               AND (k.cluster_id IS NULL OR k.cluster_id = $2)
               AND ($3::TEXT IS NULL OR k.component = $3)
               AND ($4 OR k.review_status = 'validated')
               AND c.search_document @@ plainto_tsquery('simple', $5)
             ORDER BY ts_rank(c.search_document, plainto_tsquery('simple', $5)) DESC,
                      k.updated_at DESC, c.ordinal
             LIMIT $6",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(query.cluster_id.as_uuid())
        .bind(query.component.as_deref())
        .bind(query.include_unvalidated)
        .bind(query.q.trim())
        .bind(candidate_limit)
        .fetch_all(&self.pool)
        .await?;

        let now = Utc::now();
        let mut items = Vec::new();
        let mut partial = false;
        for row in rows {
            let requirement: String = row.try_get("rocketmq_version_range")?;
            let version_matches = VersionReq::parse(&requirement)
                .map(|requirement| requirement.matches(&version))
                .unwrap_or(false);
            if !version_matches {
                continue;
            }
            let review_status = parse_review_status(row.try_get("review_status")?)?;
            let review_due_at: chrono::DateTime<Utc> = row.try_get("review_due_at")?;
            let valid_from: Option<chrono::DateTime<Utc>> = row.try_get("valid_from")?;
            let valid_until: Option<chrono::DateTime<Utc>> = row.try_get("valid_until")?;
            let conflict = row.try_get("conflict")?;
            let expired = review_due_at <= now
                || valid_from.is_some_and(|from| from > now)
                || valid_until.is_some_and(|until| until <= now)
                || review_status == KnowledgeReviewStatus::Expired;
            let mut exclusion_reasons = Vec::new();
            if review_status != KnowledgeReviewStatus::Validated {
                exclusion_reasons.push("not_validated".to_owned());
            }
            if conflict {
                exclusion_reasons.push("knowledge_conflict".to_owned());
            }
            if expired {
                exclusion_reasons.push("knowledge_expired".to_owned());
            }
            let eligible_for_diagnosis = exclusion_reasons.is_empty();
            partial |= !eligible_for_diagnosis;
            items.push(KnowledgeChunkView {
                id: KnowledgeChunkId::from_uuid(row.try_get("chunk_id")?),
                knowledge_item_id: KnowledgeItemId::from_uuid(row.try_get("item_id")?),
                title: row.try_get("title")?,
                component: row.try_get("component")?,
                heading: row.try_get("heading")?,
                content: row.try_get("content")?,
                source_uri: row.try_get("source_uri")?,
                source_version: row.try_get("source_version")?,
                sensitivity: parse_sensitivity(row.try_get("sensitivity")?)?,
                item_hash: row.try_get("item_hash")?,
                chunk_hash: row.try_get("chunk_hash")?,
                review_status,
                conflict,
                expired,
                eligible_for_diagnosis,
                exclusion_reasons,
            });
            if items.len() >= query.bounded_limit() as usize {
                break;
            }
        }
        Ok(KnowledgeSearchPage { items, partial })
    }

    pub(super) async fn record_knowledge_feedback(
        &self,
        auth: &AuthContext,
        id: KnowledgeItemId,
        request: &KnowledgeFeedbackRequest,
    ) -> Result<KnowledgeItem, ControlPlaneError> {
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
                id, knowledge_item_id, reason, status, created_at
             ) VALUES ($1, $2, $3, 'open', $4)",
        )
        .bind(Uuid::new_v4())
        .bind(id.as_uuid())
        .bind(format!("operator_feedback:{}", feedback_kind_name(request.kind)))
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(item)
    }
}

fn knowledge_item_from_row(row: &PgRow) -> Result<KnowledgeItem, ControlPlaneError> {
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

fn parse_sensitivity(value: &str) -> Result<rocketmq_sre_contracts::Sensitivity, ControlPlaneError> {
    match value {
        "public" => Ok(rocketmq_sre_contracts::Sensitivity::Public),
        "internal" => Ok(rocketmq_sre_contracts::Sensitivity::Internal),
        "confidential" => Ok(rocketmq_sre_contracts::Sensitivity::Confidential),
        "restricted" => Ok(rocketmq_sre_contracts::Sensitivity::Restricted),
        _ => Err(ControlPlaneError::configuration(
            "stored knowledge sensitivity is invalid",
        )),
    }
}

fn enforce_optional_cluster(auth: &AuthContext, cluster_id: Option<ClusterId>) -> Result<(), ControlPlaneError> {
    if cluster_id.is_some_and(|cluster_id| !auth.clusters.contains(&cluster_id)) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "knowledge cluster is outside the authenticated scope",
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

fn parse_review_status(value: &str) -> Result<KnowledgeReviewStatus, ControlPlaneError> {
    match value {
        "draft" => Ok(KnowledgeReviewStatus::Draft),
        "in_review" => Ok(KnowledgeReviewStatus::InReview),
        "validated" => Ok(KnowledgeReviewStatus::Validated),
        "deprecated" => Ok(KnowledgeReviewStatus::Deprecated),
        "expired" => Ok(KnowledgeReviewStatus::Expired),
        _ => Err(ControlPlaneError::validation(
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
