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

use chrono::Utc;
use rocketmq_sre_contracts::{
    ClusterId, IncidentId, KnowledgeFeedbackKind, KnowledgeFeedbackRecord, KnowledgeHistoryPage, KnowledgeItemId,
    KnowledgePostmortemLineage, KnowledgeReviewEventKind, KnowledgeReviewRecord, PostmortemId,
};
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

use super::{
    repository::{enforce_optional_cluster, parse_review_status},
    source::source_uri_display,
};
use crate::{ControlPlaneError, ControlPlaneRequestFailure, PostgresRepository, auth::AuthContext};

const HISTORY_LIMIT: usize = 32;

impl PostgresRepository {
    pub(super) async fn knowledge_history(
        &self,
        auth: &AuthContext,
        id: KnowledgeItemId,
    ) -> Result<KnowledgeHistoryPage, ControlPlaneRequestFailure> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SET LOCAL statement_timeout = '2s'")
            .execute(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT cluster_id, content_hash,
                CASE WHEN octet_length(source_uri)<=2000 THEN source_uri ELSE '' END AS source_uri
             FROM knowledge_items WHERE id=$1 AND tenant_id=$2",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(ControlPlaneRequestFailure::not_found)?;
        let cluster_id = row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid);
        enforce_optional_cluster(auth, cluster_id)?;
        let reviews = sqlx::query(
            "SELECT id, event_kind,
                CASE WHEN octet_length(actor_subject)<=512 THEN actor_subject ELSE NULL END AS actor_subject,
                from_review_status,to_review_status,item_content_hash,human_confirmed,ai_generated,
                CASE WHEN octet_length(reason)<=2000 THEN reason ELSE left(reason,500) END AS reason,
                status,created_at,completed_at,
                (octet_length(reason)>2000 OR COALESCE(octet_length(actor_subject)>512,FALSE)) AS details_truncated
             FROM knowledge_review_tasks WHERE knowledge_item_id=$1
               AND event_kind IS DISTINCT FROM 'feedback'
             ORDER BY created_at DESC,id DESC LIMIT $2",
        )
        .bind(id.as_uuid())
        .bind((HISTORY_LIMIT + 1) as i64)
        .fetch_all(&mut *transaction)
        .await?;
        let allowed_clusters: Vec<_> = auth.clusters.iter().map(|id| id.as_uuid()).collect();
        let feedback = sqlx::query(
            "SELECT id,cluster_id,kind,
                CASE WHEN octet_length(comment)<=2000 THEN comment ELSE left(comment,500) END AS comment,
                CASE WHEN octet_length(created_by_subject)<=512 THEN created_by_subject ELSE NULL END AS actor_subject,
                created_at,(COALESCE(octet_length(comment)>2000,FALSE) OR octet_length(created_by_subject)>512) AS details_truncated
             FROM knowledge_feedback WHERE knowledge_item_id=$1 AND tenant_id=$2
               AND (cluster_id IS NULL OR cluster_id=ANY($3))
             ORDER BY created_at DESC,id DESC LIMIT $4",
        ).bind(id.as_uuid()).bind(auth.tenant_id.as_uuid()).bind(allowed_clusters).bind((HISTORY_LIMIT+1) as i64)
            .fetch_all(&mut *transaction).await?;
        let lineage = sqlx::query(
            "SELECT p.id,p.incident_id,p.current_revision,p.confirmed_at,
                CASE WHEN octet_length(p.confirmed_by)<=512 THEN p.confirmed_by ELSE NULL END AS confirmed_by
             FROM postmortems p JOIN postmortem_revisions r
               ON r.postmortem_id=p.id AND r.revision=p.current_revision AND r.human_confirmed
             WHERE p.published_knowledge_item_id=$1 AND p.tenant_id=$2
               AND p.cluster_id IS NOT DISTINCT FROM $3 AND p.status='published'
             ORDER BY p.id LIMIT 1",
        )
        .bind(id.as_uuid())
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.map(|id| id.as_uuid()))
        .fetch_optional(&mut *transaction)
        .await?;
        transaction.commit().await?;
        let postmortem = lineage
            .map(|row| -> Result<_, ControlPlaneRequestFailure> {
                Ok(KnowledgePostmortemLineage {
                    postmortem_id: PostmortemId::from_uuid(row.try_get("id")?),
                    incident_id: IncidentId::from_uuid(row.try_get("incident_id")?),
                    revision: u32::try_from(row.try_get::<i32, _>("current_revision")?)
                        .map_err(ControlPlaneError::configuration_source)?,
                    confirmed_by: row.try_get("confirmed_by")?,
                    confirmed_at: row.try_get("confirmed_at")?,
                })
            })
            .transpose()?;
        Ok(KnowledgeHistoryPage {
            knowledge_item_id: id,
            tenant_id: auth.tenant_id,
            cluster_id,
            item_content_hash: row.try_get("content_hash")?,
            source_uri_display: source_uri_display(row.try_get("source_uri")?),
            observed_at: Utc::now(),
            reviews_truncated: reviews.len() > HISTORY_LIMIT,
            feedback_truncated: feedback.len() > HISTORY_LIMIT,
            reviews: reviews
                .iter()
                .take(HISTORY_LIMIT)
                .map(review_record)
                .collect::<Result<_, _>>()?,
            feedback: feedback
                .iter()
                .take(HISTORY_LIMIT)
                .map(feedback_record)
                .collect::<Result<_, _>>()?,
            postmortem,
        })
    }
}

fn review_record(row: &PgRow) -> Result<KnowledgeReviewRecord, ControlPlaneRequestFailure> {
    let event_kind = match row.try_get::<Option<&str>, _>("event_kind")? {
        None => None,
        Some("import") => Some(KnowledgeReviewEventKind::Import),
        Some("review") => Some(KnowledgeReviewEventKind::Review),
        Some("feedback") => Some(KnowledgeReviewEventKind::Feedback),
        Some(_) => return Err(ControlPlaneError::configuration("stored knowledge event is invalid").into()),
    };
    Ok(KnowledgeReviewRecord {
        id: row.try_get("id")?,
        event_kind,
        actor_subject: row.try_get("actor_subject")?,
        from_status: row
            .try_get::<Option<&str>, _>("from_review_status")?
            .map(parse_review_status)
            .transpose()?,
        to_status: row
            .try_get::<Option<&str>, _>("to_review_status")?
            .map(parse_review_status)
            .transpose()?,
        item_content_hash: row.try_get("item_content_hash")?,
        human_confirmed: row.try_get("human_confirmed")?,
        ai_generated: row.try_get("ai_generated")?,
        reason: row.try_get("reason")?,
        task_status: row.try_get("status")?,
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
        details_truncated: row.try_get("details_truncated")?,
    })
}

fn feedback_record(row: &PgRow) -> Result<KnowledgeFeedbackRecord, ControlPlaneRequestFailure> {
    let kind = match row.try_get::<&str, _>("kind")? {
        "useful" => KnowledgeFeedbackKind::Useful,
        "incorrect" => KnowledgeFeedbackKind::Incorrect,
        "outdated" => KnowledgeFeedbackKind::Outdated,
        _ => return Err(ControlPlaneError::configuration("stored knowledge feedback kind is invalid").into()),
    };
    Ok(KnowledgeFeedbackRecord {
        id: row.try_get("id")?,
        cluster_id: row.try_get::<Option<Uuid>, _>("cluster_id")?.map(ClusterId::from_uuid),
        kind,
        comment: row.try_get("comment")?,
        actor_subject: row.try_get("actor_subject")?,
        created_at: row.try_get("created_at")?,
        details_truncated: row.try_get("details_truncated")?,
    })
}
