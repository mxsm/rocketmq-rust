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
    KnowledgeChunkId, KnowledgeChunkView, KnowledgeItemId, KnowledgeReviewStatus, KnowledgeSearchBinding,
    KnowledgeSearchBounds, KnowledgeSearchPage, KnowledgeValidity,
};
use semver::{Version, VersionReq};
use sqlx::Row;

use super::{
    model::KnowledgeSearchQuery,
    repository::{enforce_optional_cluster, parse_review_status, parse_sensitivity},
    source::source_uri_display,
};
use crate::{ControlPlaneRequestFailure, PostgresRepository, auth::AuthContext};

const CANDIDATE_LIMIT: usize = 1024;

impl PostgresRepository {
    pub(super) async fn search_knowledge(
        &self,
        auth: &AuthContext,
        query: &KnowledgeSearchQuery,
    ) -> Result<KnowledgeSearchPage, ControlPlaneRequestFailure> {
        enforce_optional_cluster(auth, Some(query.cluster_id))?;
        let version = Version::parse(&query.rocketmq_version)
            .map_err(|_| ControlPlaneRequestFailure::validation("invalid_request", "RocketMQ version is invalid"))?;
        let mut transaction = self.pool.begin().await?;
        // Limits wall time of the SQL search, not the physical number of rows PG
        // visits. A timeout is an unavailable error, never an empty result.
        sqlx::query("SET LOCAL statement_timeout = '2s'")
            .execute(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            "SELECT c.id AS chunk_id,
                CASE WHEN octet_length(c.heading)<=1024 THEN c.heading ELSE NULL END AS heading,
                CASE WHEN octet_length(c.content)<=8192 THEN c.content ELSE left(c.content,2048) END AS content,
                (octet_length(c.content)>8192 OR COALESCE(octet_length(c.heading)>1024,FALSE)) AS content_truncated,
                c.content_hash AS chunk_hash, k.id AS item_id,
                CASE WHEN octet_length(k.title)<=300 THEN k.title ELSE left(k.title,75) END AS title,
                CASE WHEN octet_length(k.component)<=100 THEN k.component ELSE '' END AS component,
                CASE WHEN octet_length(k.rocketmq_version_range)<=512 THEN k.rocketmq_version_range ELSE '' END AS rocketmq_version_range,
                CASE WHEN octet_length(k.source_uri)<=2000 THEN k.source_uri ELSE '' END AS source_uri,
                CASE WHEN octet_length(k.source_version)<=200 THEN k.source_version ELSE '' END AS source_version,
                (octet_length(k.title)>300 OR octet_length(k.component)>100 OR octet_length(k.rocketmq_version_range)>512
                  OR octet_length(k.source_uri)>2000 OR octet_length(k.source_version)>200) AS metadata_truncated,
                k.sensitivity, k.review_status, k.review_due_at, k.valid_from, k.valid_until,
                k.content_hash AS item_hash, k.conflict
             FROM knowledge_chunks c JOIN knowledge_items k ON k.id=c.knowledge_item_id
             WHERE k.tenant_id=$1 AND (k.cluster_id IS NULL OR k.cluster_id=$2)
               AND ($3::TEXT IS NULL OR k.component=$3) AND ($4 OR k.review_status='validated')
               AND (c.search_document @@ plainto_tsquery('simple',$5)
                    OR strpos(lower(COALESCE(c.heading,'') || E'\\n' || c.content),lower($5))>0)
             ORDER BY ts_rank(c.search_document,plainto_tsquery('simple',$5)) DESC,
                      k.updated_at DESC,c.ordinal,c.id
             LIMIT $6",
        ).bind(auth.tenant_id.as_uuid()).bind(query.cluster_id.as_uuid()).bind(query.component.as_deref())
            .bind(query.include_unvalidated).bind(query.q.trim()).bind((CANDIDATE_LIMIT + 1) as i64)
            .fetch_all(&mut *transaction).await?;
        transaction.commit().await?;
        let observed_at = Utc::now();
        let bounds = KnowledgeSearchBounds {
            candidate_limit: CANDIDATE_LIMIT as u32,
            scanned_candidates: rows.len().min(CANDIDATE_LIMIT) as u32,
            candidate_limit_reached: rows.len() > CANDIDATE_LIMIT,
            results_truncated: false,
        };
        let mut items = Vec::new();
        for row in rows.into_iter().take(CANDIDATE_LIMIT) {
            let requirement: String = row.try_get("rocketmq_version_range")?;
            if !VersionReq::parse(&requirement).is_ok_and(|requirement| requirement.matches(&version)) {
                continue;
            }
            let review_status = parse_review_status(row.try_get("review_status")?)?;
            let review_due_at = row.try_get("review_due_at")?;
            let valid_from: Option<chrono::DateTime<Utc>> = row.try_get("valid_from")?;
            let valid_until: Option<chrono::DateTime<Utc>> = row.try_get("valid_until")?;
            let conflict: bool = row.try_get("conflict")?;
            let content_truncated: bool = row.try_get("content_truncated")?;
            let metadata_truncated: bool = row.try_get("metadata_truncated")?;
            let mut validity = Vec::new();
            if review_due_at <= observed_at {
                validity.push(KnowledgeValidity::ReviewOverdue);
            }
            if valid_from.is_some_and(|from| from > observed_at) {
                validity.push(KnowledgeValidity::NotYetValid);
            }
            if valid_until.is_some_and(|until| until <= observed_at) {
                validity.push(KnowledgeValidity::ValidityEnded);
            }
            if review_status == KnowledgeReviewStatus::Expired {
                validity.push(KnowledgeValidity::StatusExpired);
            }
            let expired = !validity.is_empty();
            if validity.is_empty() {
                validity.push(KnowledgeValidity::Current);
            }
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
            if content_truncated {
                exclusion_reasons.push("knowledge_content_truncated".to_owned());
            }
            if metadata_truncated {
                exclusion_reasons.push("knowledge_metadata_truncated".to_owned());
            }
            let source_uri: String = row.try_get("source_uri")?;
            items.push(KnowledgeChunkView {
                id: KnowledgeChunkId::from_uuid(row.try_get("chunk_id")?),
                knowledge_item_id: KnowledgeItemId::from_uuid(row.try_get("item_id")?),
                title: row.try_get("title")?,
                component: row.try_get("component")?,
                heading: row.try_get("heading")?,
                content: row.try_get("content")?,
                source_uri_display: source_uri_display(&source_uri),
                source_uri,
                source_version: row.try_get("source_version")?,
                rocketmq_version_range: requirement,
                valid_from,
                valid_until,
                review_due_at,
                validity,
                sensitivity: parse_sensitivity(row.try_get("sensitivity")?)?,
                item_hash: row.try_get("item_hash")?,
                chunk_hash: row.try_get("chunk_hash")?,
                review_status,
                conflict,
                expired,
                content_truncated,
                metadata_truncated,
                eligible_for_diagnosis: exclusion_reasons.is_empty(),
                exclusion_reasons,
            });
        }
        // Stable partition preserves lexical rank inside each eligibility class.
        items.sort_by_key(|item| !item.eligible_for_diagnosis);
        let results_truncated = items.len() > query.bounded_limit() as usize;
        items.truncate(query.bounded_limit() as usize);
        let partial = items.iter().any(|item| !item.eligible_for_diagnosis);
        Ok(KnowledgeSearchPage {
            items,
            partial,
            observed_at,
            bounds: KnowledgeSearchBounds {
                results_truncated,
                ..bounds
            },
            binding: KnowledgeSearchBinding {
                tenant_id: auth.tenant_id,
                cluster_id: query.cluster_id,
                q: query.q.trim().to_owned(),
                component: query.component.clone(),
                rocketmq_version: version.to_string(),
                limit: query.bounded_limit(),
                include_unvalidated: query.include_unvalidated,
            },
        })
    }
}
