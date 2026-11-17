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

use chrono::{DateTime, Utc};
use rocketmq_sre_contracts::{KnowledgeSearchBinding, KnowledgeSearchBounds};
use serde::Serialize;

use super::*;

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum KnowledgeRetrievalMetadata {
    Observed {
        observed_at: DateTime<Utc>,
        binding: KnowledgeSearchBinding,
        bounds: KnowledgeSearchBounds,
    },
    Unavailable {
        reason: &'static str,
    },
}

impl ModelGatewayService {
    pub(super) async fn validated_knowledge(
        &self,
        auth: &AuthContext,
        cluster_id: rocketmq_sre_contracts::ClusterId,
        incident_title: &str,
        pack_id: &str,
    ) -> (Vec<Value>, DataClass, KnowledgeRetrievalMetadata) {
        let cluster = match self.repository.get(cluster_id).await {
            Ok(cluster) if cluster.tenant_id == auth.tenant_id.to_string() => cluster,
            Ok(_) | Err(_) => {
                return (
                    Vec::new(),
                    DataClass::Public,
                    KnowledgeRetrievalMetadata::Unavailable {
                        reason: "knowledge_context_unavailable",
                    },
                );
            }
        };
        let query = KnowledgeSearchQuery {
            q: knowledge_query(pack_id, incident_title),
            cluster_id,
            component: pack_component(pack_id).map(ToOwned::to_owned),
            rocketmq_version: cluster.rocketmq_version,
            limit: Some(MAX_KNOWLEDGE_CHUNKS),
            include_unvalidated: false,
        };
        let page = match self.knowledge.search(auth, &query).await {
            Ok(page) => page,
            Err(_) => {
                return (
                    Vec::new(),
                    DataClass::Public,
                    KnowledgeRetrievalMetadata::Unavailable {
                        reason: "knowledge_search_unavailable",
                    },
                );
            }
        };
        let metadata = KnowledgeRetrievalMetadata::Observed {
            observed_at: page.observed_at,
            binding: page.binding,
            bounds: page.bounds,
        };
        let mut data_class = DataClass::Public;
        let items=page.items.into_iter().filter(|item|item.eligible_for_diagnosis).take(MAX_KNOWLEDGE_CHUNKS as usize).map(|item| {
            data_class=data_class.max(data_class_for_sensitivity(item.sensitivity));
            json!({"knowledge_item_id":item.knowledge_item_id,"chunk_id":item.id,"title":bounded_text(&item.title,300),
                "heading":item.heading.as_deref().map(|value|bounded_text(value,300)),"source_version":bounded_text(&item.source_version,200),
                "content":bounded_text(&item.content,MAX_KNOWLEDGE_CHARS),"content_hash":item.chunk_hash})
        }).collect();
        (items, data_class, metadata)
    }
}

fn knowledge_query(pack_id: &str, title: &str) -> String {
    let mut query = format!("{} {}", pack_id.replace(['.', '-'], " "), bounded_text(title, 200));
    if query.len() > 500 {
        let mut end = 500;
        while !query.is_char_boundary(end) {
            end -= 1;
        }
        query.truncate(end);
    }
    query
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_knowledge_query_preserves_utf8_at_the_public_byte_boundary() {
        let query = knowledge_query("consumer.lag", &"消费延迟排查".repeat(100));
        assert!(query.len() <= 500);
        assert!(query.starts_with("consumer lag "));
        assert!(query.ends_with(['消', '费', '延', '迟', '排', '查']));
    }

    #[tokio::test]
    #[ignore = "requires explicit task-owned PostgreSQL"]
    async fn postgres_model_knowledge_preserves_observed_empty_eligible_only_and_candidate_bounds() {
        use crate::knowledge::retrieval_tests::support::Fixture;
        let fixture = Fixture::new().await;
        let service = ModelGatewayService::disabled(fixture.repository.clone());
        let (empty, class, metadata) = service
            .validated_knowledge(&fixture.operator, fixture.cluster, "needle", "consumer-lag")
            .await;
        assert!(empty.is_empty());
        assert_eq!(class, DataClass::Public);
        let json = serde_json::to_value(metadata).unwrap();
        assert_eq!(json["state"], "observed");
        assert_eq!(json["bounds"]["scanned_candidates"], 0);
        assert_eq!(json["bounds"]["candidate_limit_reached"], false);
        let mut valid = fixture.document(
            "model-valid",
            "consumer",
            "*",
            "# consumer lag needle\nApplicable reviewed knowledge",
        );
        valid.sensitivity = rocketmq_sre_contracts::Sensitivity::Restricted;
        let item = fixture.import(valid.clone()).await.item;
        let mut draft = fixture.document(
            "model-draft",
            "consumer",
            "*",
            "# consumer lag needle\nUnreviewed counterexample",
        );
        draft.review_status = rocketmq_sre_contracts::KnowledgeReviewStatus::Draft;
        draft.human_validated = false;
        fixture.import(draft).await;
        let (items, class, metadata) = service
            .validated_knowledge(&fixture.operator, fixture.cluster, "needle", "consumer-lag")
            .await;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["knowledge_item_id"], item.id.to_string());
        assert_eq!(class, DataClass::Restricted);
        assert!(matches!(metadata, KnowledgeRetrievalMetadata::Observed { .. }));
        sqlx::query("UPDATE knowledge_items SET rocketmq_version_range='>=9.0.0' WHERE id=$1")
            .bind(item.id.as_uuid())
            .execute(&fixture.repository.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO knowledge_chunks(id,knowledge_item_id,ordinal,heading,content,content_hash)
            SELECT gen_random_uuid(),$1,n,'consumer lag needle','consumer lag needle',$2 FROM generate_series(1,1024) n")
            .bind(item.id.as_uuid()).bind(&item.content_hash).execute(&fixture.repository.pool).await.unwrap();
        let (items, _, metadata) = service
            .validated_knowledge(&fixture.operator, fixture.cluster, "needle", "consumer-lag")
            .await;
        assert!(items.is_empty());
        let json = serde_json::to_value(metadata).unwrap();
        assert_eq!(json["state"], "observed");
        assert_eq!(json["bounds"]["candidate_limit_reached"], true);
        assert_eq!(json["binding"]["cluster_id"], fixture.cluster.to_string());
        assert!(json["observed_at"].is_string());
    }

    #[tokio::test]
    #[ignore = "requires explicit task-owned PostgreSQL"]
    async fn postgres_model_knowledge_timeout_is_unavailable_instead_of_complete_empty() {
        use crate::knowledge::retrieval_tests::support::Fixture;
        let fixture = Fixture::new().await;
        let service = ModelGatewayService::disabled(fixture.repository.clone());
        let mut gate = fixture.repository.pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE knowledge_chunks IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *gate)
            .await
            .unwrap();
        let (items, class, metadata) = service
            .validated_knowledge(&fixture.operator, fixture.cluster, "needle", "consumer-lag")
            .await;
        gate.rollback().await.unwrap();
        assert!(items.is_empty());
        assert_eq!(class, DataClass::Public);
        let json = serde_json::to_value(metadata).unwrap();
        assert_eq!(
            json,
            json!({"state":"unavailable","reason":"knowledge_search_unavailable"})
        );
    }
}
