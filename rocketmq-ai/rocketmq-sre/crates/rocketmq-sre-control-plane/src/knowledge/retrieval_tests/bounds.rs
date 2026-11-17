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

use super::support::*;

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_candidate_exhaustion_is_distinct_from_empty_complete_search() {
    let fixture = Fixture::new().await;
    let item = fixture
        .import(fixture.document("candidate-cap", "consumer", ">=9.0.0", "# capneedle\ncapneedle"))
        .await
        .item;
    // Deliberate historical bulk fixture exceeds the normal import chunk count;
    // it tests the read bound, not a normal import or physical qualification.
    sqlx::query(
        "INSERT INTO knowledge_chunks(id,knowledge_item_id,ordinal,heading,content,content_hash)
        SELECT gen_random_uuid(),$1,n,'capneedle','capneedle',$2 FROM generate_series(1,1024) n",
    )
    .bind(item.id.as_uuid())
    .bind(&item.content_hash)
    .execute(&fixture.repository.pool)
    .await
    .unwrap();
    let page = fixture.search("capneedle", "5.3.0", 2).await;
    assert!(page.items.is_empty() && !page.partial);
    assert_eq!(page.bounds.scanned_candidates, 1024);
    assert!(page.bounds.candidate_limit_reached);
    assert!(!page.bounds.results_truncated);
    assert_eq!(page.binding.tenant_id, fixture.operator.tenant_id);
    assert_eq!(page.binding.cluster_id, fixture.cluster);
    assert_eq!(page.binding.rocketmq_version, "5.3.0");
    assert_eq!(page.binding.limit, 2);
    let empty = fixture.search("no_matching_term", "5.3.0", 2).await;
    assert!(empty.items.is_empty() && !empty.bounds.candidate_limit_reached && !empty.bounds.results_truncated);
    assert_eq!(empty.bounds.scanned_candidates, 0);
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_legacy_large_text_is_bounded_explicit_context_and_literal_punctuation_is_not_a_wildcard() {
    let fixture = Fixture::new().await;
    let item = fixture
        .import(fixture.document("legacy-large", "consumer", "*", "# largeword\nlargeword"))
        .await
        .item;
    let original_hash: String =
        sqlx::query_scalar("SELECT content_hash FROM knowledge_chunks WHERE knowledge_item_id=$1")
            .bind(item.id.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
    sqlx::query("UPDATE knowledge_items SET title=$2,source_uri=$3,source_version=$4 WHERE id=$1")
        .bind(item.id.as_uuid())
        .bind("T".repeat(4000))
        .bind("U".repeat(4000))
        .bind("V".repeat(4000))
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE knowledge_chunks SET content=$2,heading=$3 WHERE knowledge_item_id=$1")
        .bind(item.id.as_uuid())
        .bind(format!("largeword{}", "界".repeat(4000)))
        .bind(format!("largeword{}", "题".repeat(4000)))
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let page = fixture.search("largeword", "5.3.0", 2).await;
    assert_eq!(page.items.len(), 1);
    let row = &page.items[0];
    assert!(row.content_truncated && row.metadata_truncated && !row.eligible_for_diagnosis && page.partial);
    assert!(row.content.len() <= 8192 && row.title.len() <= 300 && row.heading.is_none());
    assert!(row.source_uri.is_empty() && row.source_uri_display.is_none() && row.source_version.is_empty());
    assert_eq!(row.chunk_hash, original_hash);
    fixture
        .import(fixture.document("ordinary", "ordinary", "*", "ordinary words"))
        .await;
    fixture
        .import(fixture.document("literal", "literal", "*", "literal %_\\ punctuation"))
        .await;
    let literal = fixture.search("%_\\", "5.3.0", 20).await;
    assert_eq!(literal.items.len(), 1);
    assert_eq!(literal.items[0].component, "literal");
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_history_bounds_both_streams_and_suppresses_oversized_actor_fields() {
    let fixture = Fixture::new().await;
    let item = fixture
        .import(fixture.document("history-cap", "consumer", "*", "# historycap\nhistorycap"))
        .await
        .item;
    sqlx::query(
        "INSERT INTO knowledge_review_tasks(id,knowledge_item_id,reason,status,created_at,actor_subject)
        SELECT gen_random_uuid(),$1,$2,'open',NOW(),$3 FROM generate_series(1,35)",
    )
    .bind(item.id.as_uuid())
    .bind("理".repeat(1000))
    .bind("actor".repeat(200))
    .execute(&fixture.repository.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO knowledge_feedback(id,knowledge_item_id,tenant_id,cluster_id,kind,comment,created_by_subject,created_at)
        SELECT gen_random_uuid(),$1,$2,$3,'incorrect',$4,$5,NOW() FROM generate_series(1,35)")
        .bind(item.id.as_uuid()).bind(fixture.operator.tenant_id.as_uuid()).bind(fixture.cluster.as_uuid())
        .bind("反".repeat(1000)).bind("actor".repeat(200)).execute(&fixture.repository.pool).await.unwrap();
    let page = fixture.service.history(&fixture.viewer(), item.id).await.unwrap();
    assert_eq!((page.reviews.len(), page.feedback.len()), (32, 32));
    assert!(page.reviews_truncated && page.feedback_truncated);
    assert!(
        page.reviews
            .iter()
            .all(|row| row.details_truncated && row.reason.len() <= 2000 && row.actor_subject.is_none())
    );
    assert!(page.feedback.iter().all(|row| row.details_truncated
        && row.comment.as_ref().unwrap().len() <= 2000
        && row.actor_subject.is_none()));
}
