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

use std::time::Duration;

use serde_json::json;

use super::support::*;

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_dedup_returns_stored_layout_and_never_promotes_existing_declarations() {
    let fixture = Fixture::new().await;
    let mut document = fixture.document("legacy-layout", "consumer", "*", "# Original\nOriginal content");
    document.review_status = rocketmq_sre_contracts::KnowledgeReviewStatus::Draft;
    document.human_validated = false;
    let first = fixture.import(document.clone()).await;
    sqlx::query("INSERT INTO knowledge_chunks(id,knowledge_item_id,ordinal,heading,content,content_hash)
        SELECT gen_random_uuid(),knowledge_item_id,1,'Historical layout',content,content_hash FROM knowledge_chunks WHERE knowledge_item_id=$1")
        .bind(first.item.id.as_uuid()).execute(&fixture.repository.pool).await.unwrap();
    document.review_status = rocketmq_sre_contracts::KnowledgeReviewStatus::Validated;
    document.human_validated = true;
    document.owner = "Different declaration".to_owned();
    let before = fixture.counts().await;
    let replay = fixture.import(document).await;
    assert!(replay.deduplicated);
    assert_eq!(replay.item.id, first.item.id);
    assert_eq!(replay.item.review_status, first.item.review_status);
    assert_eq!(replay.item.owner, first.item.owner);
    assert_eq!(replay.chunk_count, 2);
    assert_eq!(fixture.counts().await, before);
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_concurrent_identical_imports_reuse_one_item_without_storage_errors() {
    let fixture = Fixture::new().await;
    let document = fixture.document(
        "concurrent-source",
        "consumer",
        "*",
        "# concurrentneedle\nOriginal content",
    );
    let mut table_gate = fixture.repository.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE knowledge_items IN SHARE MODE")
        .execute(&mut *table_gate)
        .await
        .unwrap();

    // Both calls must reach the database before releasing the gate. On the old
    // implementation both SELECTs miss and their INSERTs race the unique key.
    // The source lock implementation instead blocks the second at its lock.
    let release = async {
        let waiting = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(DISTINCT a.pid) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
                     WHERE a.datname=current_database() AND NOT l.granted
                       AND (a.query LIKE '%INSERT INTO knowledge_items%'
                         OR a.query LIKE '%SELECT pg_advisory_xact_lock%')",
                )
                .fetch_one(&fixture.repository.pool)
                .await
                .unwrap();
                if count >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        table_gate.rollback().await.unwrap();
        assert!(
            waiting.is_ok(),
            "both import operations must reach the synchronized database gate"
        );
    };
    let (first, second, ()) = tokio::join!(
        fixture.service.import(&fixture.operator, document.clone()),
        fixture.service.import(&fixture.operator, document),
        release,
    );
    measured(
        "concurrent_identical_import",
        json!({"first_ok":first.is_ok(),"second_ok":second.is_ok(),"items":fixture.counts().await.0}),
    );
    let first = first.expect("first concurrent import must not fail with a unique violation");
    let second = second.expect("second concurrent import must reuse the item without a storage error");
    assert_eq!(first.item.id, second.item.id);
    assert_ne!(first.deduplicated, second.deduplicated);
    let counts = fixture.counts().await;
    assert_eq!((counts.0, counts.1), (1, 1));
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_concurrent_same_source_different_content_records_both_conflicts() {
    let fixture = Fixture::new().await;
    let first = fixture.document(
        "same-origin",
        "consumer",
        "*",
        "# sourceconflict\nFirst source snapshot",
    );
    let second = super::super::model::ImportKnowledgeRequest {
        markdown: "# sourceconflict\nDifferent source snapshot".to_owned(),
        ..first.clone()
    };
    let (first, second) = tokio::join!(
        fixture.service.import(&fixture.operator, first),
        fixture.service.import(&fixture.operator, second)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(first.item.id, second.item.id);
    assert!(
        fixture
            .service
            .item(&fixture.operator, first.item.id)
            .await
            .unwrap()
            .conflict
    );
    assert!(
        fixture
            .service
            .item(&fixture.operator, second.item.id)
            .await
            .unwrap()
            .conflict
    );
}
