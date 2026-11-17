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

use serde_json::json;

use super::support::*;

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_bilingual_lexical_baseline() {
    let fixture = Fixture::new().await;
    let cases = [
        (
            "english",
            "consumer lag",
            "# Consumer lag\nCheck consumer lag and processing rate.",
        ),
        ("chinese_exact", "消费延迟", "# 消费延迟\n检查处理速率。"),
        (
            "chinese_natural",
            "消费积压",
            "# 排查流程\n生产环境发生消费积压时先核对处理速率与到达速率。",
        ),
        ("mixed", "Broker 消费延迟", "# Broker 消费延迟\n检查消费位点和offset。"),
    ];
    let mut missing = Vec::new();
    for (label, query, markdown) in cases {
        let document = fixture
            .import(fixture.document(label, label, ">=5.0.0, <6.0.0", markdown))
            .await;
        let page = fixture.search(query, "5.3.0", 20).await;
        let found = page
            .items
            .iter()
            .any(|chunk| chunk.knowledge_item_id == document.item.id && chunk.eligible_for_diagnosis);
        let tokens: String = sqlx::query_scalar("SELECT to_tsvector('simple',$1)::TEXT")
            .bind(markdown)
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
        measured(
            label,
            json!({"expected_document_found":found,"returned":page.items.len(),"partial":page.partial,"pg_token_count":tokens.split_whitespace().count()}),
        );
        if !found {
            missing.push(label);
        }
    }
    assert!(
        missing.is_empty(),
        "relevant authored corpus cases missing: {missing:?}"
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_applicable_version_is_not_starved_by_higher_ranked_wrong_versions() {
    let fixture = Fixture::new().await;
    let valid = fixture
        .import(fixture.document(
            "applicable",
            "consumer",
            ">=5.0.0, <6.0.0",
            "# Reference\nretrievalneedle guidance",
        ))
        .await;
    for index in 0..9 {
        fixture
            .import(fixture.document(
                &format!("wrong-{index}"),
                "consumer",
                ">=4.0.0, <5.0.0",
                "# retrievalneedle\nretrievalneedle",
            ))
            .await;
    }
    let page = fixture.search("retrievalneedle", "5.3.0", 2).await;
    let found = page
        .items
        .iter()
        .any(|chunk| chunk.knowledge_item_id == valid.item.id && chunk.eligible_for_diagnosis);
    measured(
        "version_starvation",
        json!({"requested_limit":2,"seeded_wrong_version_chunks":9,"expected_document_found":found,"returned":page.items.len(),"partial":page.partial}),
    );
    assert!(
        found,
        "a relevant applicable chunk behind the pre-version-filter cap must remain discoverable"
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_semantic_version_and_component_boundaries() {
    let fixture = Fixture::new().await;
    let cases = [
        ("exact", "=5.3.0", true),
        ("range", ">=5.0.0, <6.0.0", true),
        ("old", "<5.0.0", false),
        ("prerelease", "=5.3.0-alpha.1", false),
    ];
    let mut expected = Vec::new();
    for (label, requirement, matches) in cases {
        let item = fixture
            .import(fixture.document(label, label, requirement, "# versionneedle\nversionneedle"))
            .await
            .item;
        if matches {
            expected.push(item.id);
        }
    }
    let page = fixture.search("versionneedle", "5.3.0", 20).await;
    assert_eq!(page.items.len(), expected.len());
    assert!(page.items.iter().all(|row| expected.contains(&row.knowledge_item_id)));
    let mut query = fixture.query("versionneedle", "5.3.0", 20);
    query.component = Some("exact".to_owned());
    assert_eq!(
        fixture
            .service
            .search(&fixture.operator, &query)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    query.rocketmq_version = "not-a-version".to_owned();
    assert_eq!(
        fixture
            .service
            .search(&fixture.operator, &query)
            .await
            .unwrap_err()
            .code(),
        "invalid_request"
    );
    measured(
        "version_and_component",
        json!({"matched":page.items.len(),"invalid_version_rejected":true}),
    );
}
