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

use rocketmq_sre_contracts::{ClusterId, TenantId};
use serde_json::json;

use super::support::*;

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_retrieval_enforces_tenant_cluster_and_tenant_wide_scope() {
    let fixture = Fixture::new().await;
    let own = fixture
        .import(fixture.document("own", "own", "*", "# scopeneedle\nscopeneedle"))
        .await;
    let mut global = fixture.document("global", "global", "*", "# scopeneedle\nscopeneedle");
    global.cluster_id = None;
    let mut tenant_curator = fixture.operator.clone();
    tenant_curator.roles.insert("sre-admin".to_owned());
    let global = fixture.service.import(&tenant_curator, global).await.unwrap();
    let other = ClusterId::new();
    insert_cluster(&fixture.repository, fixture.operator.tenant_id, other).await;
    let mut broader = fixture.operator.clone();
    broader.clusters.insert(other);
    let mut document = fixture.document("other-cluster", "other-cluster", "*", "# scopeneedle\nscopeneedle");
    document.cluster_id = Some(other);
    fixture.service.import(&broader, document).await.unwrap();
    let other_tenant = Fixture::new().await;
    other_tenant
        .import(other_tenant.document("other-tenant", "other-tenant", "*", "# scopeneedle\nscopeneedle"))
        .await;
    let page = fixture.search("scopeneedle", "5.3.0", 50).await;
    assert_eq!(page.items.len(), 2);
    assert!(
        page.items
            .iter()
            .all(|row| [own.item.id, global.item.id].contains(&row.knowledge_item_id))
    );
    let mut forbidden = fixture.query("scopeneedle", "5.3.0", 50);
    forbidden.cluster_id = other;
    assert_eq!(
        fixture
            .service
            .search(&fixture.operator, &forbidden)
            .await
            .unwrap_err()
            .code(),
        "cluster_not_allowed"
    );
    let wrong_tenant = crate::auth::AuthContext {
        tenant_id: TenantId::new(),
        ..fixture.operator.clone()
    };
    assert!(
        fixture
            .service
            .search(&wrong_tenant, &fixture.query("scopeneedle", "5.3.0", 50))
            .await
            .unwrap()
            .items
            .is_empty()
    );
    measured(
        "scope",
        json!({"authorized_items":page.items.len(),"foreign_items":0,"tenant_wide_included":true}),
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_distinct_source_conflict_policy_is_measured_explicitly() {
    let fixture = Fixture::new().await;
    let first = fixture
        .import(fixture.document(
            "independent-source-a",
            "consumer",
            "*",
            "# multisourceneedle\nCheck arrival rate.",
        ))
        .await;
    let second = fixture
        .import(fixture.document(
            "independent-source-b",
            "consumer",
            "*",
            "# multisourceneedle\nCheck processing rate.",
        ))
        .await;
    let first = fixture.service.item(&fixture.operator, first.item.id).await.unwrap();
    let second = fixture.service.item(&fixture.operator, second.item.id).await.unwrap();
    let page = fixture.search("multisourceneedle", "5.3.0", 20).await;
    measured(
        "distinct_sources",
        json!({"first_conflict":first.conflict,"second_conflict":second.conflict,"eligible_chunks":page.items.iter().filter(|row|row.eligible_for_diagnosis).count(),"partial":page.partial}),
    );
    assert_ne!(first.id, second.id);
    assert_ne!(first.source_uri, second.source_uri);
    assert_eq!(page.items.len(), 2);
    assert!(!first.conflict && !second.conflict);
    assert!(page.items.iter().all(|row| row.eligible_for_diagnosis));
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_deduplication_never_silently_changes_the_requested_cluster() {
    let fixture = Fixture::new().await;
    let document = fixture.document("same-source", "consumer", "*", "# dedupneedle\ndedupneedle");
    let first = fixture.import(document.clone()).await;
    let repeat = fixture.import(document.clone()).await;
    assert!(repeat.deduplicated);
    assert_eq!(first.item.id, repeat.item.id);
    let other = ClusterId::new();
    insert_cluster(&fixture.repository, fixture.operator.tenant_id, other).await;
    let mut actor = fixture.operator.clone();
    actor.clusters.insert(other);
    let mut second_scope = document;
    second_scope.cluster_id = Some(other);
    let result = fixture.service.import(&actor, second_scope.clone()).await.unwrap();
    assert_eq!(result.item.cluster_id, Some(other));
    assert_ne!(result.item.id, first.item.id);
    assert!(!result.deduplicated);
    let repeat_b = fixture.service.import(&actor, second_scope).await.unwrap();
    assert!(repeat_b.deduplicated);
    assert_eq!(repeat_b.item.id, result.item.id);
    measured(
        "dedup_scope",
        json!({"same_scope_deduplicated":true,"different_scope_rejected":false,"returned_requested_scope":true}),
    );
}
