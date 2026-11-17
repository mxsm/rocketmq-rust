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

use axum::http::StatusCode;
use rocketmq_sre_contracts::*;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    super::model::{KnowledgeFeedbackRequest, KnowledgeReviewRequest},
    support::*,
};

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_history_uses_typed_actors_and_never_infers_legacy_reviewer_or_uri_lineage() {
    let fixture = Fixture::new().await;
    let mut document = fixture.document("history", "consumer", "*", "# historyneedle\nhistoryneedle");
    document.source_uri =
        "https://declared-user:D22_PRIVATE_URI@example.invalid/source?token=D22_PRIVATE_URI#D22_PRIVATE_URI".to_owned();
    document.review_status = KnowledgeReviewStatus::Draft;
    document.human_validated = false;
    document.ai_generated = true;
    let item = fixture.import(document).await.item;
    fixture
        .service
        .feedback(
            &fixture.viewer(),
            item.id,
            &KnowledgeFeedbackRequest {
                kind: KnowledgeFeedbackKind::Incorrect,
                comment: Some("Synthetic counterexample".to_owned()),
            },
        )
        .await
        .unwrap();
    for (status, human_confirmed) in [
        (KnowledgeReviewStatus::InReview, false),
        (KnowledgeReviewStatus::Validated, true),
    ] {
        fixture
            .service
            .review(
                &fixture.operator,
                item.id,
                &KnowledgeReviewRequest {
                    status,
                    human_confirmed,
                    reason: "Source and counterexample explicitly reviewed".to_owned(),
                },
            )
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO knowledge_review_tasks(id,knowledge_item_id,reason,status,created_at) VALUES($1,$2,'pretend-reviewer: legacy opaque reason','open',NOW())")
        .bind(Uuid::new_v4()).bind(item.id.as_uuid()).execute(&fixture.repository.pool).await.unwrap();
    let page = fixture.service.history(&fixture.viewer(), item.id).await.unwrap();
    assert_eq!(page.knowledge_item_id, item.id);
    assert_eq!(page.tenant_id, fixture.operator.tenant_id);
    assert_eq!(
        page.source_uri_display.as_deref(),
        Some("https://example.invalid/source")
    );
    assert!(page.postmortem.is_none());
    let import = page
        .reviews
        .iter()
        .find(|row| row.event_kind == Some(KnowledgeReviewEventKind::Import))
        .unwrap();
    assert_eq!(import.actor_subject.as_deref(), Some(fixture.operator.subject.as_str()));
    assert_eq!(import.item_content_hash.as_deref(), Some(item.content_hash.as_str()));
    assert_eq!((import.ai_generated, import.human_confirmed), (Some(true), Some(false)));
    let validation = page
        .reviews
        .iter()
        .find(|row| {
            row.event_kind == Some(KnowledgeReviewEventKind::Review)
                && row.to_status == Some(KnowledgeReviewStatus::Validated)
        })
        .unwrap();
    assert_eq!(validation.from_status, Some(KnowledgeReviewStatus::InReview));
    assert_eq!(validation.human_confirmed, Some(true));
    let legacy = page
        .reviews
        .iter()
        .find(|row| row.reason.starts_with("pretend-reviewer:"))
        .unwrap();
    assert!(legacy.actor_subject.is_none() && legacy.event_kind.is_none() && legacy.item_content_hash.is_none());
    assert_eq!(
        page.feedback[0].actor_subject.as_deref(),
        Some(fixture.viewer().subject.as_str())
    );
    assert!(!serde_json::to_string(&page).unwrap().contains("D22_PRIVATE_URI"));
    let (_, wire) = call(
        fixture.router(),
        &fixture.viewer(),
        "GET",
        &format!("/v1/knowledge/{}/history", item.id),
        Value::Null,
    )
    .await;
    assert_eq!(wire["knowledge_item_id"], item.id.to_string());
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and signed JWT router authentication"]
async fn postgres_global_curation_requires_both_roles_but_global_read_and_feedback_remain_available() {
    let fixture = Fixture::new().await;
    let mut document = fixture.document("global-and", "consumer", "*", "# globaland\nglobaland");
    document.cluster_id = None;
    document.review_status = KnowledgeReviewStatus::InReview;
    document.human_validated = false;
    for roles in [
        vec![],
        vec!["operator"],
        vec!["sre-admin"],
        vec!["diagnose"],
        vec!["rocketmq:sre"],
        vec!["service"],
    ] {
        let actor = crate::auth::AuthContext {
            roles: roles.iter().map(|value| (*value).to_owned()).collect(),
            ..fixture.operator.clone()
        };
        let before = fixture.counts().await;
        let (status, _) = call(
            fixture.router(),
            &actor,
            "POST",
            "/v1/knowledge/import",
            serde_json::to_value(&document).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{roles:?}");
        assert_eq!(fixture.counts().await, before);
    }
    let mut curator = fixture.operator.clone();
    curator.roles.insert("sre-admin".to_owned());
    let (status, wire) = call(
        fixture.router(),
        &curator,
        "POST",
        "/v1/knowledge/import",
        serde_json::to_value(&document).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let id: KnowledgeItemId = wire["item"]["id"].as_str().unwrap().parse().unwrap();
    for roles in [vec![], vec!["operator"], vec!["sre-admin"], vec!["diagnose"]] {
        let actor = crate::auth::AuthContext {
            roles: roles.iter().map(|value| (*value).to_owned()).collect(),
            ..fixture.operator.clone()
        };
        let before = fixture.counts().await;
        let (status, _) = call(
            fixture.router(),
            &actor,
            "POST",
            &format!("/v1/knowledge/{id}/review"),
            json!({"status":"validated","human_confirmed":true,"reason":"Explicit global review"}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(fixture.counts().await, before);
        assert_eq!(
            call(
                fixture.router(),
                &actor,
                "GET",
                &format!("/v1/knowledge/{id}"),
                Value::Null
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    assert_eq!(
        call(
            fixture.router(),
            &curator,
            "POST",
            &format!("/v1/knowledge/{id}/review"),
            json!({"status":"validated","human_confirmed":true,"reason":"Explicit global review"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            fixture.router(),
            &fixture.viewer(),
            "POST",
            &format!("/v1/knowledge/{id}/feedback"),
            json!({"kind":"outdated","comment":"Global counterexample"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        fixture.service.item(&fixture.viewer(), id).await.unwrap().review_status,
        KnowledgeReviewStatus::Validated
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_global_history_filters_stored_feedback_scope_before_bounded_read() {
    let fixture = Fixture::new().await;
    let mut curator = fixture.operator.clone();
    curator.roles.insert("sre-admin".to_owned());
    let mut document = fixture.document("feedback-scope", "consumer", "*", "# globalscope\nglobalscope");
    document.cluster_id = None;
    let item = fixture.service.import(&curator, document).await.unwrap().item;
    let foreign = ClusterId::new();
    insert_cluster(&fixture.repository, fixture.operator.tenant_id, foreign).await;
    let other_tenant = TenantId::new();
    for (tenant, cluster, comment) in [
        (fixture.operator.tenant_id, Some(foreign), "D22_FOREIGN_CLUSTER"),
        (other_tenant, None, "D22_FOREIGN_TENANT"),
        (
            fixture.operator.tenant_id,
            Some(fixture.cluster),
            "Authorized scoped context",
        ),
        (fixture.operator.tenant_id, None, "Authorized global context"),
    ] {
        // Explicit legacy stored-scope fixtures. The current writer uses the item's scope.
        sqlx::query("INSERT INTO knowledge_feedback(id,knowledge_item_id,tenant_id,cluster_id,kind,comment,created_by_subject,created_at) VALUES($1,$2,$3,$4,'incorrect',$5,'legacy-reader',NOW())")
            .bind(Uuid::new_v4()).bind(item.id.as_uuid()).bind(tenant.as_uuid()).bind(cluster.map(|id|id.as_uuid())).bind(comment)
            .execute(&fixture.repository.pool).await.unwrap();
    }
    // A typed feedback task has no cluster column. Its separate feedback row is
    // the scope authority, so the task must not enter the unscoped review stream.
    sqlx::query("INSERT INTO knowledge_review_tasks(id,knowledge_item_id,reason,status,created_at,event_kind,actor_subject,item_content_hash)
        SELECT gen_random_uuid(),$1,'operator_feedback:incorrect','open',NOW(),'feedback','D22_FOREIGN_CLUSTER_ACTOR',$2
        FROM generate_series(1,35)")
        .bind(item.id.as_uuid()).bind(&item.content_hash).execute(&fixture.repository.pool).await.unwrap();
    sqlx::query("UPDATE knowledge_feedback SET created_by_subject='D22_FOREIGN_CLUSTER_ACTOR' WHERE knowledge_item_id=$1 AND cluster_id=$2")
        .bind(item.id.as_uuid()).bind(foreign.as_uuid()).execute(&fixture.repository.pool).await.unwrap();
    let page = fixture.service.history(&fixture.viewer(), item.id).await.unwrap();
    assert_eq!(page.feedback.len(), 2);
    assert_eq!(page.reviews.len(), 1);
    assert_eq!(page.reviews[0].event_kind, Some(KnowledgeReviewEventKind::Import));
    assert!(!page.reviews_truncated);
    assert!(!serde_json::to_string(&page).unwrap().contains("D22_FOREIGN"));
    let authorized_b = crate::auth::AuthContext {
        clusters: std::collections::BTreeSet::from([foreign]),
        ..fixture.viewer()
    };
    let b_page = fixture.service.history(&authorized_b, item.id).await.unwrap();
    assert_eq!(b_page.feedback.len(), 2);
    assert!(b_page.feedback.iter().any(
        |row| row.cluster_id == Some(foreign) && row.actor_subject.as_deref() == Some("D22_FOREIGN_CLUSTER_ACTOR")
    ));
    assert!(
        b_page
            .reviews
            .iter()
            .all(|row| row.event_kind != Some(KnowledgeReviewEventKind::Feedback))
    );
    let other = crate::auth::AuthContext {
        tenant_id: other_tenant,
        ..fixture.viewer()
    };
    assert!(fixture.service.history(&other, item.id).await.is_err());
}
