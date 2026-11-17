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
use chrono::{Duration, Utc};
use rocketmq_sre_contracts::KnowledgeReviewStatus;
use serde_json::{Value, json};

use super::support::*;

fn import_body(fixture: &Fixture, label: &str, status: &str) -> Value {
    json!({"cluster_id":fixture.cluster,"title":"Synthetic reviewed retrieval","component":"consumer",
        "rocketmq_version_range":">=5.0.0, <6.0.0","source_uri":format!("https://d22-fixture.invalid/{}/{label}",fixture.operator.tenant_id),
        "source_version":"synthetic-corpus-v1","owner":"declared-owner","review_due_at":Utc::now()+Duration::days(7),
        "sensitivity":"internal","review_status":status,"human_validated":true,"ai_generated":false,
        "markdown":"# httpneedle\nhttpneedle"})
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and actual signed JWT router authentication"]
async fn postgres_http_read_only_actor_cannot_import_validated_knowledge() {
    let fixture = Fixture::new().await;
    let before = fixture.counts().await;
    let (status, _) = call(
        fixture.router(),
        &fixture.viewer(),
        "POST",
        "/v1/knowledge/import",
        import_body(&fixture, "viewer", "validated"),
    )
    .await;
    let after = fixture.counts().await;
    measured(
        "viewer_import",
        json!({"status":status.as_u16(),"rows_before":before,"rows_after":after}),
    );
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        after, before,
        "rejected imports may not create items/chunks/review tasks/feedback"
    );
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and actual signed JWT router authentication"]
async fn postgres_http_read_only_actor_cannot_review_or_validate_knowledge() {
    let mut failures = Vec::new();
    for (initial, next) in [
        (KnowledgeReviewStatus::Draft, "in_review"),
        (KnowledgeReviewStatus::InReview, "validated"),
    ] {
        let fixture = Fixture::new().await;
        let mut document = fixture.document("viewer-review", "consumer", "*", "# reviewneedle\nreviewneedle");
        document.review_status = initial;
        document.human_validated = false;
        let item = fixture.import(document).await.item;
        let before = fixture.counts().await;
        let (status, _) = call(
            fixture.router(),
            &fixture.viewer(),
            "POST",
            &format!("/v1/knowledge/{}/review", item.id),
            json!({"status":next,"human_confirmed":true,"reason":"synthetic explicit review attempt"}),
        )
        .await;
        let after = fixture.counts().await;
        let stored = fixture.service.item(&fixture.operator, item.id).await.unwrap();
        measured(
            &format!("viewer_review_{next}"),
            json!({"status":status.as_u16(),"rows_before":before,"rows_after":after,"original_status_unchanged":stored.review_status==initial}),
        );
        if status != StatusCode::FORBIDDEN || before != after || stored.review_status != initial {
            failures.push(next);
        }
    }
    assert!(
        failures.is_empty(),
        "read-only actor changed knowledge review state: {failures:?}"
    );
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and actual signed JWT router authentication"]
async fn postgres_http_operator_import_reader_discovery_and_non_authoritative_feedback() {
    let fixture = Fixture::new().await;
    let (status, created) = call(
        fixture.router(),
        &fixture.operator,
        "POST",
        "/v1/knowledge/import",
        import_body(&fixture, "operator", "draft"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = created["item"]["id"].as_str().unwrap();
    let path = format!(
        "/v1/knowledge/search?cluster_id={}&q=httpneedle&rocketmq_version=5.3.0&include_unvalidated=true",
        fixture.cluster
    );
    let (read_status, page) = call(fixture.router(), &fixture.viewer(), "GET", &path, Value::Null).await;
    assert_eq!(read_status, StatusCode::OK);
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["eligible_for_diagnosis"], false);
    let (feedback_status, item) = call(
        fixture.router(),
        &fixture.viewer(),
        "POST",
        &format!("/v1/knowledge/{id}/feedback"),
        json!({"kind":"incorrect","comment":"Synthetic counterexample: applicability needs human review."}),
    )
    .await;
    assert_eq!(feedback_status, StatusCode::OK);
    assert_eq!(item["review_status"], "draft");
    let counts = fixture.counts().await;
    assert_eq!(counts, (1, 1, 2, 1));
    measured(
        "reader_feedback_control",
        json!({"operator_import":status.as_u16(),"reader_read":read_status.as_u16(),"reader_feedback":feedback_status.as_u16(),"still_draft":true,"rows":counts}),
    );
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and actual signed JWT router authentication"]
async fn postgres_http_cluster_operator_cannot_import_tenant_wide_knowledge() {
    let fixture = Fixture::new().await;
    let mut request = import_body(&fixture, "global-import", "validated");
    request["cluster_id"] = Value::Null;
    let before = fixture.counts().await;
    let (status, _) = call(
        fixture.router(),
        &fixture.operator,
        "POST",
        "/v1/knowledge/import",
        request,
    )
    .await;
    let after = fixture.counts().await;
    measured(
        "narrow_operator_global_import",
        json!({"status":status.as_u16(),"rows_before":before,"rows_after":after}),
    );
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(before, after);
}

#[tokio::test]
#[ignore = "requires task-owned PostgreSQL and actual signed JWT router authentication"]
async fn postgres_http_cluster_operator_cannot_review_tenant_wide_knowledge() {
    let fixture = Fixture::new().await;
    let mut curator = fixture.operator.clone();
    curator.roles.insert("sre-admin".to_owned());
    let mut document = fixture.document("global-review", "global-review", "*", "# globalreview\nglobalreview");
    document.cluster_id = None;
    document.review_status = KnowledgeReviewStatus::InReview;
    document.human_validated = false;
    let item = fixture.service.import(&curator, document).await.unwrap().item;
    let before = fixture.counts().await;
    let (status, _) = call(
        fixture.router(),
        &fixture.operator,
        "POST",
        &format!("/v1/knowledge/{}/review", item.id),
        json!({"status":"validated","human_confirmed":true,"reason":"Synthetic narrow-scope review attempt"}),
    )
    .await;
    let after = fixture.counts().await;
    let current = fixture.service.item(&fixture.operator, item.id).await.unwrap();
    measured(
        "narrow_operator_global_review",
        json!({"status":status.as_u16(),"rows_before":before,"rows_after":after,"status_unchanged":current.review_status==KnowledgeReviewStatus::InReview}),
    );
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(before, after);
    assert_eq!(current.review_status, KnowledgeReviewStatus::InReview);
}
