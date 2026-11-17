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

use chrono::{Duration, Utc};
use rocketmq_sre_contracts::{KnowledgeFeedbackKind, KnowledgeReviewStatus};
use serde_json::json;

use super::super::model::{KnowledgeFeedbackRequest, KnowledgeReviewRequest};
use super::support::*;

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_review_conflict_and_validity_are_not_diagnosis_authority() {
    let fixture = Fixture::new().await;
    let mut ids = Vec::new();
    for (label, status) in [
        ("fresh", KnowledgeReviewStatus::Validated),
        ("draft", KnowledgeReviewStatus::Draft),
        ("in_review", KnowledgeReviewStatus::InReview),
        ("deprecated", KnowledgeReviewStatus::Deprecated),
        ("expired_status", KnowledgeReviewStatus::Expired),
        ("review_overdue", KnowledgeReviewStatus::Validated),
        ("validity_ended", KnowledgeReviewStatus::Validated),
        ("not_yet_valid", KnowledgeReviewStatus::Validated),
    ] {
        let mut document = fixture.document(label, label, "*", "# validityneedle\nvalidityneedle");
        document.review_status = status;
        document.human_validated = status == KnowledgeReviewStatus::Validated;
        if label == "validity_ended" {
            document.valid_from = Some(Utc::now() - Duration::days(3));
            document.valid_until = Some(Utc::now() - Duration::days(1));
        }
        if label == "not_yet_valid" {
            document.valid_from = Some(Utc::now() + Duration::hours(1));
        }
        let item = fixture.import(document).await.item;
        if label == "review_overdue" {
            // Explicit historical metadata fixture, never a normal-record time change.
            sqlx::query("UPDATE knowledge_items SET review_due_at=$2 WHERE id=$1")
                .bind(item.id.as_uuid())
                .bind(Utc::now() - Duration::days(1))
                .execute(&fixture.repository.pool)
                .await
                .unwrap();
        }
        ids.push((label, item.id));
    }
    for label in ["conflict_a", "conflict_b"] {
        let mut document = fixture.document(
            label,
            "same-conflict-component",
            "*",
            &format!("# validityneedle\nDifferent source material: {label}."),
        );
        document.source_uri = format!(
            "https://d22-fixture.invalid/{}/same-conflict-source",
            fixture.operator.tenant_id
        );
        let item = fixture.import(document).await.item;
        ids.push((label, item.id));
    }
    let mut query = fixture.query("validityneedle", "5.3.0", 50);
    query.include_unvalidated = true;
    let page = fixture.service.search(&fixture.operator, &query).await.unwrap();
    assert_eq!(page.items.len(), ids.len());
    let mut facts = Vec::new();
    for (label, id) in &ids {
        let item = page.items.iter().find(|row| row.knowledge_item_id == *id).unwrap();
        assert_eq!(item.eligible_for_diagnosis, *label == "fresh", "{label}");
        facts.push(json!({"label":label,"eligible":item.eligible_for_diagnosis,"expired":item.expired,"reasons":item.exclusion_reasons}));
    }
    query.include_unvalidated = false;
    let reviewed = fixture.service.search(&fixture.operator, &query).await.unwrap();
    assert!(
        reviewed
            .items
            .iter()
            .all(|row| row.review_status == KnowledgeReviewStatus::Validated)
    );
    measured(
        "review_and_validity",
        json!({"include_context":page.items.len(),"default_rows":reviewed.items.len(),"facts":facts}),
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_eligible_material_is_not_starved_by_high_ranked_ineligible_context() {
    let fixture = Fixture::new().await;
    let valid = fixture
        .import(fixture.document(
            "fresh",
            "consumer",
            ">=5.0.0, <6.0.0",
            "# Reference\nqualityneedle guidance",
        ))
        .await
        .item;
    for index in 0..9 {
        let item = fixture
            .import(fixture.document(
                &format!("stale-{index}"),
                "consumer",
                "*",
                "# qualityneedle\nqualityneedle",
            ))
            .await
            .item;
        sqlx::query("UPDATE knowledge_items SET review_due_at=$2 WHERE id=$1")
            .bind(item.id.as_uuid())
            .bind(Utc::now() - Duration::days(1))
            .execute(&fixture.repository.pool)
            .await
            .unwrap();
    }
    let page = fixture.search("qualityneedle", "5.3.0", 2).await;
    let found = page
        .items
        .iter()
        .any(|row| row.knowledge_item_id == valid.id && row.eligible_for_diagnosis);
    measured(
        "eligibility_starvation",
        json!({"requested":2,"ineligible_distractors":9,"eligible_found":found,"returned":page.items.len(),"partial":page.partial}),
    );
    assert!(
        found,
        "ineligible context must not exhaust discovery ahead of applicable reviewed material"
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_ai_draft_feedback_and_explicit_review_preserve_human_control() {
    let fixture = Fixture::new().await;
    let mut document = fixture.document("ai-draft", "consumer", "*", "# lifecycleword\nlifecycleword");
    document.ai_generated = true;
    let before = fixture.counts().await;
    assert_eq!(
        fixture
            .service
            .import(&fixture.operator, document.clone())
            .await
            .unwrap_err()
            .code(),
        "human_validation_required"
    );
    assert_eq!(fixture.counts().await, before);
    document.review_status = KnowledgeReviewStatus::Draft;
    document.human_validated = false;
    let item = fixture.import(document).await.item;
    assert!(fixture.search("lifecycleword", "5.3.0", 10).await.items.is_empty());
    fixture
        .service
        .feedback(
            &fixture.viewer(),
            item.id,
            &KnowledgeFeedbackRequest {
                kind: KnowledgeFeedbackKind::Incorrect,
                comment: Some("Synthetic counterexample retained for manual review.".to_owned()),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .service
            .item(&fixture.operator, item.id)
            .await
            .unwrap()
            .review_status,
        KnowledgeReviewStatus::Draft
    );
    fixture
        .service
        .review(
            &fixture.operator,
            item.id,
            &KnowledgeReviewRequest {
                status: KnowledgeReviewStatus::InReview,
                human_confirmed: false,
                reason: "Submit explicit source for review".to_owned(),
            },
        )
        .await
        .unwrap();
    let request = KnowledgeReviewRequest {
        status: KnowledgeReviewStatus::Validated,
        human_confirmed: false,
        reason: "Review against source and counterexample".to_owned(),
    };
    let before_rejection = fixture.counts().await;
    assert_eq!(
        fixture
            .service
            .review(&fixture.operator, item.id, &request)
            .await
            .unwrap_err()
            .code(),
        "human_validation_required"
    );
    assert_eq!(fixture.counts().await, before_rejection);
    let request = KnowledgeReviewRequest {
        human_confirmed: true,
        ..request
    };
    let reviewed = fixture
        .service
        .review(&fixture.operator, item.id, &request)
        .await
        .unwrap();
    assert_eq!(reviewed.review_status, KnowledgeReviewStatus::Validated);
    let page = fixture.search("lifecycleword", "5.3.0", 10).await;
    assert_eq!(page.items.len(), 1);
    assert!(page.items[0].eligible_for_diagnosis);
    let feedback_actor: String =
        sqlx::query_scalar("SELECT created_by_subject FROM knowledge_feedback WHERE knowledge_item_id=$1")
            .bind(item.id.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
    assert_eq!(feedback_actor, fixture.viewer().subject);
    measured(
        "ai_human_review",
        json!({"ai_initial_validated_rejected":true,"draft_not_eligible":true,"explicit_manual_review_eligible":true,"counterexample_rows":fixture.counts().await.3}),
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_review_cas_does_not_promote_from_a_stale_status() {
    let fixture = Fixture::new().await;
    let mut document = fixture.document("review-race", "consumer", "*", "# reviewrace\nreviewrace");
    document.review_status = KnowledgeReviewStatus::Draft;
    let item = fixture.import(document).await.item;
    let (first, second) = tokio::join!(
        fixture.repository.transition_knowledge(
            &fixture.operator,
            item.clone(),
            KnowledgeReviewStatus::InReview,
            "review-a",
            false
        ),
        fixture.repository.transition_knowledge(
            &fixture.operator,
            item,
            KnowledgeReviewStatus::InReview,
            "review-b",
            false
        ),
    );
    let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());
    let error = first.err().or_else(|| second.err()).unwrap();
    assert_eq!(successes, 1);
    assert_eq!(error.code(), "knowledge_review_state_changed");
    assert_eq!(fixture.counts().await.2, 2); // import event plus exactly one review
    measured(
        "review_cas",
        json!({"successful_transitions":successes,"review_tasks":1,"stale_review_rejected":true}),
    );
}
