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
use rocketmq_sre_contracts::{IncidentId, KnowledgeReviewStatus, PostmortemStatus};
use serde_json::json;

use super::support::*;
use crate::{
    api::capability_verification_test_state,
    postmortem::{CreatePostmortemRequest, PostmortemPatchRequest, PostmortemPublishRequest},
};

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_postmortem_experience_keeps_manual_confirmation_and_revision_lineage() {
    let fixture = Fixture::new().await;
    let incident = IncidentId::new();
    let now = Utc::now();
    sqlx::query("INSERT INTO sre_incidents(id,tenant_id,cluster_id,title,resource,symptom_family,fingerprint,status,created_by_subject,created_at,updated_at)
        VALUES($1,$2,$3,'Synthetic retrieval experience','consumer:synthetic','consumer_lag',$5,'resolved','d22-engineering',$4,$4)")
        .bind(incident.as_uuid()).bind(fixture.operator.tenant_id.as_uuid()).bind(fixture.cluster.as_uuid()).bind(now)
        .bind(format!("d22-synthetic-{incident}"))
        .execute(&fixture.repository.pool).await.unwrap();
    let state = capability_verification_test_state(fixture.repository.clone()).unwrap();
    let service = &state.postmortems;
    let draft = service
        .create(
            &fixture.operator,
            incident,
            &CreatePostmortemRequest {
                operator_notes: vec!["Synthetic engineering basis; no external causal adjudication.".to_owned()],
            },
        )
        .await
        .unwrap();
    assert_eq!(draft.postmortem.status, PostmortemStatus::Draft);
    assert!(draft.knowledge_item.is_none());
    assert_eq!(fixture.counts().await.0, 0);
    let publish = PostmortemPublishRequest {
        human_confirmed: true,
        owner: fixture.operator.subject.clone(),
        component: "consumer".to_owned(),
        rocketmq_version_range: ">=5.0.0, <6.0.0".to_owned(),
        review_due_at: now + Duration::days(7),
    };
    assert!(
        service
            .publish(&fixture.operator, draft.postmortem.id, &publish)
            .await
            .is_err()
    );
    assert_eq!(fixture.counts().await.0, 0);
    let patch: PostmortemPatchRequest = serde_json::from_value(json!({"human_confirmed":true,
        "effective_actions":["Synthetic observation only; no target action was executed."],
        "ineffective_actions":["Synthetic counterexample: do not infer recovery from a successful submission."]}))
    .unwrap();
    // Existing manual Postmortem roles remain compatible. This alias does not
    // gain the direct Knowledge curation permission introduced by D22.
    let legacy_actor = crate::auth::AuthContext {
        subject: "d22-manual-postmortem-reviewer".to_owned(),
        roles: std::collections::BTreeSet::from(["rocketmq:diagnose".to_owned()]),
        ..fixture.operator.clone()
    };
    assert_eq!(
        fixture
            .service
            .import(
                &legacy_actor,
                fixture.document("alias-direct", "consumer", "*", "source")
            )
            .await
            .unwrap_err()
            .code(),
        "operator_required"
    );
    assert!(
        service
            .patch(&fixture.viewer(), draft.postmortem.id, &patch)
            .await
            .is_err()
    );
    let confirmed = service.patch(&legacy_actor, draft.postmortem.id, &patch).await.unwrap();
    assert_eq!(confirmed.postmortem.status, PostmortemStatus::Confirmed);
    assert_eq!(confirmed.revisions.len(), 2);
    assert!(!confirmed.revisions[0].human_confirmed);
    assert!(confirmed.revisions[1].human_confirmed);
    let not_confirmed = PostmortemPublishRequest {
        human_confirmed: false,
        ..publish.clone()
    };
    assert!(
        service
            .publish(&fixture.operator, draft.postmortem.id, &not_confirmed)
            .await
            .is_err()
    );
    assert_eq!(fixture.counts().await.0, 0);
    let published = service
        .publish(&legacy_actor, draft.postmortem.id, &publish)
        .await
        .unwrap();
    let knowledge = published.knowledge_item.unwrap();
    assert_eq!(knowledge.review_status, KnowledgeReviewStatus::Validated);
    assert_eq!(knowledge.source_version, "revision-2");
    assert_eq!(
        knowledge.source_uri,
        format!("rocketmq-sre://postmortems/{}", draft.postmortem.id)
    );
    assert_eq!(published.postmortem.published_knowledge_item_id, Some(knowledge.id));
    assert_eq!(published.revisions[1].ineffective_actions.len(), 1);
    assert!(published.execution_journal_empty);
    let before_replay = fixture.counts().await;
    let replay = service
        .publish(&legacy_actor, draft.postmortem.id, &publish)
        .await
        .unwrap();
    assert_eq!(replay.knowledge_item.unwrap().id, knowledge.id);
    assert_eq!(fixture.counts().await, before_replay);
    let history = fixture.service.history(&fixture.viewer(), knowledge.id).await.unwrap();
    let lineage = history.postmortem.unwrap();
    assert_eq!(lineage.postmortem_id, draft.postmortem.id);
    assert_eq!(lineage.incident_id, incident);
    assert_eq!(lineage.revision, 2);
    assert_eq!(lineage.confirmed_by.as_deref(), Some(legacy_actor.subject.as_str()));
    assert!(lineage.confirmed_at.is_some());
    measured(
        "experience_return",
        json!({"draft_not_published":true,"viewer_patch_rejected":true,"immutable_revisions":2,
        "unconfirmed_publish_rejected":true,"manual_published_validated":true,"source_revision":2,"counterexample_retained":true,"target_execution_journal_empty":true}),
    );
}

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_knowledge_inputs_and_credential_canary_fail_before_writes() {
    let fixture = Fixture::new().await;
    let before = fixture.counts().await;
    for query in ["".to_owned(), "消费".repeat(100), "x".repeat(501)] {
        let result = fixture
            .service
            .search(&fixture.operator, &fixture.query(&query, "5.3.0", 20))
            .await;
        assert_eq!(result.unwrap_err().code(), "invalid_request");
    }
    for (label, body, expected) in [
        ("empty", "".to_owned(), "output_too_large"),
        ("overlarge", "x".repeat(1024 * 1024 + 1), "output_too_large"),
        (
            "credential",
            "-----BEGIN PRIVATE KEY-----\nSYNTHETIC_D22_PRIVATE_CANARY".to_owned(),
            "sensitive_content_rejected",
        ),
    ] {
        let request = fixture.document(label, "consumer", "*", &body);
        let error = fixture.service.import(&fixture.operator, request).await.unwrap_err();
        assert_eq!(error.code(), expected);
        assert!(!error.to_string().contains("SYNTHETIC_D22_PRIVATE_CANARY"));
    }
    let mut invalid = fixture.document("invalid-range", "consumer", "not-a-semver", "safe source");
    assert_eq!(
        fixture
            .service
            .import(&fixture.operator, invalid.clone())
            .await
            .unwrap_err()
            .code(),
        "invalid_request"
    );
    invalid.rocketmq_version_range = "*".to_owned();
    invalid.valid_from = Some(Utc::now() + Duration::days(1));
    invalid.valid_until = Some(Utc::now() - Duration::days(1));
    assert_eq!(
        fixture
            .service
            .import(&fixture.operator, invalid)
            .await
            .unwrap_err()
            .code(),
        "invalid_request"
    );
    assert_eq!(fixture.counts().await, before);
    measured(
        "input_bounds",
        json!({"utf8_bytes_enforced":true,"empty_and_oversize_rejected":true,"credential_rejected_without_echo":true,"version_and_validity_rejected":true,"writes":0}),
    );
}
