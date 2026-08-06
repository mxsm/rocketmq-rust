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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ConversationAnswerMode;
use rocketmq_sre_contracts::ConversationQueryIntent;
use rocketmq_sre_contracts::ConversationQueryKind;
use rocketmq_sre_contracts::ConversationTurnStatus;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::InvestigationDiagnosisStatus;
use rocketmq_sre_contracts::TenantId;

use super::ConversationCreateRequest;
use super::ConversationTurnRequest;
use super::conversation_repository::ConversationCompletion;
use super::conversation_repository::InvestigationDiagnosisDraft;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn postgres_conversation_turns_are_scoped_terminal_and_single_flight() {
    let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
    let repository = PostgresRepository::connect(&database_url, 4)
        .await
        .expect("repository with conversation migrations");
    let tenant_id = TenantId::new();
    let cluster_id = ClusterId::new();
    seed_cluster(&repository, tenant_id, cluster_id).await;
    let auth = AuthContext {
        tenant_id,
        subject: "conversation-query-test".to_owned(),
        clusters: BTreeSet::from([cluster_id]),
        roles: BTreeSet::from(["diagnose".to_owned()]),
    };
    let conversation_view = repository
        .create_conversation(
            &auth,
            &ConversationCreateRequest {
                cluster_id,
                question: "Show the current broker health".to_owned(),
                resource: Some("metrics/instant/rocketmq_broker_up".to_owned()),
                persist_investigation: true,
            },
            CorrelationId::new(),
        )
        .await
        .expect("conversation");
    let investigation_id = conversation_view
        .investigation
        .as_ref()
        .expect("persisted investigation")
        .id;
    let conversation = conversation_view.conversation;
    let intent = ConversationQueryIntent {
        schema_version: "rocketmq-sre.conversation-query-intent.v1".to_owned(),
        kind: ConversationQueryKind::MetricInstant,
        source: "prometheus".to_owned(),
        resource: "instant/rocketmq_broker_up".to_owned(),
        window_seconds: 300,
    };
    let request = ConversationTurnRequest {
        question: "Is the broker currently up?".to_owned(),
        resource: Some("metrics/instant/rocketmq_broker_up".to_owned()),
        window_seconds: Some(300),
    };
    let turn = repository
        .begin_conversation_turn(&auth, &conversation, &request, Some(&intent), CorrelationId::new())
        .await
        .expect("first collecting turn");
    let duplicate = repository
        .begin_conversation_turn(&auth, &conversation, &request, Some(&intent), CorrelationId::new())
        .await
        .expect_err("second collecting turn must fail closed");
    assert!(matches!(
        duplicate,
        ControlPlaneError::Conflict {
            code: "conversation_query_in_progress",
            ..
        }
    ));

    let non_terminal = repository
        .complete_conversation_turn(
            &auth,
            &turn,
            ConversationCompletion {
                status: ConversationTurnStatus::Collecting,
                intent: Some(intent.clone()),
                answer: "An active turn cannot have a final answer.".to_owned(),
                mode: ConversationAnswerMode::RulesOnly,
                citations: Vec::new(),
                evidence_ids: Vec::new(),
                model_invocation_id: None,
                partial: true,
                warnings: Vec::new(),
                diagnosis: None,
            },
        )
        .await;
    assert!(non_terminal.is_err());

    let evidence_id = EvidenceId::new();
    let completed = repository
        .complete_conversation_turn(
            &auth,
            &turn,
            ConversationCompletion {
                status: ConversationTurnStatus::Answered,
                intent: Some(intent),
                answer: "The bounded evidence was evaluated by broker-health.v1.".to_owned(),
                mode: ConversationAnswerMode::RulesOnly,
                citations: Vec::new(),
                evidence_ids: vec![evidence_id],
                model_invocation_id: None,
                partial: true,
                warnings: vec!["missing_optional_evidence".to_owned()],
                diagnosis: Some(InvestigationDiagnosisDraft {
                    investigation_id,
                    pack_id: "broker-health.v1".to_owned(),
                    pack_version: "1.0.0".to_owned(),
                    status: InvestigationDiagnosisStatus::Inconclusive,
                    rule_result: serde_json::json!({"status": "inconclusive"}),
                    hypotheses: serde_json::json!([]),
                    evidence_ids: vec![evidence_id],
                    primary_model_invocation_id: None,
                    partial: true,
                }),
            },
        )
        .await
        .expect("terminal answer revision");
    assert_eq!(completed.turn.status, ConversationTurnStatus::Answered);
    assert_eq!(completed.answer.as_ref().map(|answer| answer.revision), Some(1));
    let diagnosis = completed
        .diagnosis_revision
        .as_ref()
        .expect("investigation diagnosis revision");
    assert_eq!(diagnosis.investigation_id, investigation_id);
    assert_eq!(diagnosis.pack_id, "broker-health.v1");
    assert!(!diagnosis.execution_eligible);

    let page = repository
        .conversation_turns(&auth, &conversation)
        .await
        .expect("conversation history");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].turn.id, completed.turn.id);
    assert_eq!(page.items[0].turn.status, completed.turn.status);
    assert_eq!(page.items[0].turn.query_intent, completed.turn.query_intent);
    assert_eq!(page.items[0].diagnosis_revision, completed.diagnosis_revision);
    assert_eq!(
        page.items[0]
            .answer
            .as_ref()
            .map(|answer| (&answer.answer, answer.revision, &answer.citations)),
        completed
            .answer
            .as_ref()
            .map(|answer| (&answer.answer, answer.revision, &answer.citations)),
    );
    let investigation = repository
        .investigation(&auth, investigation_id)
        .await
        .expect("investigation detail");
    assert_eq!(investigation.diagnosis_revisions.len(), 1);
    assert!(!investigation.diagnosis_revisions[0].execution_eligible);
    assert!(
        investigation
            .timeline
            .iter()
            .any(|event| event.event_type == "conversation_diagnosis_revision_created")
    );
}

async fn seed_cluster(repository: &PostgresRepository, tenant_id: TenantId, cluster_id: ClusterId) {
    sqlx::query(
        "INSERT INTO clusters (
            id, tenant_id, external_cluster_key, environment, region,
            rocketmq_version, deployment_mode, owner_name,
            requested_access_profile, effective_access_profile, onboarding_state
         ) VALUES (
            $1, $2, $3, 'test', 'local', 'test', 'test', 'conversation-query-test',
            'read_only', 'read_only', 'ready_read_only'
         )",
    )
    .bind(cluster_id.as_uuid())
    .bind(tenant_id.to_string())
    .bind(format!("conversation-query-{cluster_id}"))
    .execute(&repository.pool)
    .await
    .expect("test cluster");
}
