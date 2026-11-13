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

use super::*;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn concurrent_results_share_one_evidence_charge_and_preserve_original_replay_identity() {
    let (repo, auth, mut ctx, lease) = running().await;
    let first = collect(&lease.run);
    let mut run = repo
        .investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            lease.run.id,
            &first,
            Utc::now(),
        )
        .await
        .unwrap();
    ctx.allowed_requirements.push(InvestigationRequirementRef {
        pack: run.effective_packs[0].clone(),
        requirement_id: "consumer-state".to_owned(),
    });
    let mut second = collect(&run);
    second.spec.work = InvestigationStepKind::CollectEvidence {
        requirement_id: "consumer-state".to_owned(),
    };
    run = repo
        .investigation_run_admit(
            &RunWorkerContext {
                auth: &auth,
                context: &ctx,
                fence: &lease.fence,
            },
            run.id,
            &second,
            Utc::now(),
        )
        .await
        .unwrap();
    let evidence = EvidenceId::new();
    ctx.visible_evidence.push(InvestigationEvidenceContext {
        evidence_id: evidence,
        scope: run.effective_scope.clone(),
    });
    let worker = RunWorkerContext {
        auth: &auth,
        context: &ctx,
        fence: &lease.fence,
    };
    let mut left = result(&run, &first);
    left.evidence_ids = vec![evidence];
    left.actual_cost.evidence_items = 1;
    let mut right = result(&run, &second);
    right.evidence_ids = vec![evidence];
    right.actual_cost.evidence_items = 1;
    let before = head(&repo, run.id).await;
    let mut excessive = left.clone();
    excessive.actual_cost.evidence_items = u64::MAX;
    assert_eq!(
        repo.investigation_run_settle(&worker, &excessive, Utc::now())
            .await
            .unwrap_err()
            .code(),
        "invalid_input"
    );
    // The result receipt must roll back alongside a failed event append.
    sqlx::query("UPDATE investigation_runs SET event_sequence=$1 WHERE id=$2")
        .bind(i64::MAX)
        .bind(run.id.as_uuid())
        .execute(&repo.pool)
        .await
        .unwrap();
    assert_eq!(
        repo.investigation_run_settle(&worker, &left, Utc::now())
            .await
            .unwrap_err()
            .code(),
        "counter_exhausted"
    );
    assert_eq!(repo.investigation_run_get(&auth, run.id).await.unwrap(), run);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM investigation_run_results WHERE run_id=$1")
        .bind(run.id.as_uuid())
        .fetch_one(&repo.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("UPDATE investigation_runs SET event_sequence=$1 WHERE id=$2")
        .bind(before)
        .bind(run.id.as_uuid())
        .execute(&repo.pool)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        repo.investigation_run_settle(&worker, &left, Utc::now()),
        repo.investigation_run_settle(&worker, &right, Utc::now())
    );
    a.unwrap();
    b.unwrap();
    let completed = repo.investigation_run_get(&auth, run.id).await.unwrap();
    assert_eq!(completed.in_flight_count(), 0);
    assert_eq!(completed.budget_usage().unwrap().0.evidence_items, 1);
    assert_eq!(completed.budget_usage().unwrap().0.read_queries, 2);
    assert_eq!(completed.evidence_ids, vec![evidence]);
    assert_eq!(head(&repo, run.id).await, before + 2);
    for report in [&left, &right] {
        assert_eq!(
            repo.investigation_run_settle(&worker, report, Utc::now())
                .await
                .unwrap(),
            completed
        );
        let mut changed = report.clone();
        changed.actual_cost.evidence_items = 0;
        assert_eq!(
            repo.investigation_run_settle(&worker, &changed, Utc::now())
                .await
                .unwrap_err()
                .code(),
            "result_conflict"
        );
        changed = report.clone();
        changed.finished_at += Duration::milliseconds(1);
        assert_eq!(
            repo.investigation_run_settle(&worker, &changed, Utc::now())
                .await
                .unwrap_err()
                .code(),
            "result_conflict"
        );
    }
    assert_eq!(head(&repo, run.id).await, before + 2);
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn saved_inconclusive_assessment_survives_reload_without_inventing_a_root_cause() {
    use crate::workflow::{
        ConversationCreateRequest, ConversationTurnRequest,
        conversation_repository::{ConversationCompletion, InvestigationDiagnosisDraft},
    };

    let (repo, auth, mut ctx, mut request) = fixture().await;
    let conversation = repo
        .create_conversation(
            &auth,
            &ConversationCreateRequest {
                cluster_id: request.scope.cluster_id,
                question: "Synthetic persisted diagnosis with insufficient evidence".to_owned(),
                resource: None,
                persist_investigation: true,
            },
            CorrelationId::new(),
        )
        .await
        .unwrap();
    let parent = conversation.investigation.unwrap();
    request.scope.investigation_id = parent.id;
    ctx.authorized_scope = request.scope.clone();
    let content = serde_json::json!({"synthetic": true, "missing": "source_unavailable"});
    let mut evidence = EvidenceSnapshot::capture(
        EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.scope.cluster_id,
            source: "fixture".to_owned(),
            resource: "consumer-lag/synthetic".to_owned(),
            time_range: request.scope.time_range.clone(),
        },
        current_evidence_schema(),
        Utc::now(),
        EvidenceContent::Inline(content.clone()),
    )
    .unwrap();
    evidence.exposure = EvidenceExposure::Synthetic;
    evidence.partial = true;
    evidence.coverage = CoverageStatus::Partial;
    let evidence = repo
        .persist_evidence(
            &auth,
            &evidence,
            Some(parent.id),
            None,
            &canonical_sha256(&content).unwrap(),
        )
        .await
        .unwrap();
    let turn = repo
        .begin_conversation_turn(
            &auth,
            &conversation.conversation,
            &ConversationTurnRequest {
                question: "Synthetic rule-only fixture".to_owned(),
                resource: None,
                window_seconds: None,
            },
            None,
            CorrelationId::new(),
        )
        .await
        .unwrap();
    let completion = repo
        .complete_conversation_turn(
            &auth,
            &turn,
            ConversationCompletion {
                status: ConversationTurnStatus::Answered,
                intent: None,
                answer: "No source evidence was collected in this fixture".to_owned(),
                mode: ConversationAnswerMode::RulesOnly,
                citations: vec![],
                evidence_ids: vec![evidence.evidence_id],
                model_invocation_id: None,
                partial: true,
                warnings: vec![],
                diagnosis: Some(InvestigationDiagnosisDraft {
                    investigation_id: parent.id,
                    pack_id: request.packs[0].pack_id.clone(),
                    pack_version: request.packs[0].pack_version.clone(),
                    status: InvestigationDiagnosisStatus::Inconclusive,
                    rule_result: serde_json::json!({"status":"inconclusive"}),
                    hypotheses: serde_json::json!([]),
                    evidence_ids: vec![evidence.evidence_id],
                    primary_model_invocation_id: None,
                    partial: true,
                }),
            },
        )
        .await
        .unwrap();
    let saved = completion.diagnosis_revision.unwrap();
    repo.investigation_run_create(&auth, &ctx, &request, Utc::now())
        .await
        .unwrap();
    let lease = repo
        .investigation_run_claim(&auth, request.run_id, Uuid::new_v4(), 60, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let worker = RunWorkerContext {
        auth: &auth,
        context: &ctx,
        fence: &lease.fence,
    };
    let run = repo
        .investigation_run_start(&worker, request.run_id, lease.run.revision, Utc::now())
        .await
        .unwrap();
    let run = repo
        .investigation_run_stop(
            &worker,
            RunStopCommit {
                run_id: run.id,
                expected_revision: run.revision,
                status: InvestigationRunStatus::NeedsEvidence,
            },
            Utc::now(),
        )
        .await
        .unwrap();
    let assessment = HypothesisAssessment {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        id: HypothesisAssessmentId::new(),
        run_id: run.id,
        scope: run.effective_scope.clone(),
        control_epoch: run.control_epoch,
        diagnosis_revision_id: saved.id,
        conclusion: HypothesisAssessmentConclusion::Inconclusive {
            reason: InvestigationStopReason::EvidenceInsufficient,
            considered_evidence_ids: vec![],
            missing_requirements: ctx.allowed_requirements.clone(),
        },
        summary: "No root cause established from this saved partial revision".to_owned(),
        created_at: Utc::now(),
    };
    let run = repo
        .investigation_run_finish(
            &worker,
            RunAssessmentCommit {
                run_id: run.id,
                expected_revision: run.revision,
                assessment: &assessment,
                saved_revision: &saved,
            },
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Inconclusive);
    assert_eq!(
        repository().await.investigation_run_get(&auth, run.id).await.unwrap(),
        run
    );
    assert_eq!(run.assessments, vec![assessment]);
}
