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
#[ignore = "requires disposable Docker PostgreSQL"]
async fn postgres_late_known_evaluation_records_superseded_cost_and_exact_replay() {
    for steer in [false, true] {
        let (fixture, _, _) = prepared().await;
        let created = fixture.create().await;
        let service = &fixture.state.investigation_runs;
        let delegation = fixture
            .repository
            .investigation_run_delegation(created.run.id)
            .await
            .unwrap();
        let lease = fixture
            .repository
            .investigation_run_claim(&fixture.auth, created.run.id, uuid::Uuid::new_v4(), 30, Utc::now())
            .await
            .unwrap()
            .unwrap();
        let context = context::catalog_context(service, &fixture.auth, &created.run);
        let worker = RunWorkerContext {
            auth: &fixture.auth,
            context: &context,
            fence: &lease.fence,
        };
        let run = fixture
            .repository
            .investigation_run_start(&worker, created.run.id, created.run.revision, Utc::now())
            .await
            .unwrap();
        let admission = InvestigationStepAdmission {
            step_id: InvestigationStepId::new(),
            attempt_id: StepAttemptId::new(),
            expected_revision: run.revision,
            spec: InvestigationStepSpec {
                scope: run.effective_scope.clone(),
                pack: run.effective_packs[0].clone(),
                work: InvestigationStepKind::EvaluateRules { evidence_ids: vec![] },
            },
            reservation: InvestigationBudgetCost {
                output_bytes: OUTPUT_BYTES,
                ..Default::default()
            },
        };
        let admitted = fixture
            .repository
            .investigation_run_dispatch_admit(
                &delegation,
                &worker,
                &service.catalog,
                run.id,
                &admission,
                RunAdmissionProof::Rules,
            )
            .await
            .unwrap();
        let request = command(
            &admitted,
            if steer {
                InvestigationCommand::Steer {
                    refinement: InvestigationRefinement {
                        scope: admitted.effective_scope.clone(),
                        packs: admitted.effective_packs.clone(),
                        evidence_ids: vec![],
                    },
                }
            } else {
                InvestigationCommand::Cancel {}
            },
            "change-during-local-evaluation",
        );
        service.command(&fixture.auth, run.id, &request).await.unwrap();
        let at = Utc::now();
        let result = InvestigationStepResult {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            run_id: run.id,
            step_id: admission.step_id,
            attempt_id: admission.attempt_id,
            control_epoch: admitted.control_epoch,
            outcome: InvestigationStepOutcome::Succeeded,
            failure: None,
            evidence_ids: vec![],
            actual_cost: InvestigationBudgetCost {
                output_bytes: 123,
                ..Default::default()
            },
            finished_at: at,
        };
        let revision = SavedInvestigationRunRevision {
            schema_version: INVESTIGATION_RUN_REVISION_SCHEMA_VERSION.to_owned(),
            id: InvestigationRunRevisionId::new(),
            run_id: run.id,
            scope: admitted.effective_scope.clone(),
            control_epoch: admitted.control_epoch,
            pack: admission.spec.pack.clone(),
            step_id: admission.step_id,
            attempt_id: admission.attempt_id,
            status: InvestigationDiagnosisStatus::Inconclusive,
            partial: true,
            evidence_ids: vec![],
            requirements: vec![],
            findings: vec![],
            input_fingerprint: canonical_sha256(&admission.spec).unwrap(),
            created_at: at,
        };
        let after = fixture
            .repository
            .investigation_run_save_evaluation(&worker, &result, &revision)
            .await
            .unwrap();
        assert_eq!(
            after.status,
            if steer {
                InvestigationRunStatus::Running
            } else {
                InvestigationRunStatus::Cancelled
            }
        );
        assert!(after.pack_results.is_empty());
        let attempt = &after.steps[0].attempts[0];
        assert_eq!(attempt.disposition, Some(InvestigationResultDisposition::Superseded));
        assert!(attempt.interruption.is_none());
        assert_eq!(attempt.result.as_ref().unwrap().actual_cost.output_bytes, 123);
        let replay = fixture
            .repository
            .investigation_run_save_evaluation(&worker, &result, &revision)
            .await
            .unwrap();
        assert_eq!(replay, after);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_revisions WHERE run_id=$1")
            .bind(run.id.as_uuid())
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL"]
async fn postgres_dispatch_admission_rejects_a_different_subject_and_offboarding() {
    let (fixture, _, _) = prepared().await;
    let created = fixture.create().await;
    let delegation = fixture
        .repository
        .investigation_run_delegation(created.run.id)
        .await
        .unwrap();
    let lease = fixture
        .repository
        .investigation_run_claim(&fixture.auth, created.run.id, uuid::Uuid::new_v4(), 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let context = context::catalog_context(&fixture.state.investigation_runs, &fixture.auth, &created.run);
    let worker = RunWorkerContext {
        auth: &fixture.auth,
        context: &context,
        fence: &lease.fence,
    };
    let run = fixture
        .repository
        .investigation_run_start(&worker, created.run.id, created.run.revision, Utc::now())
        .await
        .unwrap();
    let admission = InvestigationStepAdmission {
        step_id: InvestigationStepId::new(),
        attempt_id: StepAttemptId::new(),
        expected_revision: run.revision,
        spec: InvestigationStepSpec {
            scope: run.effective_scope.clone(),
            pack: run.effective_packs[0].clone(),
            work: InvestigationStepKind::CollectEvidence {
                requirement_id: "store-pressure".to_owned(),
            },
        },
        reservation: InvestigationBudgetCost {
            read_queries: 1,
            evidence_items: 1,
            output_bytes: crate::connector_channel::MAX_RESPONSE_BYTES as u64,
            ..Default::default()
        },
    };
    let reference = InvestigationRequirementRef {
        pack: admission.spec.pack.clone(),
        requirement_id: "store-pressure".to_owned(),
    };
    let mut proof = crate::workflow::investigation_run_repository::RunReadProof {
        requirement: reference.clone(),
        consumers: vec![reference],
        scope: run.effective_scope.clone(),
        query: EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: fixture.auth.tenant_id,
            cluster_id: run.initial_scope.cluster_id,
            source: "admin-query".to_owned(),
            resource: "store-pressure/other-broker".to_owned(),
            time_range: run.effective_scope.time_range.clone(),
        },
    };
    assert!(
        fixture
            .repository
            .investigation_run_dispatch_admit(
                &delegation,
                &worker,
                &fixture.state.investigation_runs.catalog,
                run.id,
                &admission,
                RunAdmissionProof::Read(&proof)
            )
            .await
            .is_err()
    );
    proof.query.resource = "store-pressure/broker-a".to_owned();
    sqlx::query("UPDATE clusters SET onboarding_state='offboarded' WHERE id=$1")
        .bind(run.initial_scope.cluster_id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    assert!(
        fixture
            .repository
            .investigation_run_dispatch_admit(
                &delegation,
                &worker,
                &fixture.state.investigation_runs.catalog,
                run.id,
                &admission,
                RunAdmissionProof::Read(&proof)
            )
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_reads WHERE run_id=$1")
        .bind(run.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL"]
async fn postgres_delegation_cannot_expand_the_immutable_accepted_budget() {
    let (fixture, _, _) = prepared().await;
    let created = fixture.create().await;
    let mut forged = created.run.clone();
    forged.budget.limit.output_bytes += 1;
    sqlx::query("UPDATE investigation_runs SET run_snapshot=$1 WHERE id=$2")
        .bind(serde_json::to_value(&forged).unwrap())
        .bind(forged.id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .repository
            .investigation_run_delegation(forged.id)
            .await
            .err()
            .unwrap()
            .code(),
        "invalid_run_delegation"
    );
    let owner = RuntimeContext::from_current("d15-delegation-regression").service_context("dispatch");
    assert!(
        fixture
            .state
            .investigation_runs
            .dispatch_one(owner.task_group(), forged.id)
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_reads WHERE run_id=$1")
        .bind(forged.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL"]
async fn postgres_missing_saved_evidence_stops_explicitly_without_resetting_observed_history() {
    let (fixture, evidence_id, _) = saved_reference_fixture().await;
    let created = fixture.create().await;
    let service = &fixture.state.investigation_runs;
    let context = context::catalog_context(service, &fixture.auth, &created.run);
    let lease = fixture
        .repository
        .investigation_run_claim(&fixture.auth, created.run.id, uuid::Uuid::new_v4(), 30, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let worker = RunWorkerContext {
        auth: &fixture.auth,
        context: &context,
        fence: &lease.fence,
    };
    let run = fixture
        .repository
        .investigation_run_start(&worker, created.run.id, created.run.revision, Utc::now())
        .await
        .unwrap();
    let steer = command(
        &run,
        InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: run.effective_scope.clone(),
                packs: run.effective_packs.clone(),
                evidence_ids: vec![evidence_id],
            },
        },
        "saved-import-before-removal",
    );
    service.command(&fixture.auth, run.id, &steer).await.unwrap();
    let paused = fixture
        .repository
        .investigation_run_release(&fixture.auth, run.id, &lease.fence, Utc::now())
        .await
        .unwrap();
    service
        .command(
            &fixture.auth,
            run.id,
            &command(&paused, InvestigationCommand::Resume {}, "continue-saved-evidence"),
        )
        .await
        .unwrap();
    // Simulate unavailability of this synthetic fixture's retained content.
    // The saved revision itself and the run's observed references remain intact.
    let mut tx = fixture.repository.pool.begin().await.unwrap();
    sqlx::query("DELETE FROM conversation_evidence_links WHERE evidence_id=$1")
        .bind(evidence_id.as_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM evidence_links WHERE evidence_id=$1")
        .bind(evidence_id.as_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM evidence_snapshots WHERE id=$1 AND tenant_id=$2")
        .bind(evidence_id.as_uuid())
        .bind(fixture.auth.tenant_id.as_uuid())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let owner = RuntimeContext::from_current("d15-missing-reference-regression").service_context("dispatch");
    assert!(service.dispatch_one(owner.task_group(), run.id).await.is_err());
    let stopped = service.get(&fixture.auth, run.id).await.unwrap();
    assert_eq!(stopped.status, InvestigationRunStatus::NeedsEvidence);
    assert_eq!(stopped.stop_reason, Some(InvestigationStopReason::SourceUnavailable));
    assert_eq!(stopped.evidence_ids, vec![evidence_id]);
    assert_eq!(charged_reads(&stopped), 0);
    assert!(stopped.pack_results.is_empty());
}
