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
use chrono::Utc;
use rocketmq_sre_contracts::*;
use serde_json::json;

use super::test_support::*;
use crate::workflow::investigation_run_repository::RunWorkerContext;

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_http_runs_reauthorize_creation_and_receipt_replay_and_preserve_original_snapshot() {
    let fixture = fixture().await;
    let created = fixture.create().await;
    let path = format!("/v1/investigations/{}/runs", created.run.initial_scope.investigation_id);
    let commands = format!("/v1/investigation-runs/{}/commands", created.run.id);
    let cancellation = command(&created.run, InvestigationCommand::Cancel {}, "cancel-once");
    let (status, receipt) = call(
        fixture.router(),
        "POST",
        &commands,
        Some(&fixture.auth),
        Some(json!(cancellation)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["status"], "cancelled");
    let (status, replay) = call(
        fixture.router(),
        "POST",
        &path,
        Some(&fixture.auth),
        Some(json!(fixture.request)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["run"], json!(created.run));
    let (status, repeated) = call(
        fixture.router(),
        "POST",
        &commands,
        Some(&fixture.auth),
        Some(json!(cancellation)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt, repeated);
    let stale = command(&created.run, InvestigationCommand::Cancel {}, "new-key-stale-revision");
    let (status, stale_error) = call(
        fixture.router(),
        "POST",
        &commands,
        Some(&fixture.auth),
        Some(json!(stale)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale_error["code"], "revision_conflict");
    for denied in 0..4 {
        let mut auth = fixture.auth.clone();
        match denied {
            0 => auth.tenant_id = TenantId::new(),
            1 => auth.clusters.clear(),
            2 => auth.subject = "different-actor".to_owned(),
            _ => auth.roles.clear(),
        }
        let (status, _) = call(
            fixture.router(),
            "POST",
            &path,
            Some(&auth),
            Some(json!(fixture.request)),
        )
        .await;
        assert!(matches!(status, StatusCode::FORBIDDEN | StatusCode::CONFLICT));
        let (status, _) = call(
            fixture.router(),
            "POST",
            &commands,
            Some(&auth),
            Some(json!(cancellation)),
        )
        .await;
        assert!(matches!(
            status,
            StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::CONFLICT
        ));
    }
    let (status, _) = call(fixture.router(), "POST", &path, None, Some(json!(fixture.request))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, current) = call(
        fixture.router(),
        "GET",
        &format!("/v1/investigation-runs/{}", created.run.id),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(current["status"], "cancelled");
    assert_ne!(current["revision"], replay["run"]["revision"]);
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_http_expired_creation_replay_reads_its_original_receipt_without_new_admission() {
    use chrono::Duration;

    let fixture = fixture().await;
    let mut request = fixture.request.clone();
    request.scope.time_range.start -= Duration::hours(2);
    request.scope.time_range.end -= Duration::hours(2);
    let original_time = Utc::now() - Duration::hours(2);
    request.deadline = original_time + Duration::minutes(15);
    let context = fixture
        .state
        .investigation_runs
        .context(&fixture.auth, &request.scope, &request.scope, &[])
        .await
        .unwrap();
    let original = InvestigationRun::create(&context, &request, None, original_time).unwrap();
    // Restore a legitimate old queued creation, as a long-lived database would
    // contain after restart. This avoids clocks or sleeps in the HTTP boundary.
    let event = InvestigationRunEvent {
        schema_version: INVESTIGATION_RUN_EVENT_SCHEMA_VERSION.to_owned(),
        tenant_id: fixture.auth.tenant_id,
        cluster_id: request.scope.cluster_id,
        investigation_id: request.scope.investigation_id,
        run_id: request.run_id,
        sequence: InvestigationEventSequence::from_u64(1),
        revision: original.run.revision,
        control_epoch: original.run.control_epoch,
        status: original.run.status,
        actor_subject: fixture.auth.subject.clone(),
        event: InvestigationRunEventKind::Created {},
        occurred_at: original_time,
    };
    let mut transaction = fixture.repository.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO investigation_runs(id,tenant_id,cluster_id,investigation_id,run_snapshot,revision,status,event_sequence,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'0','queued',1,$6,$6)")
        .bind(request.run_id.as_uuid()).bind(fixture.auth.tenant_id.as_uuid()).bind(request.scope.cluster_id.as_uuid())
        .bind(request.scope.investigation_id.as_uuid()).bind(json!(original.run)).bind(original_time)
        .execute(&mut *transaction).await.unwrap();
    sqlx::query("INSERT INTO investigation_run_creations(tenant_id,run_id,actor_subject,idempotency_key,response_snapshot,recorded_at) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(fixture.auth.tenant_id.as_uuid()).bind(request.run_id.as_uuid()).bind(&fixture.auth.subject)
        .bind(&request.idempotency_key).bind(json!(original)).bind(original_time)
        .execute(&mut *transaction).await.unwrap();
    sqlx::query("INSERT INTO investigation_run_events(tenant_id,run_id,sequence,event_snapshot,recorded_at) VALUES($1,$2,1,$3,$4)")
        .bind(fixture.auth.tenant_id.as_uuid()).bind(request.run_id.as_uuid()).bind(json!(event)).bind(original_time)
        .execute(&mut *transaction).await.unwrap();
    transaction.commit().await.unwrap();
    let path = format!("/v1/investigations/{}/runs", request.scope.investigation_id);
    let (status, replay) = call(
        fixture.router(),
        "POST",
        &path,
        Some(&fixture.auth),
        Some(json!(request)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["run"], json!(original.run));
    request.idempotency_key = "expired-new-key".to_owned();
    let (status, error) = call(
        fixture.router(),
        "POST",
        &path,
        Some(&fixture.auth),
        Some(json!(request)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "invalid_input");
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_http_run_payloads_and_cursors_fail_closed_with_safe_errors() {
    let fixture = fixture().await;
    let created = fixture.create().await;
    let path = format!("/v1/investigations/{}/runs", fixture.request.scope.investigation_id);
    let mut invalid = json!(fixture.request);
    invalid["raw_shell"] = json!("private-marker-not-returned");
    assert_eq!(
        call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(invalid))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let wrong_parent = format!("/v1/investigations/{}/runs", InvestigationId::new());
    assert_eq!(
        call(
            fixture.router(),
            "POST",
            &wrong_parent,
            Some(&fixture.auth),
            Some(json!(fixture.request))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let commands = format!("/v1/investigation-runs/{}/commands", created.run.id);
    let mut invalid = json!(command(&created.run, InvestigationCommand::Pause {}, "pause"));
    invalid["command"]["url"] = json!("private-marker-not-returned");
    assert_eq!(
        call(fixture.router(), "POST", &commands, Some(&fixture.auth), Some(invalid))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut invalid = command(&created.run, InvestigationCommand::Cancel {}, "wrong-run");
    invalid.run_id = InvestigationRunId::new();
    assert_eq!(
        call(
            fixture.router(),
            "POST",
            &commands,
            Some(&fixture.auth),
            Some(json!(invalid))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    for query in [
        "after=-1",
        "after=00",
        "after=18446744073709551616",
        "after=0&after=1",
        "limit=0",
        "limit=101",
        "raw_request=private-marker",
    ] {
        let events = format!("/v1/investigation-runs/{}/events?{query}", created.run.id);
        assert_eq!(
            call(fixture.router(), "GET", &events, Some(&fixture.auth), None)
                .await
                .0,
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    assert_eq!(
        call(
            fixture.router(),
            "GET",
            "/v1/investigation-runs/%FF",
            Some(&fixture.auth),
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_http_run_events_page_and_explicit_cursor_resynchronization_are_scoped() {
    let fixture = fixture().await;
    let created = fixture.create().await;
    let commands = format!("/v1/investigation-runs/{}/commands", created.run.id);
    call(
        fixture.router(),
        "POST",
        &commands,
        Some(&fixture.auth),
        Some(json!(command(&created.run, InvestigationCommand::Cancel {}, "cancel"))),
    )
    .await;
    let path = format!("/v1/investigation-runs/{}/events", created.run.id);
    let (status, first) = call(
        fixture.router(),
        "GET",
        &format!("{path}?limit=1"),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["events"].as_array().unwrap().len(), 1);
    assert_eq!(first["events"][0]["event"]["kind"], "created");
    assert_eq!(first["has_more"], true);
    let (_, next) = call(
        fixture.router(),
        "GET",
        &format!("{path}?after={}&limit=1", first["next_sequence"].as_str().unwrap()),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(next["events"][0]["event"]["kind"], "command_accepted");
    assert_eq!(next["has_more"], false);
    let (_, ahead) = call(
        fixture.router(),
        "GET",
        &format!("{path}?after=18446744073709551615"),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(ahead["snapshot_required"], true);
    assert_eq!(ahead["recovery_reason"], "cursor_ahead");
    assert_eq!(ahead["current_snapshot"]["status"], "cancelled");
    sqlx::query("DELETE FROM investigation_run_events WHERE run_id=$1 AND sequence=1")
        .bind(created.run.id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE investigation_runs SET event_floor=2 WHERE id=$1")
        .bind(created.run.id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let (_, expired) = call(fixture.router(), "GET", &path, Some(&fixture.auth), None).await;
    assert_eq!(expired["snapshot_required"], true);
    assert_eq!(expired["recovery_reason"], "cursor_expired");
    sqlx::query("DELETE FROM investigation_run_events WHERE run_id=$1")
        .bind(created.run.id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let (_, gap) = call(
        fixture.router(),
        "GET",
        &format!("{path}?after=1"),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(gap["recovery_reason"], "retained_event_gap");
    let mut other = fixture.auth.clone();
    other.tenant_id = TenantId::new();
    assert_eq!(
        call(fixture.router(), "GET", &path, Some(&other), None).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
#[ignore = "requires explicit disposable Docker PostgreSQL"]
async fn postgres_http_drain_receipts_replay_without_overwriting_a_newer_snapshot() {
    for kind in [InvestigationCommand::Pause {}, InvestigationCommand::Cancel {}] {
        let fixture = fixture().await;
        let created = fixture.create().await;
        let context = fixture
            .state
            .investigation_runs
            .context(
                &fixture.auth,
                &created.run.initial_scope,
                &created.run.initial_scope,
                &[],
            )
            .await
            .unwrap();
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
        let requirement = context
            .allowed_requirements
            .iter()
            .find(|requirement| requirement.pack == fixture.request.packs[0])
            .unwrap();
        let admission = InvestigationStepAdmission {
            step_id: InvestigationStepId::new(),
            attempt_id: StepAttemptId::new(),
            expected_revision: run.revision,
            spec: InvestigationStepSpec {
                scope: run.effective_scope.clone(),
                pack: requirement.pack.clone(),
                work: InvestigationStepKind::CollectEvidence {
                    requirement_id: requirement.requirement_id.clone(),
                },
            },
            reservation: InvestigationBudgetCost {
                read_queries: 1,
                evidence_items: 1,
                output_bytes: 64,
                ..Default::default()
            },
        };
        let run = fixture
            .repository
            .investigation_run_admit(&worker, run.id, &admission, Utc::now())
            .await
            .unwrap();
        let request = command(&run, kind, "drain");
        let path = format!("/v1/investigation-runs/{}/commands", run.id);
        let (status, receipt) = call(
            fixture.router(),
            "POST",
            &path,
            Some(&fixture.auth),
            Some(json!(request)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, replay) = call(
            fixture.router(),
            "POST",
            &path,
            Some(&fixture.auth),
            Some(json!(request)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(receipt, replay);
        let result = InvestigationStepResult {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            run_id: run.id,
            step_id: admission.step_id,
            attempt_id: admission.attempt_id,
            control_epoch: run.control_epoch,
            outcome: InvestigationStepOutcome::Failed,
            failure: Some(InvestigationStepFailure::SourceUnavailable),
            evidence_ids: vec![],
            actual_cost: InvestigationBudgetCost::default(),
            finished_at: Utc::now(),
        };
        let settled = fixture
            .repository
            .investigation_run_settle(&worker, &result, Utc::now())
            .await
            .unwrap();
        let (_, replay) = call(
            fixture.router(),
            "POST",
            &path,
            Some(&fixture.auth),
            Some(json!(request)),
        )
        .await;
        assert_eq!(receipt, replay);
        assert!(settled.revision > request.expected_revision);
        assert!(matches!(
            settled.status,
            InvestigationRunStatus::Paused | InvestigationRunStatus::Cancelled
        ));
        assert_eq!(
            fixture
                .repository
                .investigation_run_get(&fixture.auth, run.id)
                .await
                .unwrap(),
            settled
        );
    }
}
