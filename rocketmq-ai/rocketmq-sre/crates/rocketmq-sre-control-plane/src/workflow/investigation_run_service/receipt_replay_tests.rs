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
#[ignore = "requires task-owned Docker PostgreSQL and real JWT authentication"]
async fn postgres_receipt_replay_authority_outlives_new_command_availability_but_not_current_auth() {
    let mut fixture = fixture().await;
    fixture.request.budget.limit.output_bytes = 131072;
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
    let started = fixture
        .repository
        .investigation_run_start(&worker, created.run.id, lease.run.revision, Utc::now())
        .await
        .unwrap();
    let pause = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: started.id,
        expected_revision: started.revision,
        idempotency_key: "accepted-pause-original".to_owned(),
        reason: "Pause this exact admitted run".to_owned(),
        command: InvestigationCommand::Pause {},
    };
    let path = format!("/v1/investigation-runs/{}/commands", started.id);
    let controls_path = format!("/v1/investigation-runs/{}/controls", started.id);
    let (status, original_receipt) =
        call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(json!(pause))).await;
    assert_eq!(status, StatusCode::OK, "{original_receipt}");
    let (status, body) = call(fixture.router(), "GET", &controls_path, Some(&fixture.auth), None).await;
    assert_eq!(status, StatusCode::OK);
    let paused: InvestigationRunControls = serde_json::from_value(body).unwrap();
    assert!(paused.receipt_replay_allowed);
    assert_eq!(paused.run.status, InvestigationRunStatus::Paused);
    assert!(
        !paused
            .commands
            .iter()
            .find(|command| command.kind == InvestigationRunCommandKind::Pause)
            .unwrap()
            .allowed
    );
    let resume = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: started.id,
        expected_revision: paused.run.revision,
        idempotency_key: "resume-after-original-pause".to_owned(),
        reason: "Continue bounded work".to_owned(),
        command: InvestigationCommand::Resume {},
    };
    assert_eq!(
        call(
            fixture.router(),
            "POST",
            &path,
            Some(&fixture.auth),
            Some(json!(resume))
        )
        .await
        .0,
        StatusCode::OK
    );
    let current = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, started.id)
        .await
        .unwrap();
    assert!(current.revision > paused.run.revision);
    let count_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_events WHERE run_id=$1")
        .bind(started.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    let (status, replay) = call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(json!(pause))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, original_receipt);
    assert_eq!(
        fixture
            .state
            .investigation_runs
            .get(&fixture.auth, started.id)
            .await
            .unwrap(),
        current
    );
    let count_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM investigation_run_events WHERE run_id=$1")
        .bind(started.id.as_uuid())
        .fetch_one(&fixture.repository.pool)
        .await
        .unwrap();
    assert_eq!(count_after, count_before);
    let mut viewer = fixture.auth.clone();
    viewer.roles = std::collections::BTreeSet::from(["viewer".to_owned()]);
    let (status, body) = call(fixture.router(), "GET", &controls_path, Some(&viewer), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["receipt_replay_allowed"], false);
    assert_eq!(
        call(fixture.router(), "POST", &path, Some(&viewer), Some(json!(pause)))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let mut revoked = fixture.auth.clone();
    revoked.clusters.clear();
    assert_eq!(
        call(fixture.router(), "GET", &controls_path, Some(&revoked), None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(fixture.router(), "POST", &path, Some(&revoked), Some(json!(pause)))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let cancel = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: current.id,
        expected_revision: current.revision,
        idempotency_key: "terminal-after-pause".to_owned(),
        reason: "Finish isolated replay fixture".to_owned(),
        command: InvestigationCommand::Cancel {},
    };
    assert_eq!(
        call(
            fixture.router(),
            "POST",
            &path,
            Some(&fixture.auth),
            Some(json!(cancel))
        )
        .await
        .0,
        StatusCode::OK
    );
    let terminal = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, started.id)
        .await
        .unwrap();
    let (status, body) = call(fixture.router(), "GET", &controls_path, Some(&fixture.auth), None).await;
    assert_eq!(status, StatusCode::OK);
    let controls: InvestigationRunControls = serde_json::from_value(body).unwrap();
    assert!(controls.receipt_replay_allowed && controls.commands.iter().all(|command| !command.allowed));
    let (status, replay) = call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(json!(pause))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, original_receipt);
    assert_eq!(
        fixture
            .state
            .investigation_runs
            .get(&fixture.auth, started.id)
            .await
            .unwrap(),
        terminal
    );
    fixture
        .repository
        .investigation_run_release(&fixture.auth, started.id, &lease.fence, Utc::now())
        .await
        .unwrap();
}
