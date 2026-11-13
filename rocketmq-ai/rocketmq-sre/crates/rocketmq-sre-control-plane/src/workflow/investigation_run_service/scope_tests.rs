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
async fn postgres_http_steer_imports_only_actual_saved_revision_metadata_with_current_scope_proof() {
    let (fixture, saved, unreferenced) = saved_reference_fixture().await;
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
    let mut run = fixture
        .repository
        .investigation_run_start(&worker, created.run.id, created.run.revision, Utc::now())
        .await
        .unwrap();
    let path = format!("/v1/investigation-runs/{}/commands", run.id);
    for (id, expected) in [
        (unreferenced, StatusCode::FORBIDDEN),
        (EvidenceId::new(), StatusCode::NOT_FOUND),
    ] {
        let steer = command(
            &run,
            InvestigationCommand::Steer {
                refinement: InvestigationRefinement {
                    scope: run.effective_scope.clone(),
                    packs: run.effective_packs.clone(),
                    evidence_ids: vec![id],
                },
            },
            &format!("reject-{id}"),
        );
        assert_eq!(
            call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(json!(steer)))
                .await
                .0,
            expected
        );
    }
    // Collection time is after this saved historical window; stale/partial
    // availability does not remove permission to reference a saved record.
    let steer = command(
        &run,
        InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: run.effective_scope.clone(),
                packs: run.effective_packs.clone(),
                evidence_ids: vec![saved],
            },
        },
        "saved-related",
    );
    let (status, receipt) = call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(json!(steer))).await;
    assert_eq!(status, StatusCode::OK);
    run = fixture
        .repository
        .investigation_run_get(&fixture.auth, run.id)
        .await
        .unwrap();
    assert_eq!(run.evidence_ids, vec![saved]);
    assert_eq!(receipt["control_epoch"], json!(run.control_epoch));
    let (_, replay) = call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(json!(steer))).await;
    assert_eq!(replay, receipt);
    let mut wrong = run.effective_scope.clone();
    wrong.resource.as_mut().unwrap().key = "broker-ab".to_owned();
    let rejected = command(
        &run,
        InvestigationCommand::Steer {
            refinement: InvestigationRefinement {
                scope: wrong,
                packs: run.effective_packs.clone(),
                evidence_ids: vec![saved],
            },
        },
        "wrong-subject",
    );
    assert_eq!(
        call(
            fixture.router(),
            "POST",
            &path,
            Some(&fixture.auth),
            Some(json!(rejected))
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    // A receipt remains bound to the current saved parent; changing its subject
    // cannot be bypassed by replaying the previously accepted refinement.
    sqlx::query("UPDATE investigations SET resource='brokers/broker-b' WHERE id=$1")
        .bind(run.initial_scope.investigation_id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    let (status, _) = call(fixture.router(), "POST", &path, Some(&fixture.auth), Some(json!(steer))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        fixture
            .repository
            .investigation_run_get(&fixture.auth, run.id)
            .await
            .unwrap(),
        run
    );
    sqlx::query("UPDATE investigations SET resource='brokers/broker-a' WHERE id=$1")
        .bind(run.initial_scope.investigation_id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
}
