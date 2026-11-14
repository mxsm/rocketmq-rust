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

use super::test_support::*;
use axum::http::StatusCode;
use rocketmq_sre_contracts::*;
use serde_json::json;

fn list_path(fixture: &Fixture) -> String {
    format!("/v1/investigations/{}/runs", fixture.request.scope.investigation_id)
}

fn filtered_path(fixture: &Fixture, window: &TimeRange) -> String {
    format!(
        "{}?limit=1&start={}&end={}",
        list_path(fixture),
        window.start.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        window.end.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    )
}

#[tokio::test]
#[ignore = "requires task-owned Docker PostgreSQL"]
async fn postgres_discovery_filters_before_paging_and_binds_cursor_to_scope() {
    let fixture = fixture().await;
    let first = fixture.create().await.run;
    let mut request = fixture.request.clone();
    request.run_id = InvestigationRunId::new();
    request.idempotency_key = "second-window-peer".to_owned();
    let second = fixture
        .state
        .investigation_runs
        .create(&fixture.auth, request.scope.investigation_id, &request)
        .await
        .unwrap()
        .run;
    let mut all = vec![first.clone(), second.clone()];
    for index in 0..51 {
        let mut request = fixture.request.clone();
        request.run_id = InvestigationRunId::new();
        request.idempotency_key = format!("other-window-{index}");
        request.scope.time_range.start -= chrono::Duration::days(1);
        request.scope.time_range.end -= chrono::Duration::days(1);
        all.push(
            fixture
                .state
                .investigation_runs
                .create(&fixture.auth, request.scope.investigation_id, &request)
                .await
                .unwrap()
                .run,
        );
    }
    let path = filtered_path(&fixture, &first.effective_scope.time_range);
    let (status, body) = call(fixture.router(), "GET", &path, Some(&fixture.auth), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let page: InvestigationRunListPage = serde_json::from_value(body).unwrap();
    assert_eq!(page.items.len(), 1);
    assert!(page.creation.allowed);
    assert!(!page.creation.compiled_packs.is_empty());
    let cursor = page.next_cursor.unwrap();
    let (status, body) = call(
        fixture.router(),
        "GET",
        &format!("{path}&cursor={cursor}"),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let next: InvestigationRunListPage = serde_json::from_value(body).unwrap();
    assert_eq!(next.items.len(), 1);
    assert!(next.next_cursor.is_none());
    let ids = std::collections::BTreeSet::from([page.items[0].id, next.items[0].id]);
    assert_eq!(ids, std::collections::BTreeSet::from([first.id, second.id]));
    for path in [
        format!("{}?cursor={cursor}", list_path(&fixture)),
        format!("{path}&resource_kind=broker&resource_key=other&cursor={cursor}"),
    ] {
        let (status, _) = call(fixture.router(), "GET", &path, Some(&fixture.auth), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let other = super::test_support::fixture().await;
    let (status, _) = call(
        other.router(),
        "GET",
        &format!("{}?cursor={cursor}", list_path(&other)),
        Some(&other.auth),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for (index, budget) in [
        page.creation.default_budget.clone(),
        page.creation.maximum_budget.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        let mut request = fixture.request.clone();
        request.run_id = InvestigationRunId::new();
        request.idempotency_key = format!("server-budget-{index}");
        request.budget = budget;
        if index == 1 {
            request.scope.resource = Some(InvestigationResource {
                kind: ResourceKind::Broker,
                key: "broker-b".to_owned(),
            });
        }
        let (status, body) = call(
            fixture.router(),
            "POST",
            &list_path(&fixture),
            Some(&fixture.auth),
            Some(json!(request)),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        all.push(
            serde_json::from_value::<CreateInvestigationRunResponse>(body)
                .unwrap()
                .run,
        );
    }
    let (status, body) = call(
        fixture.router(),
        "GET",
        &format!("{}?resource_kind=broker&resource_key=broker-b", list_path(&fixture)),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let resource_page: InvestigationRunListPage = serde_json::from_value(body).unwrap();
    assert_eq!(resource_page.items.len(), 1);
    assert_eq!(resource_page.items[0].id, all.last().unwrap().id);
    // Do not leave queued fixtures for the production dispatch inventory.
    for run in all {
        let request = InvestigationCommandRequest {
            schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
            command_id: InvestigationCommandId::new(),
            run_id: run.id,
            expected_revision: run.revision,
            idempotency_key: "fixture-cleanup".to_owned(),
            reason: "Close isolated fixture".to_owned(),
            command: InvestigationCommand::Cancel {},
        };
        fixture
            .state
            .investigation_runs
            .command(&fixture.auth, run.id, &request)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires task-owned Docker PostgreSQL and real JWT authentication"]
async fn postgres_controls_reauthorize_roles_and_never_persist_caller_affordances() {
    let fixture = fixture().await;
    let created = fixture.create().await;
    let path = format!("/v1/investigation-runs/{}/controls", created.run.id);
    let mut viewer = fixture.auth.clone();
    viewer.roles = std::collections::BTreeSet::from(["viewer".to_owned()]);
    let (status, body) = call(fixture.router(), "GET", &path, Some(&viewer), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let controls: InvestigationRunControls = serde_json::from_value(body).unwrap();
    assert_eq!(controls.run, created.run);
    assert!(!controls.receipt_replay_allowed);
    assert!(controls.commands.iter().all(|command| !command.allowed
        && command.blocked_reason == Some(InvestigationRunControlBlockReason::ControlPermissionRequired)));
    let (status, body) = call(fixture.router(), "GET", &list_path(&fixture), Some(&viewer), None).await;
    assert_eq!(status, StatusCode::OK);
    let page: InvestigationRunListPage = serde_json::from_value(body).unwrap();
    assert!(!page.creation.allowed);
    let request = InvestigationCommandRequest {
        schema_version: INVESTIGATION_RUN_SCHEMA_VERSION.to_owned(),
        command_id: InvestigationCommandId::new(),
        run_id: created.run.id,
        expected_revision: created.run.revision,
        idempotency_key: "viewer-cancel".to_owned(),
        reason: "Try current scope control".to_owned(),
        command: InvestigationCommand::Cancel {},
    };
    let command_path = format!("/v1/investigation-runs/{}/commands", created.run.id);
    let (status, _) = call(
        fixture.router(),
        "POST",
        &command_path,
        Some(&viewer),
        Some(json!(request)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = call(fixture.router(), "GET", &path, Some(&fixture.auth), None).await;
    assert_eq!(status, StatusCode::OK);
    let controls: InvestigationRunControls = serde_json::from_value(body).unwrap();
    assert!(
        controls
            .commands
            .iter()
            .find(|item| item.kind == InvestigationRunCommandKind::Cancel)
            .unwrap()
            .allowed
    );
    assert_eq!(controls.budget_usage.settled, InvestigationBudgetCost::default());
    assert!(controls.receipt_replay_allowed);
    let (status, _) = call(
        fixture.router(),
        "POST",
        &command_path,
        Some(&fixture.auth),
        Some(json!(request)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut stale = request;
    stale.command_id = InvestigationCommandId::new();
    stale.idempotency_key = "new-key-stale-revision".to_owned();
    let (status, _) = call(
        fixture.router(),
        "POST",
        &command_path,
        Some(&fixture.auth),
        Some(json!(stale)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, body) = call(fixture.router(), "GET", &path, Some(&fixture.auth), None).await;
    assert_eq!(status, StatusCode::OK);
    let controls: InvestigationRunControls = serde_json::from_value(body).unwrap();
    assert_eq!(controls.run.status, InvestigationRunStatus::Cancelled);
    assert!(controls.receipt_replay_allowed);
    assert!(controls.commands.iter().all(
        |command| !command.allowed && command.blocked_reason == Some(InvestigationRunControlBlockReason::Terminal)
    ));
    let (status, body) = call(
        fixture.router(),
        "GET",
        &format!("/v1/investigation-runs/{}", created.run.id),
        Some(&fixture.auth),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("commands").is_none() && body.get("creation").is_none());
}

#[tokio::test]
#[ignore = "requires task-owned Docker PostgreSQL and real JWT authentication"]
async fn postgres_discovery_and_controls_fail_closed_for_revoked_scope_and_invalid_pairs() {
    let fixture = fixture().await;
    let created = fixture.create().await;
    let controls = format!("/v1/investigation-runs/{}/controls", created.run.id);
    for path in [&list_path(&fixture), &controls] {
        let (status, _) = call(fixture.router(), "GET", path, None, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let mut revoked = fixture.auth.clone();
        revoked.clusters.clear();
        let (status, _) = call(fixture.router(), "GET", path, Some(&revoked), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let mut foreign = fixture.auth.clone();
        foreign.tenant_id = TenantId::new();
        let (status, _) = call(fixture.router(), "GET", path, Some(&foreign), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
    for suffix in [
        "?start=2026-01-01T00:00:00Z",
        "?resource_kind=broker",
        "?resource_kind=consumer&resource_key=g",
        "?limit=51",
        "?cursor=malformed",
        "?unexpected=1",
        "?limit=0",
    ] {
        let (status, _) = call(
            fixture.router(),
            "GET",
            &format!("{}{suffix}", list_path(&fixture)),
            Some(&fixture.auth),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{suffix}");
    }
    sqlx::query("UPDATE clusters SET onboarding_state='offboarded' WHERE id=$1")
        .bind(created.run.initial_scope.cluster_id.as_uuid())
        .execute(&fixture.repository.pool)
        .await
        .unwrap();
    for path in [&list_path(&fixture), &controls] {
        let (status, _) = call(fixture.router(), "GET", path, Some(&fixture.auth), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
}
