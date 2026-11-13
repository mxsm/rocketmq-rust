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
#[ignore = "requires disposable Docker PostgreSQL and real durable connector channel"]
async fn postgres_dispatches_history_instant_context_with_one_shared_read_and_real_run_revisions() {
    let (fixture, principal, session) = prepared().await;
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("d15-dispatch-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        let mut after = 0;
        let mut queries = Vec::new();
        for _ in 0..3 {
            let envelope = next_query(&fixture, &principal, session, after).await;
            assert!(envelope.query.resource.ends_with("/broker-a"));
            assert_eq!(envelope.query.time_range, fixture.request.scope.time_range);
            let snapshot = healthy_snapshot(&envelope.query);
            respond(&fixture, &principal, &envelope, snapshot).await;
            after = envelope.sequence;
            queries.push(envelope.query);
        }
        queries
    };
    let (result, queries) = tokio::time::timeout(Duration::from_secs(15), async { tokio::join!(drive, wire) })
        .await
        .unwrap();
    result.unwrap();
    assert_eq!(
        queries
            .iter()
            .filter(|query| query.resource == "store-trend/broker-a")
            .count(),
        1
    );
    assert_eq!(
        queries
            .iter()
            .map(|query| query.query_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        queries
            .iter()
            .map(|query| query.correlation_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    let saved = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, created.run.id)
        .await
        .unwrap();
    assert_eq!(saved.status, InvestigationRunStatus::Inconclusive, "{saved:?}");
    assert!(saved.pack_results.iter().all(|result| result.partial));
    assert_eq!(saved.pack_results.len(), 2);
    assert_eq!(saved.assessments.len(), 1);
    assert_eq!(saved.evidence_ids.len(), 3);
    assert!(saved.assessments[0].diagnosis_revision_id.is_none());
    assert!(saved.assessments[0].run_revision_id.is_some());
    assert_eq!(charged_reads(&saved), 3);
    for result in &saved.pack_results {
        let path = format!(
            "/v1/investigation-runs/{}/revisions/{}",
            saved.id, result.run_revision_id
        );
        let (status, body) = call(fixture.router(), "GET", &path, Some(&fixture.auth), None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let revision: SavedInvestigationRunRevision = serde_json::from_value(body).unwrap();
        assert_eq!(revision.pack_result().unwrap(), *result);
        let mut foreign = fixture.auth.clone();
        foreign.tenant_id = TenantId::new();
        assert!(
            !call(fixture.router(), "GET", &path, Some(&foreign), None)
                .await
                .0
                .is_success()
        );
    }
    let immutable = fixture
        .state
        .investigation_runs
        .create(&fixture.auth, fixture.request.scope.investigation_id, &fixture.request)
        .await
        .unwrap();
    assert!(immutable.replayed);
    assert_eq!(immutable.run, created.run);
    fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), saved.id)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .state
            .investigation_runs
            .get(&fixture.auth, saved.id)
            .await
            .unwrap(),
        saved
    );
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and real durable connector channel"]
async fn postgres_rejects_wrong_query_resource_and_preserves_unknown_reservation() {
    let (fixture, principal, session) = prepared().await;
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("d15-binding-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        let envelope = next_query(&fixture, &principal, session, 0).await;
        let mut snapshot = healthy_snapshot(&envelope.query);
        snapshot.resource = "store-pressure/other-broker".to_owned();
        snapshot.content_hash = snapshot.compute_content_hash().unwrap();
        respond(&fixture, &principal, &envelope, snapshot).await;
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(8), async { tokio::join!(drive, wire) })
        .await
        .unwrap();
    assert!(result.is_err());
    let run = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, created.run.id)
        .await
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::NeedsEvidence);
    assert_eq!(run.stop_reason, Some(InvestigationStopReason::SourceUnavailable));
    assert!(run.evidence_ids.is_empty());
    assert!(run.pack_results.is_empty());
    let attempt = &run.steps[0].attempts[0];
    assert!(attempt.result.is_none());
    assert_eq!(
        attempt.interruption.as_ref().unwrap().reason,
        InvestigationInterruptionReason::SourceOutcomeUnknown
    );
    assert_eq!(charged_reads(&run), 1);
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and real durable connector channel"]
async fn postgres_owner_shutdown_drops_the_remote_wait_and_does_not_refund_or_redispatch() {
    let (fixture, principal, session) = prepared().await;
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("d15-cancel-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        let envelope = next_query(&fixture, &principal, session, 0).await;
        owner.task_group().cancel();
        envelope
    };
    let (result, _) = tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(drive, wire) })
        .await
        .unwrap();
    assert!(result.is_err());
    let run = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, created.run.id)
        .await
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::NeedsEvidence);
    assert!(run.pack_results.is_empty());
    assert_eq!(
        run.steps[0].attempts[0].interruption.as_ref().unwrap().reason,
        InvestigationInterruptionReason::OwnerShutdown
    );
    assert!(run.steps[0].attempts[0].result.is_none());
    assert_eq!(charged_reads(&run), 1);
    let another = RuntimeContext::from_current("d15-next-owner").service_context("dispatch");
    fixture
        .state
        .investigation_runs
        .dispatch_one(another.task_group(), run.id)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .state
            .investigation_runs
            .get(&fixture.auth, run.id)
            .await
            .unwrap(),
        run
    );
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and real durable connector channel"]
async fn postgres_pause_during_a_remote_read_drains_to_paused_with_unknown_cost() {
    let (fixture, principal, session) = prepared().await;
    let created = fixture.create().await;
    let owner = RuntimeContext::from_current("d15-pause-regression").service_context("dispatch");
    let drive = fixture
        .state
        .investigation_runs
        .dispatch_one(owner.task_group(), created.run.id);
    let wire = async {
        next_query(&fixture, &principal, session, 0).await;
        let running = fixture
            .state
            .investigation_runs
            .get(&fixture.auth, created.run.id)
            .await
            .unwrap();
        let request = command(&running, InvestigationCommand::Pause {}, "pause-read");
        fixture
            .state
            .investigation_runs
            .command(&fixture.auth, running.id, &request)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(6), async { tokio::join!(drive, wire) })
        .await
        .unwrap();
    assert!(result.is_err());
    let run = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, created.run.id)
        .await
        .unwrap();
    assert_eq!(run.status, InvestigationRunStatus::Paused);
    assert_eq!(run.in_flight_count(), 0);
    assert_eq!(charged_reads(&run), 1);
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and owned scheduled dispatcher"]
async fn postgres_managed_dispatch_shutdown_awaits_remote_wait_cleanup_and_ignores_late_delivery() {
    let (fixture, principal, session) = prepared().await;
    let created = fixture.create().await;
    let runtime = RuntimeContext::from_current("d15-managed-dispatch-lifecycle");
    let owner = runtime.service_context("dispatch");
    fixture.state.investigation_runs.schedule_dispatch(&owner).unwrap();
    let envelope = tokio::time::timeout(Duration::from_secs(10), next_query(&fixture, &principal, session, 0))
        .await
        .unwrap();
    owner.task_group().cancel();
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        owner.task_group().shutdown(Duration::from_secs(4)),
    )
    .await
    .unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
    let stopped = fixture
        .state
        .investigation_runs
        .get(&fixture.auth, created.run.id)
        .await
        .unwrap();
    assert_eq!(stopped.status, InvestigationRunStatus::NeedsEvidence);
    assert_eq!(stopped.in_flight_count(), 0);
    assert_eq!(
        stopped.steps[0].attempts[0].interruption.as_ref().unwrap().reason,
        InvestigationInterruptionReason::OwnerShutdown
    );
    assert_eq!(charged_reads(&stopped), 1);
    assert!(stopped.evidence_ids.is_empty());
    // This only acknowledges that the Connector later completed its fixed read.
    // The closed owner's result cannot re-enter the run or release its charge.
    respond(&fixture, &principal, &envelope, healthy_snapshot(&envelope.query)).await;
    assert_eq!(
        fixture
            .state
            .investigation_runs
            .get(&fixture.auth, stopped.id)
            .await
            .unwrap(),
        stopped
    );
}
