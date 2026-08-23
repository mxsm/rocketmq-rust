// Copyright 2023 The RocketMQ Rust Authors
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use tokio::net::TcpListener;

use super::super::DefaultRequestProcessor;
use super::super::EndpointCompletionTestHook;
use super::super::MaintenanceConfig;
use super::super::TransportClient;
use super::super::TransportClientConfig;
use super::LifecycleTestBarrier;
use super::ShutdownFlight;
use super::ShutdownTargets;

#[cfg(test)]
mod runtime_test_support {
    use super::*;
    use rocketmq_runtime::RuntimeContext;

    pub(super) fn test_service_context(name: &'static str) -> ChildServiceContext {
        RuntimeContext::from_current(name).service_context("remoting-client-lifecycle-service")
    }
}

fn test_client(
    name: &'static str,
    idle_scan_interval: Option<Duration>,
) -> Arc<TransportClient<DefaultRequestProcessor>> {
    let config = TransportClientConfig {
        maintenance: MaintenanceConfig { idle_scan_interval },
        ..Default::default()
    };
    Arc::new(TransportClient::build_for_test(
        Arc::new(config),
        DefaultRequestProcessor,
        runtime_test_support::test_service_context(name),
    ))
}

#[tokio::test]
async fn lifecycle_starts_stopped() {
    let client = test_client("lifecycle-starts-stopped", None);

    assert_eq!(client.lifecycle_phase(), "stopped");
    assert!(client.worker_task_group().is_none());
}

#[tokio::test]
async fn start_without_idle_scan_publishes_one_background_task() {
    let client = test_client("lifecycle-start-one-task", None);

    let report = client.start().await.expect("start should succeed");

    assert_eq!(report.background_tasks_started, 1);
    assert!(!report.already_running);
    assert_eq!(client.lifecycle_phase(), "running");
    assert_eq!(
        client
            .background_task_group
            .lock()
            .as_ref()
            .expect("running lifecycle should publish its task group")
            .task_count(),
        1
    );
    assert!(client
        .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
        .await
        .is_healthy());
}

#[tokio::test]
async fn start_with_idle_scan_publishes_both_background_tasks() {
    let client = test_client("lifecycle-start-two-tasks", Some(Duration::from_secs(60)));

    let report = client.start().await.expect("start should succeed");

    assert_eq!(report.background_tasks_started, 2);
    assert_eq!(
        client
            .background_task_group
            .lock()
            .as_ref()
            .expect("running lifecycle should publish its task group")
            .task_count(),
        2
    );
    client.shutdown_now();
}

#[tokio::test]
async fn repeated_start_is_idempotent() {
    let client = test_client("lifecycle-repeated-start", Some(Duration::from_secs(60)));
    client.start().await.expect("initial start should succeed");
    let first_group_id = client
        .background_task_group
        .lock()
        .as_ref()
        .expect("background task group")
        .id();

    let report = client.start().await.expect("repeated start should succeed");

    assert!(report.already_running);
    assert_eq!(report.background_tasks_started, 0);
    assert_eq!(
        client
            .background_task_group
            .lock()
            .as_ref()
            .expect("background task group")
            .id(),
        first_group_id
    );
    client.shutdown_now();
}

#[tokio::test]
async fn first_background_spawn_failure_restores_stopped_state() {
    let client = test_client("lifecycle-first-spawn-failure", None);
    client.fail_background_spawn_after(0);

    assert!(client.start().await.is_err());

    assert_eq!(client.lifecycle_phase(), "stopped");
    assert!(client.background_task_group.lock().is_none());
    assert!(client.start().await.is_ok(), "retry must create a fresh generation");
    client.shutdown_now();
}

#[tokio::test]
async fn second_background_spawn_failure_rolls_back_only_the_current_generation() {
    let client = test_client("lifecycle-second-spawn-failure", Some(Duration::from_secs(60)));
    client.fail_background_spawn_after(1);

    assert!(client.start().await.is_err());

    assert_eq!(client.lifecycle_phase(), "stopped");
    assert!(client.background_task_group.lock().is_none());
    let retry = client.start().await.expect("retry should not see a partial start");
    assert_eq!(retry.background_tasks_started, 2);
    client.shutdown_now();
}

#[tokio::test]
async fn restart_uses_a_fresh_background_generation() {
    let client = test_client("lifecycle-restart", None);
    client.start().await.expect("first start should succeed");
    let first_group = client
        .background_task_group
        .lock()
        .as_ref()
        .expect("first task group")
        .id();
    client.shutdown_now();

    client.start().await.expect("restart should succeed");
    let second_group = client
        .background_task_group
        .lock()
        .as_ref()
        .expect("second task group")
        .id();

    assert_ne!(first_group, second_group);
    client.shutdown_now();
}

#[tokio::test]
async fn pre_start_worker_group_is_preserved_when_starting() {
    let client = test_client("lifecycle-pre-start-worker", None);
    client
        .spawn_worker_task("lifecycle.pre-start-worker", async {})
        .expect("pre-start worker should be accepted");
    let worker_group = client.worker_task_group().expect("pre-start worker group").id();

    client.start().await.expect("start should preserve pre-start workers");

    assert_eq!(
        client.worker_task_group().expect("worker task group").id(),
        worker_group
    );
    client.shutdown_now();
}

#[tokio::test]
async fn shutdown_requires_an_explicit_restart_before_worker_admission_reopens() {
    let client = test_client("lifecycle-worker-after-now", None);
    client
        .spawn_worker_task("lifecycle.first-worker", async {})
        .expect("first pre-start worker should be accepted");
    let first_group = client.worker_task_group().expect("first worker group").id();
    client.shutdown_now();

    assert!(
        client
            .spawn_worker_task("lifecycle.worker-before-restart", async {})
            .is_none(),
        "shutdown must close lazy worker admission"
    );
    client.start().await.expect("explicit restart should succeed");
    client
        .spawn_worker_task("lifecycle.second-worker", async {})
        .expect("worker after explicit restart should be accepted");
    let second_group = client.worker_task_group().expect("second worker group").id();

    assert_ne!(first_group, second_group);
    client.shutdown_now();
}

#[tokio::test]
async fn failed_start_keeps_initial_pre_start_worker_admission_open() {
    let client = test_client("lifecycle-pre-start-worker-failed-start", None);
    client
        .spawn_worker_task("lifecycle.pre-start-worker", async {})
        .expect("initial pre-start worker should be accepted");
    let worker_group = client.worker_task_group().expect("pre-start worker group").id();
    client.fail_background_spawn_after(0);

    assert!(client.start().await.is_err(), "injected start failure");
    assert_eq!(client.lifecycle_phase(), "stopped");
    assert_eq!(client.worker_task_group().expect("worker group").id(), worker_group);
    assert!(
        client
            .spawn_worker_task("lifecycle.pre-start-worker-after-failure", async {})
            .is_some(),
        "failed initial start must not close pre-start admission"
    );
    client.shutdown_now();
}

#[tokio::test]
async fn graceful_shutdown_returns_the_lifecycle_to_stopped() {
    let client = test_client("lifecycle-graceful-stopped", None);
    client.start().await.expect("start should succeed");

    let report = client
        .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;

    assert!(report.is_healthy(), "{report:?}");
    assert_eq!(client.lifecycle_phase(), "stopped");
    assert!(client.start().await.is_ok(), "graceful shutdown must allow restart");
    client.shutdown_now();
}

#[tokio::test]
async fn immediate_shutdown_returns_task_reports_without_drain_claims() {
    let client = test_client("lifecycle-immediate-report", None);
    client.start().await.expect("start should succeed");

    let report = client.shutdown_now();

    assert!(report.background.is_some());
    assert!(report.connections.is_empty());
    assert_eq!(client.lifecycle_phase(), "stopped");
}

#[tokio::test]
async fn concurrent_starts_publish_at_most_one_generation() {
    let client = test_client("lifecycle-concurrent-start", None);
    let first = client.start();
    let second = client.start();
    let (first, second) = tokio::join!(first, second);

    let reports = [first.expect("first start"), second.expect("second start")];
    assert_eq!(reports.iter().filter(|report| !report.already_running).count(), 1);
    assert_eq!(reports.iter().filter(|report| report.already_running).count(), 1);
    client.shutdown_now();
}

#[tokio::test]
async fn concurrent_start_and_graceful_shutdown_leave_a_restartable_client() {
    let client = test_client("lifecycle-concurrent-start-shutdown", None);
    let start = client.start();
    let shutdown = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)));
    let (start, shutdown) = tokio::join!(start, shutdown);

    assert!(start.is_ok());
    assert!(shutdown.is_healthy(), "{shutdown:?}");
    assert_eq!(client.lifecycle_phase(), "stopped");
    assert!(client.start().await.is_ok());
    client.shutdown_now();
}

#[tokio::test]
async fn concurrent_graceful_shutdown_is_idempotent() {
    let client = test_client("lifecycle-concurrent-graceful", None);
    client.start().await.expect("start should succeed");
    let first = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)));
    let second = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)));
    let (first, second) = tokio::join!(first, second);

    assert!(first.is_healthy(), "{first:?}");
    assert!(second.is_healthy(), "{second:?}");
    assert_eq!(client.lifecycle_phase(), "stopped");
}

#[tokio::test]
async fn legacy_shutdown_wrappers_preserve_restartable_behavior() {
    let client = test_client("lifecycle-legacy-wrappers", None);
    client.start().await.expect("start should succeed");

    assert!(client.shutdown_with_report(Duration::from_secs(1)).await.is_healthy());
    assert!(client.start().await.is_ok());
    client.shutdown();
    assert_eq!(client.lifecycle_phase(), "stopped");
}

#[tokio::test]
async fn stopping_rejects_replacement_worker_group() {
    let client = test_client("lifecycle-stopping-rejects-worker", None);
    client.start().await.expect("start should succeed");
    let barrier = LifecycleTestBarrier::new();
    client.install_shutdown_owner_started_barrier(barrier.clone());

    let shutdown = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)));
    let observe_stopping = async {
        barrier.wait_until_entered().await;
        assert_eq!(client.lifecycle_phase(), "stopping");
        assert!(client
            .spawn_worker_task("lifecycle.worker-while-stopping", async {})
            .is_none());
        barrier.release();
    };
    let (report, ()) = tokio::join!(shutdown, observe_stopping);

    assert!(report.is_healthy(), "{report:?}");
    assert_eq!(client.lifecycle_phase(), "stopped");
}

#[tokio::test]
async fn aborting_a_shutdown_participant_does_not_block_owner_completion_or_restart() {
    let client = test_client("lifecycle-shutdown-participant-abort", None);
    client.start().await.expect("start should succeed");
    let owner_started = LifecycleTestBarrier::new();
    let participant_joined = LifecycleTestBarrier::new();
    client.install_shutdown_owner_started_barrier(owner_started.clone());
    client.install_shutdown_participant_joined_barrier(participant_joined.clone());

    let task_group = client.service_context.task_group().clone();
    let owner_client = Arc::clone(&client);
    let (_owner_id, owner) = task_group
        .spawn_service_with_handle("lifecycle.shutdown-owner", async move {
            let _ = owner_client
                .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
                .await;
        })
        .expect("owner task should spawn");
    owner_started.wait_until_entered().await;

    let participant_client = Arc::clone(&client);
    let (participant_id, participant) = task_group
        .spawn_service_with_handle("lifecycle.shutdown-participant", async move {
            let _ = participant_client
                .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
                .await;
        })
        .expect("participant task should spawn");
    participant_joined.wait_until_entered().await;
    assert!(task_group.abort_task(participant_id));
    assert!(participant.await.is_err(), "participant task must be aborted");

    owner_started.release();
    owner.await.expect("owner must finish shutdown");
    assert_eq!(client.lifecycle_phase(), "stopped");
    assert!(client.start().await.is_ok(), "owner completion allows restart");
    client.shutdown_now();
}

#[tokio::test]
async fn aborting_the_shutdown_owner_publishes_immediate_fallback_and_shutdown_now_recovers() {
    let client = test_client("lifecycle-shutdown-owner-abort", None);
    client.start().await.expect("start should succeed");
    let owner_started = LifecycleTestBarrier::new();
    client.install_shutdown_owner_started_barrier(owner_started.clone());

    let task_group = client.service_context.task_group().clone();
    let owner_client = Arc::clone(&client);
    let (owner_id, owner) = task_group
        .spawn_service_with_handle("lifecycle.aborted-shutdown-owner", async move {
            let _ = owner_client
                .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
                .await;
        })
        .expect("owner task should spawn");
    owner_started.wait_until_entered().await;
    assert!(task_group.abort_task(owner_id));
    assert!(owner.await.is_err(), "owner task must be aborted");

    assert_eq!(client.lifecycle_phase(), "stopped");
    let recovery = client.shutdown_now();
    assert!(recovery.connections.is_empty());
    assert!(client.start().await.is_ok(), "fallback must leave a restartable client");
    client.shutdown_now();
}

#[tokio::test]
async fn shutdown_flight_report_survives_a_publication_before_waiter_subscription() {
    let (report_tx, _) = tokio::sync::watch::channel(None);
    let flight = ShutdownFlight {
        targets: ShutdownTargets {
            cancellation: None,
            background_task_group: None,
            worker_task_group: None,
        },
        report_tx,
    };
    let expected = super::super::ClientShutdownReport::default();

    flight.complete(expected.clone());
    let received = flight.wait_report().await;

    assert!(received.is_healthy());
    assert_eq!(received.connections.len(), expected.connections.len());
}

#[tokio::test]
async fn phase_finishes_before_report_but_participants_wait_for_owner_publication() {
    let client = test_client("lifecycle-phase-before-report", None);
    client.start().await.expect("start should succeed");
    let phase_finished = LifecycleTestBarrier::new();
    let participant_joined = LifecycleTestBarrier::new();
    let participant_finished = Arc::new(AtomicBool::new(false));
    client.install_shutdown_phase_finished_before_report_barrier(phase_finished.clone());
    client.install_shutdown_participant_joined_barrier(participant_joined.clone());

    let first = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)));
    let participant_finished_for_second = Arc::clone(&participant_finished);
    let second = async {
        let report = client
            .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        participant_finished_for_second.store(true, Ordering::Release);
        report
    };
    let coordinate = async {
        participant_joined.wait_until_entered().await;
        phase_finished.wait_until_entered().await;
        assert_eq!(client.lifecycle_phase(), "stopped");
        assert!(!participant_finished.load(Ordering::Acquire));
        phase_finished.release();
    };
    let (first, second, ()) = tokio::join!(first, second, coordinate);

    assert!(first.is_healthy(), "{first:?}");
    assert!(second.is_healthy(), "{second:?}");
    assert!(participant_finished.load(Ordering::Acquire));
    assert!(
        client.start().await.is_ok(),
        "report publication releases the next start"
    );
    client.shutdown_now();
}

#[tokio::test]
async fn immediate_participant_does_not_replace_the_graceful_owner_report() {
    let client = test_client("lifecycle-mixed-shutdown-report", None);
    client.start().await.expect("start should succeed");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let target = CheetahString::from_string(listener.local_addr().expect("listener address").to_string());
    assert!(
        client.create_client(&target, Duration::from_secs(1)).await.is_some(),
        "session should be registered before shutdown"
    );
    let connections_taken = LifecycleTestBarrier::new();
    let participant_joined = LifecycleTestBarrier::new();
    client.install_shutdown_connections_taken_barrier(connections_taken.clone());
    client.install_shutdown_participant_joined_barrier(participant_joined.clone());

    let first = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)));
    let second = client.shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)));
    let now = async {
        participant_joined.wait_until_entered().await;
        connections_taken.wait_until_entered().await;
        let report = client.shutdown_now();
        connections_taken.release();
        report
    };
    let (first, second, now) = tokio::join!(first, second, now);

    assert_eq!(
        first.background.as_ref().map(rocketmq_runtime::ShutdownReport::to_json),
        second
            .background
            .as_ref()
            .map(rocketmq_runtime::ShutdownReport::to_json)
    );
    assert_eq!(
        first.workers.as_ref().map(rocketmq_runtime::ShutdownReport::to_json),
        second.workers.as_ref().map(rocketmq_runtime::ShutdownReport::to_json)
    );
    assert_eq!(first.connections.len(), 1);
    assert_eq!(second.connections.len(), 1);
    assert_eq!(first.connections[0].addr, target);
    assert_eq!(second.connections[0].addr, target);
    assert!(now.connections.is_empty());
    assert_eq!(client.lifecycle_phase(), "stopped");
}

#[tokio::test]
async fn graceful_shutdown_clears_unresolved_flights_and_restart_can_connect() {
    let client = test_client("lifecycle-graceful-clears-flights", None);
    client.start().await.expect("start should succeed");
    let unresolved = CheetahString::from_static_str("127.0.0.1:65535");
    let (_, leader) = client.connection_registry.acquire_flight(unresolved, None);
    assert!(leader);
    assert_eq!(client.connection_registry.flight_count(), 1);

    let report = client
        .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    assert!(report.is_healthy(), "{report:?}");
    assert_eq!(client.connection_registry.flight_count(), 0);

    client.start().await.expect("restart should succeed");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let target = CheetahString::from_string(listener.local_addr().expect("listener address").to_string());
    assert!(
        client.create_client(&target, Duration::from_secs(1)).await.is_some(),
        "restart should accept a fresh connection"
    );
    client.shutdown_now();
}

#[tokio::test]
async fn immediate_shutdown_fences_old_connect_completion_from_registry_commit() {
    let client = test_client("lifecycle-now-fences-connect", None);
    let worker_owner = client.capture_worker_task_owner().expect("worker owner should exist");
    let fence = worker_owner.commit_fence();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let target = CheetahString::from_string(listener.local_addr().expect("listener address").to_string());
    let hook = EndpointCompletionTestHook::new();
    client.install_connect_completion_test_hook(hook.clone());

    let connect = client.connect_endpoint_until(
        &target,
        None,
        None,
        crate::deadline::RequestDeadline::after(Duration::from_secs(1)),
        &fence,
    );
    let cancel_and_release = async {
        hook.wait_until_entered().await;
        client.shutdown_now();
        hook.release();
    };
    let (connect, ()) = tokio::join!(connect, cancel_and_release);

    assert!(connect.is_err(), "cancelled worker token must reject commit");
    assert!(client.connection_registry.is_empty());
    assert_eq!(client.connection_registry.flight_count(), 0);
}

#[tokio::test]
async fn shutdown_and_restart_fence_a_leader_before_it_can_spawn_a_worker() {
    let client = test_client("lifecycle-leader-before-spawn-fence", None);
    let barrier = LifecycleTestBarrier::new();
    client.install_leader_before_worker_spawn_test_hook(barrier.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
    let target = CheetahString::from_string(listener.local_addr().expect("listener address").to_string());

    let connect = client.create_client(&target, Duration::from_secs(1));
    let restart = async {
        barrier.wait_until_entered().await;
        client.shutdown_now();
        client.start().await.expect("restart should succeed");
        barrier.release();
    };
    let (connected, ()) = tokio::join!(connect, restart);

    assert!(connected.is_none(), "old worker owner must not spawn after restart");
    assert!(client.connection_registry.is_empty());
    assert_eq!(client.connection_registry.flight_count(), 0);
    client.shutdown_now();
}

#[tokio::test]
async fn expired_graceful_deadline_bounds_multiple_connection_closes() {
    let client = test_client("lifecycle-shared-close-deadline", None);
    let first_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind first listener");
    let second_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind second listener");
    let first = CheetahString::from_string(first_listener.local_addr().expect("first address").to_string());
    let second = CheetahString::from_string(second_listener.local_addr().expect("second address").to_string());
    assert!(client.create_client(&first, Duration::from_secs(1)).await.is_some());
    assert!(client.create_client(&second, Duration::from_secs(1)).await.is_some());

    let started = Instant::now();
    let report = client.shutdown_graceful(ShutdownDeadline::at(Instant::now())).await;

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "one expired deadline bounds every close"
    );
    assert_eq!(report.connections.len(), 2);
    assert!(report
        .connections
        .iter()
        .all(|connection| connection.report.timed_out > 0));
}

#[tokio::test]
async fn shutdown_observes_a_real_starting_phase_before_second_spawn() {
    let client = test_client("lifecycle-starting-shutdown-barrier", Some(Duration::from_secs(60)));
    let barrier = LifecycleTestBarrier::new();
    client.install_start_between_background_spawns_barrier(barrier.clone());

    let start = client.start();
    let shutdown = async {
        barrier.wait_until_entered().await;
        assert_eq!(client.lifecycle_phase(), "starting");
        let report = client
            .shutdown_graceful(ShutdownDeadline::after(Duration::from_secs(1)))
            .await;
        barrier.release();
        report
    };
    let (start, shutdown) = tokio::join!(start, shutdown);

    assert!(
        start
            .expect("interrupted start returns an idempotent report")
            .already_running
    );
    assert!(shutdown.is_healthy(), "{shutdown:?}");
    assert_eq!(client.lifecycle_phase(), "stopped");
}
