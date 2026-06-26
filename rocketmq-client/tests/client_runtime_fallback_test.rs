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

use std::sync::mpsc;
use std::time::Duration;

use rocketmq_client_rust::client_runtime_fallback_snapshot;
use rocketmq_client_rust::reset_client_runtime_fallback_for_diagnostics;
use rocketmq_client_rust::spawn_client_runtime_probe_task;
use rocketmq_client_rust::ClientSharedFallbackLifecycleState;

#[test]
fn fallback_runtime_supports_no_ambient_callers_and_reset_diagnostics() {
    let _ = reset_client_runtime_fallback_for_diagnostics(Duration::from_secs(1))
        .expect("initial fallback reset should not fail");

    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();

    let handle = spawn_client_runtime_probe_task("client-fallback-compat-test", async move {
        let thread_name = std::thread::current().name().unwrap_or_default().to_string();
        started_tx.send(thread_name).expect("started receiver should be alive");
        let _ = finish_rx.await;
    })
    .expect("fallback task should spawn without an ambient Tokio runtime");

    let thread_name = started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("fallback task should start");
    assert_eq!(thread_name, "rocketmq-client-fallback");

    let active = client_runtime_fallback_snapshot().expect("fallback snapshot should exist");
    assert_eq!(active.boundary, "rocketmq-client.shared-fallback-runtime");
    assert_eq!(active.compatibility, "shared fallback runtime compatibility boundary");
    assert!(active.prefers_injected_runtime);
    assert_eq!(active.state, ClientSharedFallbackLifecycleState::Active);
    assert!(active.runtime_available);
    assert!(active.active_tasks >= 1);
    let first_generation = active.runtime_generation;

    let reset_error = reset_client_runtime_fallback_for_diagnostics(Duration::from_millis(20))
        .expect_err("reset must reject an active fallback lease");
    assert!(reset_error.to_string().contains("leases are active"));

    finish_tx.send(()).expect("fallback task should still be waiting");
    assert!(handle.wait_finished(Duration::from_secs(2)));

    let report = reset_client_runtime_fallback_for_diagnostics(Duration::from_secs(1))
        .expect("fallback reset should complete")
        .expect("reset should return a shutdown report for the active fallback runtime");
    assert!(report.is_healthy(), "{}", report.to_json());

    let reset = client_runtime_fallback_snapshot().expect("fallback snapshot should exist after reset");
    assert_eq!(reset.state, ClientSharedFallbackLifecycleState::Uninitialized);
    assert!(!reset.runtime_available);

    let (recreated_tx, recreated_rx) = mpsc::channel();
    let recreated_handle = spawn_client_runtime_probe_task("client-fallback-recreate-test", async move {
        recreated_tx.send(()).expect("recreated receiver should be alive");
    })
    .expect("reset fallback runtime should accept no-ambient callers again");

    recreated_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("recreated fallback task should run");
    assert!(recreated_handle.wait_finished(Duration::from_secs(2)));

    let recreated = client_runtime_fallback_snapshot().expect("fallback snapshot should exist after recreate");
    assert!(recreated.runtime_available);
    assert!(
        recreated.runtime_generation > first_generation,
        "reset should force a new fallback runtime generation"
    );

    let _ = reset_client_runtime_fallback_for_diagnostics(Duration::from_secs(1))
        .expect("cleanup fallback reset should complete");
}
