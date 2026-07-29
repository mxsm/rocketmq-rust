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

use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::ImpactScope;
use rocketmq_sre_contracts::ReconcileEffectState;
use serde_json::json;

use super::*;
use crate::drivers::test_support;

struct FakeCollectorClient {
    state: Mutex<TelemetryCollectorRestartState>,
    writes: Mutex<Vec<TelemetryCollectorRestartOneWrite>>,
}

impl FakeCollectorClient {
    fn healthy() -> Self {
        Self {
            state: Mutex::new(TelemetryCollectorRestartState {
                pod_uid: "collector-uid-before".to_owned(),
                pod_ready: true,
                deployment_ready: true,
                replacement_ready: false,
                exporter_connected: true,
                queue_healthy: true,
                data_plane_unaffected: true,
                active_pod: "otel-collector-before".to_owned(),
                last_operation_id: None,
                last_execution_id: None,
                last_plan_step_id: None,
            }),
            writes: Mutex::new(Vec::new()),
        }
    }
}

impl TelemetryCollectorRestartClient for FakeCollectorClient {
    fn telemetry_collector_restart_state<'a>(
        &'a self,
        _namespace: &'a str,
        _pod: &'a str,
        _pipeline: &'a str,
    ) -> DriverFuture<'a, TelemetryCollectorRestartState> {
        Box::pin(async move { Ok(self.state.lock().expect("collector state lock").clone()) })
    }

    fn restart_one_telemetry_collector<'a>(
        &'a self,
        request: &'a TelemetryCollectorRestartOneWrite,
    ) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("collector state lock");
            if state.pod_uid != request.expected_uid {
                return Err(ExecutionAgentError::DriverFailed);
            }
            state.pod_uid = "collector-uid-after".to_owned();
            state.active_pod = "otel-collector-after".to_owned();
            state.replacement_ready = true;
            state.last_operation_id = Some(request.operation_id.clone());
            state.last_execution_id = Some(request.execution_id.to_string());
            state.last_plan_step_id = Some(request.plan_step_id.to_string());
            self.writes.lock().expect("collector write lock").push(request.clone());
            Ok(())
        })
    }
}

#[tokio::test]
async fn healthy_collector_is_ready_for_one_typed_restart() {
    let handler = TelemetryCollectorRestartOneHandler::new(Arc::new(FakeCollectorClient::healthy()));

    let result = handler.read_state(&read_request()).await.expect("healthy precheck");

    assert!(result.ready);
    assert!(result.reason_codes.is_empty());
    assert_eq!(result.resource_conditions.get("collector_ready"), Some(&true));
    assert_eq!(result.resource_conditions.get("exporter_connected"), Some(&true));
}

#[tokio::test]
async fn precheck_fails_closed_when_pipeline_or_data_plane_health_is_unknown() {
    let client = Arc::new(FakeCollectorClient::healthy());
    {
        let mut state = client.state.lock().expect("collector state lock");
        state.exporter_connected = false;
        state.queue_healthy = false;
        state.data_plane_unaffected = false;
    }
    let handler = TelemetryCollectorRestartOneHandler::new(client);

    let result = handler.read_state(&read_request()).await.expect("failed precheck");

    assert!(!result.ready);
    assert_eq!(
        result.reason_codes,
        [
            "collector_exporter_not_connected",
            "collector_queue_unhealthy",
            "rocketmq_data_plane_not_proven_healthy"
        ]
    );
}

#[tokio::test]
async fn unknown_pipeline_is_rejected_without_mutation() {
    let client = Arc::new(FakeCollectorClient::healthy());
    let handler = TelemetryCollectorRestartOneHandler::new(Arc::clone(&client));
    let mut request = step_request();
    request.parameters["pipeline"] = json!("profiles");
    request.intent.step.parameters = request.parameters.clone();

    assert!(handler.dispatch(&request, "collector-invalid").await.is_err());
    assert!(client.writes.lock().expect("collector write lock").is_empty());
}

#[tokio::test]
async fn apply_reconcile_and_manual_takeover_bind_one_execution_step() {
    let client = Arc::new(FakeCollectorClient::healthy());
    let handler = TelemetryCollectorRestartOneHandler::new(Arc::clone(&client));
    let request = step_request();
    let read = read_request_for_step(&request);

    handler
        .dispatch(&request, "collector-forward")
        .await
        .expect("typed Collector restart");
    let writes = client.writes.lock().expect("collector write lock");
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].expected_uid, "collector-uid-before");
    assert_eq!(writes[0].pipeline, "combined");
    drop(writes);

    let verified = handler
        .reconcile(&read, Some("collector-forward"))
        .await
        .expect("Collector replacement verification");
    assert_eq!(verified.state, ReconcileEffectState::Applied);

    let compensation = handler
        .compensate(&request, "collector-compensation")
        .await
        .expect("manual takeover result");
    assert_eq!(
        compensation.outcome_code,
        "telemetry_collector_manual_takeover_required"
    );
    assert_eq!(client.writes.lock().expect("collector write lock").len(), 1);
}

fn read_request() -> rocketmq_sre_contracts::AgentReadRequest {
    test_support::read_request(
        ExecutionAction::TelemetryCollectorRestartOne,
        "pod/observability/otel-collector-before",
        parameters(),
    )
}

fn read_request_for_step(
    request: &rocketmq_sre_contracts::AgentStepRequest,
) -> rocketmq_sre_contracts::AgentReadRequest {
    let mut read = read_request();
    read.execution_id = request.intent.execution_id;
    read.plan_step_id = request.intent.step.id;
    read
}

fn step_request() -> rocketmq_sre_contracts::AgentStepRequest {
    test_support::step_request(
        ExecutionAction::TelemetryCollectorRestartOne,
        "pod/observability/otel-collector-before",
        parameters(),
        ImpactScope::SingleInstance,
        &["expected_uid", "pipeline_health"],
    )
}

fn parameters() -> serde_json::Value {
    json!({
        "namespace": "observability",
        "pod": "otel-collector-before",
        "expected_uid": "collector-uid-before",
        "pipeline": "combined"
    })
}
