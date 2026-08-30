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

use std::sync::Mutex;

use rocketmq_sre_contracts::ImpactScope;
use rocketmq_sre_contracts::ReconcileEffectState;
use serde_json::json;

use super::*;
use crate::drivers::test_support;

struct FakeProxyImageCanaryClient {
    state: Mutex<ProxyImageCanaryState>,
    previous_digest: Mutex<Option<String>>,
}

impl FakeProxyImageCanaryClient {
    fn new() -> Self {
        Self {
            state: Mutex::new(ProxyImageCanaryState {
                generation: 9,
                observed_generation: 9,
                image_digest: digest('a'),
                ready_canary_replicas: 0,
                old_replicas_unchanged: true,
                pdb_healthy: true,
                slo_healthy: true,
                last_operation_id: None,
            }),
            previous_digest: Mutex::new(None),
        }
    }
}

impl ProxyImageCanaryClient for FakeProxyImageCanaryClient {
    fn proxy_image_canary_state<'a>(
        &'a self,
        _namespace: &'a str,
        _workload: &'a str,
        _container: &'a str,
    ) -> DriverFuture<'a, ProxyImageCanaryState> {
        Box::pin(async move { Ok(self.state.lock().expect("state lock").clone()) })
    }

    fn rollout_proxy_image_canary<'a>(&'a self, request: &'a ProxyImageCanaryWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            if state.generation != request.expected_generation || request.canary_replicas != 1 {
                return Err(ExecutionAgentError::DriverFailed);
            }
            *self.previous_digest.lock().expect("previous digest lock") = Some(state.image_digest.clone());
            state.generation += 1;
            state.observed_generation = state.generation;
            state.image_digest.clone_from(&request.image_digest);
            state.ready_canary_replicas = 1;
            state.last_operation_id = Some(request.operation_id.clone());
            Ok(())
        })
    }

    fn restore_proxy_image<'a>(&'a self, request: &'a ProxyImageCanaryRestore) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let previous = self
                .previous_digest
                .lock()
                .expect("previous digest lock")
                .clone()
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let mut state = self.state.lock().expect("state lock");
            state.generation += 1;
            state.observed_generation = state.generation;
            state.image_digest = previous;
            state.ready_canary_replicas = 0;
            state.last_operation_id = Some(request.operation_id.clone());
            Ok(())
        })
    }
}

#[tokio::test]
async fn precheck_requires_pdb_slo_and_idle_generation() {
    let client = Arc::new(FakeProxyImageCanaryClient::new());
    {
        let mut state = client.state.lock().expect("state lock");
        state.observed_generation = 8;
        state.pdb_healthy = false;
        state.slo_healthy = false;
    }
    let result = ProxyImageCanaryHandler::new(client)
        .read_state(&read_request(parameters()))
        .await
        .expect("precheck");
    assert!(!result.ready);
    assert!(
        result
            .reason_codes
            .contains(&"proxy_rollout_already_in_progress".to_owned())
    );
    assert!(result.reason_codes.contains(&"proxy_pdb_not_healthy".to_owned()));
    assert!(result.reason_codes.contains(&"proxy_slo_not_healthy".to_owned()));
}

#[tokio::test]
async fn digest_canary_apply_verify_and_rollback_are_closed() {
    let client = Arc::new(FakeProxyImageCanaryClient::new());
    let handler = ProxyImageCanaryHandler::new(Arc::clone(&client));
    let step = step_request(parameters());

    assert_eq!(
        handler
            .dispatch(&step, "canary-forward")
            .await
            .expect("dispatch")
            .outcome_code,
        "proxy_image_canary_started"
    );
    assert_eq!(
        handler
            .reconcile(&read_request(parameters()), Some("canary-forward"))
            .await
            .expect("reconcile")
            .state,
        ReconcileEffectState::Applied
    );
    let conditions = handler
        .read_state(&read_request(parameters()))
        .await
        .expect("conditions");
    assert_eq!(conditions.resource_conditions.get("canary_ready"), Some(&true));
    assert_eq!(
        conditions.resource_conditions.get("old_replicas_unchanged"),
        Some(&true)
    );

    assert_eq!(
        handler
            .compensate(&step, "canary-rollback")
            .await
            .expect("rollback")
            .outcome_code,
        "proxy_image_canary_rolled_back"
    );
    assert_eq!(client.state.lock().expect("state lock").image_digest, digest('a'));
}

#[tokio::test]
async fn tags_repositories_and_multiple_canaries_fail_closed() {
    let handler = ProxyImageCanaryHandler::new(Arc::new(FakeProxyImageCanaryClient::new()));
    for (image, replicas) in [
        ("proxy:latest".to_owned(), 1),
        ("registry.example/proxy@sha256:deadbeef".to_owned(), 1),
        (digest('b'), 2),
    ] {
        let value = json!({
            "namespace": "rocketmq",
            "workload": "proxy",
            "container": "proxy",
            "expected_generation": 9,
            "image_digest": image,
            "canary_replicas": replicas
        });
        assert!(
            !handler
                .read_state(&read_request(value))
                .await
                .expect("bounded precheck")
                .ready
        );
    }
}

fn parameters() -> serde_json::Value {
    json!({
        "namespace": "rocketmq",
        "workload": "proxy",
        "container": "proxy",
        "expected_generation": 9,
        "image_digest": digest('b'),
        "canary_replicas": 1
    })
}

fn read_request(parameters: serde_json::Value) -> AgentReadRequest {
    test_support::read_request(
        ExecutionAction::ProxyRolloutImageCanary,
        "deployment/rocketmq/proxy",
        parameters,
    )
}

fn step_request(parameters: serde_json::Value) -> AgentStepRequest {
    test_support::step_request(
        ExecutionAction::ProxyRolloutImageCanary,
        "deployment/rocketmq/proxy",
        parameters,
        ImpactScope::OneReplica,
        &["previous_image_digest", "expected_generation"],
    )
}

fn digest(fill: char) -> String {
    format!("sha256:{}", fill.to_string().repeat(64))
}
