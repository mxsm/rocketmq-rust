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

use chrono::TimeDelta;
use chrono::Utc;
use rocketmq_sre_contracts::ImpactScope;
use rocketmq_sre_contracts::ReconcileEffectState;
use serde_json::json;

use super::*;
use crate::drivers::test_support;

struct FakeCredentialRotationClient {
    state: Mutex<CredentialRotationState>,
    observed_reference: Mutex<Option<String>>,
}

impl FakeCredentialRotationClient {
    fn new() -> Self {
        Self {
            state: Mutex::new(CredentialRotationState {
                active_version: "credential-v1".to_owned(),
                retiring_version: None,
                active_healthy: true,
                candidate_probe_healthy: false,
                overlap_deadline: None,
                last_operation_id: None,
            }),
            observed_reference: Mutex::new(None),
        }
    }
}

impl CredentialRotationClient for FakeCredentialRotationClient {
    fn credential_rotation_state<'a>(&'a self, _credential_set: &'a str) -> DriverFuture<'a, CredentialRotationState> {
        Box::pin(async move { Ok(self.state.lock().expect("state lock").clone()) })
    }

    fn begin_credential_overlap<'a>(&'a self, request: &'a CredentialOverlapWrite) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            if state.active_version != request.active_version || state.retiring_version.is_some() {
                return Err(ExecutionAgentError::DriverFailed);
            }
            *self.observed_reference.lock().expect("reference lock") = Some(request.candidate_secret_ref.clone());
            state.retiring_version = Some(state.active_version.clone());
            state.active_version.clone_from(&request.candidate_version);
            state.candidate_probe_healthy = true;
            state.overlap_deadline = Some(Utc::now() + TimeDelta::seconds(i64::from(request.overlap_seconds)));
            state.last_operation_id = Some(request.operation_id.clone());
            Ok(())
        })
    }

    fn restore_previous_credential<'a>(&'a self, request: &'a CredentialOverlapRestore) -> DriverFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            state.active_version = state.retiring_version.take().ok_or(ExecutionAgentError::DriverFailed)?;
            state.candidate_probe_healthy = false;
            state.overlap_deadline = None;
            state.last_operation_id = Some(request.operation_id.clone());
            Ok(())
        })
    }
}

#[tokio::test]
async fn precheck_rejects_active_version_drift_and_unhealthy_credentials() {
    let client = Arc::new(FakeCredentialRotationClient::new());
    {
        let mut state = client.state.lock().expect("state lock");
        state.active_version = "unexpected-v9".to_owned();
        state.active_healthy = false;
    }
    let result = CredentialRotationHandler::new(client)
        .read_state(&read_request(parameters()))
        .await
        .expect("precheck");
    assert!(!result.ready);
    assert!(
        result
            .reason_codes
            .contains(&"active_credential_version_changed".to_owned())
    );
    assert!(result.reason_codes.contains(&"active_credential_unhealthy".to_owned()));
}

#[tokio::test]
async fn overlap_apply_verify_and_rollback_never_return_secret_reference() {
    let client = Arc::new(FakeCredentialRotationClient::new());
    let handler = CredentialRotationHandler::new(Arc::clone(&client));
    let step = step_request(parameters());

    let dispatched = handler.dispatch(&step, "credential-forward").await.expect("dispatch");
    assert_eq!(dispatched.outcome_code, "credential_overlap_started");
    assert_eq!(
        handler
            .reconcile(&read_request(parameters()), Some("credential-forward"))
            .await
            .expect("reconcile")
            .state,
        ReconcileEffectState::Applied
    );
    let observed = handler
        .read_state(&read_request(parameters()))
        .await
        .expect("conditions");
    assert_eq!(observed.resource_conditions.get("candidate_active"), Some(&true));
    assert_eq!(observed.resource_conditions.get("previous_retiring"), Some(&true));
    let encoded = serde_json::to_string(&observed).expect("sanitized read result");
    assert!(!encoded.contains("credential-v2-secret"));

    assert_eq!(
        handler
            .compensate(&step, "credential-rollback")
            .await
            .expect("rollback")
            .outcome_code,
        "credential_overlap_rolled_back"
    );
    let state = client.state.lock().expect("state lock");
    assert_eq!(state.active_version, "credential-v1");
    assert!(state.retiring_version.is_none());
}

#[tokio::test]
async fn raw_values_unsafe_references_and_acl_fields_fail_closed() {
    let handler = CredentialRotationHandler::new(Arc::new(FakeCredentialRotationClient::new()));
    for reference in [
        "plain-text-secret",
        "vault://rocketmq/credential?token=raw",
        "kubernetes://rocketmq/secret with space",
        "https://secret-store/credential",
    ] {
        let mut value = parameters();
        value["candidate_secret_ref"] = json!(reference);
        assert!(
            !handler
                .read_state(&read_request(value))
                .await
                .expect("precheck response")
                .ready
        );
    }

    let mut value = parameters();
    value["secret_value"] = json!("never-accepted");
    assert!(matches!(
        handler.read_state(&read_request(value)).await,
        Err(ExecutionAgentError::InvalidRequest)
    ));
}

fn parameters() -> serde_json::Value {
    json!({
        "credential_set": "broker-api",
        "active_version": "credential-v1",
        "candidate_version": "credential-v2",
        "candidate_secret_ref": "vault://rocketmq/credential-v2-secret",
        "overlap_seconds": 300,
        "validation_probe_topic": "SRE_PROBE_CREDENTIAL_ROTATION"
    })
}

fn read_request(parameters: serde_json::Value) -> AgentReadRequest {
    test_support::read_request(
        ExecutionAction::SecurityCredentialRotateOverlap,
        "credential-set/broker-api",
        parameters,
    )
}

fn step_request(parameters: serde_json::Value) -> AgentStepRequest {
    test_support::step_request(
        ExecutionAction::SecurityCredentialRotateOverlap,
        "credential-set/broker-api",
        parameters,
        ImpactScope::SingleResource,
        &["active_version", "candidate_version"],
    )
}
