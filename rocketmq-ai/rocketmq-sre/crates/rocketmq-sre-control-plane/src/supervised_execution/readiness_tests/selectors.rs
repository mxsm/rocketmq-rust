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
use rocketmq_sre_contracts::*;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::super::signing::GrantSigner;
use super::super::target_scope::ActionTargetScope;
use super::support::*;

struct Case {
    action: &'static str,
    target: &'static str,
    lookup: Value,
    parameters: Value,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            action: ACTION,
            target: TARGET,
            lookup: json!({"kind":"logger","component":"broker","logger":"rocketmq_broker::processor"}),
            parameters: json!({"component":"broker","logger":"rocketmq_broker::processor","level":"DEBUG","ttl_seconds":120}),
        },
        Case {
            action: "broker.config.patch_allowlisted.v1",
            target: TARGET,
            lookup: json!({"kind":"broker","broker":"127.0.0.1:10911"}),
            parameters: json!({"broker":"127.0.0.1:10911","expected_generation":1,"patch":{"max_client_event_count":100}}),
        },
        Case {
            action: "topic.config.patch_allowlisted.v1",
            target: "topic/orders",
            lookup: json!({"kind":"topic","topic":"orders"}),
            parameters: json!({"topic":"orders","expected_version":1,"patch":{"read_queue_nums":16}}),
        },
        Case {
            action: "subscription_group.patch_allowlisted.v1",
            target: "subscription-group/orders-consumer",
            lookup: json!({"kind":"subscription_group","group":"orders-consumer"}),
            parameters: json!({"group":"orders-consumer","expected_version":1,"patch":{"retry_max_times":4}}),
        },
        // Existing shortened target literals are not expanded into namespaces;
        // the operator explicitly declares the actual namespace/selectors.
        Case {
            action: "proxy.scale_out_one.v1",
            target: "deployment/proxy",
            lookup: json!({"kind":"workload","namespace":"default","workload":"proxy"}),
            parameters: json!({"namespace":"default","workload":"proxy","expected_replicas":2}),
        },
        Case {
            action: "proxy.restart_one.v1",
            target: "pod/proxy-0",
            lookup: json!({"kind":"pod","namespace":"default","pod":"proxy-0"}),
            parameters: json!({"namespace":"default","pod":"proxy-0","expected_uid":"fixture-uid"}),
        },
        Case {
            action: "proxy.rollout_image_canary.v1",
            target: "deployment/proxy-canary",
            lookup: json!({"kind":"canary","namespace":"default","workload":"proxy","container":"proxy"}),
            parameters: json!({"namespace":"default","workload":"proxy","container":"proxy","expected_generation":1,"image_digest":format!("sha256:{}","a".repeat(64)),"canary_replicas":1}),
        },
        Case {
            action: "telemetry.collector.restart_one.v1",
            target: "pod/collector-0",
            lookup: json!({"kind":"telemetry","namespace":"telemetry","pod":"collector-0","pipeline":"metrics"}),
            parameters: json!({"namespace":"telemetry","pod":"collector-0","expected_uid":"fixture-uid","pipeline":"metrics"}),
        },
        Case {
            action: "security.credential_rotate_overlap.v1",
            target: "credential-set/broker-api",
            lookup: json!({"kind":"credential_set","credential_set":"broker-api"}),
            parameters: json!({"credential_set":"broker-api","active_version":"a","candidate_version":"b","candidate_secret_ref":"fixture-reference","overlap_seconds":120,"validation_probe_topic":"fixture-probe"}),
        },
    ]
}

fn different_selector(key: &str) -> &'static str {
    match key {
        // Broker is the descriptor's only enabled component. Other selectors
        // deliberately remain valid inputs for their respective drivers.
        "component" => "unsupported-component",
        "logger" => "rocketmq_broker::other",
        "broker" => "127.0.0.2:10911",
        "topic" => "other-topic",
        "group" => "other-consumer",
        "namespace" => "other-namespace",
        "workload" => "other-workload",
        "pod" => "other-pod-0",
        "container" => "other-container",
        "pipeline" => "logs",
        "credential_set" => "other-credential-set",
        _ => panic!("unexpected selector field: {key}"),
    }
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL; actual CP readonly HTTP dispatch is observed, remote target responses are SyntheticFixture"]
async fn postgres_readiness_all_enabled_lookup_selectors_are_checked_before_agent_reads() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    let cases = cases();
    let rules = cases
        .iter()
        .map(|case| {
            json!({"tenant_id":fixture.auth.tenant_id,"cluster_id":fixture.request.cluster_id,
        "action_id":case.action,"descriptor_version":"1.0.0","incident_resource":"broker/broker-a","target":case.target,
        "lookup":case.lookup,"security_revision":"1"})
        })
        .collect::<Vec<_>>();
    let scope = Arc::new(
        ActionTargetScope::parse(Some(
            &json!({"schema_version":"rocketmq-sre.action-target-scope.v1","rules":rules}).to_string(),
        ))
        .unwrap(),
    );
    fixture.state.supervised_execution = fixture.state.supervised_execution.clone().with_target_scope(scope);
    let path = format!("/v1/incidents/{}/execution-preconditions", fixture.incident_id);
    for case in &cases {
        fixture.request.action_id = case.action.to_owned();
        fixture.request.resource = case.target.to_owned();
        fixture.request.parameters = case.parameters.clone();
        fixture.request.precondition_evidence_id = None;
        let baseline = fixture.observer.reads.load(Ordering::SeqCst);
        for key in case.lookup.as_object().unwrap().keys() {
            if key == "kind" {
                continue;
            }
            let mut wrong = fixture.prepare_body();
            wrong["parameters"][key] = json!(different_selector(key));
            let (status, body) = fixture.call(&path, &fixture.auth, wrong).await;
            assert!(status.is_client_error(), "{} / {key}: {body}", case.action);
            let mut request = fixture.request.clone();
            request.parameters[key] = json!(different_selector(key));
            let readiness = fixture.readiness(&fixture.auth, &request).await;
            assert!(
                !readiness.operations.prepare_precondition.allowed,
                "{} / {key}",
                case.action
            );
            assert_ne!(readiness.stages.target.state, ActionReadinessState::Satisfied);
            assert_eq!(
                fixture.observer.reads.load(Ordering::SeqCst),
                baseline,
                "{} / {key}",
                case.action
            );
        }
        let before = fixture.readiness(&fixture.auth, &fixture.request).await;
        assert!(
            before.operations.prepare_precondition.allowed,
            "{}: {before:?}",
            case.action
        );
        let snapshot = fixture.prepare().await;
        assert_eq!(snapshot.resource, case.target);
        assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), baseline + 1);
        let view = fixture.readiness(&fixture.auth, &fixture.request).await;
        assert_eq!(view.stages.handler.state, ActionReadinessState::Satisfied);
        assert_eq!(view.stages.qualification.state, ActionReadinessState::Unknown);
    }
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), cases.len());
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL and an immutable sealed observation"]
async fn postgres_readiness_valid_origin_cannot_bypass_removed_or_changed_lookup_authorization() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    let snapshot = fixture.prepare().await;
    let plan = fixture.draft().await;
    let EvidenceContent::Inline(value) = &snapshot.content else {
        panic!("inline proof");
    };
    let proof: ActionPreparationEvidence = serde_json::from_value(value.clone()).unwrap();
    GrantSigner::new(KEY).unwrap().verify_preparation(&proof).unwrap();
    let rule = json!({"tenant_id":fixture.auth.tenant_id,"cluster_id":fixture.request.cluster_id,"action_id":ACTION,"descriptor_version":"1.0.0",
        "incident_resource":"broker/broker-a","target":TARGET,"security_revision":"1",
        "lookup":{"kind":"logger","component":"broker","logger":"rocketmq_broker::other"}});
    let scope = Arc::new(
        ActionTargetScope::parse(Some(
            &json!({"schema_version":"rocketmq-sre.action-target-scope.v1","rules":[rule]}).to_string(),
        ))
        .unwrap(),
    );
    fixture.state.supervised_execution = fixture.state.supervised_execution.clone().with_target_scope(scope);
    let view = fixture.readiness(&fixture.auth, &fixture.request).await;
    assert_eq!(view.stages.target.state, ActionReadinessState::Blocked);
    assert_eq!(
        view.stages.evidence.reason_codes,
        vec![ActionReadinessReason::TargetScopeUnverified]
    );
    assert!(!view.operations.create_plan.allowed);
    let (status, result) = fixture.call("/v1/plans", &fixture.auth, fixture.create_body()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(result["code"], "target_scope_unverified");
    let (status, result) = fixture.call(&format!("/v1/plans/{}/approve",plan.id), &fixture.approver(),
        json!({"plan_hash":plan.plan_hash,"precondition_hash":plan.compute_precondition_hash().unwrap(),"reason":"old MAC cannot authorize a removed actual lookup"})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(result["code"], "target_scope_unverified");
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    fixture.close().await;
}
