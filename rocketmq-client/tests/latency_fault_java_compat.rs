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

#![recursion_limit = "256"]

use rocketmq_client_rust::test_support::LatencyFaultJavaCompatHarness;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Corpus {
    schema_version: u32,
    java_version: String,
    latency_max_ms: Vec<u64>,
    not_available_duration_ms: Vec<u64>,
    state_cases: Vec<StateCase>,
    selection_cases: Vec<SelectionCase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StateCase {
    id: String,
    initial_now_ms: u64,
    latency_ms: u64,
    isolation: bool,
    reachable: bool,
    expected_duration_ms: u64,
    checks: Vec<StateCheck>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StateCheck {
    at_ms: u64,
    available: bool,
    reachable: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SelectionCase {
    id: String,
    initial_now_ms: u64,
    updates: Vec<FaultUpdate>,
    selection_at_ms: u64,
    queues: Vec<String>,
    last_broker: Option<String>,
    expected_broker: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FaultUpdate {
    at_ms: u64,
    broker: String,
    latency_ms: u64,
    isolation: bool,
    reachable: bool,
}

fn corpus() -> Corpus {
    serde_json::from_str(include_str!("../../scripts/fixtures/latency-fault-corpus.json"))
        .expect("valid Java 5.5 latency-fault corpus")
}

#[test]
fn latency_windows_match_java_55_with_a_manual_clock() {
    let corpus = corpus();
    assert_eq!(corpus.schema_version, 1);
    assert_eq!(corpus.java_version, "5.5.0");
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("latency-fault-java-compat"))
        .expect("runtime owner should start");

    for case in corpus.state_cases {
        let harness = LatencyFaultJavaCompatHarness::new(
            owner.root_context().component(case.id.clone()),
            case.initial_now_ms,
            corpus.latency_max_ms.clone(),
            corpus.not_available_duration_ms.clone(),
        );
        assert_eq!(
            harness.not_available_duration(case.latency_ms, case.isolation),
            case.expected_duration_ms,
            "{}",
            case.id
        );
        owner.block_on(harness.update("broker-a", case.latency_ms, case.isolation, case.reachable));
        for check in case.checks {
            harness.set_now_ms(check.at_ms);
            let state = harness.state("broker-a").expect("fault item should exist");
            assert_eq!(state.available, check.available, "{} at {}", case.id, check.at_ms);
            assert_eq!(state.reachable, check.reachable, "{} at {}", case.id, check.at_ms);
        }
    }
}

#[test]
fn selection_avoidance_recovery_and_route_changes_match_java_55() {
    let corpus = corpus();
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("latency-fault-selection-compat"))
        .expect("runtime owner should start");

    for case in corpus.selection_cases {
        let harness = LatencyFaultJavaCompatHarness::new(
            owner.root_context().component(case.id.clone()),
            case.initial_now_ms,
            corpus.latency_max_ms.clone(),
            corpus.not_available_duration_ms.clone(),
        );
        for update in case.updates {
            harness.set_now_ms(update.at_ms);
            owner.block_on(harness.update(&update.broker, update.latency_ms, update.isolation, update.reachable));
        }
        harness.set_now_ms(case.selection_at_ms);
        assert_eq!(
            harness.select(&case.queues, case.last_broker.as_deref()),
            Some(case.expected_broker.clone()),
            "{}",
            case.id
        );
    }
}
