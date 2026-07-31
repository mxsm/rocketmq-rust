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

const BROKER_RUNTIME: &str = include_str!("../src/broker_runtime.rs");
const COMPOSITION: &str = include_str!("../src/broker_runtime/composition.rs");
const CONTROL_PLANE: &str = include_str!("../src/broker_runtime/control_plane.rs");
const DATA_PLANE: &str = include_str!("../src/broker_runtime/data_plane.rs");
const REQUEST_PIPELINE: &str = include_str!("../src/broker_runtime/request_pipeline.rs");
const REQUEST_PIPELINE_STARTUP: &str = include_str!("../src/broker_runtime/request_pipeline/startup.rs");
const LIFECYCLE: &str = include_str!("../src/broker_runtime/lifecycle.rs");
const METADATA: &str = include_str!("../src/broker_runtime/metadata.rs");
const PROXY_FACADE: &str = include_str!("../src/proxy_facade.rs");

#[test]
fn broker_runtime_facade_owns_only_composition_and_lifecycle() {
    let declaration = BROKER_RUNTIME
        .split("pub(crate) struct BrokerRuntime {")
        .nth(1)
        .and_then(|source| source.split('}').next())
        .expect("BrokerRuntime declaration should exist");
    let fields = declaration
        .lines()
        .map(str::trim)
        .filter(|line| line.contains(':'))
        .collect::<Vec<_>>();

    assert_eq!(
        fields,
        ["composition: BrokerComposition,", "lifecycle: BrokerLifecycle,"]
    );
}

#[test]
fn broker_components_have_narrow_constructor_contracts() {
    for (name, source) in [
        ("BrokerControlPlane", CONTROL_PLANE),
        ("BrokerDataPlane", DATA_PLANE),
        ("BrokerRequestPipeline", REQUEST_PIPELINE),
        ("BrokerLifecycle", LIFECYCLE),
        ("BrokerMetadata", METADATA),
    ] {
        let contract = source
            .split("impl BrokerRuntime")
            .next()
            .expect("component declaration should precede runtime behavior");
        assert!(
            contract.contains("fn new"),
            "{name} must define an explicit constructor"
        );
        assert!(
            !contract.contains("BrokerRuntimeState"),
            "{name} must not accept or retain the complete BrokerRuntimeState"
        );
    }

    assert!(
        COMPOSITION.contains("state: Box<BrokerRuntimeState<BrokerMessageStore>>"),
        "BrokerComposition must remain the exclusive BrokerRuntimeState owner"
    );
}

#[test]
fn broker_runtime_is_split_into_reviewable_production_modules() {
    assert!(
        BROKER_RUNTIME.lines().count() <= 1_500,
        "the BrokerRuntime facade should remain below the review threshold"
    );
    assert!(REQUEST_PIPELINE.lines().count() <= 800);
    assert!(REQUEST_PIPELINE_STARTUP.lines().count() <= 800);
    assert!(CONTROL_PLANE.lines().count() <= 800);
    assert!(DATA_PLANE.lines().count() <= 800);
    assert!(METADATA.lines().count() <= 800);
}

#[test]
fn transient_requests_do_not_create_component_groups() {
    let process_request = PROXY_FACADE
        .split("pub async fn process_request")
        .nth(1)
        .and_then(|source| source.split("\n    }\n}").next())
        .expect("ProxyBrokerFacade::process_request should exist");

    assert!(
        process_request.contains("self.local_request_tasks.clone()"),
        "local requests must reuse the component owner created during facade composition"
    );
    assert!(
        !process_request.contains(".child("),
        "a transient local request must not create a task-group child"
    );
}
