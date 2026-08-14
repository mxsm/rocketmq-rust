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

use std::collections::BTreeSet;

use rocketmq_proxy_core::remoting::java_proxy_active_route_policies;
use rocketmq_proxy_core::remoting::RemotingRouteClass;
use serde::Deserialize;

#[derive(Deserialize)]
struct JavaInventory {
    proxy_routes: Vec<JavaProxyRoute>,
}

#[derive(Deserialize)]
struct JavaProxyRoute {
    request_code: String,
    classification: String,
}

#[test]
fn every_generated_java_proxy_route_has_one_supported_policy() {
    let inventory: JavaInventory = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../scripts/fixtures/java-5.5-core-inventory.json"
    )))
    .expect("Java 5.5 inventory fixture should decode");
    let expected = inventory
        .proxy_routes
        .into_iter()
        .filter(|route| route.classification == "active")
        .map(|route| route.request_code)
        .collect::<BTreeSet<_>>();
    let policies = java_proxy_active_route_policies();
    let actual = policies
        .iter()
        .map(|policy| policy.java_request_name().to_owned())
        .collect::<BTreeSet<_>>();

    assert_eq!(actual, expected);
    assert_eq!(actual.len(), policies.len(), "active request codes must be unique");
    assert!(
        policies
            .iter()
            .all(|policy| policy.route_class() != RemotingRouteClass::Unsupported),
        "Java-active routes must never fall through to Unsupported"
    );
}
