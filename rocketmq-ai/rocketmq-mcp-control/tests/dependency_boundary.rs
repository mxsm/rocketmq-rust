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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::process::Command;

fn metadata(features: Option<&str>) -> serde_json::Value {
    let mut command = Command::new(env!("CARGO"));
    command.args(["metadata", "--locked", "--format-version", "1"]);
    if let Some(features) = features {
        command.args(["--features", features]);
    }
    let output = command
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata starts");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).expect("cargo metadata is JSON")
}

fn resolve(document: &serde_json::Value) -> (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>) {
    let packages = document["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|package| {
            (
                package["id"].as_str().unwrap().to_string(),
                package["name"].as_str().unwrap().to_string(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut names = BTreeSet::new();
    let mut features = BTreeMap::new();
    for node in document["resolve"]["nodes"].as_array().unwrap() {
        let name = packages[node["id"].as_str().unwrap()].clone();
        names.insert(name.clone());
        features.insert(
            name,
            node["features"]
                .as_array()
                .unwrap()
                .iter()
                .map(|feature| feature.as_str().unwrap().to_string())
                .collect(),
        );
    }
    (names, features)
}

#[test]
fn cargo_metadata_proves_default_and_write_only_dependency_closures() {
    let (default_names, _) = resolve(&metadata(None));
    assert!(!default_names.contains("rocketmq-admin-core"));
    assert!(!default_names.contains("rocketmq-client-rust"));

    let (write_names, write_features) = resolve(&metadata(Some("write-tools")));
    assert!(write_names.contains("rocketmq-admin-core"));
    assert!(write_names.contains("rocketmq-client-rust"));
    assert!(write_features["rocketmq-admin-core"].contains("mutation-client-adapter"));
    assert!(!write_features["rocketmq-admin-core"].contains("read-client-adapter"));
    assert!(!write_features["rocketmq-admin-core"].contains("client-adapter"));
    assert!(write_features["rocketmq-client-rust"].contains("admin-mutation"));
    assert!(!write_features["rocketmq-client-rust"].contains("admin-read"));
    assert!(!write_features["rocketmq-client-rust"].contains("admin-full"));
}
