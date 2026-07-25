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

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

#[test]
fn cluster_mode_excludes_local_broker_and_store_dependencies() {
    let packages = resolved_proxy_packages("cluster-mode");

    assert!(packages.contains("rocketmq-proxy-cluster"));
    for forbidden in [
        "rocketmq-proxy-local",
        "rocketmq-broker",
        "rocketmq-store",
        "rocketmq-store-api",
        "rocketmq-store-local",
    ] {
        assert!(
            !packages.contains(forbidden),
            "Cluster-only closure contains {forbidden}"
        );
    }
}

#[test]
fn local_mode_excludes_cluster_client_dependencies() {
    let packages = resolved_proxy_packages("local-mode");

    assert!(packages.contains("rocketmq-proxy-local"));
    for forbidden in ["rocketmq-proxy-cluster", "rocketmq-client-rust"] {
        assert!(!packages.contains(forbidden), "Local-only closure contains {forbidden}");
    }
}

fn resolved_proxy_packages(feature: &str) -> HashSet<String> {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version=1",
            "--no-default-features",
            "--features",
            feature,
            "--manifest-path",
        ])
        .arg(manifest_path)
        .output()
        .expect("cargo metadata must start");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata: Value = serde_json::from_slice(&output.stdout).expect("cargo metadata must be valid JSON");
    let resolve = metadata
        .get("resolve")
        .and_then(Value::as_object)
        .expect("cargo metadata must include a resolve graph");
    let root = resolve
        .get("root")
        .and_then(Value::as_str)
        .expect("member manifest metadata must identify the Proxy root")
        .to_owned();
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .expect("cargo metadata must include packages")
        .iter()
        .map(|package| {
            (
                package
                    .get("id")
                    .and_then(Value::as_str)
                    .expect("package id")
                    .to_owned(),
                package
                    .get("name")
                    .and_then(Value::as_str)
                    .expect("package name")
                    .to_owned(),
            )
        })
        .collect::<HashMap<_, _>>();
    let nodes = resolve
        .get("nodes")
        .and_then(Value::as_array)
        .expect("cargo metadata must include resolve nodes")
        .iter()
        .map(|node| {
            let id = node.get("id").and_then(Value::as_str).expect("node id").to_owned();
            let dependencies = node
                .get("dependencies")
                .and_then(Value::as_array)
                .expect("node dependencies")
                .iter()
                .map(|dependency| dependency.as_str().expect("dependency id").to_owned())
                .collect::<Vec<_>>();
            (id, dependencies)
        })
        .collect::<HashMap<_, _>>();

    let mut pending = vec![root];
    let mut visited = HashSet::new();
    let mut names = HashSet::new();
    while let Some(package_id) = pending.pop() {
        if !visited.insert(package_id.clone()) {
            continue;
        }
        if let Some(name) = packages.get(&package_id) {
            names.insert(name.clone());
        }
        if let Some(dependencies) = nodes.get(&package_id) {
            pending.extend(dependencies.iter().cloned());
        }
    }
    names
}
