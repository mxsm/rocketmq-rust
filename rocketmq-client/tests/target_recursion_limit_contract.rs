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

#![recursion_limit = "256"]

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

const RECURSION_LIMIT_ATTRIBUTE: &str = "#![recursion_limit = \"";

fn cargo_target_roots(manifest_dir: &Path) -> Vec<PathBuf> {
    // Cargo's target inventory respects disabled automatic discovery and excludes
    // shared support modules that happen to live beside target entry files.
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--locked",
            "--offline",
            "--format-version",
            "1",
        ])
        .arg("--manifest-path")
        .arg(manifest_dir.join("Cargo.toml"))
        .output()
        .expect("Cargo target metadata should be available");
    assert!(
        output.status.success(),
        "Cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).expect("valid Cargo metadata");
    let package = metadata["packages"]
        .as_array()
        .expect("Cargo packages")
        .iter()
        .find(|package| package["name"] == "rocketmq-client-rust")
        .expect("client package metadata");

    package["targets"]
        .as_array()
        .expect("client Cargo targets")
        .iter()
        .map(|target| PathBuf::from(target["src_path"].as_str().expect("Cargo target source path")))
        .collect()
}

#[test]
fn every_client_cargo_target_declares_a_recursion_limit() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut target_roots = cargo_target_roots(&manifest_dir);
    target_roots.sort();
    target_roots.dedup();

    let missing = target_roots
        .iter()
        .filter(|path| {
            !fs::read_to_string(path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
                .contains(RECURSION_LIMIT_ATTRIBUTE)
        })
        .map(|path| path.strip_prefix(&manifest_dir).unwrap_or(path).display().to_string())
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "Cargo target crates must declare a recursion limit; missing:\n{}",
        missing.join("\n")
    );
}
