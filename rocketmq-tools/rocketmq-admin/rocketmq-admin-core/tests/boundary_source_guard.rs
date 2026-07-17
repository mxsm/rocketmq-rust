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

use std::fs;
use std::path::Path;

#[test]
fn core_has_no_client_or_legacy_common_source_edge() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let core_root = source_root.join("core");
    let mut violations = Vec::new();

    visit_rust_files(&core_root, &mut |path, source| {
        for forbidden in [
            "rocketmq_client_rust::",
            "rocketmq_common::",
            "MQAdminExt",
            "DefaultMQAdminExt",
            "DefaultMQProducer",
            "ClientConfig",
            "ArcMut<MessageExt>",
        ] {
            if source.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    });

    assert!(
        violations.is_empty(),
        "core boundary violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn direct_client_and_common_imports_are_adapter_only() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let adapter_root = source_root.join("client_adapter");
    let adapter_module = source_root.join("client_adapter.rs");
    let mut violations = Vec::new();

    visit_rust_files(&source_root, &mut |path, source| {
        let is_adapter = path.starts_with(&adapter_root) || path == adapter_module;
        if !is_adapter && (source.contains("rocketmq_client_rust::") || source.contains("rocketmq_common::")) {
            violations.push(path.display().to_string());
        }
    });

    assert!(
        violations.is_empty(),
        "direct Client/Common imports outside client_adapter:\n{}",
        violations.join("\n")
    );
}

#[test]
fn feature_implication_is_explicit() {
    let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    assert!(manifest.contains("default        = [\"legacy-common-compat\"]"));
    assert!(manifest.contains("client-adapter = [\"dep:rocketmq-client-rust\"]"));
    assert!(manifest.contains("\"client-adapter\","));
    assert!(manifest.contains("\"dep:rocketmq-common\","));
}

fn visit_rust_files(root: &Path, visit: &mut impl FnMut(&Path, &str)) {
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            visit_rust_files(&path, visit);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            let source = fs::read_to_string(&path).unwrap();
            visit(&path, &source);
        }
    }
}
