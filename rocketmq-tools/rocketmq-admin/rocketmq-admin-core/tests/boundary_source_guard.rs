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
fn core_has_no_client_common_or_remoting_source_edge() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let core_root = source_root.join("core");
    let mut violations = Vec::new();

    visit_rust_files(&core_root, &mut |path, source| {
        for forbidden in [
            "rocketmq_client_rust::",
            "rocketmq_common::",
            "rocketmq_transport::",
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
fn direct_sdk_imports_are_adapter_only() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let adapter_root = source_root.join("client_adapter");
    let adapter_module = source_root.join("client_adapter.rs");
    let mut violations = Vec::new();

    visit_rust_files(&source_root, &mut |path, source| {
        let is_adapter = path.starts_with(&adapter_root) || path == adapter_module;
        if !is_adapter
            && (source.contains("rocketmq_client_rust::")
                || source.contains("rocketmq_common::")
                || source.contains("rocketmq_transport::"))
        {
            violations.push(path.display().to_string());
        }
    });

    assert!(
        violations.is_empty(),
        "direct Client/Common/Remoting imports outside client_adapter:\n{}",
        violations.join("\n")
    );
}

#[test]
fn client_adapter_feature_owns_all_sdk_dependencies() {
    let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")).unwrap();
    assert!(
        manifest.lines().any(|line| {
            line.split_once('=')
                .is_some_and(|(name, value)| name.trim() == "default" && value.trim() == "[]")
        }),
        "default feature set must stay empty"
    );

    let read_feature = feature_block(&manifest, "read-client-adapter");
    for dependency in [
        "dep:rocketmq-client-rust",
        "rocketmq-client-rust/admin-read",
        "dep:rocketmq-observability",
        "dep:rocketmq-runtime",
        "dep:rocketmq-transport",
    ] {
        assert!(
            read_feature.contains(dependency),
            "read adapter is missing dependency {dependency}"
        );
    }

    let mutation_feature = feature_block(&manifest, "mutation-client-adapter");
    for dependency in [
        "dep:rocketmq-client-rust",
        "rocketmq-client-rust/admin-mutation",
        "dep:rocketmq-observability",
        "dep:rocketmq-runtime",
        "dep:rocketmq-transport",
    ] {
        assert!(
            mutation_feature.contains(dependency),
            "mutation adapter is missing dependency {dependency}"
        );
    }

    let compatibility_feature = feature_block(&manifest, "client-adapter");
    for dependency in [
        "read-client-adapter",
        "mutation-client-adapter",
        "rocketmq-client-rust/admin-full",
    ] {
        assert!(
            compatibility_feature.contains(dependency),
            "compatibility adapter is missing feature {dependency}"
        );
    }
    assert!(!manifest.contains("dep:rocketmq-common"));
    assert!(!manifest.contains("legacy-common-compat"));
}

#[test]
fn removed_facades_cannot_reappear() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_root = crate_root.join("src");
    assert!(!source_root.join("admin.rs").exists());
    assert!(!source_root.join("admin").exists());
    assert!(!source_root.join("client_adapter").join("legacy").exists());

    let lib_rs = fs::read_to_string(source_root.join("lib.rs")).unwrap();
    assert!(!lib_rs.contains("extern crate self"));
    assert!(!lib_rs.contains("DefaultMQAdminExt"));
}

#[test]
fn sdk_admin_handles_are_not_part_of_the_public_adapter_surface() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let adapter_root = source_root.join("client_adapter");
    let adapter_module = fs::read_to_string(source_root.join("client_adapter.rs")).unwrap();
    assert!(!adapter_module.contains("DefaultMQAdminExt"));
    assert!(!adapter_module.contains("RPCHook"));
    assert!(!adapter_module.contains("admin_acl_rpc_hook"));

    let mut violations = Vec::new();
    visit_rust_files(&adapter_root.join("services"), &mut |path, source| {
        for line in source.lines() {
            if line.contains("pub async fn") && line.contains("_with_admin") {
                violations.push(format!("{} exports {}", path.display(), line.trim()));
            }
        }
    });

    assert!(
        violations.is_empty(),
        "raw SDK admin helpers must remain crate-private:\n{}",
        violations.join("\n")
    );
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

fn feature_block<'a>(manifest: &'a str, feature: &str) -> &'a str {
    let start = manifest
        .match_indices(feature)
        .find_map(|(offset, _)| {
            (offset == 0 || manifest.as_bytes().get(offset.wrapping_sub(1)) == Some(&b'\n')).then_some(offset)
        })
        .unwrap_or_else(|| panic!("missing feature `{feature}`"));
    let remainder = &manifest[start..];
    assert!(
        remainder
            .lines()
            .next()
            .and_then(|line| line.split_once('='))
            .is_some_and(|(name, value)| name.trim() == feature && value.trim() == "["),
        "feature `{feature}` must use a list definition"
    );
    let end = remainder
        .find("\n]")
        .map(|index| index + 2)
        .unwrap_or_else(|| panic!("feature `{feature}` has no closing bracket"));
    &remainder[..end]
}
