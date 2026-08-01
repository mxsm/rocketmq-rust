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

fn adapter_bypasses(root: &Path) -> Vec<String> {
    let source_root = root.join("src");
    let allowed = [
        "read_gateway/admin.rs",
        "read_gateway/mcp.rs",
        "sources.rs",
        "sources/admin_query.rs",
        "sources/mcp.rs",
    ];
    let forbidden = [
        "McpSource",
        "AdminQuerySource",
        ".query_producer_connections(",
        ".query_consumer_connections(",
    ];
    let mut pending = vec![source_root.clone()];
    let mut findings = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("connector source directory") {
            let path = entry.expect("connector source entry").path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
                continue;
            }
            let relative = path
                .strip_prefix(&source_root)
                .expect("connector source path")
                .to_string_lossy()
                .replace('\\', "/");
            if allowed.contains(&relative.as_str()) {
                continue;
            }
            let source = fs::read_to_string(&path).expect("connector source");
            for marker in forbidden {
                if source.contains(marker) {
                    findings.push(format!("{relative}:{marker}"));
                }
            }
        }
    }
    findings
}

#[test]
fn source_manager_owns_one_read_gateway_without_adapter_bypasses() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = fs::read_to_string(root.join("src/sources.rs")).expect("sources.rs");

    assert!(sources.contains("read_gateway: ConnectorReadGateway"));
    assert!(sources.contains("read_gateway.mcp_query"));
    assert!(sources.contains("read_gateway.admin_query"));
    for forbidden in [
        "mcp: McpSource",
        "admin: AdminQuerySource",
        "self.mcp.query",
        "self.admin.query",
    ] {
        assert!(!sources.contains(forbidden), "adapter bypass remains: {forbidden}");
    }
    assert_eq!(adapter_bypasses(root), Vec::<String>::new());
}

#[test]
fn connector_admin_dependency_is_read_only() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(root.join("Cargo.toml")).expect("connector manifest");
    assert!(manifest.contains(r#"features = ["read-client-adapter"]"#));
    for forbidden in ["mutation-client-adapter", "dangerous-tools", "admin-full"] {
        assert!(
            !manifest.contains(forbidden),
            "mutation feature is reachable: {forbidden}"
        );
    }
}
