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

use std::process::Command;

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocketmq-mcp"));
    command.env_remove("ROCKETMQ_MCP_MODE");
    command.env_remove("ROCKETMQ_MCP_CONFIG");
    command.env_remove("ROCKETMQ_MCP_CONTROL_CONFIG");
    for (key, _) in std::env::vars_os() {
        if key.to_str().is_some_and(|key| key.starts_with("ROCKETMQ_SECURITY_")) {
            command.env_remove(key);
        }
    }
    command
}

#[test]
fn invalid_mode_exits_without_protocol_output_or_value_disclosure() {
    let output = command()
        .env("ROCKETMQ_MCP_MODE", "private-mode-sentinel")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("InvalidMode"));
    assert!(!stderr.contains("private-mode-sentinel"));
}

#[cfg(not(feature = "query"))]
#[test]
fn default_mode_requires_query_even_in_control_builds() {
    let output = command().output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8(output.stderr).unwrap().contains("FeatureDisabled"));
}

#[cfg(not(feature = "control"))]
#[test]
fn unavailable_control_exits_before_loading_its_configuration() {
    let output = command()
        .env("ROCKETMQ_MCP_MODE", "control")
        .env("ROCKETMQ_MCP_CONTROL_CONFIG", "private-config-sentinel")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("FeatureDisabled"));
    assert!(!stderr.contains("private-config-sentinel"));
}

#[cfg(feature = "control")]
#[test]
fn control_mode_uses_its_own_configuration_without_query_arguments() {
    let directory = tempfile::tempdir().unwrap();
    let output = command()
        .env("ROCKETMQ_MCP_MODE", "control")
        .env(
            "ROCKETMQ_MCP_CONTROL_CONFIG",
            directory.path().join("private-config-sentinel.toml"),
        )
        .arg("--query-private-sentinel")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Control("), "{stderr}");
    assert!(!stderr.contains("private-config-sentinel"));
    assert!(!stderr.contains("query-private-sentinel"));
}

#[cfg(feature = "query")]
fn query_configuration(directory: &std::path::Path) -> std::path::PathBuf {
    let examples = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("conf");
    let config = std::fs::read_to_string(examples.join("mcp.example.toml"))
        .unwrap()
        .replace("sink = \"file\"", "sink = \"memory\"")
        .replace("path = \"./logs/rocketmq-mcp-audit.log\"", "path = \"\"");
    let path = directory.join("mcp.toml");
    std::fs::write(&path, config).unwrap();
    std::fs::copy(
        examples.join("permissions.example.toml"),
        directory.join("permissions.example.toml"),
    )
    .unwrap();
    path
}

#[cfg(all(feature = "query", not(feature = "stdio")))]
#[test]
fn unavailable_stdio_refuses_before_query_startup() {
    let directory = tempfile::tempdir().unwrap();
    let path = query_configuration(directory.path());
    let output = command()
        .arg("--config")
        .arg(path)
        .arg("--transport")
        .arg("stdio")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("MCP transport feature is disabled"), "{stderr}");
    assert!(!stderr.contains("startup initialized"));
}

#[cfg(all(feature = "query", not(feature = "stdio")))]
#[test]
fn default_query_requires_stdio_feature_without_configuration() {
    let output = command().output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("MCP transport feature is disabled"), "{stderr}");
    assert!(!stderr.contains("startup initialized"));
}

#[cfg(all(feature = "query", feature = "streamable-http"))]
#[test]
fn http_requires_explicit_configuration_before_startup() {
    let output = command().arg("--transport").arg("streamable-http").output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("MCP configuration is invalid"), "{stderr}");
    assert!(!stderr.contains("startup initialized"));
}

#[cfg(all(feature = "query", feature = "stdio"))]
#[test]
fn default_query_mode_preserves_stdio_and_excludes_control_tools() {
    let directory = tempfile::tempdir().unwrap();
    let path = query_configuration(directory.path());
    let _legacy_error: fn(rocketmq_mcp::query::McpError) -> rocketmq_mcp::McpError = |error| error;
    let _legacy_config: fn(rocketmq_mcp::query::config::McpConfig) -> rocketmq_mcp::config::McpConfig = |config| config;
    let mut command = command();
    command.arg("--config").arg(path).arg("--transport").arg("stdio");
    assert_query_stdio(command, "local-dev");
}

#[cfg(all(feature = "query", feature = "stdio"))]
#[test]
fn default_query_starts_without_arguments_or_configuration_files() {
    let directory = tempfile::tempdir().unwrap();
    let mut command = command();
    command.current_dir(directory.path());
    assert_query_stdio(command, "local-dev");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[cfg(all(feature = "query", feature = "stdio"))]
#[test]
fn query_configuration_environment_overrides_builtin_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let path = query_configuration(directory.path());
    let config = std::fs::read_to_string(&path)
        .unwrap()
        .replace("name = \"local-dev\"", "name = \"from-env\"");
    std::fs::write(&path, config).unwrap();
    let mut command = command();
    command.env("ROCKETMQ_MCP_CONFIG", path);
    assert_query_stdio(command, "from-env");
}

#[cfg(all(feature = "query", feature = "stdio"))]
#[test]
fn query_configuration_argument_overrides_environment() {
    let directory = tempfile::tempdir().unwrap();
    let path = query_configuration(directory.path());
    let mut command = command();
    command
        .env("ROCKETMQ_MCP_CONFIG", directory.path().join("missing.toml"))
        .arg("--config")
        .arg(path);
    assert_query_stdio(command, "local-dev");
}

#[cfg(all(feature = "query", feature = "stdio"))]
fn assert_query_stdio(mut command: Command, cluster_name: &str) {
    use std::io::Write;
    use std::process::Stdio;

    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(
        br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"entrypoint-test","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"rocketmq_list_clusters","arguments":{}}}
"#,
    ).unwrap();
    drop(child.stdin.take());
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let frames: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let initialized = frames.iter().find(|frame| frame["id"] == 1).unwrap();
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    let listed = frames.iter().find(|frame| frame["id"] == 2).unwrap();
    let names: Vec<_> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"rocketmq_list_clusters"));
    assert!(!names.contains(&"rocketmq_upsert_topic"));
    assert!(!names.contains(&"rocketmq_get_mutation_result"));
    let clusters = frames.iter().find(|frame| frame["id"] == 3).unwrap();
    assert_eq!(clusters["result"]["isError"], false, "{clusters}");
    assert_eq!(
        clusters["result"]["structuredContent"]["data"]["clusters"][0]["name"],
        cluster_name
    );
}
