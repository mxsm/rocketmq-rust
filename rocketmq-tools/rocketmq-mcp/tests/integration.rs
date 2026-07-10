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

use std::collections::HashMap;
use std::io::Write;
use std::process::Command;
use std::process::Stdio;

use serde_json::Value;

#[test]
fn mcp_inspector_stdio_surface_integration() {
    let output = run_stdio_server(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"integration-inspector","version":"1.0.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
{"jsonrpc":"2.0","id":3,"method":"resources/list","params":{}}
{"jsonrpc":"2.0","id":4,"method":"resources/templates/list","params":{}}
{"jsonrpc":"2.0","id":5,"method":"prompts/list","params":{}}
{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"rocketmq://clusters/local-dev/overview"}}
{"jsonrpc":"2.0","id":7,"method":"prompts/get","params":{"name":"broker_health_check","arguments":{"cluster":"local-dev"}}}
{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"rocketmq_get_cluster_overview","arguments":{"cluster":"missing-cluster"}}}
{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"rocketmq_get_cluster_overview","arguments":{}}}
"#,
    );

    assert!(output.status.success(), "server failed: {}", output.stderr);
    let responses = parse_stdout_json(&output.stdout);

    assert_eq!(responses[&1]["result"]["protocolVersion"], "2025-11-25");
    assert!(responses[&1]["result"]["capabilities"]["tools"].is_object());
    assert!(responses[&1]["result"]["capabilities"]["resources"].is_object());
    assert!(responses[&1]["result"]["capabilities"]["prompts"].is_object());
    assert_json_array_contains_field_value(
        &responses[&2]["result"]["tools"],
        "name",
        "rocketmq_get_cluster_overview",
    );
    assert_json_array_contains_field_value(
        &responses[&3]["result"]["resources"],
        "uri",
        "rocketmq://clusters/local-dev/overview",
    );
    assert!(responses[&4]["result"]["resourceTemplates"].is_array());
    assert_json_array_contains_field_value(&responses[&5]["result"]["prompts"], "name", "diagnose_consumer_lag");
    assert!(responses[&6]["result"]["contents"][0]["text"]
        .as_str()
        .unwrap()
        .contains("local-dev"));
    assert!(responses[&7]["result"]["messages"][0]["content"]["text"]
        .as_str()
        .unwrap()
        .contains("Broker Health Check Task"));
    assert_eq!(responses[&8]["result"]["isError"], true);
    assert!(responses[&8]["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("missing-cluster"));
    assert_eq!(responses[&9]["result"]["isError"], true);
    let tool_error: Value =
        serde_json::from_str(responses[&9]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(tool_error["code"], "invalid_arguments");
    assert_eq!(tool_error["request_id"], "9");
    assert_eq!(tool_error["retryable"], false);
    assert!(!tool_error["suggestions"].as_array().unwrap().is_empty());
}

#[test]
fn unsupported_protocol_version_is_rejected_during_initialize() {
    let output = run_stdio_server(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"old-client","version":"1.0.0"}}}
"#,
    );
    let responses = parse_stdout_json(&output.stdout);

    assert!(!output.status.success());
    assert_eq!(responses[&1]["error"]["code"], -32602);
    assert!(responses[&1]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("requires 2025-11-25"));
}

struct StdioOutput {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}

fn run_stdio_server(input: &str) -> StdioOutput {
    let binary = env!("CARGO_BIN_EXE_rocketmq-mcp");
    let config = temporary_memory_audit_config();
    let mut child = Command::new(binary)
        .arg("--config")
        .arg(&config)
        .arg("--transport")
        .arg("stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rocketmq-mcp");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write json-rpc input");
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("wait for rocketmq-mcp");
    let _ = std::fs::remove_file(config);
    StdioOutput {
        status: output.status,
        stdout: String::from_utf8(output.stdout).expect("stdout utf8"),
        stderr: String::from_utf8(output.stderr).expect("stderr utf8"),
    }
}

fn temporary_memory_audit_config() -> std::path::PathBuf {
    let example = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("conf")
        .join("mcp.example.toml");
    let config = std::fs::read_to_string(example).expect("read example config");
    let config = config
        .replace("sink = \"file\"", "sink = \"memory\"")
        .replace("path = \"./logs/rocketmq-mcp-audit.log\"", "path = \"\"");
    let path = std::env::temp_dir().join(format!("rocketmq-mcp-integration-{}.toml", std::process::id()));
    std::fs::write(&path, config).expect("write temp config");
    path
}

fn parse_stdout_json(stdout: &str) -> HashMap<i64, Value> {
    let mut responses = HashMap::new();
    for (line_number, line) in stdout.lines().enumerate() {
        let value = serde_json::from_str::<Value>(line)
            .unwrap_or_else(|error| panic!("stdout line {} was not JSON: {error}: {line}", line_number + 1));
        if let Some(id) = value.get("id").and_then(Value::as_i64) {
            responses.insert(id, value);
        }
    }
    responses
}

fn assert_json_array_contains_field_value(array: &Value, field: &str, expected: &str) {
    let values = array.as_array().expect("json array");
    assert!(
        values
            .iter()
            .any(|value| value.get(field).and_then(Value::as_str) == Some(expected)),
        "expected {field}={expected} in {values:?}",
    );
}
