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

#[test]
fn namesrv_toml_observability_config_parses() {
    let root = tempfile::tempdir().expect("create isolated NameServer config root");
    let config_path = root.path().join("namesrv-observability.toml");
    std::fs::write(
        &config_path,
        r#"rocketmqHome = "target/namesrv-observability"

[observability.traces]
exporter = "otlp_grpc"
sampleRatio = 0.2

[observability.otlp]
endpoint = "http://file-collector:4317"
protocol = "grpc"
"#,
    )
    .expect("write NameServer observability config");

    let config = rocketmq_namesrv::parse_command_and_config_file(config_path)
        .expect("NameServer observability TOML should parse");

    assert_eq!(config.observability.traces.sample_ratio, Some(0.2));
}

#[test]
fn namesrv_without_listen_port_override_reports_9876() {
    let root = tempfile::tempdir().expect("create isolated NameServer config root");
    let config_path = root.path().join("namesrv.toml");
    let rocketmq_home = root.path().to_string_lossy().replace('\\', "/");
    let config_store_path = root
        .path()
        .join("namesrv.properties")
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::write(
        &config_path,
        format!("rocketmqHome = \"{rocketmq_home}\"\nconfigStorePath = \"{config_store_path}\"\n"),
    )
    .expect("write isolated NameServer config");

    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-namesrv-rust"))
        .arg("--configFile")
        .arg(&config_path)
        .arg("--printConfigItem")
        .output()
        .expect("run NameServer config inspection");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "NameServer config inspection failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.lines().any(|line| line.trim() == "listenPort = 9876"),
        "NameServer should default to port 9876\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}
