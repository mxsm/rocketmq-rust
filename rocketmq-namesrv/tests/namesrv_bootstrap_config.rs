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

fn inspect_runtime_config(runtime_toml: &str) -> std::process::Output {
    let root = tempfile::tempdir().expect("isolated NameServer config root");
    let config_path = root.path().join("namesrv-runtime.toml");
    let rocketmq_home = root.path().to_string_lossy().replace('\\', "/");
    let config_store_path = root
        .path()
        .join("namesrv.properties")
        .to_string_lossy()
        .replace('\\', "/");
    std::fs::write(
        &config_path,
        format!(
            "rocketmqHome = \"{rocketmq_home}\"\nconfigStorePath = \"{config_store_path}\"\n\
             clientRequestThreadPoolNums = 11\nlistenPort = 9876\n{runtime_toml}\n"
        ),
    )
    .expect("write config");
    Command::new(env!("CARGO_BIN_EXE_rocketmq-namesrv-rust"))
        .arg("--configFile")
        .arg(&config_path)
        .args(["--listenPort", "19876", "--printConfigItem"])
        .output()
        .expect("run NameServer config inspection")
}

#[test]
fn namesrv_runtime_overrides_preserve_request_config_and_cli_precedence() {
    for contents in [
        "",
        "[runtime]",
        "[runtime]\nworkerThreads = 2\nmaxBlockingThreads = 8",
        "[runtime]\nworker_threads = 2\nmax_blocking_threads = 8",
        "[runtime]\nworkerThreads = 2",
        "[runtime]\nmaxBlockingThreads = 8",
    ] {
        let output = inspect_runtime_config(contents);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{contents}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.lines().any(|line| line.trim() == "listenPort = 19876"),
            "{stdout}"
        );
        assert!(
            stdout
                .lines()
                .any(|line| line.trim() == "clientRequestThreadPoolNums = 11"),
            "{stdout}"
        );
        assert!(
            !stdout.contains("workerThreads"),
            "startup-only properties must not enter NameServer configuration"
        );
    }
}

#[test]
fn namesrv_runtime_invalid_settings_report_safe_actionable_errors_without_panic() {
    for (contents, expected) in [
        (
            "[runtime]\nworkerThreads = 0",
            "runtime.workerThreads must be greater than 0",
        ),
        (
            "[runtime]\nmaxBlockingThreads = 2",
            "runtime.maxBlockingThreads must be within the supported range (3..=512)",
        ),
        (
            "[runtime]\nmaxBlockingThreads = 513",
            "runtime.maxBlockingThreads must be within the supported range (3..=512)",
        ),
        ("[runtime]\nworkerThread = 2", "unknown configuration field"),
        (
            "[runtime]\nworker_threads = 0",
            "runtime.workerThreads must be greater than 0",
        ),
        (
            "[runtime]\nworkerThreads = 2\nworker_threads = 3",
            "invalid [runtime] section",
        ),
        (
            "[runtime]\nworkerThreads = 'RUNTIME_CONFIG_SECRET'",
            "invalid [runtime] section",
        ),
        (
            "[runtime]\nmaxBlockingThreads = 'RUNTIME_CONFIG_SECRET'",
            "invalid [runtime] section",
        ),
        ("[runtime]\nworkerThreads = -1", "invalid [runtime] section"),
        (
            "[runtime]\nworkerThreads = 'RUNTIME_CONFIG_SECRET' trailing",
            "configuration file parse failed",
        ),
        (
            "[runtime]\n'RUNTIME_CONFIG_SECRET\\nforged-output' = 2",
            "configuration processing failed",
        ),
    ] {
        let output = inspect_runtime_config(contents);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(rocketmq_error::CliExitCode::CONFIG.as_i32()),
            "{stderr}"
        );
        assert!(stderr.contains("core.configuration.invalid"), "{stderr}");
        assert!(stderr.contains(expected), "expected {expected:?}, got {stderr}");
        assert!(!stderr.contains("panicked at"), "{stderr}");
        assert!(!stderr.contains("RUNTIME_CONFIG_SECRET"), "{stderr}");
        assert!(!stderr.contains("forged-output"), "{stderr}");
        assert!(stderr.len() < 512, "diagnostic must remain bounded");
        assert_eq!(stderr.lines().count(), 1, "{stderr}");
    }
}
