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
fn invalid_arguments_use_catalog_exit_and_safe_default_stderr() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-cli-rust"))
        .arg("--definitely-invalid=password=plain-text")
        .output()
        .expect("run rocketmq-cli-rust with invalid arguments");

    assert_eq!(output.status.code(), Some(64));
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "ERROR core.argument.invalid: Argument is invalid\n"
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("plain-text"));
}

#[test]
fn help_is_a_successful_outcome() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-cli-rust"))
        .arg("--help")
        .output()
        .expect("run rocketmq-cli-rust --help");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(String::from_utf8_lossy(&output.stdout).contains("RocketMQ CLI"));
}

#[test]
fn required_subcommand_arguments_show_help_as_a_successful_outcome() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-cli-rust"))
        .arg("read-message-log")
        .output()
        .expect("run rocketmq-cli-rust read-message-log");

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert!(String::from_utf8_lossy(&output.stdout).contains("read message log file"));
}

#[test]
fn verbose_release_version_is_order_independent() {
    for arguments in [["--version", "--verbose"], ["--verbose", "--version"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-cli-rust"))
            .args(arguments)
            .output()
            .expect("run rocketmq-cli-rust verbose version");

        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("rocketmq-store-inspect"));
        assert!(stdout.contains("artifact_id="));
        assert!(stdout.contains("requested_features="));
        assert!(stdout.contains("effective_features="));
    }
}

#[test]
fn read_message_log_version_matches_release_version() {
    let release = Command::new(env!("CARGO_BIN_EXE_rocketmq-cli-rust"))
        .arg("--version")
        .output()
        .expect("run rocketmq-cli-rust --version");
    let read_message_log = Command::new(env!("CARGO_BIN_EXE_rocketmq-cli-rust"))
        .args(["read-message-log", "--version"])
        .output()
        .expect("run rocketmq-cli-rust read-message-log --version");

    assert!(release.status.success());
    assert!(read_message_log.status.success());
    assert!(release.stderr.is_empty());
    assert!(read_message_log.stderr.is_empty());

    let release_stdout = String::from_utf8_lossy(&release.stdout);
    let release_version = release_stdout
        .lines()
        .find_map(|line| line.strip_prefix("version="))
        .expect("release output contains a version");
    let read_message_log_stdout = String::from_utf8_lossy(&read_message_log.stdout);
    let read_message_log_version = read_message_log_stdout
        .split_whitespace()
        .last()
        .expect("read-message-log output contains a version");

    assert_eq!(read_message_log_version, release_version);
}
