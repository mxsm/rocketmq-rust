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
