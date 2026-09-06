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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use clap::CommandFactory;
use rocketmq_admin_cli::rocketmq_cli::RocketMQCli;
use rocketmq_error::CliErrorView;
use rocketmq_error::CliVerbosity;
use rocketmq_error::RocketMQError;
use serde::Deserialize;

const GOLDENS: &str = include_str!("../../../../scripts/fixtures/admin-java-55/operation-goldens.json");

#[derive(Debug, Deserialize)]
struct GoldenFixture {
    operations: Vec<GoldenOperation>,
}

#[derive(Debug, Deserialize)]
struct GoldenOperation {
    operation_id: String,
    cli_command_id: String,
    scenarios: Vec<GoldenScenario>,
}

#[derive(Debug, Deserialize)]
struct GoldenScenario {
    outcome: String,
    error_kind: Option<String>,
    expected_error_code: Option<String>,
    expected_exit_code: i32,
}

fn fixture() -> GoldenFixture {
    serde_json::from_str(GOLDENS).expect("committed Admin golden fixture must be valid JSON")
}

fn error_for(kind: &str) -> RocketMQError {
    match kind {
        "invalid-input" => RocketMQError::IllegalArgument("invalid Admin golden input".to_string()),
        "not-found" => RocketMQError::query_not_found("Admin golden target"),
        "partial-failure" => RocketMQError::broker_operation_failed("ADMIN_GOLDEN", -1, "one target failed"),
        "timeout" => RocketMQError::Timeout {
            operation: "ADMIN_GOLDEN",
            timeout_ms: 1,
        },
        "permission" => RocketMQError::BrokerPermissionDenied {
            operation: "ADMIN_GOLDEN".to_string(),
        },
        other => panic!("unsupported Admin golden error kind: {other}"),
    }
}

#[test]
fn active_golden_routes_remain_reachable_from_the_real_cli_catalog() {
    let command = RocketMQCli::command();
    let routes = command
        .get_subcommands()
        .map(|domain| {
            (
                domain.get_name().to_string(),
                domain
                    .get_subcommands()
                    .map(|command| command.get_name().to_string())
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    for operation in fixture().operations {
        let (domain, command) = operation
            .cli_command_id
            .split_once('.')
            .unwrap_or_else(|| panic!("invalid CLI command id for {}", operation.operation_id));
        assert!(
            routes.get(domain).is_some_and(|commands| commands.contains(command)),
            "{} is not reachable as {}",
            operation.operation_id,
            operation.cli_command_id,
        );
    }
}

#[test]
fn golden_error_classes_use_the_real_cli_exit_and_error_code_mappings() {
    let mut error_kinds = BTreeSet::new();
    for operation in fixture().operations {
        for scenario in operation.scenarios {
            if scenario.outcome == "success" {
                assert_eq!(scenario.expected_exit_code, 0);
                assert!(scenario.expected_error_code.is_none());
                continue;
            }
            let error_kind = scenario
                .error_kind
                .as_deref()
                .expect("error golden must declare an error kind");
            error_kinds.insert(error_kind.to_string());
            let error = error_for(error_kind);
            assert_eq!(
                error.descriptor().code().as_str(),
                scenario.expected_error_code.as_deref().unwrap()
            );
            assert_eq!(
                CliErrorView::from_error(&error)
                    .output(CliVerbosity::Default)
                    .exit_code()
                    .as_i32(),
                scenario.expected_exit_code,
                "{} error mapping drifted for {}",
                error_kind,
                operation.operation_id,
            );
        }
    }
    assert_eq!(
        error_kinds,
        BTreeSet::from([
            "invalid-input".to_string(),
            "not-found".to_string(),
            "partial-failure".to_string(),
            "permission".to_string(),
            "timeout".to_string(),
        ]),
    );
}
