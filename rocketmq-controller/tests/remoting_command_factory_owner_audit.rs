// Copyright 2023 The RocketMQ Rust Authors
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

const RUNTIME_OWNER_SOURCES: &[(&str, &str, bool)] = &[
    (
        "controller_manager.rs",
        include_str!("../src/controller/controller_manager.rs"),
        true,
    ),
    (
        "open_raft_controller.rs",
        include_str!("../src/controller/open_raft_controller.rs"),
        true,
    ),
    (
        "raft_controller.rs",
        include_str!("../src/controller/raft_controller.rs"),
        true,
    ),
    ("processor.rs", include_str!("../src/processor.rs"), false),
    (
        "controller_request_processor.rs",
        include_str!("../src/processor/controller_request_processor.rs"),
        true,
    ),
    (
        "broker_role_notifier.rs",
        include_str!("../src/controller/broker_role_notifier.rs"),
        false,
    ),
    (
        "broker_role_notifier/actor.rs",
        include_str!("../src/controller/broker_role_notifier/actor.rs"),
        false,
    ),
];

const STATIC_COMMAND_CONSTRUCTORS: &[&str] = &[
    "RemotingCommand::create_remoting_command(",
    "RemotingCommand::create_request(",
    "RemotingCommand::create_request_command(",
    "RemotingCommand::create_response_command",
    "RemotingCommand::create_success_response_command",
    "RemotingCommand::create_java_default_error_response_command",
];

#[test]
fn controller_runtime_owners_do_not_bypass_their_command_factory() {
    let mut bypasses = Vec::new();

    for (path, source, has_inline_test_module) in RUNTIME_OWNER_SOURCES {
        let production = if *has_inline_test_module {
            source.split("mod tests {").next().unwrap_or(source)
        } else {
            source
        };
        for (line_index, line) in production.lines().enumerate() {
            if STATIC_COMMAND_CONSTRUCTORS
                .iter()
                .any(|constructor| line.contains(constructor))
            {
                bypasses.push(format!("{path}:{}: {}", line_index + 1, line.trim()));
            }
        }
    }

    assert!(
        bypasses.is_empty(),
        "Controller runtime owners must construct commands through their injected factory:\n{}",
        bypasses.join("\n")
    );
}
