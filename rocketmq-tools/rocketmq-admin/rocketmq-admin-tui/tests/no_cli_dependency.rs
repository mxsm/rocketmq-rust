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

#[test]
fn tui_depends_on_core_not_cli_adapter() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read rocketmq-admin-tui Cargo.toml");

    assert!(
        manifest.contains("rocketmq-admin-core"),
        "rocketmq-admin-tui should reuse rocketmq-admin-core services"
    );
    assert!(
        !manifest.contains("rocketmq-admin-cli"),
        "rocketmq-admin-tui should not depend on the CLI adapter crate"
    );

    for dependency in ["clap", "clap_complete", "tabled", "colored", "dialoguer", "indicatif"] {
        assert!(
            !manifest.contains(dependency),
            "rocketmq-admin-tui should not depend on CLI-only crate {dependency}"
        );
    }
}

#[test]
fn tui_facade_does_not_reach_into_cli_modules() {
    let facade_rs = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/admin_facade.rs"))
        .expect("read rocketmq-admin-tui src/admin_facade.rs");
    let commands_rs =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/commands.rs")).expect("read src/commands.rs");

    assert!(
        facade_rs.contains("rocketmq_admin_core::"),
        "TuiAdminFacade should be backed by admin-core DTOs and services"
    );
    assert!(
        !facade_rs.contains("rocketmq_admin_cli"),
        "TuiAdminFacade should not call into rocketmq-admin-cli"
    );
    assert!(
        !facade_rs.contains("crate::commands"),
        "TuiAdminFacade should not call CLI command modules"
    );
    assert!(
        !commands_rs.contains("rocketmq_admin_cli") && !commands_rs.contains("clap::"),
        "rocketmq-admin-tui command catalog should not call into CLI command modules or parsers"
    );
}
