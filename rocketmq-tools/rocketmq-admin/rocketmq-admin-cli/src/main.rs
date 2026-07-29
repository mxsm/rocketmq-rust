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

#![recursion_limit = "256"]

use rocketmq_admin_cli::rocketmq_cli::RocketMQCli;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::client_adapter::TelemetryHandle;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::mq_version::CURRENT_VERSION;
use rocketmq_protocol::protocol::remoting_command_facade::initialize_remoting_version;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

const CLI_RUNTIME_STACK_SIZE: usize = 16 * 1024 * 1024;

fn main() {
    let handle = match std::thread::Builder::new()
        .name("rocketmq-admin-cli-main".to_string())
        .stack_size(CLI_RUNTIME_STACK_SIZE)
        .spawn(run_cli_main_thread)
    {
        Ok(handle) => handle,
        Err(error) => {
            eprintln!("failed to spawn rocketmq-admin-cli main thread: {error}");
            std::process::exit(1);
        }
    };

    let exit_code = match handle.join() {
        Ok(Ok(exit_code)) => exit_code,
        Ok(Err(error)) => {
            eprintln!("failed to initialize or shut down rocketmq-admin-cli: {error}");
            1
        }
        Err(_) => {
            eprintln!("rocketmq-admin-cli main thread terminated unexpectedly");
            1
        }
    };
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn run_cli_main_thread() -> RocketMQResult<i32> {
    let owner = RuntimeOwner::new(admin_cli_runtime_config())
        .map_err(|source| RocketMQError::internal("build rocketmq-admin-cli runtime", source))?;
    let client_runtime = ClientRuntime::try_new(
        owner.root_context().child("rocketmq-admin-client"),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )?;
    let exit_code = owner.block_on(async_main(client_runtime.clone()));
    let client_report = owner.block_on(client_runtime.shutdown());
    if !client_report.is_healthy() {
        tracing::warn!(
            report = %client_report.to_json(),
            "rocketmq-admin client runtime shutdown report is unhealthy"
        );
    }
    let report = owner
        .shutdown_runtime_blocking()
        .map_err(|source| RocketMQError::internal("shut down rocketmq-admin-cli runtime", source))?;
    if !report.is_healthy() {
        tracing::warn!(
            report = %report.to_json(),
            "rocketmq-admin-cli runtime shutdown report is unhealthy"
        );
    }
    Ok(exit_code)
}

fn admin_cli_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::server_default("rocketmq-admin-cli");
    config.thread_stack_size = Some(CLI_RUNTIME_STACK_SIZE);
    config
}

async fn async_main(client_runtime: std::sync::Arc<ClientRuntime>) -> i32 {
    if let Err(error) = initialize_remoting_version(CURRENT_VERSION as i32) {
        eprintln!("failed to initialize the immutable admin CLI remoting version: {error}");
        return 1;
    }

    let cli = RocketMQCli::parse_from_java_compatible_args();
    cli.handle(client_runtime).await
}
