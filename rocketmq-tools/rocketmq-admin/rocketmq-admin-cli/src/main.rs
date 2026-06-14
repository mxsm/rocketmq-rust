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

use rocketmq_admin_cli::rocketmq_cli::RocketMQCli;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_remoting::protocol::remoting_command;

const CLI_RUNTIME_STACK_SIZE: usize = 16 * 1024 * 1024;

fn main() {
    let handle = std::thread::Builder::new()
        .name("rocketmq-admin-cli-main".to_string())
        .stack_size(CLI_RUNTIME_STACK_SIZE)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(CLI_RUNTIME_STACK_SIZE)
                .build()
                .expect("failed to build rocketmq-admin-cli runtime");
            runtime.block_on(async_main());
        })
        .expect("failed to spawn rocketmq-admin-cli main thread");

    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}

async fn async_main() {
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    let cli = RocketMQCli::parse_from_java_compatible_args();
    cli.handle().await;
}
