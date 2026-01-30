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

use std::sync::Arc;

use clap::Parser;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct AllocateMQSubCommand {
    /// Topic name
    #[arg(short = 't', long = "topic", required = true)]
    topic: String,

    /// Comma-separated list of IP addresses
    #[arg(short = 'i', long = "ipList", required = true)]
    ip_list: String,
}

impl CommandExecute for AllocateMQSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        unimplemented!("AllocateMQSubCommand is not implemented yet");
    }
}
