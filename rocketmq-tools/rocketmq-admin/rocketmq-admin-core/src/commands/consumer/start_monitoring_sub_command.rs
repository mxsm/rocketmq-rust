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
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct StartMonitoringSubCommand {
    #[arg(
        short = 'n',
        long = "namesrvAddr",
        required = false,
        help = "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'"
    )]
    namesrv_addr: Option<String>,
}

impl CommandExecute for StartMonitoringSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> rocketmq_error::RocketMQResult<()> {
        todo!("Depends on MonitorService implementation")
    }
}
