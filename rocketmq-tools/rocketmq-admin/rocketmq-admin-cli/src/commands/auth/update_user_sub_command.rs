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

use crate::commands::CommandExecute;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_admin_core::core::auth::AuthService;
use rocketmq_admin_core::core::auth::UpdateUserRequest;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use std::sync::Arc;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])),
    group(ArgGroup::new("update_field")
    .required(true)
    .args(&["password", "user_type","user_status"]))
)]
pub struct UpdateUserSubCommand {
    #[arg(short = 'c', long = "clusterName", required = false)]
    cluster_name: Option<String>,

    #[arg(short = 'b', long = "brokerAddr", required = false)]
    broker_addr: Option<String>,

    #[arg(short = 'u', long = "username", required = true)]
    username: String,

    #[arg(short = 'p', long = "password")]
    password: Option<String>,

    #[arg(short = 't', long = "userType")]
    user_type: Option<String>,

    #[arg(short = 's', long = "userStatus")]
    user_status: Option<String>,
}

impl CommandExecute for UpdateUserSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = UpdateUserRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.username.clone(),
            self.password.clone(),
            self.user_type.clone(),
            self.user_status.clone(),
        )?;
        let result = AuthService::update_user_by_request_with_rpc_hook(request, rpc_hook).await?;
        for broker_addr in result.broker_addrs {
            println!("update user to {} success.", broker_addr);
        }
        Ok(())
    }
}
