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
use rocketmq_admin_core::core::auth::AuthService;
use rocketmq_admin_core::core::auth::CopyUsersRequest;
use rocketmq_admin_core::core::auth::CopyUsersResult;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct CopyUsersSubCommand {
    #[arg(
        short = 'f',
        long = "fromBroker",
        required = true,
        help = "the source broker that the users copy from"
    )]
    from_broker: String,

    #[arg(
        short = 't',
        long = "toBroker",
        required = true,
        help = "the target broker that the users copy to"
    )]
    to_broker: String,

    #[arg(
        short = 'u',
        long = "usernames",
        required = false,
        help = "the username list of user to copy"
    )]
    usernames: Option<String>,
}

impl CommandExecute for CopyUsersSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request =
            CopyUsersRequest::try_new(self.from_broker.clone(), self.to_broker.clone(), self.usernames.clone())?;
        let from_broker = request.from_broker().clone();
        let to_broker = request.to_broker().clone();
        let result = AuthService::copy_users_by_request_with_rpc_hook(request, rpc_hook).await?;
        render_copy_users_result(result, from_broker.as_str(), to_broker.as_str());
        Ok(())
    }
}

fn render_copy_users_result(result: CopyUsersResult, source_broker: &str, target_broker: &str) {
    if result.copied_usernames.is_empty() && result.skipped_usernames.is_empty() && result.failures.is_empty() {
        println!("No users found to copy from {}.", source_broker);
        return;
    }

    for username in result.skipped_usernames {
        if username.is_empty() {
            eprintln!("Warning: User has no username, skipping.");
        } else {
            eprintln!("Warning: Could not find user {} at broker {}", username, source_broker);
        }
    }
    for username in result.copied_usernames {
        println!(
            "copy user of {} from {} to {} success.",
            username, source_broker, target_broker
        );
    }
    for failure in result.failures {
        eprintln!(
            "copy user from {} to {} failed: {}",
            source_broker, target_broker, failure.error
        );
    }
}
