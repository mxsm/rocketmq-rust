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
use rocketmq_admin_core::core::auth::CopyAclRequest;
use rocketmq_admin_core::core::auth::CopyAclResult;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct CopyAclSubCommand {
    #[arg(
        short = 'f',
        long = "fromBroker",
        required = true,
        help = "the source broker that the acls copy from"
    )]
    from_broker: String,

    #[arg(
        short = 't',
        long = "toBroker",
        required = true,
        help = "the target broker that the acls copy to"
    )]
    to_broker: String,

    #[arg(
        short = 's',
        long = "subjects",
        required = false,
        help = "the subject list of acl to copy"
    )]
    subjects: Option<String>,
}

impl CommandExecute for CopyAclSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = CopyAclRequest::try_new(self.from_broker.clone(), self.to_broker.clone(), self.subjects.clone())?;
        let from_broker = request.from_broker().clone();
        let to_broker = request.to_broker().clone();
        let result = AuthService::copy_acl_by_request_with_rpc_hook(request, rpc_hook).await?;
        render_copy_acl_result(result, from_broker.as_str(), to_broker.as_str());
        Ok(())
    }
}

fn render_copy_acl_result(result: CopyAclResult, source_broker: &str, target_broker: &str) {
    if result.copied_subjects.is_empty() && result.skipped_subjects.is_empty() && result.failures.is_empty() {
        println!("No ACLs found to copy from {}.", source_broker);
        return;
    }

    for subject in result.skipped_subjects {
        if subject.is_empty() {
            eprintln!("Warning: ACL has no subject, skipping.");
        } else {
            eprintln!("Warning: Could not find ACL {} at broker {}", subject, source_broker);
        }
    }
    for subject in result.copied_subjects {
        println!(
            "copy acl of {} from {} to {} success.",
            subject, source_broker, target_broker
        );
    }
    for failure in result.failures {
        if failure.broker_addr.as_str() == source_broker {
            eprintln!(
                "Warning: Failed to get ACL from {} while copying to {}: {}",
                source_broker, target_broker, failure.error
            );
        } else {
            eprintln!(
                "copy acl from {} to {} failed: {}",
                source_broker, target_broker, failure.error
            );
        }
    }
}
