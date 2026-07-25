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

use clap::ArgGroup;
use clap::Parser;
use rocketmq_admin_core::client_adapter::services::auth::AuthService;
use rocketmq_admin_core::client_adapter::services::auth::CreateAclRequest;
use rocketmq_error::RocketMQResult;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct CreateAclSubCommand {
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create acl to which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        help = "create acl to which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 's',
        long = "subject",
        required = true,
        help = "the subject of acl to create"
    )]
    subject: String,

    #[arg(
        short = 'r',
        long = "resources",
        required = true,
        help = "the resources of acl to create"
    )]
    resources: String,

    #[arg(
        short = 'a',
        long = "actions",
        required = true,
        help = "the actions of acl to create"
    )]
    actions: String,

    #[arg(
        short = 'd',
        long = "decision",
        required = true,
        help = "the decision of acl to create"
    )]
    decision: String,

    #[arg(
        short = 'i',
        long = "sourceIp",
        required = false,
        help = "the sourceIps of acl to create"
    )]
    source_ip: Option<String>,
}

impl CommandExecute for CreateAclSubCommand {
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> RocketMQResult<()> {
        let request = CreateAclRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.subject.clone(),
            self.resources.clone(),
            self.actions.clone(),
            self.decision.clone(),
            self.source_ip.clone(),
        )?;
        let result =
            AuthService::create_acl_by_request_with_credentials(request, credentials, client_runtime.clone()).await?;
        for broker_addr in result.broker_addrs {
            println!("create acl to {} success.", broker_addr);
        }
        Ok(())
    }
}
