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
use rocketmq_admin_core::client_adapter::services::auth::DeleteAclRequest;
use rocketmq_error::RocketMQResult;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["cluster_name", "broker_addr"])))]
pub struct DeleteAclSubCommand {
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "delete acl from which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        help = "delete acl from which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 's',
        long = "subject",
        required = true,
        help = "the subject of acl to delete"
    )]
    subject: String,

    #[arg(
        short = 'r',
        long = "resources",
        required = false,
        help = "the resources of acl to delete"
    )]
    resources: Option<String>,
}

impl CommandExecute for DeleteAclSubCommand {
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> RocketMQResult<()> {
        let request = DeleteAclRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.subject.clone(),
            self.resources.clone(),
        )?;
        let result =
            AuthService::delete_acl_by_request_with_credentials(request, credentials, client_runtime.clone()).await?;
        for broker_addr in result.broker_addrs {
            println!("delete acl to {} success.", broker_addr);
        }
        Ok(())
    }
}
