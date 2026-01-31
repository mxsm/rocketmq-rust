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

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
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
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        MQAdminExt::start(&mut default_mqadmin_ext)
            .await
            .map_err(|e| RocketMQError::Internal(format!("UpdateUserSubCommand: Failed to start MQAdminExt: {}", e)))?;

        let username = CheetahString::from(self.username.trim());
        let password = self
            .password
            .as_deref()
            .map(|s| CheetahString::from(s.trim()))
            .unwrap_or_default();
        let user_type = self
            .user_type
            .as_deref()
            .map(|s| CheetahString::from(s.trim()))
            .unwrap_or_default();
        let user_status = self
            .user_status
            .as_deref()
            .map(|s| CheetahString::from(s.trim()))
            .unwrap_or_default();

        if let Some(broker) = self.broker_addr.as_deref().map(str::trim) {
            default_mqadmin_ext
                .update_user(
                    broker.into(),
                    username.clone(),
                    password.clone(),
                    user_type.clone(),
                    user_status.clone(),
                )
                .await?;
        } else if let Some(cluster_name) = self.cluster_name.as_deref().map(str::trim) {
            let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

            match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name) {
                Ok(addresses) => {
                    for address in addresses {
                        default_mqadmin_ext
                            .update_user(
                                address,
                                username.clone(),
                                password.clone(),
                                user_type.clone(),
                                user_status.clone(),
                            )
                            .await?;
                    }
                }
                Err(e) => {
                    return Err(RocketMQError::Internal(format!(
                        "UpdateUserSubCommand: Failed to update user: {}",
                        e
                    )));
                }
            }
        }

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;

        Ok(())
    }
}
