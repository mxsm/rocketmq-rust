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

use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct GetControllerMetadataSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        value_name = "HOST:PORT",
        required = true,
        help = "Address of the controller to query"
    )]
    controller_address: String,
}

impl CommandExecute for GetControllerMetadataSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let controller_address: CheetahString = self.controller_address.as_str().into();

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetControllerMetadataSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let meta_data = default_mqadmin_ext
                .get_controller_meta_data(controller_address)
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetControllerMetadataSubCommand: Failed to get controller meta data: {}",
                        e
                    ))
                })?;

            let group = meta_data
                .group
                .map(|group| group.to_string())
                .unwrap_or(String::from("<NONE>"));
            println!("ControllerGroup\t{}", group);

            let controller_leader_id = meta_data
                .controller_leader_id
                .map(|controller_leader_id| controller_leader_id.to_string())
                .unwrap_or(String::from("<NONE>"));
            println!("ControllerLeaderId\t{}", controller_leader_id);

            let controller_leader_address = meta_data
                .controller_leader_address
                .map(|controller_leader_address| controller_leader_address.to_string())
                .unwrap_or(String::from("<NONE>"));
            println!("ControllerLeaderAddress\t{}", controller_leader_address);

            let is_leader = meta_data.is_leader.unwrap_or(false).to_string();
            println!("IsLeader\t{}", is_leader);

            if let Some(peers) = meta_data.peers {
                peers.split(";").for_each(|peer| println!("#Peer:\t{}", peer));
            } else {
                println!("No peers found");
            }

            Ok(())
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
