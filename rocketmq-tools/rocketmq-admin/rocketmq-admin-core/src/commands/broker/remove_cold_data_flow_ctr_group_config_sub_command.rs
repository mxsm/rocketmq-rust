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

use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::target::Target;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(ArgGroup::new("target")
    .required(true)
    .args(&["broker_addr", "cluster_name"]))
)]
pub struct RemoveColdDataFlowCtrGroupConfigSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "update which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "update which cluster")]
    cluster_name: Option<String>,

    #[arg(
        short = 'g',
        long = "consumerGroup",
        required = true,
        help = "the consumer group will remove from the config"
    )]
    consumer_group: String,
}

struct ParsedCommand {
    consumer_group: String,
}

impl ParsedCommand {
    fn new(command: &RemoveColdDataFlowCtrGroupConfigSubCommand) -> Result<Self, RocketMQError> {
        let consumer_group = command.consumer_group.trim();
        if consumer_group.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "RemoveColdDataFlowCtrGroupConfigSubCommand: consumer_group is empty".into(),
            ));
        }
        Ok(Self {
            consumer_group: consumer_group.into(),
        })
    }
}

impl CommandExecute for RemoveColdDataFlowCtrGroupConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let target = Target::new(&self.cluster_name, &self.broker_addr).map_err(|_| {
            RocketMQError::IllegalArgument(
                "RemoveColdDataFlowCtrGroupConfigSubCommand: Specify exactly one of --brokerAddr (-b) or \
                 --clusterName (-c)"
                    .into(),
            )
        })?;
        let command = ParsedCommand::new(self)?;

        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
            RocketMQError::Internal(format!(
                "RemoveColdDataFlowCtrGroupConfigSubCommand: Failed to start MQAdminExt: {}",
                e
            ))
        })?;

        let operation_result = async {
            match target {
                Target::BrokerAddr(broker_addr) => {
                    remove_config_for_broker(&default_mqadmin_ext, &broker_addr, &command.consumer_group).await
                }
                Target::ClusterName(cluster_name) => {
                    let failed_broker_addr =
                        remove_config_for_cluster(&default_mqadmin_ext, &cluster_name, &command.consumer_group).await?;
                    if failed_broker_addr.is_empty() {
                        Ok(())
                    } else {
                        Err(RocketMQError::Internal(format!(
                            "RemoveColdDataFlowCtrGroupConfigSubCommand: Failed to remove for brokers {}",
                            failed_broker_addr.join(", ")
                        )))
                    }
                }
            }
        }
        .await;
        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

async fn remove_config_for_broker(
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
    consumer_group: &str,
) -> Result<(), RocketMQError> {
    match default_mqadmin_ext
        .remove_cold_data_flow_ctr_group_config(CheetahString::from(broker_addr), CheetahString::from(consumer_group))
        .await
    {
        Ok(_) => {
            println!("remove broker cold read threshold success, {}", broker_addr);
            Ok(())
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "RemoveColdDataFlowCtrGroupConfigSubCommand: Failed to remove for broker {}: {}",
            broker_addr, e
        ))),
    }
}

async fn remove_config_for_cluster(
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: &str,
    consumer_group: &str,
) -> Result<Vec<CheetahString>, RocketMQError> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

    match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name) {
        Ok(addresses) => {
            let failed_brokers: Vec<CheetahString> =
                futures::future::join_all(addresses.into_iter().map(|addr| async move {
                    remove_config_for_broker(default_mqadmin_ext, addr.as_str(), consumer_group)
                        .await
                        .map_err(|_err| addr)
                }))
                .await
                .iter()
                .filter_map(|result| result.clone().err())
                .collect();

            Ok(failed_brokers)
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "RemoveColdDataFlowCtrGroupConfigSubCommand: Failed to fetch broker addresses by cluster name {}",
            e
        ))),
    }
}
