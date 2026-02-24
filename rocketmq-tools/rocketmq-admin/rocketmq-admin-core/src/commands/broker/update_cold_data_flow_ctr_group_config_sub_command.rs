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

use std::collections::HashMap;
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
pub struct UpdateColdDataFlowCtrGroupConfigSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "update which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "update which cluster")]
    cluster_name: Option<String>,

    #[arg(
        short = 'g',
        long = "consumerGroup",
        required = true,
        help = "specific consumerGroup"
    )]
    consumer_group: String,

    #[arg(short = 'v', long = "threshold", required = true, help = "cold read threshold value")]
    threshold: String,
}

struct ParsedCommand {
    consumer_group: String,
    threshold: String,
}

impl ParsedCommand {
    fn new(command: &UpdateColdDataFlowCtrGroupConfigSubCommand) -> Result<Self, RocketMQError> {
        let consumer_group = command.consumer_group.trim();
        if consumer_group.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateColdDataFlowCtrGroupConfigSubCommand: consumer_group is empty".into(),
            ));
        }
        let threshold = command.threshold.trim();
        if threshold.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateColdDataFlowCtrGroupConfigSubCommand: threshold is empty".into(),
            ));
        }
        Ok(Self {
            consumer_group: consumer_group.into(),
            threshold: threshold.into(),
        })
    }
}

impl CommandExecute for UpdateColdDataFlowCtrGroupConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let target = Target::new(&self.cluster_name, &self.broker_addr).map_err(|_| {
            RocketMQError::IllegalArgument(
                "UpdateColdDataFlowCtrGroupConfigSubCommand: Specify exactly one of --brokerAddr (-b) or \
                 --clusterName (-c)"
                    .into(),
            )
        })?;
        let command = ParsedCommand::new(self)?;
        let mut properties = HashMap::<CheetahString, CheetahString>::new();
        properties.insert(command.consumer_group.clone().into(), command.threshold.clone().into());

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
                "UpdateColdDataFlowCtrGroupConfigSubCommand: Failed to start MQAdminExt: {}",
                e
            ))
        })?;

        let operation_result = async {
            match target {
                Target::BrokerAddr(broker_addr) => {
                    update_config_for_broker(&default_mqadmin_ext, &broker_addr, properties).await
                }
                Target::ClusterName(cluster_name) => {
                    let failed_broker_addr =
                        update_config_for_cluster(&default_mqadmin_ext, &cluster_name, properties).await?;
                    if failed_broker_addr.is_empty() {
                        Ok(())
                    } else {
                        Err(RocketMQError::Internal(format!(
                            "UpdateColdDataFlowCtrGroupConfigSubCommand: Failed to update for brokers {}",
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

async fn update_config_for_broker(
    default_mqadmin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
    properties: HashMap<CheetahString, CheetahString>,
) -> Result<(), RocketMQError> {
    match default_mqadmin_ext
        .update_cold_data_flow_ctr_group_config(broker_addr.into(), properties)
        .await
    {
        Ok(_) => {
            println!(
                "Update cold data flow control group config was successful for broker {}.",
                broker_addr
            );
            Ok(())
        }
        Err(e) => Err(RocketMQError::Internal(format!(
            "UpdateColdDataFlowCtrGroupConfigSubCommand: Failed to update for broker {}: {}",
            broker_addr, e
        ))),
    }
}

async fn update_config_for_cluster(
    default_mqadmin_ext: &DefaultMQAdminExt,
    cluster_name: &str,
    properties: HashMap<CheetahString, CheetahString>,
) -> Result<Vec<CheetahString>, RocketMQError> {
    let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await?;

    match CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name) {
        Ok(addresses) => {
            let failed_brokers: Vec<CheetahString> =
                futures::future::join_all(addresses.into_iter().map(|addr| async {
                    update_config_for_broker(default_mqadmin_ext, addr.as_str(), properties.clone())
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
            "UpdateColdDataFlowCtrGroupConfigSubCommand: Failed to fetch broker addresses by cluster name {}",
            e
        ))),
    }
}
