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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct CleanBrokerMetadataSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        required = true,
        help = "The address of controller"
    )]
    controller_address: String,

    #[arg(
        short = 'b',
        long = "brokerControllerIdsToClean",
        required = false,
        help = "The brokerController id list which requires to clean metadata. eg: 1;2;3, means that clean broker-1, \
                broker-2 and broker-3"
    )]
    broker_controller_ids_to_clean: Option<String>,

    #[arg(
        long = "brokerName",
        visible_alias = "bn",
        required = true,
        help = "The broker name of the replicas that require to be manipulated"
    )]
    broker_name: String,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "The clusterName of broker"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'l',
        long = "cleanLivingBroker",
        required = false,
        help = "Whether clean up living brokers, default value is false"
    )]
    clean_living_broker: bool,
}

impl CleanBrokerMetadataSubCommand {
    fn validate_broker_controller_ids(ids: &str) -> RocketMQResult<()> {
        for id_str in ids.split(';') {
            let trimmed = id_str.trim();
            if !trimmed.is_empty() {
                trimmed.parse::<i64>().map_err(|_| {
                    RocketMQError::IllegalArgument(format!(
                        "please set the option <brokerControllerIdsToClean> according to the format, invalid id: {}",
                        id_str
                    ))
                })?;
            }
        }
        Ok(())
    }
}

impl CommandExecute for CleanBrokerMetadataSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let controller_address = self.controller_address.trim();
        let broker_name = self.broker_name.trim();

        if let Some(ref ids) = self.broker_controller_ids_to_clean {
            Self::validate_broker_controller_ids(ids)?;
        }

        if !self.clean_living_broker && self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "cleanLivingBroker option is false, clusterName option can not be empty.".to_string(),
            ));
        }

        let cluster_name = self.cluster_name.clone().unwrap_or_default();

        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "CleanBrokerMetadataSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            default_mqadmin_ext
                .clean_controller_broker_data(
                    controller_address.into(),
                    cluster_name.into(),
                    broker_name.into(),
                    self.broker_controller_ids_to_clean.clone().map(Into::into),
                    self.clean_living_broker,
                )
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "CleanBrokerMetadataSubCommand: Failed to clean broker metadata: {}",
                        e
                    ))
                })?;

            println!("clear broker {} metadata from controller success!", broker_name);
            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
