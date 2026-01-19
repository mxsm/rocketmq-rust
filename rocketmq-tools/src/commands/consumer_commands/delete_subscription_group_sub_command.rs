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
use rocketmq_common::common::mix_all::get_dlq_topic;
use rocketmq_common::common::mix_all::get_retry_topic;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct DeleteSubscriptionGroupSubCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,

    /// Broker address
    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "delete subscription group from which broker"
    )]
    broker_addr: Option<String>,

    /// Cluster name
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "delete subscription group from which cluster"
    )]
    cluster_name: Option<String>,

    /// Subscription group name
    #[arg(short = 'g', long = "groupName", required = true, help = "subscription group name")]
    group_name: String,

    /// Whether to remove consumer offset
    #[arg(
        short = 'r',
        long = "removeOffset",
        required = false,
        default_value = "false",
        help = "remove offset"
    )]
    remove_offset: bool,
}

impl DeleteSubscriptionGroupSubCommand {
    async fn delete_from_broker(
        admin_ext: &mut DefaultMQAdminExt,
        broker_addr: &str,
        group_name: &str,
        clean_offset: bool,
    ) -> RocketMQResult<()> {
        admin_ext
            .delete_subscription_group(broker_addr.into(), group_name.into(), Some(clean_offset))
            .await?;
        println!(
            "delete subscription group [{}] from broker [{}] success.",
            group_name, broker_addr
        );
        Ok(())
    }

    async fn delete_from_cluster(
        admin_ext: &mut DefaultMQAdminExt,
        cluster_name: &str,
        group_name: &str,
        clean_offset: bool,
    ) -> RocketMQResult<()> {
        let cluster_info = admin_ext.examine_broker_cluster_info().await?;
        let master_set = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;

        for master_addr in &master_set {
            admin_ext
                .delete_subscription_group(master_addr.clone(), group_name.into(), Some(clean_offset))
                .await?;
            println!(
                "delete subscription group [{}] from broker [{}] in cluster [{}] success.",
                group_name, master_addr, cluster_name
            );
        }

        let retry_topic = get_retry_topic(group_name);
        let dlq_topic = get_dlq_topic(group_name);

        if let Err(e) = admin_ext
            .delete_topic(retry_topic.clone().into(), cluster_name.into())
            .await
        {
            eprintln!("{}", e);
        }

        if let Err(e) = admin_ext
            .delete_topic(dlq_topic.clone().into(), cluster_name.into())
            .await
        {
            eprintln!("{}", e);
        }

        Ok(())
    }
}

impl CommandExecute for DeleteSubscriptionGroupSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.broker_addr.is_none() && self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "DeleteSubscriptionGroupSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ));
        }

        let group_name = self.group_name.trim();
        if group_name.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "DeleteSubscriptionGroupSubCommand: groupName (-g) cannot be empty".into(),
            ));
        }

        let mut admin_ext = DefaultMQAdminExt::new();
        admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        if let Some(addr) = &self.common_args.namesrv_addr {
            admin_ext.set_namesrv_addr(addr.trim());
        }

        admin_ext.start().await.map_err(|e| {
            RocketMQError::Internal(format!(
                "DeleteSubscriptionGroupSubCommand: Failed to start MQAdminExt: {}",
                e
            ))
        })?;

        let result = if let Some(broker_addr) = &self.broker_addr {
            Self::delete_from_broker(&mut admin_ext, broker_addr.trim(), group_name, self.remove_offset).await
        } else if let Some(cluster_name) = &self.cluster_name {
            Self::delete_from_cluster(&mut admin_ext, cluster_name.trim(), group_name, self.remove_offset).await
        } else {
            Err(RocketMQError::IllegalArgument(
                "DeleteSubscriptionGroupSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ))
        };

        admin_ext.shutdown().await;
        result
    }
}
