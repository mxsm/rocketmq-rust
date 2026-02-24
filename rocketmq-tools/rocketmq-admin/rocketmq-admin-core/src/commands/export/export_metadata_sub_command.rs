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
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

const DEFAULT_FILE_PATH: &str = "/tmp/rocketmq/export";
const TIMEOUT_MILLIS: u64 = 10000;

#[derive(Debug, Clone, Parser)]
pub struct ExportMetadataSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "choose a cluster to export"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "choose a broker to export"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'f',
        long = "filePath",
        required = false,
        default_value = DEFAULT_FILE_PATH,
        help = "export metadata.json path | default /tmp/rocketmq/export"
    )]
    file_path: String,

    #[arg(
        short = 't',
        long = "topic",
        required = false,
        default_value = "false",
        help = "only export topic metadata"
    )]
    topic: bool,

    #[arg(
        short = 'g',
        long = "subscriptionGroup",
        required = false,
        default_value = "false",
        help = "only export subscriptionGroup metadata"
    )]
    subscription_group: bool,

    #[arg(
        short = 's',
        long = "specialTopic",
        required = false,
        default_value = "false",
        help = "need retryTopic and dlqTopic"
    )]
    special_topic: bool,
}

impl ExportMetadataSubCommand {
    async fn export_from_broker(
        admin_ext: &DefaultMQAdminExt,
        broker_addr: &str,
        file_path: &str,
        topic_only: bool,
        subscription_group_only: bool,
        special_topic: bool,
    ) -> RocketMQResult<()> {
        let addr: CheetahString = broker_addr.into();

        if topic_only {
            let export_path = format!("{}/topic.json", file_path);
            let topic_config_wrapper = admin_ext
                .get_user_topic_config(addr, special_topic, TIMEOUT_MILLIS)
                .await?;
            let json_content = serde_json::to_string_pretty(&topic_config_wrapper)
                .map_err(|e| RocketMQError::Internal(format!("Failed to serialize topic config: {}", e)))?;
            rocketmq_common::FileUtils::string_to_file(&json_content, &export_path)?;
            println!("export {} success", export_path);
        } else if subscription_group_only {
            let export_path = format!("{}/subscriptionGroup.json", file_path);
            let subscription_group_wrapper = admin_ext.get_user_subscription_group(addr, TIMEOUT_MILLIS).await?;
            let json_content = serde_json::to_string_pretty(&subscription_group_wrapper).map_err(|e| {
                RocketMQError::Internal(format!("Failed to serialize subscription group config: {}", e))
            })?;
            rocketmq_common::FileUtils::string_to_file(&json_content, &export_path)?;
            println!("export {} success", export_path);
        } else {
            return Err(RocketMQError::IllegalArgument(
                "ExportMetadataSubCommand: When using -b, you must specify -t (topic) or -g (subscriptionGroup)".into(),
            ));
        }

        Ok(())
    }

    async fn export_from_cluster(
        admin_ext: &DefaultMQAdminExt,
        cluster_name: &str,
        file_path: &str,
        topic_only: bool,
        subscription_group_only: bool,
        special_topic: bool,
    ) -> RocketMQResult<()> {
        let cluster_info = admin_ext.examine_broker_cluster_info().await?;
        let master_set = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name)?;

        let mut topic_config_map: HashMap<CheetahString, TopicConfig> = HashMap::new();
        let mut sub_group_config_map: HashMap<CheetahString, SubscriptionGroupConfig> = HashMap::new();

        for addr in &master_set {
            let topic_config_wrapper = admin_ext
                .get_user_topic_config(addr.clone(), special_topic, TIMEOUT_MILLIS)
                .await?;

            let subscription_group_wrapper = admin_ext
                .get_user_subscription_group(addr.clone(), TIMEOUT_MILLIS)
                .await?;

            if let Some(topic_table) = topic_config_wrapper.topic_config_table() {
                for (key, value) in topic_table {
                    if let Some(existing) = topic_config_map.get(key) {
                        let mut merged = value.clone();
                        merged.write_queue_nums = existing.write_queue_nums + value.write_queue_nums;
                        merged.read_queue_nums = existing.read_queue_nums + value.read_queue_nums;
                        topic_config_map.insert(key.clone(), merged);
                    } else {
                        topic_config_map.insert(key.clone(), value.clone());
                    }
                }
            }

            for entry in subscription_group_wrapper.get_subscription_group_table().iter() {
                let key = entry.key().clone();
                let value = entry.value();
                if let Some(existing) = sub_group_config_map.get(&key) {
                    let mut merged = (**value).clone();
                    merged.set_retry_queue_nums(existing.retry_queue_nums() + value.retry_queue_nums());
                    sub_group_config_map.insert(key, merged);
                } else {
                    sub_group_config_map.insert(key, (**value).clone());
                }
            }
        }

        let mut result = serde_json::Map::new();

        let export_path;
        if topic_only {
            result.insert(
                "topicConfigTable".to_string(),
                serde_json::to_value(&topic_config_map)
                    .map_err(|e| RocketMQError::Internal(format!("Failed to serialize topic config: {}", e)))?,
            );
            export_path = format!("{}/topic.json", file_path);
        } else if subscription_group_only {
            result.insert(
                "subscriptionGroupTable".to_string(),
                serde_json::to_value(&sub_group_config_map).map_err(|e| {
                    RocketMQError::Internal(format!("Failed to serialize subscription group config: {}", e))
                })?,
            );
            export_path = format!("{}/subscriptionGroup.json", file_path);
        } else {
            result.insert(
                "topicConfigTable".to_string(),
                serde_json::to_value(&topic_config_map)
                    .map_err(|e| RocketMQError::Internal(format!("Failed to serialize topic config: {}", e)))?,
            );
            result.insert(
                "subscriptionGroupTable".to_string(),
                serde_json::to_value(&sub_group_config_map).map_err(|e| {
                    RocketMQError::Internal(format!("Failed to serialize subscription group config: {}", e))
                })?,
            );
            export_path = format!("{}/metadata.json", file_path);
        }

        result.insert(
            "exportTime".to_string(),
            serde_json::Value::Number(serde_json::Number::from(get_current_millis())),
        );

        let json_content = serde_json::to_string_pretty(&result)
            .map_err(|e| RocketMQError::Internal(format!("Failed to serialize export result: {}", e)))?;

        rocketmq_common::FileUtils::string_to_file(&json_content, &export_path)?;
        println!("export {} success", export_path);

        Ok(())
    }
}

impl CommandExecute for ExportMetadataSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.broker_addr.is_none() && self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "ExportMetadataSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
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
            RocketMQError::Internal(format!("ExportMetadataSubCommand: Failed to start MQAdminExt: {}", e))
        })?;

        let file_path = self.file_path.trim();

        let result = if let Some(broker_addr) = &self.broker_addr {
            Self::export_from_broker(
                &admin_ext,
                broker_addr.trim(),
                file_path,
                self.topic,
                self.subscription_group,
                self.special_topic,
            )
            .await
        } else if let Some(cluster_name) = &self.cluster_name {
            Self::export_from_cluster(
                &admin_ext,
                cluster_name.trim(),
                file_path,
                self.topic,
                self.subscription_group,
                self.special_topic,
            )
            .await
        } else {
            Err(RocketMQError::IllegalArgument(
                "ExportMetadataSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ))
        };

        admin_ext.shutdown().await;
        result
    }
}
