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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::FileUtils::file_to_string;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper::TopicRemappingDetailWrapper;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper::{self};
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::admin::mq_admin_utils::MQAdminUtils;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

const DEFAULT_BLOCK_SEQ_SIZE: i64 = 10000;

#[derive(Debug, Clone, Parser)]
pub struct RemappingStaticTopicSubCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,

    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    /// Broker names to remapping
    #[arg(
        short = 'b',
        long = "brokers",
        required = false,
        help = "remapping static topic to brokers, comma separated"
    )]
    brokers: Option<String>,

    /// Cluster names
    #[arg(
        short = 'c',
        long = "clusters",
        required = false,
        help = "remapping static topic to clusters, comma separated"
    )]
    clusters: Option<String>,

    /// Map file
    #[arg(short = 'm', long = "mapFile", required = false, help = "The mapping data file name")]
    mapping_file: Option<String>,

    /// Force replace
    #[arg(
        short = 'f',
        long = "forceReplace",
        required = false,
        help = "Force replace the old mapping"
    )]
    force_replace: Option<bool>,
}

impl RemappingStaticTopicSubCommand {
    pub async fn execute_from_file(&self, map_file_name: &str) -> RocketMQResult<()> {
        let mut default_mq_admin_ext = DefaultMQAdminExt::new();
        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        if let Some(addr) = &self.common_args.namesrv_addr {
            default_mq_admin_ext.set_namesrv_addr(addr.trim());
        }
        default_mq_admin_ext.start().await?;

        let topic = self.topic.trim();
        let map_file_name = map_file_name.trim();
        if let Ok(map_data) = file_to_string(map_file_name) {
            if let Ok(mut wrapper) = serde_json::from_str::<TopicRemappingDetailWrapper>(&map_data) {
                TopicQueueMappingUtils::check_name_epoch_num_consistence(
                    &CheetahString::from(topic),
                    wrapper.broker_config_map(),
                )?;

                TopicQueueMappingUtils::check_and_build_mapping_items(
                    TopicQueueMappingUtils::get_mapping_detail_from_config(
                        wrapper.broker_config_map().values().cloned().collect(),
                    )?,
                    false,
                    true,
                )?;

                let force = self.force_replace.unwrap_or(false);

                let brokers_to_map_in = wrapper.broker_to_map_in().clone();
                let brokers_to_map_out = wrapper.broker_to_map_out().clone();

                MQAdminUtils::remapping_static_topic(
                    &CheetahString::from(topic),
                    &brokers_to_map_in,
                    &brokers_to_map_out,
                    wrapper.broker_config_map_mut(),
                    DEFAULT_BLOCK_SEQ_SIZE,
                    force,
                    &default_mq_admin_ext,
                )
                .await?;
            }
        }

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}

impl CommandExecute for RemappingStaticTopicSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.topic.trim().is_empty() {
            return Err(RocketMQError::Internal("Topic name is required".to_string()));
        }

        if let Some(f_name) = &self.mapping_file {
            return self.execute_from_file(f_name).await;
        }

        let mut default_mq_admin_ext = DefaultMQAdminExt::new();
        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        if let Some(addr) = &self.common_args.namesrv_addr {
            default_mq_admin_ext.set_namesrv_addr(addr.trim());
        }
        default_mq_admin_ext.start().await?;

        if self.brokers.is_none() && self.clusters.is_none() {
            default_mq_admin_ext.shutdown().await;
            return Err(RocketMQError::Internal(
                "Either brokers (-b) or clusters (-c) must be provided".to_string(),
            ));
        }

        let topic = self.topic.trim();
        let mut target_brokers: HashSet<CheetahString> = HashSet::new();
        let client_metadata = ClientMetadata::new();

        let cluster_info = default_mq_admin_ext.examine_broker_cluster_info().await?;
        if cluster_info.cluster_addr_table.is_none() || cluster_info.cluster_addr_table.as_ref().unwrap().is_empty() {
            default_mq_admin_ext.shutdown().await;
            return Err(RocketMQError::Internal("The Cluster info is empty".to_string()));
        }
        client_metadata.refresh_cluster_info(Some(&cluster_info));

        if let Some(broker_strs) = &self.brokers {
            for broker in broker_strs.split(',') {
                target_brokers.insert(CheetahString::from(broker.trim()));
            }
        }

        if let Some(cluster_strs) = &self.clusters {
            if let Some(cluster_addr_table) = &cluster_info.cluster_addr_table {
                for cluster in cluster_strs.split(',') {
                    let cluster = cluster.trim();
                    if let Some(brokers) = cluster_addr_table.get(cluster) {
                        target_brokers.extend(brokers.iter().cloned());
                    }
                }
            }
        }

        if target_brokers.is_empty() {
            default_mq_admin_ext.shutdown().await;
            return Err(RocketMQError::Internal("Find none brokers, do nothing".to_string()));
        }

        for broker in &target_brokers {
            let addr = client_metadata.find_master_broker_addr(broker);
            if addr.is_none() {
                default_mq_admin_ext.shutdown().await;
                return Err(RocketMQError::Internal(format!(
                    "Can't find addr for broker {}",
                    broker
                )));
            }
        }

        let mut broker_config_map =
            MQAdminUtils::examine_topic_config_all(&CheetahString::from(topic), &default_mq_admin_ext).await?;

        if broker_config_map.is_empty() {
            default_mq_admin_ext.shutdown().await;
            return Err(RocketMQError::Internal(
                "No topic route to do the remapping".to_string(),
            ));
        }

        let max_epoch_and_num =
            TopicQueueMappingUtils::check_name_epoch_num_consistence(&CheetahString::from(topic), &broker_config_map)?;

        let old_wrapper = TopicRemappingDetailWrapper::new(
            CheetahString::from(topic),
            CheetahString::from(topic_remapping_detail_wrapper::TYPE_CREATE_OR_UPDATE),
            max_epoch_and_num.0 as u64,
            broker_config_map.clone(),
            HashSet::new(),
            HashSet::new(),
        );
        let old_mapping_data_file = TopicQueueMappingUtils::write_to_temp(&old_wrapper, false)?;
        println!("The old mapping data is written to file {}", old_mapping_data_file);

        let new_wrapper =
            TopicQueueMappingUtils::remapping_static_topic(topic, &mut broker_config_map, &target_brokers)?;

        let new_mapping_data_file = TopicQueueMappingUtils::write_to_temp(&new_wrapper, true)?;
        println!("The new mapping data is written to file {}", new_mapping_data_file);

        MQAdminUtils::complete_no_target_brokers(broker_config_map.clone(), &default_mq_admin_ext).await?;

        let force = self.force_replace.unwrap_or(false);
        MQAdminUtils::remapping_static_topic(
            &CheetahString::from(topic),
            new_wrapper.broker_to_map_in(),
            new_wrapper.broker_to_map_out(),
            &mut broker_config_map,
            DEFAULT_BLOCK_SEQ_SIZE,
            force,
            &default_mq_admin_ext,
        )
        .await?;

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remapping_static_topic_sub_command_parse_broker_mode() {
        let args = vec!["remapping-static-topic", "-t", "test-topic", "-b", "broker-a,broker-b"];

        let cmd = RemappingStaticTopicSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.topic, "test-topic");
        assert_eq!(cmd.brokers, Some("broker-a,broker-b".to_string()));
    }

    #[test]
    fn remapping_static_topic_sub_command_parse_cluster_mode() {
        let args = vec!["remapping-static-topic", "-t", "test-topic", "-c", "DefaultCluster"];

        let cmd = RemappingStaticTopicSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.topic, "test-topic");
        assert_eq!(cmd.clusters, Some("DefaultCluster".to_string()));
    }

    #[test]
    fn remapping_static_topic_sub_command_parse_with_file() {
        let args = vec!["remapping-static-topic", "-t", "test-topic", "-m", "/tmp/mapping.json"];

        let cmd = RemappingStaticTopicSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.mapping_file, Some("/tmp/mapping.json".to_string()));
    }
}
