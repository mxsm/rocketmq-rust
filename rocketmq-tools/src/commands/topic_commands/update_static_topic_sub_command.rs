use std::collections::HashSet;

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

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::admin::mq_admin_utils::MQAdminUtils;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct UpdateStaticTopicSubCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,
    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
    /// Broker address
    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = true,
        help = "create topic to which broker"
    )]
    broker_addr: String,
    /// "totalQueueNum"
    #[arg(short = 'q', long = "totalQueueNum", required = true, help = "totalQueueNum")]
    queue_num: String,
    /// Cluster name
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,
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
    force_replace: Option<String>,
}
impl UpdateStaticTopicSubCommand {
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
            if let Ok(wrapper) = serde_json::from_str::<TopicRemappingDetailWrapper>(&map_data) {
                //double check the config
                TopicQueueMappingUtils::check_name_epoch_num_consistence(
                    &CheetahString::from(topic),
                    wrapper.broker_config_map(),
                )?;
                let mut force = false;
                if let Some(fr) = &self.force_replace {
                    if let Ok(f) = fr.parse::<bool>() {
                        force = f;
                    }
                }
                TopicQueueMappingUtils::get_mapping_detail_from_config(
                    wrapper.broker_config_map().values().cloned().collect(),
                )?;
                TopicQueueMappingUtils::check_and_build_mapping_items(vec![], force, true)?;

                MQAdminUtils::complete_no_target_brokers(wrapper.broker_config_map().clone(), &default_mq_admin_ext)
                    .await?;
                MQAdminUtils::update_topic_config_mapping_all(
                    wrapper.broker_config_map(),
                    &default_mq_admin_ext,
                    force,
                )
                .await?;
            }
        }
        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
impl CommandExecute for UpdateStaticTopicSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> RocketMQResult<()> {
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
        let mut target_brokers = HashSet::new();

        let topic = self.topic.trim();

        let cluster_info = default_mq_admin_ext.examine_broker_cluster_info().await?;
        if let Some(info) = &cluster_info.cluster_addr_table {
            if info.is_empty() {
                return Err(rocketmq_error::RocketMQError::Internal(
                    "The Cluster info is empty".to_string(),
                ));
            }

            let broker_strs = self.broker_addr.trim();
            for broker in broker_strs.split(",") {
                target_brokers.insert(CheetahString::from(broker.trim()));
            }
            if let Some(cluster) = &self.cluster_name {
                let clusters = cluster.trim();
                for cluster in clusters.split(",") {
                    let cluster = cluster.trim();
                    if let Some(bs) = info.get(cluster) {
                        let s_iter = bs.iter().cloned();
                        target_brokers.extend(s_iter);
                    }
                }
            }
        }
        if target_brokers.is_empty() {
            return Err(rocketmq_error::RocketMQError::Internal(
                "Find none brokers, do nothing".to_string(),
            ));
        }

        //get the existed topic config and mapping

        let mut broker_config_map =
            MQAdminUtils::examine_topic_config_all(&CheetahString::from(topic), &default_mq_admin_ext).await?;
        let queue_num = self
            .queue_num
            .trim()
            .parse::<i32>()
            .map_err(|_e| RocketMQError::Internal("queue num parse to i32 failed".to_string()))?;

        let mut max_epoch_and_num = (get_current_millis(), queue_num);
        if !broker_config_map.is_empty() {
            let new_max_epoch_and_num = TopicQueueMappingUtils::check_name_epoch_num_consistence(
                &CheetahString::from(topic),
                &broker_config_map,
            )?;
            max_epoch_and_num.0 = new_max_epoch_and_num.0 as u64;
            max_epoch_and_num.1 = new_max_epoch_and_num.1;
        }

        let old_wrapper = TopicRemappingDetailWrapper::new(
            CheetahString::from(topic),
            CheetahString::from(topic_remapping_detail_wrapper::TYPE_CREATE_OR_UPDATE),
            max_epoch_and_num.0,
            broker_config_map.clone(),
            HashSet::new(),
            HashSet::new(),
        );
        let old_mapping_data_file = TopicQueueMappingUtils::write_to_temp(&old_wrapper, false)?;
        println!("The old mapping data is written to file {}", old_mapping_data_file);

        //add the existed brokers to target brokers
        target_brokers.extend(broker_config_map.keys().cloned());

        //calculate the new data
        let new_wrapper = TopicQueueMappingUtils::create_topic_config_mapping(
            topic,
            queue_num,
            &target_brokers,
            &mut broker_config_map,
        )?;

        let new_mapping_data_file = TopicQueueMappingUtils::write_to_temp(&new_wrapper, true)?;
        println!("The new mapping data is written to file {}", new_mapping_data_file);

        MQAdminUtils::complete_no_target_brokers(broker_config_map.clone(), &default_mq_admin_ext).await?;
        MQAdminUtils::update_topic_config_mapping_all(&broker_config_map, &default_mq_admin_ext, false).await?;

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
