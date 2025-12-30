use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use rocketmq_remoting::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
use rocketmq_remoting::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_rust::ArcMut;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;

pub struct MQAdminUtils {}
impl MQAdminUtils {
    pub async fn get_all_brokers_in_same_cluster(
        brokers: Vec<CheetahString>,
        default_mq_admin_ext: &DefaultMQAdminExt,
    ) -> RocketMQResult<HashSet<CheetahString>> {
        let cluster_info = default_mq_admin_ext.examine_broker_cluster_info().await?;
        if cluster_info.cluster_addr_table.is_none() {
            return Err(rocketmq_error::RocketMQError::Internal(
                "The Cluster info is empty".to_string(),
            ));
        } else if let Some(c_table) = &cluster_info.cluster_addr_table {
            let mut all_brokers = HashSet::new();
            for broker in &brokers {
                if all_brokers.contains(&CheetahString::from(broker)) {
                    continue;
                }
                for cluster_brokers in c_table.values() {
                    if cluster_brokers.contains(&CheetahString::from(broker)) {
                        all_brokers.extend(cluster_brokers.clone());
                        break;
                    }
                }
            }
            return Ok(all_brokers);
        }
        Err(rocketmq_error::RocketMQError::Internal(
            "get_all_brokers_in_same_cluster err".to_string(),
        ))
    }
    pub async fn complete_no_target_brokers(
        mut broker_config_map: HashMap<CheetahString, TopicConfigAndQueueMapping>,
        default_mq_admin_ext: &DefaultMQAdminExt,
    ) -> RocketMQResult<()> {
        let config_mapping = broker_config_map.values_mut().next().cloned();
        if let Some(config_mapping) = config_mapping {
            if let Some(topic) = &config_mapping.topic_config.topic_name {
                if let Some(detail) = &config_mapping.topic_queue_mapping_detail {
                    let queue_num = detail.topic_queue_mapping_info.total_queues;
                    let new_epoch = detail.topic_queue_mapping_info.epoch;
                    let all_brokers = MQAdminUtils::get_all_brokers_in_same_cluster(
                        broker_config_map.keys().cloned().collect(),
                        default_mq_admin_ext,
                    )
                    .await;
                    if let Ok(all_brokers) = &all_brokers {
                        for broker in all_brokers {
                            broker_config_map.entry(broker.clone()).or_insert_with(|| {
                                TopicConfigAndQueueMapping::new(
                                    TopicConfig::new(topic.clone()),
                                    Some(ArcMut::new(TopicQueueMappingDetail {
                                        topic_queue_mapping_info: TopicQueueMappingInfo::new(
                                            topic.clone(),
                                            queue_num,
                                            broker.clone(),
                                            new_epoch,
                                        ),
                                        hosted_queues: None,
                                    })),
                                )
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub async fn refresh_cluster_info(
        default_mq_admin_ext: &DefaultMQAdminExt,
        client_metadata: &ClientMetadata,
    ) -> RocketMQResult<()> {
        let cluster_info = default_mq_admin_ext.examine_broker_cluster_info().await;
        if let Ok(info) = &cluster_info {
            client_metadata.refresh_cluster_info(Some(info));
            return Ok(());
        }
        Err(RocketMQError::Internal("The Cluster info is empty".to_string()))
    }
    pub async fn get_broker_metadata(default_mq_admin_ext: &DefaultMQAdminExt) -> RocketMQResult<ClientMetadata> {
        let client_metadata = ClientMetadata::new();
        MQAdminUtils::refresh_cluster_info(default_mq_admin_ext, &client_metadata).await?;
        Ok(client_metadata)
    }
    pub fn check_if_master_alive(brokers: Vec<CheetahString>, client_metadata: &ClientMetadata) -> RocketMQResult<()> {
        for broker in &brokers {
            let addr = client_metadata.find_master_broker_addr(broker);
            if addr.is_none() {
                return Err(RocketMQError::Internal(format!(
                    "Can't find addr for broker {}",
                    broker
                )));
            }
        }
        Ok(())
    }
    pub async fn update_topic_config_mapping_all(
        broker_config_map: &HashMap<CheetahString, TopicConfigAndQueueMapping>,
        default_mq_admin_ext: &DefaultMQAdminExt,
        force: bool,
    ) -> RocketMQResult<()> {
        let client_meta_data = MQAdminUtils::get_broker_metadata(default_mq_admin_ext).await?;
        MQAdminUtils::check_if_master_alive(broker_config_map.keys().cloned().collect(), &client_meta_data)?;
        //If some succeed, and others fail, it will cause inconsistent data
        for entry in broker_config_map {
            let broker = entry.0;
            let addr = client_meta_data.find_master_broker_addr(broker);
            let config_mapping = entry.1;
            if let Some(addr) = &addr {
                if let Some(topic) = &config_mapping.topic_config.topic_name {
                    if let Some(mapping_detail) = &config_mapping.topic_queue_mapping_detail {
                        default_mq_admin_ext
                            .create_static_topic(
                                addr.clone(),
                                topic.clone(),
                                config_mapping.topic_config.clone(),
                                (**mapping_detail).clone(),
                                force,
                            )
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
    pub async fn examine_topic_config_all(
        topic: &CheetahString,
        default_mq_admin_ext: &DefaultMQAdminExt,
    ) -> RocketMQResult<HashMap<CheetahString, TopicConfigAndQueueMapping>> {
        let mut broker_config_map = HashMap::new();
        let client_metadata = ClientMetadata::new();
        //check all the brokers
        let cluster_info = default_mq_admin_ext.examine_broker_cluster_info().await?;
        if cluster_info.broker_addr_table.is_some() {
            client_metadata.refresh_cluster_info(Some(&cluster_info));
            let keys = {
                let addr_table = client_metadata.broker_addr_table();
                let addr_table = addr_table.read();
                addr_table.keys().cloned().collect::<Vec<CheetahString>>()
            };
            for broker in keys {
                let addr = client_metadata.find_master_broker_addr(&broker);
                if let Some(addr) = &addr {
                    let mapping = TopicConfigAndQueueMapping::new(
                        default_mq_admin_ext
                            .examine_topic_config(addr.clone(), topic.to_string().into())
                            .await?,
                        None,
                    );
                    //allow the config is null
                    broker_config_map.insert(broker.clone(), mapping);
                }
            }
        }

        Ok(broker_config_map)
    }
    pub async fn remapping_static_topic(
        topic: &CheetahString,
        brokers_to_map_in: &HashSet<CheetahString>,
        brokers_to_map_out: &HashSet<CheetahString>,
        broker_config_map: &mut HashMap<CheetahString, TopicConfigAndQueueMapping>,
        block_seq_size: i64,
        force: bool,
        default_mq_admin_ext: &DefaultMQAdminExt,
    ) -> RocketMQResult<()> {
        let client_metadata = MQAdminUtils::get_broker_metadata(default_mq_admin_ext).await?;
        MQAdminUtils::check_if_master_alive(broker_config_map.keys().cloned().collect(), &client_metadata)?;

        for broker in brokers_to_map_in {
            let addr = client_metadata.find_master_broker_addr(broker);
            if let Some(addr) = addr {
                if let Some(config_mapping) = broker_config_map.get(broker) {
                    if let Some(mapping_detail) = &config_mapping.topic_queue_mapping_detail {
                        default_mq_admin_ext
                            .create_static_topic(
                                addr,
                                topic.clone(),
                                config_mapping.topic_config.clone(),
                                (**mapping_detail).clone(),
                                force,
                            )
                            .await?;
                    }
                }
            }
        }

        for broker in brokers_to_map_out {
            let addr = client_metadata.find_master_broker_addr(broker);
            if let Some(addr) = addr {
                if let Some(config_mapping) = broker_config_map.get(broker) {
                    if let Some(mapping_detail) = &config_mapping.topic_queue_mapping_detail {
                        default_mq_admin_ext
                            .create_static_topic(
                                addr,
                                topic.clone(),
                                config_mapping.topic_config.clone(),
                                (**mapping_detail).clone(),
                                force,
                            )
                            .await?;
                    }
                }
            }
        }

        let mut updates_to_apply: Vec<(CheetahString, i32, Vec<LogicQueueMappingItem>)> = vec![];

        for broker in brokers_to_map_out {
            let addr = client_metadata.find_master_broker_addr(broker);
            if let Some(addr) = addr {
                let stats_table = default_mq_admin_ext
                    .examine_topic_stats(topic.clone(), Some(addr.clone()))
                    .await?;
                let offset_table = stats_table.get_offset_table();

                if let Some(map_out_config) = broker_config_map.get_mut(broker) {
                    if let Some(mapping_detail) = &mut map_out_config.topic_queue_mapping_detail {
                        if let Some(hosted_queues) = &mut mapping_detail.hosted_queues {
                            for (global_id, items) in hosted_queues.iter_mut() {
                                if items.len() < 2 {
                                    continue;
                                }
                                let items_len = items.len();
                                let old_leader = items.get(items_len - 2).cloned();
                                let new_leader = items.last_mut();

                                if let (Some(new_leader), Some(old_leader)) = (new_leader, old_leader) {
                                    if new_leader.logic_offset > 0 {
                                        continue;
                                    }

                                    let old_broker_name = old_leader.bname.clone().unwrap_or_default();
                                    let old_queue_id = old_leader.queue_id;

                                    let topic_offset = offset_table.get(
                                        &rocketmq_common::common::message::message_queue::MessageQueue::from_parts(
                                            topic.clone(),
                                            old_broker_name.clone(),
                                            old_queue_id,
                                        ),
                                    );

                                    if let Some(topic_offset) = topic_offset {
                                        if topic_offset.get_max_offset() < old_leader.start_offset {
                                            return Err(RocketMQError::Internal(format!(
                                                "The max offset is smaller than the start offset {:?} {}",
                                                old_leader,
                                                topic_offset.get_max_offset()
                                            )));
                                        }
                                        new_leader.logic_offset = TopicQueueMappingUtils::block_seq_round_up(
                                            old_leader
                                                .compute_static_queue_offset_strictly(topic_offset.get_max_offset()),
                                            block_seq_size,
                                        );

                                        // collect updates for map_in_config
                                        if let Some(new_leader_bname) = &new_leader.bname {
                                            updates_to_apply.push((
                                                new_leader_bname.clone(),
                                                *global_id,
                                                items.clone(),
                                            ));
                                        }
                                    } else {
                                        return Err(RocketMQError::Internal(format!(
                                            "Cannot get the max offset for old leader {:?}",
                                            old_leader
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // apply collected updates to map_in_config
        for (broker_name, global_id, items) in updates_to_apply {
            if let Some(map_in_config) = broker_config_map.get_mut(&broker_name) {
                if let Some(map_in_detail) = &mut map_in_config.topic_queue_mapping_detail {
                    if let Some(map_in_queues) = &mut map_in_detail.hosted_queues {
                        map_in_queues.insert(global_id, items);
                    }
                }
            }
        }

        // write to the new leader with logic offset
        for broker in brokers_to_map_in {
            let addr = client_metadata.find_master_broker_addr(broker);
            if let Some(addr) = addr {
                if let Some(config_mapping) = broker_config_map.get(broker) {
                    if let Some(mapping_detail) = &config_mapping.topic_queue_mapping_detail {
                        default_mq_admin_ext
                            .create_static_topic(
                                addr,
                                topic.clone(),
                                config_mapping.topic_config.clone(),
                                (**mapping_detail).clone(),
                                force,
                            )
                            .await?;
                    }
                }
            }
        }

        // write the non-target brokers
        for (broker, config_mapping) in broker_config_map.iter() {
            if brokers_to_map_in.contains(broker) || brokers_to_map_out.contains(broker) {
                continue;
            }
            let addr = client_metadata.find_master_broker_addr(broker);
            if let Some(addr) = addr {
                if let Some(mapping_detail) = &config_mapping.topic_queue_mapping_detail {
                    default_mq_admin_ext
                        .create_static_topic(
                            addr,
                            topic.clone(),
                            config_mapping.topic_config.clone(),
                            (**mapping_detail).clone(),
                            force,
                        )
                        .await?;
                }
            }
        }

        Ok(())
    }
}
