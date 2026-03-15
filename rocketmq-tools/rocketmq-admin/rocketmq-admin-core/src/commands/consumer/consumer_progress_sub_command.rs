use std::collections::HashMap;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::utils::util_all;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ConsumerProgressSubCommand {
    #[arg(short = 'g', long = "groupName", required = false, help = "consumer group name")]
    consumer_group: Option<String>,

    #[arg(short = 't', long = "topicName", required = false, help = "topic name")]
    topic_name: Option<String>,

    #[arg(
        short = 's',
        long = "showClientIP",
        required = false,
        help = "Show Client IP per Queue",
        default_value_t = false
    )]
    show_client_ip: bool,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "Cluster name or lmq parent topic, lmq is used to find the route."
    )]
    cluster: Option<String>,

    #[arg(
        short = 'n',
        long = "name server address",
        required = false,
        help = "input name server address"
    )]
    namesrv_addr: Option<String>,
}

impl ConsumerProgressSubCommand {
    async fn get_message_queue_allocation_result(
        default_mq_admin_ext: &DefaultMQAdminExt,
        group_name: &str,
    ) -> HashMap<MessageQueue, String> {
        let mut results = HashMap::new();
        if let Ok(consumer_connection) = default_mq_admin_ext
            .examine_consumer_connection_info(group_name.into(), None)
            .await
        {
            for connection in consumer_connection.get_connection_set() {
                let client_id = connection.get_client_id().clone();
                if let Ok(consumer_running_info) = default_mq_admin_ext
                    .get_consumer_running_info(group_name.into(), client_id.clone(), false, None)
                    .await
                {
                    for mq in consumer_running_info.mq_table.keys() {
                        results.insert(mq.clone(), client_id.split('@').next().unwrap_or("").to_string());
                    }
                }
            }
        }
        results
    }
}

impl CommandExecute for ConsumerProgressSubCommand {
    async fn execute(
        &self,
        rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let rpc_hook = rpc_hook.ok_or(RocketMQError::Internal(
            "rpc hook for ConsumerProgressSubCommand is empty!".to_string(),
        ))?;

        let mut default_mq_admin_ext = DefaultMQAdminExt::with_rpc_hook(rpc_hook);

        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(namesrv) = &self.namesrv_addr {
            default_mq_admin_ext.set_namesrv_addr(namesrv.trim());
        }

        default_mq_admin_ext.start().await.map_err(|e| {
            RocketMQError::Internal(format!("ConsumerProgressSubCommand: Failed to start MQAdminExt: {}", e))
        })?;

        if let Some(consumer_group) = self.consumer_group.as_deref().map(str::trim) {
            let cluster = self
                .cluster
                .as_ref()
                .map(|s| CheetahString::from_string(s.trim().to_string()));
            let topic_name = self
                .topic_name
                .as_ref()
                .map(|s| CheetahString::from_string(s.trim().to_string()));

            let consume_stats = default_mq_admin_ext
                .examine_consume_stats(consumer_group.into(), topic_name, cluster, None, None)
                .await?;

            let offset_table = consume_stats.get_offset_table();
            let mut mq_list: Vec<_> = offset_table.keys().cloned().collect();
            mq_list.sort();

            let mut message_queue_allocation_result: HashMap<MessageQueue, String> = HashMap::new();
            if self.show_client_ip {
                message_queue_allocation_result =
                    Self::get_message_queue_allocation_result(&default_mq_admin_ext, consumer_group).await;
            }

            if self.show_client_ip {
                println!(
                    "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} {:<20} {}",
                    "#Topic",
                    "#Broker Name",
                    "#QID",
                    "#Broker Offset",
                    "#Consumer Offset",
                    "#Client IP",
                    "#Diff",
                    "#Inflight",
                    "#LastTime"
                );
            } else {
                println!(
                    "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} {}",
                    "#Topic",
                    "#Broker Name",
                    "#QID",
                    "#Broker Offset",
                    "#Consumer Offset",
                    "#Diff",
                    "#Inflight",
                    "#LastTime"
                );
            }

            let mut diff_total = 0i64;
            let mut inflight_total = 0i64;
            for mq in mq_list {
                if let Some(offset_wrapper) = offset_table.get(&mq) {
                    let diff = offset_wrapper.get_broker_offset() - offset_wrapper.get_consumer_offset();
                    let inflight = offset_wrapper.get_pull_offset() - offset_wrapper.get_consumer_offset();
                    diff_total += diff;
                    inflight_total += inflight;

                    let last_time = if offset_wrapper.get_last_timestamp() == 0 {
                        "N/A".to_string()
                    } else {
                        util_all::time_millis_to_human_string2(offset_wrapper.get_last_timestamp())
                    };

                    if self.show_client_ip {
                        let client_ip = message_queue_allocation_result
                            .get(&mq)
                            .map(|s| s.as_str())
                            .unwrap_or("N/A");
                        println!(
                            "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} {:<20} {}",
                            mq.topic_str(),
                            mq.broker_name(),
                            mq.queue_id(),
                            offset_wrapper.get_broker_offset(),
                            offset_wrapper.get_consumer_offset(),
                            client_ip,
                            diff,
                            inflight,
                            last_time
                        );
                    } else {
                        println!(
                            "{:<64}  {:<32}  {:<4}  {:<20}  {:<20}  {:<20} {:<20} {}",
                            mq.topic_str(),
                            mq.broker_name(),
                            mq.queue_id(),
                            offset_wrapper.get_broker_offset(),
                            offset_wrapper.get_consumer_offset(),
                            diff,
                            inflight,
                            last_time
                        );
                    }
                }
            }

            println!("\nConsume TPS: {:.2}", consume_stats.get_consume_tps());
            println!("Consume Diff Total: {}", diff_total);
            println!("Consume Inflight Total: {}", inflight_total);
        } else {
            println!(
                "{:<64}  {:<6}  {:<24} {:<5}  {:<14}  {:<7}  {}",
                "#Group", "#Count", "#Version", "#Type", "#Model", "#TPS", "#Diff Total"
            );

            let topic_list = default_mq_admin_ext.fetch_all_topic_list().await?;
            for topic in topic_list.topic_list {
                if topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                    let consumer_group = KeyBuilder::parse_group(&topic);
                    let mut group_consume_info = GroupConsumeInfo {
                        group: consumer_group.clone(),
                        ..Default::default()
                    };

                    if let Ok(consume_stats) = default_mq_admin_ext
                        .examine_consume_stats(consumer_group.clone().into(), None, None, None, None)
                        .await
                    {
                        group_consume_info.consume_tps = consume_stats.get_consume_tps() as i32;
                        group_consume_info.diff_total = consume_stats.compute_total_diff();
                    }

                    if let Ok(cc) = default_mq_admin_ext
                        .examine_consumer_connection_info(consumer_group.into(), None)
                        .await
                    {
                        group_consume_info.count = cc.get_connection_set().len() as i32;
                        group_consume_info.message_model = cc.get_message_model().unwrap_or(MessageModel::Clustering);
                        group_consume_info.consume_type =
                            cc.get_consume_type().unwrap_or(ConsumeType::ConsumePassively);
                        group_consume_info.version = cc.compute_min_version();
                    }

                    println!(
                        "{:<64}  {:<6}  {:<24} {:<5}  {:<14}  {:<7}  {}",
                        group_consume_info.group,
                        group_consume_info.count,
                        if group_consume_info.count > 0 {
                            group_consume_info.version_desc()
                        } else {
                            "OFFLINE".to_string()
                        },
                        group_consume_info.consume_type_desc(),
                        group_consume_info.message_model_desc(),
                        group_consume_info.consume_tps,
                        group_consume_info.diff_total
                    );
                }
            }
        }

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct GroupConsumeInfo {
    group: String,
    version: i32,
    count: i32,
    consume_type: ConsumeType,
    message_model: MessageModel,
    consume_tps: i32,
    diff_total: i64,
}

impl GroupConsumeInfo {
    fn consume_type_desc(&self) -> &str {
        if self.count != 0 {
            if self.consume_type == ConsumeType::ConsumeActively {
                "PULL"
            } else {
                "PUSH"
            }
        } else {
            ""
        }
    }

    fn message_model_desc(&self) -> String {
        if self.count != 0 && self.consume_type == ConsumeType::ConsumePassively {
            self.message_model.to_string()
        } else {
            "".to_string()
        }
    }

    fn version_desc(&self) -> String {
        if self.count != 0 {
            RocketMqVersion::from_ordinal(self.version as u32).name().to_string()
        } else {
            "".to_string()
        }
    }
}
