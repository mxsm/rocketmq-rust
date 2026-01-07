use std::collections::HashMap;

use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct TopicStatusSubCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,
    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
    /// Cluster name
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,
}
impl CommandExecute for TopicStatusSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut default_mq_admin_ext = DefaultMQAdminExt::new();
        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        if let Some(addr) = &self.common_args.namesrv_addr {
            default_mq_admin_ext.set_namesrv_addr(addr.trim());
        }
        default_mq_admin_ext.start().await?;
        let topic = self.topic.trim();
        let topic_status = {
            if let Some(cluster) = &self.cluster_name {
                let cluster = cluster.trim();
                let topic_route_data = default_mq_admin_ext.examine_topic_route_info(cluster.into()).await?;
                let mut topic_stats_table = TopicStatsTable::new();
                if let Some(route_data) = &topic_route_data {
                    let mut total_offset_table = HashMap::new();
                    for bd in &route_data.broker_datas {
                        let addr = bd.select_broker_addr();
                        let offset_table = default_mq_admin_ext
                            .examine_topic_stats(topic.into(), addr)
                            .await?
                            .get_offset_table();
                        total_offset_table.extend(offset_table.into_iter());
                    }
                    topic_stats_table.set_offset_table(total_offset_table);
                }
                topic_stats_table
            } else {
                default_mq_admin_ext.examine_topic_stats(topic.into(), None).await?
            }
        };

        let mut mq_list = vec![];
        for item in topic_status.get_offset_table().keys() {
            mq_list.push(item.clone());
        }
        mq_list.sort();

        println!("#Broker Name #QID #Min Offset #Max Offset #Last Updated");

        for queue in &mq_list {
            let offset_table = topic_status.get_offset_table();
            let topic_offset = offset_table.get(queue);
            if let Some(offset) = topic_offset {
                if offset.get_last_update_timestamp() > 0 {
                    let human_timestamp = time_millis_to_human_string2(offset.get_last_update_timestamp());
                    println!(
                        "{}  {}  {}  {}  {}",
                        queue.get_broker_name(),
                        queue.get_queue_id(),
                        offset.get_min_offset(),
                        offset.get_max_offset(),
                        human_timestamp
                    );
                }
            }
        }

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
