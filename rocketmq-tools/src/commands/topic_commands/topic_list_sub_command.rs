use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct TopicListSubCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,

    /// Cluster name
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,
}
impl TopicListSubCommand {
    async fn find_topic_belong_to_which_cluster(
        &self,
        topic: &CheetahString,
        cluster_info: &ClusterInfo,
        default_mq_admin_ext: &DefaultMQAdminExt,
    ) -> RocketMQResult<String> {
        let topic_route_data = default_mq_admin_ext
            .examine_topic_route_info(topic.clone())
            .await?
            .unwrap();

        let broker_data = topic_route_data.broker_datas.first().unwrap();

        let broker_name = broker_data.broker_name();

        let it = cluster_info.cluster_addr_table.iter();
        for cluster_addr in it {
            for (k, v) in cluster_addr.iter() {
                if v.contains(broker_name) {
                    return Ok(k.to_string());
                }
            }
        }
        Err(RocketMQError::Internal(
            "find_topic_belong_to_which_cluster err".to_string(),
        ))
    }
}
impl CommandExecute for TopicListSubCommand {
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
        if self.cluster_name.is_some() {
            let cluster_info = default_mq_admin_ext.examine_broker_cluster_info().await?;

            println!("#Cluster Name #Topic #Consumer Group");

            let topic_list = default_mq_admin_ext.fetch_all_topic_list().await?;
            for topic in &topic_list.topic_list {
                if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                    continue;
                }

                let _ = self
                    .find_topic_belong_to_which_cluster(topic, &cluster_info, &default_mq_admin_ext)
                    .await?;
                let mut group_list = default_mq_admin_ext.query_topic_consume_by_who(topic.clone()).await?;

                if group_list.get_group_list().is_empty() {
                    group_list.group_list.insert("".into());
                }
            }
        } else {
            let topic_list = default_mq_admin_ext.fetch_all_topic_list().await?;
            for topic in &topic_list.topic_list {
                println!("{}", topic);
            }
        }
        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
