use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct DeleteTopicSubCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,

    /// Cluster name
    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,

    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
}
impl DeleteTopicSubCommand {
    async fn delete_topic(
        admin_ext: &mut DefaultMQAdminExt,
        cluster_name: String,
        topic: String,
    ) -> RocketMQResult<()> {
        let cluster_info = admin_ext.examine_broker_cluster_info().await?;
        let master_broker_address_set = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, &cluster_name)?;
        let master_broker_address_set = master_broker_address_set.into_iter().collect();
        admin_ext
            .delete_topic_in_broker(master_broker_address_set, topic.clone().into())
            .await?;
        println!("delete topic {} from cluster {} success", topic, cluster_name);

        let name_server_set = admin_ext.get_name_server_address_list().await.into_iter().collect();

        admin_ext
            .delete_topic_in_name_server(name_server_set, Some(cluster_name.into()), topic.clone().into())
            .await?;
        println!("delete topic {} from NameServer success", topic);
        Ok(())
    }
}
impl CommandExecute for DeleteTopicSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "DeleteTopicSubCommand: clusterName (-c) must be provided".into(),
            ));
        }
        let validation_result = TopicValidator::validate_topic(&self.topic);
        if !validation_result.valid() {
            return Err(RocketMQError::IllegalArgument(format!(
                "DeleteTopicSubCommand: Invalid topic name: {}",
                validation_result.remark().as_str()
            )));
        }
        let mut admin_ext = DefaultMQAdminExt::new();
        admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        if let Some(addr) = &self.common_args.namesrv_addr {
            admin_ext.set_namesrv_addr(addr.trim());
        }
        let topic = self.topic.trim();

        if let Some(cluster_name) = &self.cluster_name {
            let cluster_name = cluster_name.trim();

            admin_ext.start().await.map_err(|e| {
                RocketMQError::Internal(format!("DeleteTopicSubCommand: Failed to start MQAdminExt: {}", e))
            })?;
            DeleteTopicSubCommand::delete_topic(&mut admin_ext, cluster_name.to_string(), topic.to_string()).await?;
            admin_ext.shutdown().await;
            return Ok(());
        }

        admin_ext.shutdown().await;
        Err(RocketMQError::Internal(
            "DeleteTopicSubCommand: Failed to delete topic: {}".to_string(),
        ))
    }
}
