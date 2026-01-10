use std::path::PathBuf;

use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

#[derive(Parser)]
pub struct UpdateTopicListSubCommand {
    /// Config file path (JSON or YAML)
    #[arg(short = 'f', long)]
    file: PathBuf,

    /// Broker address (mutually exclusive with -c)
    #[arg(short = 'b', long, conflicts_with = "cluster")]
    broker_addr: Option<String>,

    /// Cluster name (mutually exclusive with -b)
    #[arg(short = 'c', long, conflicts_with = "broker_addr")]
    cluster: Option<String>,

    /// Dry run mode (validate only, don't apply)
    #[arg(long)]
    dry_run: bool,
}
impl CommandExecute for UpdateTopicListSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut default_mq_admin_ext = DefaultMQAdminExt::new();
        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        default_mq_admin_ext.start().await?;

        if !self.file.is_file() {
            return Err(RocketMQError::Internal(
                "the file path doesn't point to a valid file".to_string(),
            ));
        }

        let mut topic_config_list_bytes = vec![];
        File::open(&self.file)
            .await
            .map_err(|e| RocketMQError::Internal(format!("open file error {}", e)))?
            .read_to_end(&mut topic_config_list_bytes)
            .await?;
        let topic_configs =
            if let Ok(topic_configs) = serde_json::from_slice::<Vec<TopicConfig>>(&topic_config_list_bytes) {
                topic_configs
            } else if let Ok(topic_configs) = serde_yaml::from_slice::<Vec<TopicConfig>>(&topic_config_list_bytes) {
                topic_configs
            } else {
                return Err(RocketMQError::Internal(
                    "the file isn't in json or yaml format".to_string(),
                ));
            };

        if let Some(broker) = &self.broker_addr {
            let broker_address = broker.trim();
            default_mq_admin_ext
                .create_and_update_topic_config_list(broker_address.into(), topic_configs)
                .await?;
            println!(
                "submit batch of topic config to {} success, please check the result later",
                broker_address
            );
        } else if let Some(cluster) = &self.cluster {
            let cluster_name = cluster.trim();
            let master_set = CommandUtil::fetch_master_addr_by_cluster_name(
                &default_mq_admin_ext.examine_broker_cluster_info().await?,
                cluster_name,
            )?;
            for broker_in_cluster in &master_set {
                default_mq_admin_ext
                    .create_and_update_topic_config_list(broker_in_cluster.into(), topic_configs.clone())
                    .await?;
                println!(
                    "submit batch of topic config to {} success, please check the result later",
                    broker_in_cluster
                );
            }
        } else {
            return Err(RocketMQError::Internal(
                "a broker or cluster is required for command UpdateTopicList".to_string(),
            ));
        }
        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
