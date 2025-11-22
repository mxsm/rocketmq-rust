use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
#[derive(Debug, Clone, Parser)]
pub struct TopicClusterSubCommand {
    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
}
impl CommandExecute for TopicClusterSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut default_mq_admin_ext = DefaultMQAdminExt::new();
        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        let topic = self.topic.trim();

        default_mq_admin_ext.start().await?;
        let clusters = default_mq_admin_ext
            .get_cluster_list(topic.to_string())
            .await?;
        for value in &clusters {
            println!("{}", value);
        }
        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
