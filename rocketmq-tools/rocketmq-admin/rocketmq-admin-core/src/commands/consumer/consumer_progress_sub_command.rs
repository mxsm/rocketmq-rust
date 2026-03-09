use std::collections::HashMap;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;

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
    async fn get_message_queue_allocation_result() -> HashMap<MessageQueue, String> {
        todo!()
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
            let consume_stats: ConsumeStats;
            let cluster = self
                .cluster
                .as_ref()
                .map(|s| CheetahString::from_string(s.trim().to_string()));
            let topic_name = self
                .topic_name
                .as_ref()
                .map(|s| CheetahString::from_string(s.trim().to_string()));

            if topic_name.is_some() {
                consume_stats = default_mq_admin_ext
                    .examine_consume_stats(consumer_group.into(), None, None, None, None)
                    .await?;
            } else {
                consume_stats = default_mq_admin_ext
                    .examine_consume_stats(consumer_group.into(), topic_name, cluster, None, None)
                    .await?;
            }

            let offset_table = consume_stats.get_offset_table();
            let mut mq_list: Vec<_> = offset_table.keys().cloned().collect();
            mq_list.sort();

            let mut message_queue_allocation_result: HashMap<MessageQueue, String> = HashMap::new();
            if self.show_client_ip {
                message_queue_allocation_result = Self::get_message_queue_allocation_result().await;
            }
        }

        Ok(())
    }
}
