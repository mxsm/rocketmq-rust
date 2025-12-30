use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::topic_commands::NAMESPACE_ORDER_TOPIC_CONFIG;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct UpdateOrderConfCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,
    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
    /// Method
    #[arg(
        short = 'm',
        long = "method",
        required = true,
        help = "option type [eg. put|get|delete"
    )]
    method: String,
    /// OrderConf
    #[arg(
        short = 'v',
        long = "orderConf",
        required = false,
        help = "set order conf [eg. brokerName1:num;brokerName2:num]"
    )]
    order_conf: Option<String>,
}
impl CommandExecute for UpdateOrderConfCommand {
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
        let method = self.method.trim();

        if "get".eq(method) {
            let order_conf = default_mq_admin_ext
                .get_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG.into(), topic.into())
                .await?;
            println!("get orderConf success. topic={}, orderConf={}", topic, order_conf);
        } else if "put".eq(method) {
            if let Some(order_conf) = &self.order_conf {
                default_mq_admin_ext
                    .create_or_update_order_conf(topic.into(), order_conf.into(), true)
                    .await?;

                println!("update orderConf success. topic={}, orderConf={}", topic, order_conf);
            } else if self.order_conf.is_none() {
                default_mq_admin_ext.shutdown().await;
                return Err(RocketMQError::Internal(
                    "please set orderConf with option -v.".to_string(),
                ));
            }
        } else if "delete".eq(method) {
            default_mq_admin_ext
                .delete_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG.into(), topic.into())
                .await?;
            println!("delete orderConf success. topic={}", topic);
        }

        println!("mqadmin UpdateOrderConf");
        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
