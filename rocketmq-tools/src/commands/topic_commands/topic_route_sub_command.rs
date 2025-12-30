use std::cmp::Ordering;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingSerializable;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct TopicRouteSubCommand {
    /// Common arguments
    #[command(flatten)]
    common_args: CommonArgs,
    /// Topic name
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
    /// List format
    #[arg(short = 'l', long = "list format", required = false, help = "list format")]
    list_format: Option<bool>,
}
impl TopicRouteSubCommand {
    fn broker_name_compare(a: &CheetahString, b: &CheetahString) -> Ordering {
        if a > b {
            return std::cmp::Ordering::Greater;
        } else if a < b {
            return std::cmp::Ordering::Less;
        }
        std::cmp::Ordering::Equal
    }
    fn print_data(&self, topic_route_data: &TopicRouteData, use_list_format: bool) -> RocketMQResult<()> {
        if !use_list_format {
            println!("{}", topic_route_data.serialize_json()?);
            return Ok(());
        }

        let (mut total_read_queue, mut total_write_queue) = (0, 0);
        let mut queue_data_list = topic_route_data.queue_datas.clone();
        let mut map = HashMap::new();
        for queue_data in &queue_data_list {
            map.insert(queue_data.broker_name().clone(), queue_data.clone())
                .unwrap();
        }
        queue_data_list.sort_by(|a, b| TopicRouteSubCommand::broker_name_compare(a.broker_name(), b.broker_name()));

        let mut broker_data_list = topic_route_data.broker_datas.clone();
        broker_data_list.sort_by(|a, b| TopicRouteSubCommand::broker_name_compare(a.broker_name(), b.broker_name()));

        println!("#ClusterName #BrokerName #BrokerAddrs #ReadQueue #WriteQueue #Perm");

        for broker_data in &broker_data_list {
            let broker_name = broker_data.broker_name();
            let queue_data = map.get(broker_name).unwrap();
            total_read_queue += queue_data.read_queue_nums();
            total_write_queue += queue_data.write_queue_nums();
            println!(
                "{} {} {:?} {} {} {}",
                broker_data.cluster(),
                broker_name,
                broker_data.broker_addrs(),
                queue_data.read_queue_nums(),
                queue_data.write_queue_nums(),
                queue_data.perm()
            );
        }

        for _i in 0..158 {
            print!("-");
        }
        println!("Total: {} {} {}", map.len(), total_read_queue, total_write_queue,);
        Ok(())
    }
}
impl CommandExecute for TopicRouteSubCommand {
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

        let topic_route_data = default_mq_admin_ext
            .examine_topic_route_info(self.topic.clone().into())
            .await?
            .unwrap();
        self.print_data(&topic_route_data, self.list_format.is_some())?;
        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
