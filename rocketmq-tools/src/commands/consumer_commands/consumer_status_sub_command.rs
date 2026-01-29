use std::collections::BTreeMap;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ConsumerStatusSubCommand {
    #[arg(short = 'g', long = "consumerGroup", required = true, help = "consumer group name")]
    consumer_group: String,
    #[arg(short = 'i', long = "clientId", required = false, help = "The consumer's client id")]
    client_id: Option<String>,
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "broker address")]
    broker_addr: Option<String>,
    #[arg(
        short = 's',
        long = "jstack",
        required = false,
        help = "Run jstack command in the consumer progress"
    )]
    jstack: Option<bool>,
    #[arg(
        short = 'n',
        long = "name server address",
        required = false,
        help = "input name server address"
    )]
    namesrv_addr: Option<String>,
}
impl CommandExecute for ConsumerStatusSubCommand {
    async fn execute(
        &self,
        rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let rpc_hook = rpc_hook.ok_or(RocketMQError::Internal(
            "rpc hook for ConsumerStatusSubCommand is empty!".to_string(),
        ))?;
        let mut default_mq_admin_ext = DefaultMQAdminExt::with_rpc_hook(rpc_hook);

        default_mq_admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        if let Some(namesrv) = &self.namesrv_addr {
            default_mq_admin_ext.set_namesrv_addr(namesrv.trim());
        }

        default_mq_admin_ext.start().await?;
        let group = self.consumer_group.trim();

        let cc = default_mq_admin_ext
            .examine_consumer_connection_info(
                group.into(),
                self.broker_addr.clone().map(|i| CheetahString::from_string(i)),
            )
            .await?;
        let jstack = if let Some(jstack) = &self.jstack {
            *jstack
        } else {
            false
        };
        if let Some(client_id) = &self.client_id {
            let client_id = client_id.trim();
            if let Ok(consumer_running_info) = default_mq_admin_ext
                .get_consumer_running_info(group.into(), client_id.into(), jstack, None)
                .await
            {
                println!("{}", consumer_running_info);
            }
        } else {
            let mut i = 1;
            let now = get_current_millis();
            let mut cri_table = BTreeMap::new();
            println!("#Index #ClientId #Version #ConsumerRunningInfoFile");
            for conn in cc.get_connection_set() {
                if let Ok(consumer_running_info) = default_mq_admin_ext
                    .get_consumer_running_info(group.into(), conn.get_client_id(), jstack, None)
                    .await
                {
                    cri_table.insert(conn.get_client_id().to_string(), consumer_running_info.clone());
                    let file_path = format!("{}/{}", now, conn.get_client_id());
                    string_to_file(&format!("{}", consumer_running_info), file_path.clone())?;
                    println!(
                        "{} {} version:{} {}",
                        i,
                        conn.get_client_id(),
                        conn.get_version(),
                        file_path
                    );
                    i += 1;
                }
            }
            if !cri_table.is_empty() {
                let analyze_subscription_res = ConsumerRunningInfo::analyze_subscription(cri_table.clone()).await;

                if analyze_subscription_res.is_ok() {
                    println!("Same subscription in the same group of consumer");
                    println!("Rebalance: Ok");
                    for (k, v) in &cri_table {
                        let result = ConsumerRunningInfo::analyze_process_queue(k.clone(), v.clone()).await?;
                        if !result.is_empty() {
                            println!("{}", result);
                        }
                    }
                } else {
                    println!("WARN: Different subscription in the same group of consumer!!!");
                }
            }
        }

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
