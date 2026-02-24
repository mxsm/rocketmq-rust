// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;

use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct ConsumerSubCommand {
    #[arg(short = 'g', long = "consumerGroup", required = true, help = "consumer group name")]
    consumer_group: String,

    #[arg(
        short = 's',
        long = "jstack",
        required = false,
        default_value = "false",
        help = "Run jstack command in the consumer progress"
    )]
    jstack: bool,

    #[arg(short = 'i', long = "clientId", required = false, help = "The consumer's client id")]
    client_id: Option<String>,

    #[arg(
        short = 'n',
        long = "namesrvAddr",
        required = false,
        help = "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'"
    )]
    namesrv_addr: Option<String>,
}

impl CommandExecute for ConsumerSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> rocketmq_error::RocketMQResult<()> {
        let rpc_hook = rpc_hook.ok_or(RocketMQError::Internal(
            "rpc hook for ConsumerSubCommand is empty!".to_string(),
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
            .examine_consumer_connection_info(group.into(), None)
            .await?;

        let jstack = self.jstack;

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
            let mut cri_table: BTreeMap<String, ConsumerRunningInfo> = BTreeMap::new();

            for conn in cc.get_connection_set() {
                if let Ok(consumer_running_info) = default_mq_admin_ext
                    .get_consumer_running_info(group.into(), conn.get_client_id(), jstack, None)
                    .await
                {
                    cri_table.insert(conn.get_client_id().to_string(), consumer_running_info.clone());
                    let file_path = format!("{}/{}", now, conn.get_client_id());
                    if let Err(e) = string_to_file(&format!("{}", consumer_running_info), file_path.clone()) {
                        eprintln!("Failed to write consumer running info to file: {}", e);
                    }
                    let version_desc = RocketMqVersion::from_ordinal(conn.get_version() as u32).name();
                    println!(
                        "{:03}  {:<40} {:<20} {}",
                        i,
                        conn.get_client_id(),
                        version_desc,
                        file_path
                    );
                    i += 1;
                }
            }

            if !cri_table.is_empty() {
                let analyze_subscription_res = ConsumerRunningInfo::analyze_subscription(cri_table.clone()).await;

                if analyze_subscription_res.is_ok() {
                    println!("\n\nSame subscription in the same group of consumer");
                    println!("\n\nRebalance OK");

                    for (client_id, running_info) in &cri_table {
                        if let Ok(result) =
                            ConsumerRunningInfo::analyze_process_queue(client_id.clone(), running_info.clone()).await
                        {
                            if !result.is_empty() {
                                println!("{}", result);
                            }
                        }
                    }
                } else {
                    println!("\n\nWARN: Different subscription in the same group of consumer!!!");
                }
            }
        }

        default_mq_admin_ext.shutdown().await;
        Ok(())
    }
}
