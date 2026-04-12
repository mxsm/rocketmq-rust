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

use std::sync::Arc;

use chrono::FixedOffset;
use chrono::Utc;
use clap::Parser;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::cluster::ClusterBrokerNameQueryRequest;
use rocketmq_admin_core::core::cluster::ClusterBrokerNameQueryResult;
use rocketmq_admin_core::core::cluster::ClusterService;

fn get_string_by_size(size: u64) -> Vec<u8> {
    vec![b'a'; size as usize]
}

fn get_cur_time() -> String {
    let offset = FixedOffset::east_opt(8 * 3600).unwrap();
    let now = Utc::now().with_timezone(&offset);
    now.format("%Y-%m-%d %H:%M:%S").to_string()
}

#[derive(Debug, Clone, Parser)]
pub struct ClusterSendMsgRTSubCommand {
    #[arg(
        short = 'a',
        long = "amount",
        required = false,
        default_value = "100",
        help = "message amount | default 100"
    )]
    amount: u64,

    #[arg(
        short = 's',
        long = "size",
        required = false,
        default_value = "128",
        help = "message size | default 128 Byte"
    )]
    size: u64,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "cluster name | default display all cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'p',
        long = "printLog",
        required = false,
        default_value = "false",
        help = "print as tlog | default false"
    )]
    print_as_tlog: bool,

    #[arg(
        short = 'm',
        long = "machineRoom",
        required = false,
        default_value = "noname",
        help = "machine room name | default noname"
    )]
    machine_room: String,

    #[arg(
        short = 'i',
        long = "interval",
        required = false,
        default_value = "10",
        help = "print interval | default 10 seconds"
    )]
    interval: u64,
}

impl ClusterSendMsgRTSubCommand {
    fn request(&self) -> ClusterBrokerNameQueryRequest {
        ClusterBrokerNameQueryRequest::new(self.cluster_name.clone())
    }
}

impl CommandExecute for ClusterSendMsgRTSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let instance_name = format!("PID_ClusterRTCommand_{}", current_millis());
        let mut client_config = ClientConfig::default();
        client_config.set_instance_name(instance_name.into());

        let mut builder = DefaultMQProducer::builder()
            .producer_group(current_millis().to_string())
            .client_config(client_config);
        if let Some(ref hook) = rpc_hook {
            builder = builder.rpc_hook(hook.clone());
        }
        let mut producer = builder.build();

        let operation_result = async {
            producer.start().await.map_err(|e| {
                RocketMQError::Internal(format!("ClusterSendMsgRTSubCommand: Failed to start producer: {}", e))
            })?;

            let broker_names =
                ClusterService::query_cluster_broker_names_by_request_with_rpc_hook(self.request(), rpc_hook.clone())
                    .await?;

            if !self.print_as_tlog {
                println!(
                    "{:<24}  {:<24}  {:<4}  {:<8}  {:<8}",
                    "#Cluster Name", "#Broker Name", "#RT", "#successCount", "#failCount"
                );
            }

            loop {
                Self::print_missing_clusters(&broker_names);

                for (cluster_name, broker_names) in &broker_names.broker_names_by_cluster {
                    for broker_name in broker_names {
                        let msg_body = get_string_by_size(self.size);
                        let msg = Message::builder()
                            .topic(broker_name)
                            .body_slice(&msg_body)
                            .build_unchecked();

                        let mut elapsed: u64 = 0;
                        let mut success_count: u64 = 0;
                        let mut fail_count: u64 = 0;

                        for i in 0..self.amount {
                            let start = current_millis();
                            match producer.send(msg.clone()).await {
                                Ok(_) => {
                                    success_count += 1;
                                }
                                Err(_) => {
                                    fail_count += 1;
                                }
                            }
                            let end = current_millis();

                            if i != 0 {
                                elapsed += end - start;
                            }
                        }

                        let rt = if self.amount > 1 {
                            elapsed as f64 / (self.amount - 1) as f64
                        } else {
                            elapsed as f64
                        };

                        if !self.print_as_tlog {
                            println!(
                                "{:<24}  {:<24}  {:<8}  {:<16}  {:<16}",
                                cluster_name,
                                broker_name,
                                format!("{:.2}", rt),
                                success_count,
                                fail_count
                            );
                        } else {
                            println!(
                                "{}|{}|{}|{}|{}",
                                get_cur_time(),
                                self.machine_room,
                                cluster_name,
                                broker_name,
                                rt.round() as u64
                            );
                        }
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
            }
        }
        .await;

        producer.shutdown().await;
        operation_result
    }
}

impl ClusterSendMsgRTSubCommand {
    fn print_missing_clusters(result: &ClusterBrokerNameQueryResult) {
        for cluster_name in &result.missing_clusters {
            println!("cluster [{}] not exist", cluster_name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_send_msg_rt_sub_command_parse_request() {
        let cmd = ClusterSendMsgRTSubCommand::try_parse_from(["clusterRT", "-c", " DefaultCluster "]).unwrap();

        assert_eq!(
            cmd.request().cluster_name().map(|name| name.as_str()),
            Some("DefaultCluster")
        );
    }
}
