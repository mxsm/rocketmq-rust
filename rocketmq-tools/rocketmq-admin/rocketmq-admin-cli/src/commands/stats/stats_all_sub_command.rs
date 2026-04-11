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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::stats::Stats;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_remoting::runtime::RPCHook;
use tracing::warn;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct StatsAllSubCommand {
    #[arg(
        short = 'a',
        long = "activeTopic",
        help = "print active topic only",
        required = false
    )]
    active_topic: bool,

    #[arg(short = 't', long = "topic", required = false, help = "print select topic only")]
    topic: Option<String>,
}

impl StatsAllSubCommand {
    fn compute_24_hour_sum(bsd: &BrokerStatsData) -> u64 {
        if bsd.get_stats_day().get_sum() != 0 {
            return bsd.get_stats_day().get_sum();
        }
        if bsd.get_stats_hour().get_sum() != 0 {
            return bsd.get_stats_hour().get_sum();
        }
        if bsd.get_stats_minute().get_sum() != 0 {
            return bsd.get_stats_minute().get_sum();
        }
        0
    }

    async fn print_topic_detail(admin: &DefaultMQAdminExt, topic: &str, active_topic: bool) -> RocketMQResult<()> {
        let topic_route_data = admin.examine_topic_route_info(CheetahString::from(topic)).await?;

        let group_list = admin.query_topic_consume_by_who(CheetahString::from(topic)).await?;

        let mut in_tps: f64 = 0.0;
        let mut in_msg_cnt_today: u64 = 0;

        if let Some(route_data) = &topic_route_data {
            for bd in &route_data.broker_datas {
                if let Some(master_addr) = bd.broker_addrs().get(&mix_all::MASTER_ID) {
                    if let Ok(bsd) = admin
                        .view_broker_stats_data(
                            master_addr.clone(),
                            CheetahString::from(Stats::TOPIC_PUT_NUMS),
                            CheetahString::from(topic),
                        )
                        .await
                    {
                        in_tps += bsd.get_stats_minute().get_tps();
                        in_msg_cnt_today += Self::compute_24_hour_sum(&bsd);
                    }
                }
            }
        }

        if !group_list.group_list.is_empty() {
            for group in &group_list.group_list {
                let mut out_tps: f64 = 0.0;
                let mut out_msg_cnt_today: u64 = 0;

                if let Some(route_data) = &topic_route_data {
                    for bd in &route_data.broker_datas {
                        if let Some(master_addr) = bd.broker_addrs().get(&mix_all::MASTER_ID) {
                            let stats_key = format!("{}@{}", topic, group);
                            if let Ok(bsd) = admin
                                .view_broker_stats_data(
                                    master_addr.clone(),
                                    CheetahString::from(Stats::GROUP_GET_NUMS),
                                    CheetahString::from(stats_key.as_str()),
                                )
                                .await
                            {
                                out_tps += bsd.get_stats_minute().get_tps();
                                out_msg_cnt_today += Self::compute_24_hour_sum(&bsd);
                            }
                        }
                    }
                }

                let mut accumulate: i64 = 0;
                if let Ok(consume_stats) = admin
                    .examine_consume_stats(group.clone(), Some(CheetahString::from(topic)), None, None, None)
                    .await
                {
                    accumulate = consume_stats.compute_total_diff();
                    if accumulate < 0 {
                        accumulate = 0;
                    }
                }

                if !active_topic || in_msg_cnt_today > 0 || out_msg_cnt_today > 0 {
                    println!(
                        "{:<64}  {:<64} {:>12} {:>11.2} {:>11.2} {:>14} {:>14}",
                        Self::front_string_at_least(topic, 64),
                        Self::front_string_at_least(group.as_str(), 64),
                        accumulate,
                        in_tps,
                        out_tps,
                        in_msg_cnt_today,
                        out_msg_cnt_today,
                    );
                }
            }
        } else if !active_topic || in_msg_cnt_today > 0 {
            println!(
                "{:<64}  {:<64} {:>12} {:>11.2} {:>11} {:>14} {:>14}",
                Self::front_string_at_least(topic, 64),
                "",
                0,
                in_tps,
                "",
                in_msg_cnt_today,
                "NO_CONSUMER",
            );
        }

        Ok(())
    }

    fn front_string_at_least(s: &str, size: usize) -> String {
        s.chars().take(size).collect()
    }
}

impl CommandExecute for StatsAllSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("StatsAllSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            let topic_list = default_mqadmin_ext.fetch_all_topic_list().await?;

            println!(
                "{:<64}  {:<64} {:>12} {:>11} {:>11} {:>14} {:>14}",
                "#Topic", "#Consumer Group", "#Accumulation", "#InTPS", "#OutTPS", "#InMsg24Hour", "#OutMsg24Hour",
            );

            let active_topic = self.active_topic;
            let select_topic = self.topic.clone();

            for topic in &topic_list.topic_list {
                if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                    continue;
                }

                if let Some(ref select) = select_topic {
                    if !select.is_empty() && topic.as_str() != select.as_str() {
                        continue;
                    }
                }

                if let Err(e) = Self::print_topic_detail(&default_mqadmin_ext, topic.as_str(), active_topic).await {
                    warn!("statsAll: failed to collect stats for topic '{}': {}", topic, e);
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;

        operation_result
    }
}
