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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::runtime::RPCHook;
use tabled::settings::themes::Colorization;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicOperations;
use crate::core::RocketMQResult;
use crate::ui::output;
use crate::ui::progress;

/// List all topics
#[derive(Debug, Clone, Parser)]
pub struct TopicListCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    /// Filter by cluster name (optional)
    #[arg(short = 'c', long = "cluster", help = "Filter topics by cluster name")]
    cluster: Option<String>,
}

impl CommandExecute for TopicListCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // Build admin client with RAII guard
        output::print_operation_start("Fetching topic list");
        let spinner = progress::create_spinner("Connecting to NameServer...");

        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common_args.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;
        spinner.finish_and_clear();

        // Fetch all topics
        let spinner = progress::create_spinner("Fetching topics...");
        let all_topics = TopicOperations::list_all_topics(&mut admin).await?;
        spinner.finish_and_clear();

        // Filter system topics
        let user_topics: Vec<_> = all_topics
            .into_iter()
            .filter(|topic| {
                !topic.starts_with("TBW102")
                    && !topic.starts_with("SELF_TEST_")
                    && !topic.starts_with("OFFSET_MOVED_EVENT")
                    && !topic.starts_with("BenchmarkTest")
                    && topic != "DefaultCluster"
                    && topic != "rmq_sys_TRANS_CHECK_MAX_TIME_TOPIC"
                    && topic != "SCHEDULE_TOPIC_XXXX"
                    && topic != "%RETRY%"
                    && topic != "%DLQ%"
            })
            .collect();

        if user_topics.is_empty() {
            output::print_empty_result("user topics");
            return Ok(());
        }

        // Get cluster info if filtering by cluster
        if let Some(ref filter_cluster) = self.cluster {
            let cluster_info = admin
                .examine_broker_cluster_info()
                .await
                .map_err(|e| eprintln!("Failed to get cluster info: {e}"))
                .ok();

            let mut topics_with_clusters = Vec::new();
            for topic in user_topics {
                // Get topic route to determine cluster
                if let Ok(Some(route)) = admin.examine_topic_route_info(topic.clone()).await {
                    let brokers: Vec<_> = route.broker_datas.iter().map(|bd| bd.broker_name().clone()).collect();

                    // Check if any broker belongs to the filter cluster
                    if let Some(ref info) = cluster_info {
                        let in_cluster = info
                            .cluster_addr_table
                            .as_ref()
                            .and_then(|table| table.get(filter_cluster.as_str()))
                            .map(|cluster_brokers: &std::collections::HashSet<CheetahString>| {
                                brokers.iter().any(|b| cluster_brokers.contains(b))
                            })
                            .unwrap_or(false);

                        if in_cluster {
                            topics_with_clusters.push(TopicInfo {
                                topic: topic.to_string(),
                                cluster: filter_cluster.clone(),
                            });
                        }
                    }
                }
            }

            if topics_with_clusters.is_empty() {
                output::print_empty_result(&format!("topics in cluster '{}'", filter_cluster));
                return Ok(());
            }

            let count = topics_with_clusters.len();
            output::print_header("Topics");
            let mut table = Table::new(topics_with_clusters);
            table.with(Style::modern());
            table.with(Colorization::exact(
                [tabled::settings::Color::FG_CYAN],
                tabled::settings::object::Rows::first(),
            ));
            println!("{table}");
            output::print_info(&format!(
                "Found {} in cluster '{}'",
                output::format_count(count, "topic", "topics"),
                filter_cluster
            ));
        } else {
            // Simple list without cluster filtering
            let topics: Vec<_> = user_topics
                .into_iter()
                .map(|topic| TopicInfo {
                    topic: topic.to_string(),
                    cluster: "-".to_string(),
                })
                .collect();

            let count = topics.len();
            output::print_header("Topics");
            let mut table = Table::new(topics);
            table.with(Style::modern());
            table.with(Colorization::exact(
                [tabled::settings::Color::FG_CYAN],
                tabled::settings::object::Rows::first(),
            ));
            println!("{table}");
            output::print_info(&format!("Found {}", output::format_count(count, "topic", "topics")));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Tabled)]
struct TopicInfo {
    #[tabled(rename = "Topic")]
    topic: String,
    #[tabled(rename = "Cluster")]
    cluster: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_parsing() {
        let cmd = TopicListCommand::try_parse_from(["topicList", "-n", "127.0.0.1:9876"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert!(cmd.cluster.is_none());
    }

    #[test]
    fn test_command_with_cluster_filter() {
        let cmd = TopicListCommand::try_parse_from(["topicList", "-n", "127.0.0.1:9876", "-c", "DefaultCluster"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.cluster, Some("DefaultCluster".to_string()));
    }
}
