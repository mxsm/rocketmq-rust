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

//! Topic route command - CLI layer
//!
//! Query topic route information (broker and queue details)

use clap::Parser;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::RPCHook;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

use crate::cli::formatters::OutputFormat;
use crate::cli::validators;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicService;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;

#[derive(Debug, Clone, Parser)]
pub struct TopicRouteCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(
        short = 'f',
        long = "format",
        default_value = "table",
        help = "Output format: table, json, yaml"
    )]
    format: String,
}

#[derive(Tabled)]
struct BrokerRow {
    #[tabled(rename = "Cluster")]
    cluster: String,
    #[tabled(rename = "Broker Name")]
    broker_name: String,
    #[tabled(rename = "Read Queues")]
    read_queues: i32,
    #[tabled(rename = "Write Queues")]
    write_queues: i32,
    #[tabled(rename = "Perm")]
    perm: i32,
}

impl CommandExecute for TopicRouteCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // 1. Validate input
        validators::validate_topic_name(&self.topic)?;

        // 2. Build admin client with RAII guard
        let mut builder = AdminBuilder::new();

        if let Some(addr) = &self.common_args.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }

        let mut admin = builder.build_with_guard().await?;

        // 3. Call core business logic
        let route_data = TopicService::get_topic_route(&mut admin, self.topic.as_str())
            .await
            .map_err(|e| {
                eprintln!("Failed to get route for topic '{}': {e}", self.topic);
                e
            })?;

        let Some(route_data) = route_data else {
            println!("No route data found for topic '{}'", self.topic);
            return Ok(());
        };

        // 4. Format and display result
        let format = OutputFormat::from(self.format.as_str());
        match format {
            OutputFormat::Json => {
                let json = route_data
                    .serialize_json()
                    .map_err(|e| crate::core::ToolsError::internal(format!("Failed to serialize JSON: {e}")))?;
                println!("{json}");
            }
            OutputFormat::Yaml => {
                let yaml = serde_yaml::to_string(&route_data).map_err(|e| {
                    RocketMQError::Serialization(rocketmq_error::SerializationError::encode_failed(
                        "YAML",
                        e.to_string(),
                    ))
                })?;
                println!("{yaml}");
            }
            OutputFormat::Table => {
                println!("\nTopic Route for '{}':\n", self.topic);

                // Create queue data map for quick lookup
                let queue_map: std::collections::HashMap<_, _> = route_data
                    .queue_datas
                    .iter()
                    .map(|q| (q.broker_name().clone(), q))
                    .collect();

                // Build table rows
                let mut rows = Vec::new();
                for broker in &route_data.broker_datas {
                    if let Some(queue) = queue_map.get(broker.broker_name()) {
                        rows.push(BrokerRow {
                            cluster: broker.cluster().to_string(),
                            broker_name: broker.broker_name().to_string(),
                            read_queues: queue.read_queue_nums() as i32,
                            write_queues: queue.write_queue_nums() as i32,
                            perm: queue.perm() as i32,
                        });
                    }
                }

                if rows.is_empty() {
                    println!("No broker data found");
                } else {
                    let mut table = Table::new(rows);
                    table.with(Style::rounded());
                    println!("{table}");

                    let total_read: i32 = route_data.queue_datas.iter().map(|q| q.read_queue_nums() as i32).sum();
                    let total_write: i32 = route_data.queue_datas.iter().map(|q| q.write_queue_nums() as i32).sum();

                    println!(
                        "\nTotal: {} brokers, {} read queues, {} write queues",
                        route_data.broker_datas.len(),
                        total_read,
                        total_write
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_parsing() {
        let cmd = TopicRouteCommand::try_parse_from(["topicRoute", "-t", "TestTopic", "-n", "127.0.0.1:9876"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "TestTopic");
    }

    #[test]
    fn test_command_with_format() {
        let cmd = TopicRouteCommand::try_parse_from(["topicRoute", "-t", "TestTopic", "-f", "json"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.format, "json");
    }

    #[test]
    fn test_command_requires_topic() {
        let cmd = TopicRouteCommand::try_parse_from(["topicRoute"]);
        assert!(cmd.is_err());
    }
}
