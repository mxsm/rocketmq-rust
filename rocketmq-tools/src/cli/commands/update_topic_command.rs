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

//! Update/create topic command
//!
//! This command creates or updates a topic configuration. It can target either
//! a specific broker or all master brokers in a cluster.

use clap::Args;

use crate::cli::validators;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicConfig;
use crate::core::topic::TopicService;
use crate::core::topic::TopicTarget;
use crate::core::RocketMQResult;

/// Update or create topic configuration
#[derive(Debug, Args, Clone)]
pub struct UpdateTopicCommand {
    /// Topic name to create/update
    #[arg(short = 't', long = "topic")]
    pub topic: String,

    /// Broker address (mutually exclusive with cluster)
    #[arg(short = 'b', long = "brokerAddr", conflicts_with = "cluster")]
    pub broker_addr: Option<String>,

    /// Cluster name (mutually exclusive with broker_addr)
    #[arg(short = 'c', long = "clusterName", conflicts_with = "broker_addr")]
    pub cluster: Option<String>,

    /// Number of read queues
    #[arg(short = 'r', long = "readQueueNums", default_value = "8")]
    pub read_queue_nums: i32,

    /// Number of write queues
    #[arg(short = 'w', long = "writeQueueNums", default_value = "8")]
    pub write_queue_nums: i32,

    /// Permission value (2=W, 4=R, 6=RW)
    #[arg(short = 'p', long = "perm", default_value = "6")]
    pub perm: i32,

    /// Whether this is an order topic
    #[arg(short = 'o', long = "order", default_value = "false")]
    pub order: bool,

    /// Unit mode flag
    #[arg(short = 'u', long = "unit", default_value = "false")]
    pub unit: bool,

    /// Has unit subscription flag
    #[arg(short = 's', long = "hasUnitSub", default_value = "false")]
    pub has_unit_sub: bool,

    /// NameServer address
    #[arg(short = 'n', long = "namesrvAddr", value_parser = validators::validate_namesrv_addr)]
    pub namesrv_addr: String,
}

impl UpdateTopicCommand {
    /// Execute the update topic command
    pub async fn execute(&self) -> RocketMQResult<()> {
        // Validate inputs
        self.validate()?;

        // Build topic configuration
        let topic_config = self.build_config()?;

        // Determine target (broker or cluster)
        let target = self.determine_target()?;

        // Create admin client with RAII guard
        let mut admin = AdminBuilder::new()
            .namesrv_addr(&self.namesrv_addr)
            .build_with_guard()
            .await?;

        // Execute core operation
        TopicService::create_or_update_topic(&mut admin, topic_config, target.clone()).await?;

        // Success message
        self.print_success(&target);

        Ok(())
    }

    /// Validate command parameters
    fn validate(&self) -> RocketMQResult<()> {
        // Validate topic name
        validators::validate_topic_name(&self.topic)?;

        // Ensure either broker or cluster is specified
        if self.broker_addr.is_none() && self.cluster.is_none() {
            return Err(crate::core::ToolsError::validation_error(
                "broker_addr or cluster",
                "Either broker address (-b) or cluster name (-c) must be specified",
            )
            .into());
        }

        // Validate queue numbers
        if self.read_queue_nums <= 0 {
            return Err(crate::core::ToolsError::validation_error(
                "read_queue_nums",
                "Read queue count must be positive",
            )
            .into());
        }

        if self.write_queue_nums <= 0 {
            return Err(crate::core::ToolsError::validation_error(
                "write_queue_nums",
                "Write queue count must be positive",
            )
            .into());
        }

        // Validate permission value
        if !(2..=6).contains(&self.perm) || self.perm == 3 || self.perm == 5 {
            return Err(crate::core::ToolsError::validation_error(
                "perm",
                "Permission must be 2 (W), 4 (R), or 6 (RW)",
            )
            .into());
        }

        Ok(())
    }

    /// Build topic configuration from command arguments
    fn build_config(&self) -> RocketMQResult<TopicConfig> {
        use cheetah_string::CheetahString;

        // Calculate topic_sys_flag based on unit flags
        let mut topic_sys_flag = 0;
        if self.unit {
            topic_sys_flag |= 1 << 0; // Unit flag
        }
        if self.has_unit_sub {
            topic_sys_flag |= 1 << 1; // Has unit subscription flag
        }

        Ok(TopicConfig {
            topic_name: CheetahString::from(self.topic.clone()),
            read_queue_nums: self.read_queue_nums,
            write_queue_nums: self.write_queue_nums,
            perm: self.perm,
            topic_filter_type: None, // Use default
            topic_sys_flag: if topic_sys_flag > 0 { Some(topic_sys_flag) } else { None },
            order: self.order,
        })
    }

    /// Determine the target for topic creation/update
    fn determine_target(&self) -> RocketMQResult<TopicTarget> {
        use cheetah_string::CheetahString;

        if let Some(ref broker) = self.broker_addr {
            Ok(TopicTarget::Broker(CheetahString::from(broker.clone())))
        } else if let Some(ref cluster) = self.cluster {
            Ok(TopicTarget::Cluster(CheetahString::from(cluster.clone())))
        } else {
            Err(
                crate::core::ToolsError::validation_error("target", "Either broker_addr or cluster must be specified")
                    .into(),
            )
        }
    }

    /// Print success message
    fn print_success(&self, target: &TopicTarget) {
        let target_str = match target {
            TopicTarget::Broker(addr) => format!("broker {}", addr),
            TopicTarget::Cluster(cluster) => format!("cluster {}", cluster),
        };

        println!("Topic '{}' created/updated successfully on {}", self.topic, target_str);

        // Print configuration details
        println!("\nConfiguration:");
        println!("  Read Queues:  {}", self.read_queue_nums);
        println!("  Write Queues: {}", self.write_queue_nums);
        println!("  Permission:   {} ({})", self.perm, self.perm_string());
        println!("  Order Topic:  {}", if self.order { "Yes" } else { "No" });

        if self.unit || self.has_unit_sub {
            println!("\nUnit Flags:");
            if self.unit {
                println!("  Unit Mode:             Enabled");
            }
            if self.has_unit_sub {
                println!("  Has Unit Subscription: Enabled");
            }
        }

        // Note about order topics
        if self.order {
            println!("\nNote: Order topic creation requires additional configuration on the broker.");
            println!("   Ensure the broker supports order message configuration.");
        }
    }

    /// Convert permission value to string
    fn perm_string(&self) -> &'static str {
        match self.perm {
            2 => "Write Only",
            4 => "Read Only",
            6 => "Read & Write",
            _ => "Invalid",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_update_topic_with_broker() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster: None,
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            order: false,
            unit: false,
            has_unit_sub: false,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(cmd.perm, 6);
    }

    #[test]
    fn test_parse_update_topic_with_cluster() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: None,
            cluster: Some("DefaultCluster".to_string()),
            read_queue_nums: 16,
            write_queue_nums: 16,
            perm: 6,
            order: true,
            unit: false,
            has_unit_sub: false,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert_eq!(cmd.cluster, Some("DefaultCluster".to_string()));
        assert_eq!(cmd.read_queue_nums, 16);
        assert!(cmd.order);
    }

    #[test]
    fn test_validate_queue_nums() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster: None,
            read_queue_nums: 0,
            write_queue_nums: 8,
            perm: 6,
            order: false,
            unit: false,
            has_unit_sub: false,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert!(cmd.validate().is_err());
    }

    #[test]
    fn test_validate_perm() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster: None,
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 7, // Invalid
            order: false,
            unit: false,
            has_unit_sub: false,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert!(cmd.validate().is_err());
    }

    #[test]
    fn test_build_config() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster: None,
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            order: true,
            unit: true,
            has_unit_sub: true,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        let config = cmd.build_config().unwrap();
        assert_eq!(config.read_queue_nums, 8);
        assert_eq!(config.write_queue_nums, 8);
        assert_eq!(config.perm, 6);
        assert!(config.order);
        assert_eq!(config.topic_sys_flag, Some(3)); // 0b11 = both unit flags
    }

    #[test]
    fn test_determine_target_broker() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster: None,
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            order: false,
            unit: false,
            has_unit_sub: false,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        let target = cmd.determine_target().unwrap();
        assert!(matches!(target, TopicTarget::Broker(_)));
    }

    #[test]
    fn test_determine_target_cluster() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: None,
            cluster: Some("DefaultCluster".to_string()),
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            order: false,
            unit: false,
            has_unit_sub: false,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        let target = cmd.determine_target().unwrap();
        assert!(matches!(target, TopicTarget::Cluster(_)));
    }

    #[test]
    fn test_perm_string() {
        let cmd = UpdateTopicCommand {
            topic: "TestTopic".to_string(),
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster: None,
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            order: false,
            unit: false,
            has_unit_sub: false,
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert_eq!(cmd.perm_string(), "Read & Write");
    }
}
