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

use clap::Parser;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerConfigQueryRequest;
use rocketmq_admin_core::core::broker::BrokerConfigQueryResult;
use rocketmq_admin_core::core::broker::BrokerConfigSection;
use rocketmq_admin_core::core::broker::BrokerConfigSectionTarget;
use rocketmq_admin_core::core::broker::BrokerService;

#[derive(Debug, Clone, Parser)]
#[command(group(
    clap::ArgGroup::new("target")
        .required(true)
        .args(&["broker_addr", "cluster_name"])
))]
pub struct GetBrokerConfigSubCommand {
    #[arg(short = 'b', long = "brokerAddr", help = "get which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "get which cluster")]
    cluster_name: Option<String>,

    #[arg(
        long = "keyPattern",
        value_name = "REGEX",
        help = "Filter broker configuration keys by regex pattern"
    )]
    key_pattern: Option<String>,
}

impl GetBrokerConfigSubCommand {
    fn request(&self) -> RocketMQResult<BrokerConfigQueryRequest> {
        BrokerConfigQueryRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.key_pattern.clone(),
        )
    }
}

impl CommandExecute for GetBrokerConfigSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = BrokerService::query_broker_config_by_request(self.request()?).await?;
        print_broker_config_result(&result);
        Ok(())
    }
}

fn print_broker_config_result(result: &BrokerConfigQueryResult) {
    for section in &result.sections {
        if matches!(&section.target, BrokerConfigSectionTarget::NoMaster) {
            println!("============(No master found - skipping)============\n");
            continue;
        }

        print_section_header(section);
        if section.entries.is_empty() {
            print_empty_section(section, result.key_pattern.as_deref());
        } else {
            for entry in &section.entries {
                println!("{:<50}=  {}", entry.key, entry.value);
            }
        }
        println!();
    }
}

fn print_section_header(section: &BrokerConfigSection) {
    match &section.target {
        BrokerConfigSectionTarget::Broker(addr) => println!("============{}============", addr),
        BrokerConfigSectionTarget::Master(addr) => println!("============Master: {}============", addr),
        BrokerConfigSectionTarget::Slave {
            master_addr,
            slave_addr,
        } => println!(
            "============My Master: {}=====Slave: {}============",
            master_addr, slave_addr
        ),
        BrokerConfigSectionTarget::NoMaster => {}
    }
}

fn print_empty_section(section: &BrokerConfigSection, key_pattern: Option<&str>) {
    let Some(addr) = broker_addr_for_section(section) else {
        return;
    };

    if let Some(pattern) = key_pattern {
        println!(
            "Broker[{}] has no config property matching key pattern /{}/!",
            addr, pattern
        );
    } else {
        println!("Broker[{}] has no config property!", addr);
    }
}

fn broker_addr_for_section(section: &BrokerConfigSection) -> Option<&str> {
    match &section.target {
        BrokerConfigSectionTarget::Broker(addr) | BrokerConfigSectionTarget::Master(addr) => Some(addr.as_str()),
        BrokerConfigSectionTarget::Slave { slave_addr, .. } => Some(slave_addr.as_str()),
        BrokerConfigSectionTarget::NoMaster => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_master_placeholder_check() {
        let result = BrokerConfigQueryResult {
            sections: vec![BrokerConfigSection {
                target: BrokerConfigSectionTarget::NoMaster,
                entries: vec![],
            }],
            key_pattern: None,
        };

        print_broker_config_result(&result);
    }

    #[test]
    fn test_parse_with_broker_addr() {
        let args = vec!["get-broker-config", "-b", "192.168.1.1:10911"];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.broker_addr, Some("192.168.1.1:10911".to_string()));
        assert_eq!(cmd.cluster_name, None);
    }

    #[test]
    fn test_parse_with_cluster_name() {
        let args = vec!["get-broker-config", "-c", "DefaultCluster"];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.broker_addr, None);
        assert_eq!(cmd.cluster_name, Some("DefaultCluster".to_string()));
        assert_eq!(cmd.key_pattern, None);
    }

    #[test]
    fn test_parse_with_long_option_broker_addr() {
        let args = vec!["get-broker-config", "--brokerAddr", "127.0.0.1:10911"];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(cmd.cluster_name, None);
    }

    #[test]
    fn test_parse_with_long_option_cluster_name() {
        let args = vec!["get-broker-config", "--clusterName", "TestCluster"];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.broker_addr, None);
        assert_eq!(cmd.cluster_name, Some("TestCluster".to_string()));
        assert_eq!(cmd.key_pattern, None);
    }

    #[test]
    fn test_parse_with_key_pattern() {
        let args = vec!["get-broker-config", "-b", "127.0.0.1:10911", "--keyPattern", "^flush.*"];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(cmd.cluster_name, None);
        assert_eq!(cmd.key_pattern, Some("^flush.*".to_string()));
    }

    #[test]
    fn test_invalid_key_pattern_regex_returns_error() {
        let cmd = GetBrokerConfigSubCommand {
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            key_pattern: Some("[".to_string()),
        };

        let err = cmd.request();
        assert!(err.is_err());

        let err_msg = format!("{}", err.unwrap_err());
        assert!(err_msg.contains("invalid key regex pattern"));
    }

    #[test]
    fn test_parse_missing_required_option() {
        let args = vec!["get-broker-config"];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_parse_both_options_provided() {
        // Both broker_addr and cluster_name provided (should fail due to mutual exclusivity)
        let args = vec!["get-broker-config", "-b", "192.168.1.1:10911", "-c", "DefaultCluster"];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_broker_addr_with_spaces() {
        let args = vec!["get-broker-config", "-b", "  192.168.1.1:10911  "];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.broker_addr.as_ref().unwrap().trim(), "192.168.1.1:10911");
    }

    #[test]
    fn test_cluster_name_with_spaces() {
        let args = vec!["get-broker-config", "-c", "  DefaultCluster  "];

        let cmd = GetBrokerConfigSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.cluster_name.as_ref().unwrap().trim(), "DefaultCluster");
    }
}
