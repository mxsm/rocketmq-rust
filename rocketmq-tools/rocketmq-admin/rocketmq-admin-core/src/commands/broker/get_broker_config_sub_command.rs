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
use regex::Regex;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

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
    fn key_pattern_regex(&self) -> RocketMQResult<Option<Regex>> {
        self.key_pattern
            .as_deref()
            .map(str::trim)
            .filter(|pattern| !pattern.is_empty())
            .map(|pattern| {
                Regex::new(pattern).map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetBrokerConfigSubCommand: Invalid key regex pattern '{}': {}",
                        pattern, e
                    ))
                })
            })
            .transpose()
    }

    async fn get_and_print(
        &self,
        default_mqadmin_ext: &DefaultMQAdminExt,
        print_prefix: &str,
        addr: &str,
        key_pattern_regex: Option<&Regex>,
    ) -> RocketMQResult<()> {
        if addr == CommandUtil::NO_MASTER_PLACEHOLDER {
            println!("============(No master found — skipping)============\n");
            return Ok(());
        }

        print!("{}", print_prefix);

        let properties = default_mqadmin_ext
            .get_broker_config(CheetahString::from(addr))
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!("GetBrokerConfigSubCommand: Failed to get broker config: {}", e))
            })?;

        if properties.is_empty() {
            println!("Broker[{}] has no config property!", addr);
            return Ok(());
        }

        let mut sorted_keys: Vec<_> = properties.keys().collect();
        sorted_keys.sort();

        let mut matched = false;
        for key in &sorted_keys {
            let value = &properties[*key];
            if key_pattern_regex.is_some_and(|regex| !regex.is_match(key.as_str())) {
                continue;
            }
            println!("{:<50}=  {}", key, value);
            matched = true;
        }

        if !matched {
            if let Some(regex) = key_pattern_regex {
                println!(
                    "Broker[{}] has no config property matching key pattern /{}/!",
                    addr,
                    regex.as_str()
                );
            } else {
                println!("Broker[{}] has no config property!", addr);
            }
        }

        println!();

        Ok(())
    }
}

impl CommandExecute for GetBrokerConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        let key_pattern_regex = self.key_pattern_regex()?;

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("GetBrokerConfigSubCommand: Failed to start MQAdminExt: {}", e))
            })?;
            if let Some(broker_addr) = &self.broker_addr {
                let broker_addr = broker_addr.trim();
                self.get_and_print(
                    &default_mqadmin_ext,
                    &format!("============{}============\n", broker_addr),
                    broker_addr,
                    key_pattern_regex.as_ref(),
                )
                .await?;
            } else if let Some(cluster_name) = &self.cluster_name {
                let cluster_name = cluster_name.trim();
                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetBrokerConfigSubCommand: Failed to examine broker cluster info: {}",
                        e
                    ))
                })?;

                let master_and_slave_map =
                    CommandUtil::fetch_master_and_slave_distinguish(&cluster_info, cluster_name)?;

                let mut sorted_masters: Vec<_> = master_and_slave_map.keys().collect();
                sorted_masters.sort();

                for master_addr in &sorted_masters {
                    let slave_addrs = &master_and_slave_map[*master_addr];
                    self.get_and_print(
                        &default_mqadmin_ext,
                        &format!("============Master: {}============\n", master_addr),
                        master_addr.as_str(),
                        key_pattern_regex.as_ref(),
                    )
                    .await?;

                    let mut sorted_slaves: Vec<_> = slave_addrs.iter().collect();
                    sorted_slaves.sort();

                    for slave_addr in &sorted_slaves {
                        self.get_and_print(
                            &default_mqadmin_ext,
                            &format!(
                                "============My Master: {}=====Slave: {}============\n",
                                master_addr, slave_addr
                            ),
                            slave_addr.as_str(),
                            key_pattern_regex.as_ref(),
                        )
                        .await?;
                    }
                }
            }
            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_master_placeholder_check() {
        // Ensure NO_MASTER_PLACEHOLDER constant is correctly defined
        assert_eq!(CommandUtil::NO_MASTER_PLACEHOLDER, "NO_MASTER");
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

        let err = cmd.key_pattern_regex();
        assert!(err.is_err());

        let err_msg = format!("{}", err.unwrap_err());
        assert!(err_msg.contains("Invalid key regex pattern"));
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
