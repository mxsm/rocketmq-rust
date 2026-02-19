// Copyright 2026 The RocketMQ Rust Authors
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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

const MADV_NORMAL: &str = "0";
const MADV_RANDOM: &str = "1";
const COMMIT_LOG_READ_AHEAD_MODE_KEY: &str = "commitLogReadAheadMode";
const DATA_READ_AHEAD_ENABLE_KEY: &str = "dataReadAheadEnable";
const DEFAULT_READ_AHEAD_SIZE_KEY: &str = "dataReadAheadSize";
const KNOWN_READ_AHEAD_SIZE_KEYS: [&str; 3] = [DEFAULT_READ_AHEAD_SIZE_KEY, "commitLogReadAheadSize", "readAheadSize"];

#[derive(Debug, Clone, Parser)]
#[command(group(
    ArgGroup::new("target")
        .required(true)
        .args(&["broker_addr", "cluster_name"])
))]
#[command(group(
    ArgGroup::new("mode")
        .required(false)
        .args(&["commit_log_read_ahead_mode", "enable", "disable"])
))]
pub struct CommitLogSetReadAheadSubCommand {
    #[arg(short = 'b', long = "brokerAddr", help = "Set which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "Set which cluster")]
    cluster_name: Option<String>,

    #[arg(
        short = 'm',
        long = "commitLogReadAheadMode",
        value_name = "MODE",
        help = "CommitLog read-ahead mode; 0 is default (read-ahead enabled), 1 random read (read-ahead disabled)"
    )]
    commit_log_read_ahead_mode: Option<String>,

    #[arg(long = "enable", help = "Enable commit log read-ahead")]
    enable: bool,

    #[arg(long = "disable", help = "Disable commit log read-ahead (random read mode)")]
    disable: bool,

    #[arg(
        short = 's',
        long = "readAheadSize",
        value_name = "BYTES",
        help = "Set commit log read-ahead size in bytes (positive integer)"
    )]
    read_ahead_size: Option<String>,

    #[arg(
        long = "readAheadSizeKey",
        value_name = "KEY",
        help = "Broker config key to use for read-ahead size; auto-detected when omitted"
    )]
    read_ahead_size_key: Option<String>,

    #[arg(
        long = "showOnly",
        visible_alias = "show-only",
        help = "Only display current read-ahead configuration without applying changes"
    )]
    show_only: bool,
}

#[derive(Debug, Clone, Copy)]
enum ReadAheadMode {
    Normal,
    Random,
}

impl ReadAheadMode {
    fn as_str(self) -> &'static str {
        match self {
            ReadAheadMode::Normal => MADV_NORMAL,
            ReadAheadMode::Random => MADV_RANDOM,
        }
    }

    fn read_ahead_enabled(self) -> bool {
        matches!(self, ReadAheadMode::Normal)
    }
}

#[derive(Debug, Clone)]
struct BrokerTarget {
    addr: String,
    print_prefix: String,
}

impl CommitLogSetReadAheadSubCommand {
    fn resolve_read_ahead_mode(&self) -> RocketMQResult<Option<ReadAheadMode>> {
        if self.enable {
            return Ok(Some(ReadAheadMode::Normal));
        }
        if self.disable {
            return Ok(Some(ReadAheadMode::Random));
        }

        match self.commit_log_read_ahead_mode.as_deref().map(str::trim) {
            Some(MADV_NORMAL) => Ok(Some(ReadAheadMode::Normal)),
            Some(MADV_RANDOM) => Ok(Some(ReadAheadMode::Random)),
            Some(mode) => Err(RocketMQError::IllegalArgument(format!(
                "CommitLogSetReadAheadSubCommand: Invalid commitLogReadAheadMode '{}', expected 0 or 1",
                mode
            ))),
            None => Ok(None),
        }
    }

    fn parse_read_ahead_size(&self) -> RocketMQResult<Option<u64>> {
        let Some(raw_size) = self.read_ahead_size.as_deref() else {
            return Ok(None);
        };

        let read_ahead_size = raw_size.trim().parse::<u64>().map_err(|e| {
            RocketMQError::IllegalArgument(format!(
                "CommitLogSetReadAheadSubCommand: Invalid readAheadSize '{}': {}",
                raw_size, e
            ))
        })?;

        if read_ahead_size == 0 {
            return Err(RocketMQError::IllegalArgument(
                "CommitLogSetReadAheadSubCommand: readAheadSize must be greater than 0".to_string(),
            ));
        }

        Ok(Some(read_ahead_size))
    }

    fn resolve_read_ahead_size_key(&self, config: &HashMap<CheetahString, CheetahString>) -> RocketMQResult<String> {
        if let Some(size_key) = self.read_ahead_size_key.as_deref().map(str::trim) {
            if size_key.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "CommitLogSetReadAheadSubCommand: readAheadSizeKey cannot be empty".to_string(),
                ));
            }
            return Ok(size_key.to_string());
        }

        for candidate in KNOWN_READ_AHEAD_SIZE_KEYS {
            if get_config_value(config, candidate).is_some() {
                return Ok(candidate.to_string());
            }
        }

        Ok(DEFAULT_READ_AHEAD_SIZE_KEY.to_string())
    }

    async fn resolve_targets(&self, admin_ext: &DefaultMQAdminExt) -> RocketMQResult<Vec<BrokerTarget>> {
        if let Some(broker_addr) = &self.broker_addr {
            let broker_addr = broker_addr.trim();
            if broker_addr.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "CommitLogSetReadAheadSubCommand: brokerAddr cannot be empty".to_string(),
                ));
            }
            return Ok(vec![BrokerTarget {
                addr: broker_addr.to_string(),
                print_prefix: format!("============{}============\n", broker_addr),
            }]);
        }

        let cluster_name = self
            .cluster_name
            .as_deref()
            .map(str::trim)
            .filter(|cluster_name| !cluster_name.is_empty())
            .ok_or_else(|| {
                RocketMQError::IllegalArgument(
                    "CommitLogSetReadAheadSubCommand: clusterName cannot be empty".to_string(),
                )
            })?;

        let cluster_info = admin_ext.examine_broker_cluster_info().await.map_err(|e| {
            RocketMQError::Internal(format!(
                "CommitLogSetReadAheadSubCommand: Failed to examine broker cluster info: {}",
                e
            ))
        })?;

        let master_and_slave_map = CommandUtil::fetch_master_and_slave_distinguish(&cluster_info, cluster_name)?;

        let mut sorted_master_and_slave_map: Vec<_> = master_and_slave_map.into_iter().collect();
        sorted_master_and_slave_map.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));

        let mut broker_targets = Vec::new();
        for (master_addr, mut slave_addrs) in sorted_master_and_slave_map {
            slave_addrs.sort_by(|left, right| left.as_str().cmp(right.as_str()));

            if master_addr.as_str() != CommandUtil::NO_MASTER_PLACEHOLDER {
                broker_targets.push(BrokerTarget {
                    addr: master_addr.to_string(),
                    print_prefix: format!("============Master: {}============\n", master_addr),
                });
            }

            for slave_addr in slave_addrs {
                broker_targets.push(BrokerTarget {
                    addr: slave_addr.to_string(),
                    print_prefix: format!(
                        "============My Master: {}=====Slave: {}============\n",
                        master_addr, slave_addr
                    ),
                });
            }
        }

        if broker_targets.is_empty() {
            return Err(RocketMQError::Internal(format!(
                "CommitLogSetReadAheadSubCommand: Cluster {} has no broker address",
                cluster_name
            )));
        }

        Ok(broker_targets)
    }

    async fn apply_to_target(
        &self,
        admin_ext: &DefaultMQAdminExt,
        target: &BrokerTarget,
        mode: Option<ReadAheadMode>,
        read_ahead_size: Option<u64>,
        show_only: bool,
    ) -> RocketMQResult<()> {
        print!("{}", target.print_prefix);

        let current_config = admin_ext
            .get_broker_config(CheetahString::from(target.addr.as_str()))
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "CommitLogSetReadAheadSubCommand: Failed to get broker config for {}: {}",
                    target.addr, e
                ))
            })?;

        let size_key_for_update = if read_ahead_size.is_some() {
            Some(self.resolve_read_ahead_size_key(&current_config)?)
        } else {
            self.read_ahead_size_key
                .as_deref()
                .map(str::trim)
                .filter(|key| !key.is_empty())
                .map(ToString::to_string)
        };

        println!("Current read-ahead configuration:");
        print_read_ahead_config(&current_config, size_key_for_update.as_deref());

        if show_only || (mode.is_none() && read_ahead_size.is_none()) {
            println!("No read-ahead update requested for broker {}.\n", target.addr);
            return Ok(());
        }

        let mut properties = HashMap::new();
        if let Some(mode) = mode {
            properties.insert(
                CheetahString::from_static_str(COMMIT_LOG_READ_AHEAD_MODE_KEY),
                CheetahString::from_static_str(mode.as_str()),
            );
            properties.insert(
                CheetahString::from_static_str(DATA_READ_AHEAD_ENABLE_KEY),
                CheetahString::from(mode.read_ahead_enabled().to_string()),
            );
        }

        if let Some(read_ahead_size) = read_ahead_size {
            let size_key = size_key_for_update.as_deref().unwrap_or(DEFAULT_READ_AHEAD_SIZE_KEY);
            properties.insert(
                CheetahString::from(size_key),
                CheetahString::from(read_ahead_size.to_string()),
            );
        }

        admin_ext
            .update_broker_config(CheetahString::from(target.addr.as_str()), properties)
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "CommitLogSetReadAheadSubCommand: Failed to update broker {}: {}",
                    target.addr, e
                ))
            })?;

        println!("Read-ahead update applied successfully.");

        let updated_config = admin_ext
            .get_broker_config(CheetahString::from(target.addr.as_str()))
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "CommitLogSetReadAheadSubCommand: Failed to fetch updated config for {}: {}",
                    target.addr, e
                ))
            })?;

        println!("Updated read-ahead configuration:");
        print_read_ahead_config(&updated_config, size_key_for_update.as_deref());
        Ok(())
    }
}

impl CommandExecute for CommitLogSetReadAheadSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mode = self.resolve_read_ahead_mode()?;
        let read_ahead_size = self.parse_read_ahead_size()?;
        let has_updates = mode.is_some() || read_ahead_size.is_some();

        if self.show_only && has_updates {
            return Err(RocketMQError::IllegalArgument(
                "CommitLogSetReadAheadSubCommand: --showOnly cannot be used with update options".to_string(),
            ));
        }

        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "CommitLogSetReadAheadSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let targets = self.resolve_targets(&default_mqadmin_ext).await?;
            let mut failures = Vec::new();

            for target in targets {
                if let Err(error) = self
                    .apply_to_target(&default_mqadmin_ext, &target, mode, read_ahead_size, self.show_only)
                    .await
                {
                    failures.push(format!("{}: {}", target.addr, error));
                }
            }

            if failures.is_empty() {
                Ok(())
            } else {
                Err(RocketMQError::Internal(format!(
                    "CommitLogSetReadAheadSubCommand: Failed on {} broker(s): {}",
                    failures.len(),
                    failures.join("; ")
                )))
            }
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

fn get_config_value(config: &HashMap<CheetahString, CheetahString>, key: &str) -> Option<String> {
    config
        .iter()
        .find(|(config_key, _)| config_key.as_str() == key)
        .map(|(_, value)| value.to_string())
}

fn parse_bool_like(raw_value: &str) -> Option<bool> {
    match raw_value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn derive_read_ahead_enabled(enable: Option<&str>, mode: Option<&str>) -> Option<bool> {
    if let Some(enable) = enable.and_then(parse_bool_like) {
        return Some(enable);
    }

    match mode.map(str::trim) {
        Some(MADV_NORMAL) => Some(true),
        Some(MADV_RANDOM) => Some(false),
        _ => None,
    }
}

fn resolve_size_entry(
    config: &HashMap<CheetahString, CheetahString>,
    preferred_size_key: Option<&str>,
) -> Option<(String, String)> {
    if let Some(preferred_size_key) = preferred_size_key {
        if let Some(value) = get_config_value(config, preferred_size_key) {
            return Some((preferred_size_key.to_string(), value));
        }
    }

    for key in KNOWN_READ_AHEAD_SIZE_KEYS {
        if let Some(value) = get_config_value(config, key) {
            return Some((key.to_string(), value));
        }
    }

    None
}

fn print_read_ahead_config(config: &HashMap<CheetahString, CheetahString>, preferred_size_key: Option<&str>) {
    let mode = get_config_value(config, COMMIT_LOG_READ_AHEAD_MODE_KEY);
    let data_read_ahead_enable = get_config_value(config, DATA_READ_AHEAD_ENABLE_KEY);

    println!(
        "{:<32}= {}",
        COMMIT_LOG_READ_AHEAD_MODE_KEY,
        mode.as_deref().unwrap_or("<not-set>")
    );
    println!(
        "{:<32}= {}",
        DATA_READ_AHEAD_ENABLE_KEY,
        data_read_ahead_enable.as_deref().unwrap_or("<not-set>")
    );

    if let Some((size_key, size_value)) = resolve_size_entry(config, preferred_size_key) {
        println!("{:<32}= {}", size_key, size_value);
    } else {
        let size_key = preferred_size_key.unwrap_or(DEFAULT_READ_AHEAD_SIZE_KEY);
        println!("{:<32}= <not-set>", size_key);
    }

    if let Some(enabled) = derive_read_ahead_enabled(data_read_ahead_enable.as_deref(), mode.as_deref()) {
        println!("{:<32}= {}", "readAheadEnabled(summary)", enabled);
    }

    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_java_compatible_mode_for_single_broker() {
        let args = [
            "setCommitLogReadAheadMode",
            "--brokerAddr",
            "127.0.0.1:10911",
            "--commitLogReadAheadMode",
            "1",
        ];

        let command = CommitLogSetReadAheadSubCommand::try_parse_from(args).unwrap();

        assert_eq!(command.broker_addr.as_deref(), Some("127.0.0.1:10911"));
        assert!(command.cluster_name.is_none());
        assert_eq!(
            command.resolve_read_ahead_mode().unwrap().map(ReadAheadMode::as_str),
            Some("1")
        );
    }

    #[test]
    fn parse_enable_with_size_for_cluster() {
        let args = [
            "setCommitLogReadAheadMode",
            "-c",
            "DefaultCluster",
            "--enable",
            "--readAheadSize",
            "65536",
        ];

        let command = CommitLogSetReadAheadSubCommand::try_parse_from(args).unwrap();

        assert!(command.broker_addr.is_none());
        assert_eq!(command.cluster_name.as_deref(), Some("DefaultCluster"));
        assert_eq!(
            command.resolve_read_ahead_mode().unwrap().map(ReadAheadMode::as_str),
            Some("0")
        );
        assert_eq!(command.parse_read_ahead_size().unwrap(), Some(65536));
    }

    #[test]
    fn parse_rejects_mode_conflict() {
        let args = [
            "setCommitLogReadAheadMode",
            "-b",
            "127.0.0.1:10911",
            "--enable",
            "-m",
            "0",
        ];

        let result = CommitLogSetReadAheadSubCommand::try_parse_from(args);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_read_ahead_mode_rejects_invalid_mode_value() {
        let command = CommitLogSetReadAheadSubCommand {
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            commit_log_read_ahead_mode: Some("2".to_string()),
            enable: false,
            disable: false,
            read_ahead_size: None,
            read_ahead_size_key: None,
            show_only: false,
        };

        assert!(command.resolve_read_ahead_mode().is_err());
    }

    #[test]
    fn parse_read_ahead_size_rejects_zero() {
        let command = CommitLogSetReadAheadSubCommand {
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            commit_log_read_ahead_mode: None,
            enable: false,
            disable: false,
            read_ahead_size: Some("0".to_string()),
            read_ahead_size_key: None,
            show_only: false,
        };

        assert!(command.parse_read_ahead_size().is_err());
    }

    #[test]
    fn resolve_size_key_prefers_existing_key() {
        let mut config = HashMap::new();
        config.insert(
            CheetahString::from_static_str("commitLogReadAheadSize"),
            CheetahString::from_static_str("4096"),
        );

        let command = CommitLogSetReadAheadSubCommand {
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            commit_log_read_ahead_mode: None,
            enable: false,
            disable: false,
            read_ahead_size: Some("8192".to_string()),
            read_ahead_size_key: None,
            show_only: false,
        };

        assert_eq!(
            command.resolve_read_ahead_size_key(&config).unwrap(),
            "commitLogReadAheadSize"
        );
    }

    #[test]
    fn derive_read_ahead_enabled_prefers_boolean_value() {
        assert_eq!(derive_read_ahead_enabled(Some("true"), Some("1")), Some(true));
        assert_eq!(derive_read_ahead_enabled(Some("false"), Some("0")), Some(false));
    }

    #[test]
    fn derive_read_ahead_enabled_falls_back_to_mode() {
        assert_eq!(derive_read_ahead_enabled(None, Some("0")), Some(true));
        assert_eq!(derive_read_ahead_enabled(None, Some("1")), Some(false));
        assert_eq!(derive_read_ahead_enabled(None, Some("2")), None);
    }
}
