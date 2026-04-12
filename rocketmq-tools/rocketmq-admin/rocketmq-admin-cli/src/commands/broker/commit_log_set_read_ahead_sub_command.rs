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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerConfigSectionTarget;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::broker::CommitLogReadAheadRequest;
use rocketmq_admin_core::core::broker::CommitLogReadAheadResult;
use rocketmq_admin_core::core::broker::CommitLogReadAheadSection;

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

impl CommitLogSetReadAheadSubCommand {
    fn request(&self) -> RocketMQResult<CommitLogReadAheadRequest> {
        CommitLogReadAheadRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.commit_log_read_ahead_mode.clone(),
            self.enable,
            self.disable,
            self.read_ahead_size.clone(),
            self.read_ahead_size_key.clone(),
            self.show_only,
        )
    }

    fn print_result(request: &CommitLogReadAheadRequest, result: CommitLogReadAheadResult) -> RocketMQResult<()> {
        for section in result.sections {
            print_section(request, section);
        }

        if result.failures.is_empty() {
            Ok(())
        } else {
            Err(RocketMQError::Internal(format!(
                "CommitLogSetReadAheadSubCommand: Failed on {} broker(s): {}",
                result.failures.len(),
                result
                    .failures
                    .iter()
                    .map(|failure| format!("{}: {}", failure.broker_addr, failure.error))
                    .collect::<Vec<_>>()
                    .join("; ")
            )))
        }
    }
}

impl CommandExecute for CommitLogSetReadAheadSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let result =
            BrokerService::set_commit_log_read_ahead_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        Self::print_result(&request, result)
    }
}

fn print_section(request: &CommitLogReadAheadRequest, section: CommitLogReadAheadSection) {
    print!("{}", section_prefix(&section.target));
    println!("Current read-ahead configuration:");
    print_read_ahead_config(
        &section.current_config,
        section.size_key_for_update.as_ref().map(|key| key.as_str()),
    );

    if !section.applied {
        println!("No read-ahead update requested for broker {}.\n", section.broker_addr);
        return;
    }

    println!("Read-ahead update applied successfully.");
    if let Some(updated_config) = section.updated_config.as_ref() {
        println!("Updated read-ahead configuration:");
        print_read_ahead_config(
            updated_config,
            section.size_key_for_update.as_ref().map(|key| key.as_str()),
        );
    }

    if !request.has_updates() {
        println!("No read-ahead update requested for broker {}.\n", section.broker_addr);
    }
}

fn section_prefix(target: &BrokerConfigSectionTarget) -> String {
    match target {
        BrokerConfigSectionTarget::Broker(addr) => format!("============{}============\n", addr),
        BrokerConfigSectionTarget::Master(master_addr) => format!("============Master: {}============\n", master_addr),
        BrokerConfigSectionTarget::Slave {
            master_addr,
            slave_addr,
        } => format!(
            "============My Master: {}=====Slave: {}============\n",
            master_addr, slave_addr
        ),
        BrokerConfigSectionTarget::NoMaster => "============NO_MASTER============\n".to_string(),
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
    use rocketmq_admin_core::core::broker::CommitLogReadAheadMode;

    #[test]
    fn parse_java_compatible_mode_for_single_broker() {
        let command = CommitLogSetReadAheadSubCommand::try_parse_from([
            "setCommitLogReadAheadMode",
            "--brokerAddr",
            "127.0.0.1:10911",
            "--commitLogReadAheadMode",
            "1",
        ])
        .unwrap();
        let request = command.request().unwrap();

        assert_eq!(request.mode(), Some(CommitLogReadAheadMode::Random));
        assert!(request.read_ahead_size().is_none());
    }

    #[test]
    fn parse_enable_with_size_for_cluster() {
        let command = CommitLogSetReadAheadSubCommand::try_parse_from([
            "setCommitLogReadAheadMode",
            "-c",
            "DefaultCluster",
            "--enable",
            "--readAheadSize",
            "65536",
        ])
        .unwrap();
        let request = command.request().unwrap();

        assert_eq!(request.mode(), Some(CommitLogReadAheadMode::Normal));
        assert_eq!(request.read_ahead_size(), Some(65536));
    }

    #[test]
    fn parse_rejects_mode_conflict() {
        let result = CommitLogSetReadAheadSubCommand::try_parse_from([
            "setCommitLogReadAheadMode",
            "-b",
            "127.0.0.1:10911",
            "--enable",
            "-m",
            "0",
        ]);

        assert!(result.is_err());
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
