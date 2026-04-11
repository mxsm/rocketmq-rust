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

use std::collections::BTreeMap;
use std::sync::Arc;

use clap::ArgAction;
use clap::ArgGroup;
use clap::Parser;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::broker::BrokerConfigUpdateApplyResult;
use rocketmq_admin_core::core::broker::BrokerConfigUpdatePlanResult;
use rocketmq_admin_core::core::broker::BrokerConfigUpdateRequest;
use rocketmq_admin_core::core::broker::BrokerService;

#[derive(Debug, Clone, Parser)]
#[command(group(
    ArgGroup::new("target")
        .required(true)
        .args(&["broker_addr", "cluster_name"])
))]
#[command(group(
    ArgGroup::new("config")
        .required(true)
        .args(&["key", "properties"])
))]
pub struct UpdateBrokerConfigSubCommand {
    #[arg(short = 'b', long = "brokerAddr", help = "Update which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "Update all brokers in cluster")]
    cluster_name: Option<String>,

    #[arg(
        short = 'k',
        long = "key",
        requires = "value",
        conflicts_with = "properties",
        help = "Single config key to update"
    )]
    key: Option<String>,

    #[arg(
        short = 'v',
        long = "value",
        requires = "key",
        conflicts_with = "properties",
        help = "Single config value to update"
    )]
    value: Option<String>,

    #[arg(
        short = 'p',
        long = "property",
        value_name = "KEY=VALUE",
        action = ArgAction::Append,
        help = "Config entry to update; repeat for multiple entries"
    )]
    properties: Vec<String>,

    #[arg(
        long = "dryRun",
        visible_alias = "dry-run",
        action = ArgAction::SetTrue,
        help = "Preview validated changes without applying updates"
    )]
    dry_run: bool,

    #[arg(
        long = "noRollback",
        visible_alias = "no-rollback",
        help = "Disable automatic rollback when cluster update partially fails"
    )]
    no_rollback: bool,

    #[arg(
    short = 'y',
    long = "yes",
    action = ArgAction::SetTrue,
    help = "Skip confirmation prompt and apply changes immediately"
)]
    yes: bool,
}

impl UpdateBrokerConfigSubCommand {
    fn request(&self) -> RocketMQResult<BrokerConfigUpdateRequest> {
        Ok(BrokerConfigUpdateRequest::try_new(
            self.broker_addr.clone(),
            self.cluster_name.clone(),
            self.parse_update_entries()?,
        )?
        .with_rollback_enabled(!self.no_rollback))
    }

    fn parse_update_entries(&self) -> RocketMQResult<BTreeMap<String, String>> {
        let mut entries = BTreeMap::new();

        if let (Some(key), Some(value)) = (self.key.as_deref(), self.value.as_deref()) {
            insert_update_entry(&mut entries, key, value, "--key/--value")?;
        }

        for property in &self.properties {
            let (key, value) = parse_property_entry(property)?;
            insert_update_entry(&mut entries, key.as_str(), value.as_str(), "--property")?;
        }

        if entries.is_empty() {
            return Err(RocketMQError::IllegalArgument(
                "UpdateBrokerConfigSubCommand: No config entries provided".to_string(),
            ));
        }

        Ok(entries)
    }
}

impl CommandExecute for UpdateBrokerConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let plan =
            BrokerService::build_broker_config_update_plan_by_request_with_rpc_hook(request.clone(), rpc_hook.clone())
                .await?;

        print_update_plan(&plan);
        if self.dry_run {
            println!("Dry-run mode enabled, no broker config has been changed.");
            return Ok(());
        }

        if !self.yes && !prompt_confirmation() {
            println!("Aborted by user, no broker config has been changed.");
            return Ok(());
        }

        let apply_result =
            BrokerService::apply_broker_config_update_plan_by_request_with_rpc_hook(&request, &plan, rpc_hook).await?;
        print_apply_result(&apply_result, plan.changed_broker_count());
        Ok(())
    }
}

fn insert_update_entry(
    entries: &mut BTreeMap<String, String>,
    key: &str,
    value: &str,
    source: &str,
) -> RocketMQResult<()> {
    let key = key.trim();
    let value = value.trim();

    match entries.get(key) {
        Some(existing) if existing != value => Err(RocketMQError::IllegalArgument(format!(
            "UpdateBrokerConfigSubCommand: Conflicting values for key '{}' from {}",
            key, source
        ))),
        Some(_) => Ok(()),
        None => {
            entries.insert(key.to_string(), value.to_string());
            Ok(())
        }
    }
}

fn parse_property_entry(property: &str) -> RocketMQResult<(String, String)> {
    let (key, value) = property.split_once('=').ok_or_else(|| {
        RocketMQError::IllegalArgument(format!(
            "UpdateBrokerConfigSubCommand: Invalid property '{}', expected KEY=VALUE",
            property
        ))
    })?;
    Ok((key.trim().to_string(), value.trim().to_string()))
}

fn print_update_plan(result: &BrokerConfigUpdatePlanResult) {
    println!("Planned broker configuration changes:");
    for plan in &result.plans {
        println!("============{}============", plan.broker_addr);
        if plan.changes.is_empty() {
            println!("(no effective changes)\n");
            continue;
        }

        for change in &plan.changes {
            let old_value = change
                .old_value
                .as_ref()
                .map(|value| value.as_str())
                .unwrap_or("<missing>");
            println!("{:<50} {} -> {}", change.key, old_value, change.new_value);
        }
        println!();
    }
}

fn print_apply_result(result: &BrokerConfigUpdateApplyResult, updated_broker_count: usize) {
    for broker_addr in &result.skipped_brokers {
        println!("Broker {} has no effective config changes, skipped.", broker_addr);
    }

    for applied in &result.applied_updates {
        println!("Updated broker {} successfully.", applied.broker_addr);
        if !applied.non_rollbackable_keys.is_empty() {
            let keys = applied
                .non_rollbackable_keys
                .iter()
                .map(|key| key.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            println!(
                "Warning: broker {} has newly added keys [{}]; rollback can only restore existing keys.",
                applied.broker_addr, keys
            );
        }
    }

    println!(
        "UpdateBrokerConfigSubCommand: Updated broker config on {} broker(s).",
        updated_broker_count
    );
}

fn prompt_confirmation() -> bool {
    use std::io::BufRead;
    use std::io::Write;
    use std::io::{self};

    print!("Apply these changes? [y/N] ");
    io::stdout().flush().unwrap_or(());

    let stdin = io::stdin();
    let mut line = String::new();
    if stdin.lock().read_line(&mut line).is_err() {
        return false;
    }

    matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_key_value() {
        let args = [
            "updateBrokerConfig",
            "-b",
            "127.0.0.1:10911",
            "-k",
            "flushDiskType",
            "-v",
            "ASYNC_FLUSH",
        ];
        let cmd = UpdateBrokerConfigSubCommand::try_parse_from(args).unwrap();

        assert_eq!(cmd.broker_addr.as_deref(), Some("127.0.0.1:10911"));
        assert!(cmd.cluster_name.is_none());
        assert!(!cmd.yes);

        let entries = cmd.parse_update_entries().unwrap();
        assert_eq!(entries.get("flushDiskType"), Some(&"ASYNC_FLUSH".to_string()));
    }

    #[test]
    fn test_parse_multiple_properties() {
        let args = [
            "updateBrokerConfig",
            "--clusterName",
            "DefaultCluster",
            "--property",
            "maxTransferCountOnMessageInMemory=256",
            "--property",
            "enableControllerMode=true",
            "--dryRun",
        ];
        let cmd = UpdateBrokerConfigSubCommand::try_parse_from(args).unwrap();

        assert!(cmd.broker_addr.is_none());
        assert_eq!(cmd.cluster_name.as_deref(), Some("DefaultCluster"));
        assert!(cmd.dry_run);
        assert!(!cmd.yes);

        let entries = cmd.parse_update_entries().unwrap();
        assert_eq!(
            entries.get("maxTransferCountOnMessageInMemory"),
            Some(&"256".to_string())
        );
        assert_eq!(entries.get("enableControllerMode"), Some(&"true".to_string()));
    }

    #[test]
    fn test_parse_yes_confirmation_flag() {
        let args = [
            "updateBrokerConfig",
            "-b",
            "127.0.0.1:10911",
            "-k",
            "flushDiskType",
            "-v",
            "ASYNC_FLUSH",
            "--yes",
        ];
        let cmd = UpdateBrokerConfigSubCommand::try_parse_from(args).unwrap();
        assert!(cmd.yes);
    }

    #[test]
    fn test_parse_requires_target() {
        let args = ["updateBrokerConfig", "-k", "flushDiskType", "-v", "ASYNC_FLUSH"];
        let result = UpdateBrokerConfigSubCommand::try_parse_from(args);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_update_entries_reject_invalid_property() {
        let cmd = UpdateBrokerConfigSubCommand {
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            key: None,
            value: None,
            properties: vec!["invalid_property".to_string()],
            dry_run: false,
            no_rollback: false,
            yes: false,
        };

        let result = cmd.parse_update_entries();
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_update_entries_reject_conflicting_values() {
        let cmd = UpdateBrokerConfigSubCommand {
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            key: None,
            value: None,
            properties: vec![
                "flushDiskType=ASYNC_FLUSH".to_string(),
                "flushDiskType=SYNC_FLUSH".to_string(),
            ],
            dry_run: false,
            no_rollback: false,
            yes: false,
        };

        let result = cmd.parse_update_entries();
        assert!(result.is_err());
    }

    #[test]
    fn test_request_disables_rollback_for_no_rollback_flag() {
        let args = [
            "updateBrokerConfig",
            "-b",
            "127.0.0.1:10911",
            "-k",
            "flushDiskType",
            "-v",
            "ASYNC_FLUSH",
            "--noRollback",
        ];
        let cmd = UpdateBrokerConfigSubCommand::try_parse_from(args).unwrap();

        assert!(!cmd.request().unwrap().rollback_enabled());
    }
}
