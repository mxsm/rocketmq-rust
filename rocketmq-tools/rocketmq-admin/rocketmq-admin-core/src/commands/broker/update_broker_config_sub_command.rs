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

use futures::future::try_join_all;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::ArgAction;
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

#[derive(Debug, Clone)]
struct ConfigChange {
    key: String,
    old_value: Option<String>,
    new_value: String,
}

#[derive(Debug, Clone)]
struct BrokerUpdatePlan {
    broker_addr: String,
    changes: Vec<ConfigChange>,
}

#[derive(Debug, Clone)]
struct AppliedBrokerUpdate {
    broker_addr: String,
    rollback_properties: HashMap<CheetahString, CheetahString>,
    non_rollbackable_keys: Vec<String>,
}

impl UpdateBrokerConfigSubCommand {
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

    async fn resolve_targets(&self, admin_ext: &DefaultMQAdminExt) -> RocketMQResult<Vec<String>> {
        if let Some(broker_addr) = &self.broker_addr {
            let addr = broker_addr.trim();
            if addr.is_empty() {
                return Err(RocketMQError::IllegalArgument(
                    "UpdateBrokerConfigSubCommand: brokerAddr cannot be empty".to_string(),
                ));
            }
            return Ok(vec![addr.to_string()]);
        }

        let cluster_name = self
            .cluster_name
            .as_deref()
            .map(str::trim)
            .filter(|cluster_name| !cluster_name.is_empty())
            .ok_or_else(|| {
                RocketMQError::IllegalArgument("UpdateBrokerConfigSubCommand: clusterName cannot be empty".to_string())
            })?;

        let cluster_info = admin_ext.examine_broker_cluster_info().await.map_err(|e| {
            RocketMQError::Internal(format!(
                "UpdateBrokerConfigSubCommand: Failed to examine broker cluster info: {}",
                e
            ))
        })?;

        let mut broker_addrs = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?
            .into_iter()
            .map(|addr| addr.to_string())
            .filter(|addr| addr != CommandUtil::NO_MASTER_PLACEHOLDER)
            .collect::<Vec<_>>();

        broker_addrs.sort();
        broker_addrs.dedup();

        if broker_addrs.is_empty() {
            return Err(RocketMQError::Internal(format!(
                "UpdateBrokerConfigSubCommand: Cluster {} has no broker address",
                cluster_name
            )));
        }

        Ok(broker_addrs)
    }

    async fn build_update_plans(
        &self,
        admin_ext: &DefaultMQAdminExt,
        targets: &[String],
        update_entries: &BTreeMap<String, String>,
    ) -> RocketMQResult<Vec<BrokerUpdatePlan>> {
        let config = try_join_all(
            targets
                .iter()
                .map(|target| fetch_broker_config_snapshot(admin_ext, target)),
        )
        .await?;

        targets
            .iter()
            .zip(config)
            .map(|(broker_addr, current)| {
                let mut changes = Vec::new();
                for (key, new_value) in update_entries {
                    let old_value = current.get(key).cloned();
                    validate_update_value(key, new_value, old_value.as_deref())
                        .map_err(|e| RocketMQError::IllegalArgument(format!("Broker {}: {}", broker_addr, e)))?;
                    if old_value.as_deref() != Some(new_value.as_str()) {
                        changes.push(ConfigChange {
                            key: key.clone(),
                            old_value,
                            new_value: new_value.clone(),
                        });
                    }
                }
                Ok(BrokerUpdatePlan {
                    broker_addr: broker_addr.clone(),
                    changes,
                })
            })
            .collect()
    }

    async fn apply_update_plans(
        &self,
        admin_ext: &DefaultMQAdminExt,
        plans: &[BrokerUpdatePlan],
    ) -> RocketMQResult<()> {
        let mut applied_updates = Vec::new();

        for plan in plans {
            if plan.changes.is_empty() {
                println!("Broker {} has no effective config changes, skipped.", plan.broker_addr);
                continue;
            }

            let mut update_properties = HashMap::with_capacity(plan.changes.len());
            let mut rollback_properties = HashMap::new();
            let mut non_rollbackable_keys = Vec::new();

            for change in &plan.changes {
                update_properties.insert(
                    CheetahString::from(change.key.as_str()),
                    CheetahString::from(change.new_value.as_str()),
                );
                if let Some(old_value) = &change.old_value {
                    rollback_properties.insert(
                        CheetahString::from(change.key.as_str()),
                        CheetahString::from(old_value.as_str()),
                    );
                } else {
                    non_rollbackable_keys.push(change.key.clone());
                }
            }

            match admin_ext
                .update_broker_config(CheetahString::from(plan.broker_addr.as_str()), update_properties)
                .await
            {
                Ok(_) => {
                    println!("Updated broker {} successfully.", plan.broker_addr);
                    if !non_rollbackable_keys.is_empty() {
                        println!(
                            "Warning: broker {} has newly added keys [{}]; rollback can only restore existing keys.",
                            plan.broker_addr,
                            non_rollbackable_keys.join(", ")
                        );
                    }
                    applied_updates.push(AppliedBrokerUpdate {
                        broker_addr: plan.broker_addr.clone(),
                        rollback_properties,
                        non_rollbackable_keys,
                    });
                }
                Err(e) => {
                    let base_error = format!(
                        "UpdateBrokerConfigSubCommand: Failed to update broker {}: {}",
                        plan.broker_addr, e
                    );

                    if self.no_rollback {
                        return Err(RocketMQError::Internal(format!(
                            "{}. Automatic rollback is disabled, previous successful updates are retained.",
                            base_error
                        )));
                    }

                    let rollback_failures = rollback_applied_updates(admin_ext, &applied_updates).await;
                    if rollback_failures.is_empty() {
                        return Err(RocketMQError::Internal(format!(
                            "{}. Automatic rollback succeeded for {} previously updated broker(s).",
                            base_error,
                            applied_updates.len()
                        )));
                    }

                    return Err(RocketMQError::Internal(format!(
                        "{}. Rollback encountered issues: {}",
                        base_error,
                        rollback_failures.join("; ")
                    )));
                }
            }
        }

        Ok(())
    }
}

impl CommandExecute for UpdateBrokerConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let update_entries = self.parse_update_entries()?;

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
                    "UpdateBrokerConfigSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let targets = self.resolve_targets(&default_mqadmin_ext).await?;
            let plans = self
                .build_update_plans(&default_mqadmin_ext, &targets, &update_entries)
                .await?;

            print_update_plan(&plans);
            if self.dry_run {
                println!("Dry-run mode enabled, no broker config has been changed.");
                return Ok(());
            }

            if !self.yes && !prompt_confirmation() {
                println!("Aborted by user, no broker config has been changed.");
                return Ok(());
            }

            self.apply_update_plans(&default_mqadmin_ext, &plans).await?;

            let updated_broker_count = plans.iter().filter(|plan| !plan.changes.is_empty()).count();
            println!(
                "UpdateBrokerConfigSubCommand: Updated broker config on {} broker(s).",
                updated_broker_count
            );
            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
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

    validate_config_key(key)?;
    validate_update_value(key, value, None)?;

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

fn validate_config_key(key: &str) -> RocketMQResult<()> {
    if key.is_empty() {
        return Err(RocketMQError::IllegalArgument(
            "UpdateBrokerConfigSubCommand: Config key cannot be empty".to_string(),
        ));
    }
    if key.contains('=') {
        return Err(RocketMQError::IllegalArgument(format!(
            "UpdateBrokerConfigSubCommand: Invalid config key '{}', '=' is not allowed",
            key
        )));
    }
    if key.chars().any(char::is_whitespace) {
        return Err(RocketMQError::IllegalArgument(format!(
            "UpdateBrokerConfigSubCommand: Invalid config key '{}', whitespace is not allowed",
            key
        )));
    }
    Ok(())
}

fn validate_update_value(key: &str, new_value: &str, old_value: Option<&str>) -> RocketMQResult<()> {
    if new_value.trim().is_empty() {
        return Err(RocketMQError::IllegalArgument(format!(
            "UpdateBrokerConfigSubCommand: Config value for key '{}' cannot be empty",
            key
        )));
    }
    if new_value.contains('\n') || new_value.contains('\r') {
        return Err(RocketMQError::IllegalArgument(format!(
            "UpdateBrokerConfigSubCommand: Config value for key '{}' cannot contain line breaks",
            key
        )));
    }

    if let Some(old_value) = old_value {
        if parse_bool(old_value).is_some() && parse_bool(new_value).is_none() {
            return Err(RocketMQError::IllegalArgument(format!(
                "UpdateBrokerConfigSubCommand: Config key '{}' expects boolean value, old='{}', new='{}'",
                key, old_value, new_value
            )));
        } else if old_value.parse::<i64>().is_ok() && new_value.parse::<i64>().is_err() {
            return Err(RocketMQError::IllegalArgument(format!(
                "UpdateBrokerConfigSubCommand: Config key '{}' expects integer, old='{}', new='{}'",
                key, old_value, new_value
            )));
        } else if old_value.parse::<f64>().is_ok() && new_value.parse::<f64>().is_err() {
            return Err(RocketMQError::IllegalArgument(format!(
                "UpdateBrokerConfigSubCommand: Config key '{}' expects numeric value, old='{}', new='{}'",
                key, old_value, new_value
            )));
        }
    }

    Ok(())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn print_update_plan(plans: &[BrokerUpdatePlan]) {
    println!("Planned broker configuration changes:");
    for plan in plans {
        println!("============{}============", plan.broker_addr);
        if plan.changes.is_empty() {
            println!("(no effective changes)\n");
            continue;
        }

        for change in &plan.changes {
            let old_value = change.old_value.as_deref().unwrap_or("<missing>");
            println!("{:<50} {} -> {}", change.key, old_value, change.new_value);
        }
        println!();
    }
}

async fn fetch_broker_config_snapshot(
    admin_ext: &DefaultMQAdminExt,
    broker_addr: &str,
) -> RocketMQResult<HashMap<String, String>> {
    let config = admin_ext
        .get_broker_config(CheetahString::from(broker_addr))
        .await
        .map_err(|e| {
            RocketMQError::Internal(format!(
                "UpdateBrokerConfigSubCommand: Failed to get broker config for {}: {}",
                broker_addr, e
            ))
        })?;

    Ok(config
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect())
}

async fn rollback_applied_updates(
    admin_ext: &DefaultMQAdminExt,
    applied_updates: &[AppliedBrokerUpdate],
) -> Vec<String> {
    if applied_updates.is_empty() {
        return Vec::new();
    }

    println!(
        "Applying automatic rollback for {} previously updated broker(s)...",
        applied_updates.len()
    );

    let mut failures = Vec::new();
    for applied in applied_updates.iter().rev() {
        if applied.rollback_properties.is_empty() {
            if !applied.non_rollbackable_keys.is_empty() {
                failures.push(format!(
                    "broker {} has only newly added keys [{}], cannot rollback to non-existent state",
                    applied.broker_addr,
                    applied.non_rollbackable_keys.join(", ")
                ));
            }
            continue;
        }

        match admin_ext
            .update_broker_config(
                CheetahString::from(applied.broker_addr.as_str()),
                applied.rollback_properties.clone(),
            )
            .await
        {
            Ok(_) => {
                println!("Rolled back broker {} successfully.", applied.broker_addr);
                if !applied.non_rollbackable_keys.is_empty() {
                    failures.push(format!(
                        "broker {} has newly added keys [{}], removal is not supported by rollback",
                        applied.broker_addr,
                        applied.non_rollbackable_keys.join(", ")
                    ));
                }
            }
            Err(e) => {
                failures.push(format!("failed to rollback broker {}: {}", applied.broker_addr, e));
            }
        }
    }

    failures
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
    fn test_validate_update_value_boolean_compatibility() {
        assert!(validate_update_value("enableControllerMode", "true", Some("false")).is_ok());
        assert!(validate_update_value("enableControllerMode", "not_bool", Some("false")).is_err());
    }

    #[test]
    fn test_validate_update_value_numeric_compatibility() {
        assert!(validate_update_value("maxTransferCount", "1024", Some("512")).is_ok());
        assert!(validate_update_value("maxTransferCount", "abc", Some("512")).is_err());
    }
}
