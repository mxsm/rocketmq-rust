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

//! Broker operations - core business logic.

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use regex::Regex;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_remoting::runtime::RPCHook;

use super::types::validate_update_value;
use super::types::AppliedBrokerConfigUpdate;
use super::types::BrokerConfigEntry;
use super::types::BrokerConfigQueryRequest;
use super::types::BrokerConfigQueryResult;
use super::types::BrokerConfigSection;
use super::types::BrokerConfigSectionTarget;
use super::types::BrokerConfigUpdateApplyResult;
use super::types::BrokerConfigUpdatePlan;
use super::types::BrokerConfigUpdatePlanResult;
use super::types::BrokerConfigUpdateRequest;
use super::types::BrokerRuntimeStatsEntry;
use super::types::BrokerRuntimeStatsFailure;
use super::types::BrokerRuntimeStatsQueryRequest;
use super::types::BrokerRuntimeStatsResult;
use super::types::BrokerRuntimeStatsSection;
use super::types::BrokerTarget;
use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;

pub struct BrokerService;

impl BrokerService {
    pub async fn query_broker_config_by_request(
        request: BrokerConfigQueryRequest,
    ) -> RocketMQResult<BrokerConfigQueryResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_broker_config_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_runtime_stats_by_request(
        request: BrokerRuntimeStatsQueryRequest,
    ) -> RocketMQResult<BrokerRuntimeStatsResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_broker_runtime_stats_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_runtime_stats_by_request_with_rpc_hook(
        request: BrokerRuntimeStatsQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerRuntimeStatsResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_broker_runtime_stats_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_runtime_stats_with_admin(
        admin: &DefaultMQAdminExt,
        request: &BrokerRuntimeStatsQueryRequest,
    ) -> RocketMQResult<BrokerRuntimeStatsResult> {
        match request.target() {
            BrokerTarget::BrokerAddr(addr) => {
                let entries = Self::get_broker_runtime_stats_entries(admin, addr).await?;
                Ok(BrokerRuntimeStatsResult {
                    sections: vec![BrokerRuntimeStatsSection {
                        broker_addr: addr.clone(),
                        entries,
                    }],
                    failures: Vec::new(),
                })
            }
            BrokerTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
                })?;
                let mut broker_addrs =
                    BrokerAddressResolver::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?
                        .into_iter()
                        .filter(|addr| addr.as_str() != BrokerAddressResolver::NO_MASTER_PLACEHOLDER)
                        .collect::<Vec<_>>();
                broker_addrs.sort();
                broker_addrs.dedup();

                let mut sections = Vec::new();
                let mut failures = Vec::new();
                for broker_addr in broker_addrs {
                    match Self::get_broker_runtime_stats_entries(admin, &broker_addr).await {
                        Ok(entries) => sections.push(BrokerRuntimeStatsSection { broker_addr, entries }),
                        Err(error) => failures.push(BrokerRuntimeStatsFailure {
                            broker_addr,
                            error: error.to_string(),
                        }),
                    }
                }

                Ok(BrokerRuntimeStatsResult { sections, failures })
            }
        }
    }

    pub async fn query_broker_config_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &BrokerConfigQueryRequest,
    ) -> RocketMQResult<BrokerConfigQueryResult> {
        let key_pattern = request.key_pattern_regex()?;
        let mut sections = Vec::new();

        match request.target() {
            BrokerTarget::BrokerAddr(addr) => {
                let entries = Self::get_broker_config_entries(admin, addr, key_pattern.as_ref()).await?;
                sections.push(BrokerConfigSection {
                    target: BrokerConfigSectionTarget::Broker(addr.clone()),
                    entries,
                });
            }
            BrokerTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
                })?;
                let master_and_slave_map =
                    BrokerAddressResolver::fetch_master_and_slave_distinguish(&cluster_info, cluster_name.as_str())?;

                let mut sorted_masters: Vec<_> = master_and_slave_map.keys().cloned().collect();
                sorted_masters.sort();

                for master_addr in sorted_masters {
                    let slave_addrs = master_and_slave_map.get(&master_addr).cloned().unwrap_or_default();
                    if master_addr.as_str() == BrokerAddressResolver::NO_MASTER_PLACEHOLDER {
                        sections.push(BrokerConfigSection {
                            target: BrokerConfigSectionTarget::NoMaster,
                            entries: Vec::new(),
                        });
                    } else {
                        let entries =
                            Self::get_broker_config_entries(admin, &master_addr, key_pattern.as_ref()).await?;
                        sections.push(BrokerConfigSection {
                            target: BrokerConfigSectionTarget::Master(master_addr.clone()),
                            entries,
                        });
                    }

                    let mut sorted_slaves = slave_addrs;
                    sorted_slaves.sort();
                    for slave_addr in sorted_slaves {
                        let entries = Self::get_broker_config_entries(admin, &slave_addr, key_pattern.as_ref()).await?;
                        sections.push(BrokerConfigSection {
                            target: BrokerConfigSectionTarget::Slave {
                                master_addr: master_addr.clone(),
                                slave_addr,
                            },
                            entries,
                        });
                    }
                }
            }
        }

        Ok(BrokerConfigQueryResult {
            sections,
            key_pattern: request.key_pattern().map(ToOwned::to_owned),
        })
    }

    pub async fn build_broker_config_update_plan_by_request(
        request: BrokerConfigUpdateRequest,
    ) -> RocketMQResult<BrokerConfigUpdatePlanResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::build_broker_config_update_plan_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn build_broker_config_update_plan_by_request_with_rpc_hook(
        request: BrokerConfigUpdateRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerConfigUpdatePlanResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::build_broker_config_update_plan_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn build_broker_config_update_plan_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &BrokerConfigUpdateRequest,
    ) -> RocketMQResult<BrokerConfigUpdatePlanResult> {
        let targets = Self::resolve_update_targets(admin, request).await?;
        let configs =
            futures::future::try_join_all(targets.iter().map(|target| fetch_broker_config_snapshot(admin, target)))
                .await?;

        let plans = targets
            .into_iter()
            .zip(configs)
            .map(|(broker_addr, current)| build_update_plan_for_snapshot(broker_addr, current, request))
            .collect::<RocketMQResult<Vec<_>>>()?;

        Ok(BrokerConfigUpdatePlanResult { plans })
    }

    pub async fn apply_broker_config_update_by_request(
        request: BrokerConfigUpdateRequest,
    ) -> RocketMQResult<BrokerConfigUpdateApplyResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = async {
            let plan = Self::build_broker_config_update_plan_with_admin(&mut admin, &request).await?;
            Self::apply_broker_config_update_plan_with_admin(&admin, &plan, request.rollback_enabled()).await
        }
        .await;
        admin.shutdown().await;
        result
    }

    pub async fn apply_broker_config_update_plan_by_request(
        request: &BrokerConfigUpdateRequest,
        plan_result: &BrokerConfigUpdatePlanResult,
    ) -> RocketMQResult<BrokerConfigUpdateApplyResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result =
            Self::apply_broker_config_update_plan_with_admin(&admin, plan_result, request.rollback_enabled()).await;
        admin.shutdown().await;
        result
    }

    pub async fn apply_broker_config_update_plan_by_request_with_rpc_hook(
        request: &BrokerConfigUpdateRequest,
        plan_result: &BrokerConfigUpdatePlanResult,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerConfigUpdateApplyResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result =
            Self::apply_broker_config_update_plan_with_admin(&admin, plan_result, request.rollback_enabled()).await;
        admin.shutdown().await;
        result
    }

    pub async fn apply_broker_config_update_plan_with_admin(
        admin: &DefaultMQAdminExt,
        plan_result: &BrokerConfigUpdatePlanResult,
        rollback_enabled: bool,
    ) -> RocketMQResult<BrokerConfigUpdateApplyResult> {
        let mut applied_updates = Vec::new();
        let mut skipped_brokers = Vec::new();

        for plan in &plan_result.plans {
            if plan.changes.is_empty() {
                skipped_brokers.push(plan.broker_addr.clone());
                continue;
            }

            let rollback_properties = plan.rollback_properties();
            let non_rollbackable_keys = plan.non_rollbackable_keys();
            match admin
                .update_broker_config(plan.broker_addr.clone(), plan.update_properties())
                .await
            {
                Ok(_) => applied_updates.push(AppliedBrokerConfigUpdate {
                    broker_addr: plan.broker_addr.clone(),
                    rollback_properties,
                    non_rollbackable_keys,
                }),
                Err(error) => {
                    let base_error = format!("BrokerService: failed to update broker {}: {}", plan.broker_addr, error);

                    if !rollback_enabled {
                        return Err(RocketMQError::Internal(format!(
                            "{}. Automatic rollback is disabled, previous successful updates are retained.",
                            base_error
                        )));
                    }

                    let rollback_failures = rollback_applied_updates(admin, &applied_updates).await;
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

        Ok(BrokerConfigUpdateApplyResult {
            applied_updates,
            skipped_brokers,
        })
    }

    async fn resolve_update_targets(
        admin: &DefaultMQAdminExt,
        request: &BrokerConfigUpdateRequest,
    ) -> RocketMQResult<Vec<CheetahString>> {
        match request.target() {
            BrokerTarget::BrokerAddr(addr) => Ok(vec![addr.clone()]),
            BrokerTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
                })?;

                let mut broker_addrs =
                    BrokerAddressResolver::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?
                        .into_iter()
                        .filter(|addr| addr.as_str() != BrokerAddressResolver::NO_MASTER_PLACEHOLDER)
                        .collect::<Vec<_>>();
                broker_addrs.sort();
                broker_addrs.dedup();

                if broker_addrs.is_empty() {
                    return Err(RocketMQError::Internal(format!(
                        "BrokerService: cluster {} has no broker address",
                        cluster_name
                    )));
                }

                Ok(broker_addrs)
            }
        }
    }

    async fn get_broker_config_entries(
        admin: &DefaultMQAdminExt,
        broker_addr: &CheetahString,
        key_pattern: Option<&Regex>,
    ) -> RocketMQResult<Vec<BrokerConfigEntry>> {
        let properties = admin.get_broker_config(broker_addr.clone()).await.map_err(|error| {
            RocketMQError::Internal(format!(
                "BrokerService: failed to get broker config for {}: {}",
                broker_addr, error
            ))
        })?;

        Ok(filter_and_sort_properties(properties, key_pattern))
    }

    async fn get_broker_runtime_stats_entries(
        admin: &DefaultMQAdminExt,
        broker_addr: &CheetahString,
    ) -> RocketMQResult<Vec<BrokerRuntimeStatsEntry>> {
        let kv_table = admin
            .fetch_broker_runtime_stats(broker_addr.clone())
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "BrokerService: failed to fetch broker runtime stats from {}: {}",
                    broker_addr, error
                ))
            })?;

        Ok(sort_runtime_stats_entries(kv_table.table))
    }
}

fn filter_and_sort_properties(
    properties: HashMap<CheetahString, CheetahString>,
    key_pattern: Option<&Regex>,
) -> Vec<BrokerConfigEntry> {
    let mut entries = properties
        .into_iter()
        .filter(|(key, _)| key_pattern.is_none_or(|regex| regex.is_match(key.as_str())))
        .map(|(key, value)| BrokerConfigEntry { key, value })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

fn sort_runtime_stats_entries(properties: HashMap<CheetahString, CheetahString>) -> Vec<BrokerRuntimeStatsEntry> {
    let mut entries = properties
        .into_iter()
        .map(|(key, value)| BrokerRuntimeStatsEntry { key, value })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

async fn fetch_broker_config_snapshot(
    admin: &DefaultMQAdminExt,
    broker_addr: &CheetahString,
) -> RocketMQResult<HashMap<CheetahString, CheetahString>> {
    admin.get_broker_config(broker_addr.clone()).await.map_err(|error| {
        RocketMQError::Internal(format!(
            "BrokerService: failed to get broker config for {}: {}",
            broker_addr, error
        ))
    })
}

fn build_update_plan_for_snapshot(
    broker_addr: CheetahString,
    current: HashMap<CheetahString, CheetahString>,
    request: &BrokerConfigUpdateRequest,
) -> RocketMQResult<BrokerConfigUpdatePlan> {
    let mut changes = Vec::new();
    for (key, new_value) in request.update_entries() {
        let old_value = current.get(key).cloned();
        validate_update_value(
            key.as_str(),
            new_value.as_str(),
            old_value.as_ref().map(|value| value.as_str()),
        )
        .map_err(|error| RocketMQError::IllegalArgument(format!("Broker {}: {}", broker_addr, error)))?;
        if old_value.as_ref().map(|value| value.as_str()) != Some(new_value.as_str()) {
            changes.push(super::types::BrokerConfigChange {
                key: key.clone(),
                old_value,
                new_value: new_value.clone(),
            });
        }
    }

    Ok(BrokerConfigUpdatePlan { broker_addr, changes })
}

async fn rollback_applied_updates(
    admin: &DefaultMQAdminExt,
    applied_updates: &[AppliedBrokerConfigUpdate],
) -> Vec<String> {
    let mut failures = Vec::new();
    for applied in applied_updates.iter().rev() {
        if applied.rollback_properties.is_empty() {
            if !applied.non_rollbackable_keys.is_empty() {
                failures.push(format!(
                    "broker {} has only newly added keys [{}], cannot rollback to non-existent state",
                    applied.broker_addr,
                    join_cheetah_strings(&applied.non_rollbackable_keys)
                ));
            }
            continue;
        }

        match admin
            .update_broker_config(applied.broker_addr.clone(), applied.rollback_properties.clone())
            .await
        {
            Ok(_) => {
                if !applied.non_rollbackable_keys.is_empty() {
                    failures.push(format!(
                        "broker {} has newly added keys [{}], removal is not supported by rollback",
                        applied.broker_addr,
                        join_cheetah_strings(&applied.non_rollbackable_keys)
                    ));
                }
            }
            Err(error) => {
                failures.push(format!("failed to rollback broker {}: {}", applied.broker_addr, error));
            }
        }
    }

    failures
}

fn join_cheetah_strings(values: &[CheetahString]) -> String {
    values.iter().map(|value| value.as_str()).collect::<Vec<_>>().join(", ")
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_and_sort_properties_applies_key_pattern() {
        let mut properties = HashMap::new();
        properties.insert(CheetahString::from("brokerRole"), CheetahString::from("ASYNC_MASTER"));
        properties.insert(CheetahString::from("flushDiskType"), CheetahString::from("ASYNC_FLUSH"));
        properties.insert(CheetahString::from("flushInterval"), CheetahString::from("500"));

        let pattern = Regex::new("^flush").unwrap();
        let entries = filter_and_sort_properties(properties, Some(&pattern));

        assert_eq!(
            entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(),
            vec!["flushDiskType", "flushInterval"]
        );
    }

    #[test]
    fn build_update_plan_for_snapshot_tracks_old_and_missing_values() {
        let mut update_entries = std::collections::BTreeMap::new();
        update_entries.insert("flushDiskType".to_string(), "SYNC_FLUSH".to_string());
        update_entries.insert("maxTransferCount".to_string(), "1024".to_string());
        update_entries.insert("newKey".to_string(), "newValue".to_string());

        let request = BrokerConfigUpdateRequest::try_new(Some("127.0.0.1:10911".into()), None, update_entries).unwrap();
        let mut current = HashMap::new();
        current.insert(CheetahString::from("flushDiskType"), CheetahString::from("SYNC_FLUSH"));
        current.insert(CheetahString::from("maxTransferCount"), CheetahString::from("512"));

        let plan = build_update_plan_for_snapshot(CheetahString::from("127.0.0.1:10911"), current, &request).unwrap();

        assert_eq!(plan.broker_addr.as_str(), "127.0.0.1:10911");
        assert_eq!(plan.changes.len(), 2);
        assert!(plan.changes.iter().any(|change| {
            change.key.as_str() == "maxTransferCount"
                && change.old_value.as_ref().map(|value| value.as_str()) == Some("512")
                && change.new_value.as_str() == "1024"
        }));
        assert!(plan.changes.iter().any(|change| {
            change.key.as_str() == "newKey" && change.old_value.is_none() && change.new_value.as_str() == "newValue"
        }));

        let rollback = plan.rollback_properties();
        assert_eq!(rollback.get(&CheetahString::from("maxTransferCount")).unwrap(), "512");
        assert_eq!(plan.non_rollbackable_keys(), vec![CheetahString::from("newKey")]);
    }

    #[test]
    fn build_update_plan_for_snapshot_validates_value_compatibility() {
        let mut update_entries = std::collections::BTreeMap::new();
        update_entries.insert("maxTransferCount".to_string(), "not_numeric".to_string());
        let request = BrokerConfigUpdateRequest::try_new(Some("127.0.0.1:10911".into()), None, update_entries).unwrap();
        let mut current = HashMap::new();
        current.insert(CheetahString::from("maxTransferCount"), CheetahString::from("512"));

        let result = build_update_plan_for_snapshot(CheetahString::from("127.0.0.1:10911"), current, &request);

        assert!(result.is_err());
    }

    #[test]
    fn sort_runtime_stats_entries_orders_by_key() {
        let mut table = HashMap::new();
        table.insert(CheetahString::from("putTps"), CheetahString::from("1.0"));
        table.insert(CheetahString::from("brokerVersion"), CheetahString::from("5.0"));

        let entries = sort_runtime_stats_entries(table);

        assert_eq!(
            entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(),
            vec!["brokerVersion", "putTps"]
        );
    }
}
