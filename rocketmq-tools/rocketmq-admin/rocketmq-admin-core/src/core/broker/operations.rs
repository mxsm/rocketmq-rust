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
use rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_remoting::runtime::RPCHook;

use super::types::validate_update_value;
use super::types::AppliedBrokerConfigUpdate;
use super::types::BrokerBooleanOperationResult;
use super::types::BrokerConfigEntry;
use super::types::BrokerConfigQueryRequest;
use super::types::BrokerConfigQueryResult;
use super::types::BrokerConfigSection;
use super::types::BrokerConfigSectionTarget;
use super::types::BrokerConfigUpdateApplyResult;
use super::types::BrokerConfigUpdatePlan;
use super::types::BrokerConfigUpdatePlanResult;
use super::types::BrokerConfigUpdateRequest;
use super::types::BrokerConsumeStatsQueryRequest;
use super::types::BrokerConsumeStatsResult;
use super::types::BrokerConsumeStatsRow;
use super::types::BrokerEpochEntry;
use super::types::BrokerEpochQueryRequest;
use super::types::BrokerEpochQueryResult;
use super::types::BrokerEpochQueryTarget;
use super::types::BrokerEpochSection;
use super::types::BrokerOperationFailure;
use super::types::BrokerOperationResult;
use super::types::BrokerOptionalTarget;
use super::types::BrokerRuntimeStatsEntry;
use super::types::BrokerRuntimeStatsFailure;
use super::types::BrokerRuntimeStatsQueryRequest;
use super::types::BrokerRuntimeStatsResult;
use super::types::BrokerRuntimeStatsSection;
use super::types::BrokerTarget;
use super::types::CleanExpiredConsumeQueueReport;
use super::types::CleanExpiredConsumeQueueRequest;
use super::types::CleanExpiredConsumeQueueTargetResult;
use super::types::ColdDataFlowCtrGroupConfigRemoveRequest;
use super::types::ColdDataFlowCtrGroupConfigUpdateRequest;
use super::types::ColdDataFlowCtrInfoQueryRequest;
use super::types::ColdDataFlowCtrInfoQueryResult;
use super::types::ColdDataFlowCtrInfoSection;
use super::types::CommitLogReadAheadRequest;
use super::types::CommitLogReadAheadResult;
use super::types::CommitLogReadAheadSection;
use super::types::ResetMasterFlushOffsetRequest;
use super::types::SwitchTimerEngineRequest;
use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;

pub struct BrokerService;

const COMMIT_LOG_READ_AHEAD_MODE_KEY: &str = "commitLogReadAheadMode";
const DATA_READ_AHEAD_ENABLE_KEY: &str = "dataReadAheadEnable";
const DEFAULT_READ_AHEAD_SIZE_KEY: &str = "dataReadAheadSize";
const KNOWN_READ_AHEAD_SIZE_KEYS: [&str; 3] = [DEFAULT_READ_AHEAD_SIZE_KEY, "commitLogReadAheadSize", "readAheadSize"];

impl BrokerService {
    pub async fn query_broker_config_by_request(
        request: BrokerConfigQueryRequest,
    ) -> RocketMQResult<BrokerConfigQueryResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_broker_config_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn delete_expired_commit_log_by_request_with_rpc_hook(
        request: BrokerOptionalTarget,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerBooleanOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = admin
            .delete_expired_commit_log(request.cluster_name().cloned(), request.broker_addr().cloned())
            .await
            .map(|success| BrokerBooleanOperationResult { success })
            .map_err(|error| {
                RocketMQError::Internal(format!("BrokerService: failed to delete expired commit log: {error}"))
            });
        admin.shutdown().await;
        result
    }

    pub async fn clean_unused_topic_by_request_with_rpc_hook(
        request: BrokerOptionalTarget,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerBooleanOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = admin
            .clean_unused_topic(request.cluster_name().cloned(), request.broker_addr().cloned())
            .await
            .map(|success| BrokerBooleanOperationResult { success })
            .map_err(|error| RocketMQError::Internal(format!("BrokerService: failed to clean unused topic: {error}")));
        admin.shutdown().await;
        result
    }

    pub async fn reset_master_flush_offset_by_request_with_rpc_hook(
        request: ResetMasterFlushOffsetRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = admin
            .reset_master_flush_offset(request.broker_addr().clone(), request.master_flush_offset())
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "BrokerService: failed to reset master flush offset for {}: {}",
                    request.broker_addr(),
                    error
                ))
            });
        admin.shutdown().await;
        result
    }

    pub async fn switch_timer_engine_by_request_with_rpc_hook(
        request: SwitchTimerEngineRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::switch_timer_engine_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn switch_timer_engine_with_admin(
        admin: &DefaultMQAdminExt,
        request: &SwitchTimerEngineRequest,
    ) -> RocketMQResult<BrokerOperationResult> {
        let broker_addrs = Self::resolve_master_targets(admin, request.target()).await?;
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for broker_addr in broker_addrs {
            match admin
                .switch_timer_engine(broker_addr.clone(), request.engine_type().clone())
                .await
            {
                Ok(()) => successes.push(broker_addr),
                Err(error) => failures.push(BrokerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }

        Ok(BrokerOperationResult {
            broker_addrs: successes,
            failures,
        })
    }

    pub async fn update_cold_data_flow_ctr_group_config_by_request_with_rpc_hook(
        request: ColdDataFlowCtrGroupConfigUpdateRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::update_cold_data_flow_ctr_group_config_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn update_cold_data_flow_ctr_group_config_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ColdDataFlowCtrGroupConfigUpdateRequest,
    ) -> RocketMQResult<BrokerOperationResult> {
        let broker_addrs = Self::resolve_master_targets(admin, request.target()).await?;
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for broker_addr in broker_addrs {
            let mut properties = HashMap::new();
            properties.insert(request.consumer_group().clone(), request.threshold().clone());
            match admin
                .update_cold_data_flow_ctr_group_config(broker_addr.clone(), properties)
                .await
            {
                Ok(()) => successes.push(broker_addr),
                Err(error) => failures.push(BrokerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }

        Ok(BrokerOperationResult {
            broker_addrs: successes,
            failures,
        })
    }

    pub async fn remove_cold_data_flow_ctr_group_config_by_request_with_rpc_hook(
        request: ColdDataFlowCtrGroupConfigRemoveRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerOperationResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::remove_cold_data_flow_ctr_group_config_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn remove_cold_data_flow_ctr_group_config_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ColdDataFlowCtrGroupConfigRemoveRequest,
    ) -> RocketMQResult<BrokerOperationResult> {
        let broker_addrs = Self::resolve_master_targets(admin, request.target()).await?;
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for broker_addr in broker_addrs {
            match admin
                .remove_cold_data_flow_ctr_group_config(broker_addr.clone(), request.consumer_group().clone())
                .await
            {
                Ok(()) => successes.push(broker_addr),
                Err(error) => failures.push(BrokerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }

        Ok(BrokerOperationResult {
            broker_addrs: successes,
            failures,
        })
    }

    pub async fn query_cold_data_flow_ctr_info_by_request_with_rpc_hook(
        request: ColdDataFlowCtrInfoQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<ColdDataFlowCtrInfoQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_cold_data_flow_ctr_info_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_cold_data_flow_ctr_info_with_admin(
        admin: &DefaultMQAdminExt,
        request: &ColdDataFlowCtrInfoQueryRequest,
    ) -> RocketMQResult<ColdDataFlowCtrInfoQueryResult> {
        let mut sections = Vec::new();

        match request.target() {
            BrokerTarget::BrokerAddr(addr) => {
                sections.push(
                    Self::get_cold_data_flow_ctr_info_section(
                        admin,
                        BrokerConfigSectionTarget::Broker(addr.clone()),
                        addr.clone(),
                    )
                    .await?,
                );
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
                    if master_addr.as_str() != BrokerAddressResolver::NO_MASTER_PLACEHOLDER {
                        sections.push(
                            Self::get_cold_data_flow_ctr_info_section(
                                admin,
                                BrokerConfigSectionTarget::Master(master_addr.clone()),
                                master_addr.clone(),
                            )
                            .await?,
                        );
                    }

                    let mut sorted_slaves = slave_addrs;
                    sorted_slaves.sort();
                    for slave_addr in sorted_slaves {
                        sections.push(
                            Self::get_cold_data_flow_ctr_info_section(
                                admin,
                                BrokerConfigSectionTarget::Slave {
                                    master_addr: master_addr.clone(),
                                    slave_addr: slave_addr.clone(),
                                },
                                slave_addr,
                            )
                            .await?,
                        );
                    }
                }
            }
        }

        Ok(ColdDataFlowCtrInfoQueryResult { sections })
    }

    pub async fn query_broker_epoch_by_request_with_rpc_hook(
        request: BrokerEpochQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerEpochQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_broker_epoch_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_epoch_with_admin(
        admin: &DefaultMQAdminExt,
        request: &BrokerEpochQueryRequest,
    ) -> RocketMQResult<BrokerEpochQueryResult> {
        let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
            RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
        })?;
        let mut broker_addrs = match request.target() {
            BrokerEpochQueryTarget::BrokerName(broker_name) => {
                BrokerAddressResolver::fetch_master_and_slave_addr_by_broker_name(&cluster_info, broker_name.as_str())?
            }
            BrokerEpochQueryTarget::ClusterName(cluster_name) => {
                BrokerAddressResolver::fetch_master_and_slave_addr_by_cluster_name(
                    &cluster_info,
                    cluster_name.as_str(),
                )?
            }
        };
        broker_addrs.sort();
        broker_addrs.dedup();

        let mut sections = Vec::new();
        for broker_addr in broker_addrs {
            sections.push(Self::get_broker_epoch_section(admin, broker_addr).await?);
        }

        Ok(BrokerEpochQueryResult { sections })
    }

    pub async fn clean_expired_consume_queue_by_request_with_rpc_hook(
        request: CleanExpiredConsumeQueueRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<CleanExpiredConsumeQueueReport> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::clean_expired_consume_queue_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn clean_expired_consume_queue_with_admin(
        admin: &DefaultMQAdminExt,
        request: &CleanExpiredConsumeQueueRequest,
    ) -> RocketMQResult<CleanExpiredConsumeQueueReport> {
        let targets = if request.is_global_mode() {
            Self::fetch_all_broker_targets(admin).await?
        } else {
            Self::resolve_clean_expired_cq_targets(admin, request).await?
        };

        let mut report = CleanExpiredConsumeQueueReport {
            scanned_brokers: targets.len(),
            targets,
            target_results: Vec::new(),
            dry_run: request.dry_run(),
            is_global_mode: request.is_global_mode(),
            cleanup_invocations: 0,
            cleanup_successes: 0,
            cleanup_false_results: 0,
            failures: Vec::new(),
        };

        if request.dry_run() {
            return Ok(report);
        }

        if request.is_global_mode() {
            report.cleanup_invocations = 1;
            match admin.clean_expired_consumer_queue(None, None).await {
                Ok(success) => {
                    report.target_results.push(CleanExpiredConsumeQueueTargetResult {
                        broker_addr: CheetahString::from("global"),
                        success,
                    });
                    if success {
                        report.cleanup_successes = 1;
                    } else {
                        report.cleanup_false_results = 1;
                    }
                }
                Err(error) => report.failures.push(BrokerOperationFailure {
                    broker_addr: CheetahString::from("global"),
                    error: error.to_string(),
                }),
            }
            return Ok(report);
        }

        if report.targets.is_empty() {
            return Err(RocketMQError::Internal(
                "BrokerService: no broker targets matched the cleanExpiredCQ scope".to_string(),
            ));
        }

        for target in report.targets.clone() {
            report.cleanup_invocations += 1;
            match admin.clean_expired_consumer_queue(None, Some(target.clone())).await {
                Ok(success) => {
                    report.target_results.push(CleanExpiredConsumeQueueTargetResult {
                        broker_addr: target,
                        success,
                    });
                    if success {
                        report.cleanup_successes += 1;
                    } else {
                        report.cleanup_false_results += 1;
                    }
                }
                Err(error) => report.failures.push(BrokerOperationFailure {
                    broker_addr: target,
                    error: error.to_string(),
                }),
            }
        }

        Ok(report)
    }

    pub async fn set_commit_log_read_ahead_by_request_with_rpc_hook(
        request: CommitLogReadAheadRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<CommitLogReadAheadResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::set_commit_log_read_ahead_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn set_commit_log_read_ahead_with_admin(
        admin: &DefaultMQAdminExt,
        request: &CommitLogReadAheadRequest,
    ) -> RocketMQResult<CommitLogReadAheadResult> {
        let targets = Self::resolve_commit_log_read_ahead_targets(admin, request).await?;
        let mut sections = Vec::new();
        let mut failures = Vec::new();

        for (target, broker_addr) in targets {
            match Self::apply_commit_log_read_ahead_to_target(admin, request, target, broker_addr.clone()).await {
                Ok(section) => sections.push(section),
                Err(error) => failures.push(BrokerOperationFailure {
                    broker_addr,
                    error: error.to_string(),
                }),
            }
        }

        Ok(CommitLogReadAheadResult { sections, failures })
    }

    pub async fn query_broker_runtime_stats_by_request(
        request: BrokerRuntimeStatsQueryRequest,
    ) -> RocketMQResult<BrokerRuntimeStatsResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_broker_runtime_stats_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_consume_stats_by_request(
        request: BrokerConsumeStatsQueryRequest,
    ) -> RocketMQResult<BrokerConsumeStatsResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_broker_consume_stats_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_consume_stats_by_request_with_rpc_hook(
        request: BrokerConsumeStatsQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<BrokerConsumeStatsResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_broker_consume_stats_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_consume_stats_with_admin(
        admin: &DefaultMQAdminExt,
        request: &BrokerConsumeStatsQueryRequest,
    ) -> RocketMQResult<BrokerConsumeStatsResult> {
        let consume_stats_list = admin
            .fetch_consume_stats_in_broker(
                request.broker_addr().clone(),
                request.is_order(),
                request.timeout_millis(),
            )
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "BrokerService: failed to fetch consume stats from {}: {}",
                    request.broker_addr(),
                    error
                ))
            })?;

        Ok(build_broker_consume_stats_result(
            consume_stats_list,
            request.diff_level(),
        ))
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

    async fn resolve_master_targets(
        admin: &DefaultMQAdminExt,
        target: &BrokerTarget,
    ) -> RocketMQResult<Vec<CheetahString>> {
        match target {
            BrokerTarget::BrokerAddr(addr) => Ok(vec![addr.clone()]),
            BrokerTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
                })?;
                let mut broker_addrs =
                    BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.as_str())?;
                broker_addrs.sort();
                broker_addrs.dedup();
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

    async fn get_cold_data_flow_ctr_info_section(
        admin: &DefaultMQAdminExt,
        target: BrokerConfigSectionTarget,
        broker_addr: CheetahString,
    ) -> RocketMQResult<ColdDataFlowCtrInfoSection> {
        let raw_info = admin
            .get_cold_data_flow_ctr_info(broker_addr.clone())
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "BrokerService: failed to get cold data flow ctr info from {}: {}",
                    broker_addr, error
                ))
            })?;

        Ok(ColdDataFlowCtrInfoSection {
            target,
            broker_addr,
            raw_info,
        })
    }

    async fn get_broker_epoch_section(
        admin: &DefaultMQAdminExt,
        broker_addr: CheetahString,
    ) -> RocketMQResult<BrokerEpochSection> {
        let mut epoch_cache = admin
            .get_broker_epoch_cache(broker_addr.clone())
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "BrokerService: failed to get broker epoch cache from {}: {}",
                    broker_addr, error
                ))
            })?;

        let max_offset = epoch_cache.get_max_offset();
        let epoch_list = epoch_cache.get_epoch_list_mut();
        let len = epoch_list.len();
        let mut epochs = Vec::with_capacity(len);
        for (index, epoch_entry) in epoch_list.iter_mut().enumerate() {
            if index == len.saturating_sub(1) {
                epoch_entry.set_end_offset(max_offset as i64);
            }
            epochs.push(BrokerEpochEntry {
                epoch: epoch_entry.get_epoch(),
                start_offset: epoch_entry.get_start_offset(),
                end_offset: epoch_entry.get_end_offset(),
            });
        }

        Ok(BrokerEpochSection {
            cluster_name: epoch_cache.get_cluster_name().clone(),
            broker_name: epoch_cache.get_broker_name().clone(),
            broker_addr,
            broker_id: epoch_cache.get_broker_id(),
            epochs,
        })
    }

    async fn resolve_clean_expired_cq_targets(
        admin: &DefaultMQAdminExt,
        request: &CleanExpiredConsumeQueueRequest,
    ) -> RocketMQResult<Vec<CheetahString>> {
        if let Some(broker_addr) = request.broker_addr() {
            if let Some(topic) = request.topic() {
                let topic_targets = Self::fetch_topic_broker_targets(admin, topic).await?;
                if !topic_targets.iter().any(|target| target == broker_addr) {
                    return Err(RocketMQError::Internal(format!(
                        "BrokerService: broker {} is not found in route info of topic {}",
                        broker_addr, topic
                    )));
                }
            }
            return Ok(vec![broker_addr.clone()]);
        }

        let cluster_targets = if let Some(cluster_name) = request.cluster_name() {
            let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
            })?;
            Some(BrokerAddressResolver::fetch_master_and_slave_addr_by_cluster_name(
                &cluster_info,
                cluster_name.as_str(),
            )?)
        } else {
            None
        };

        let topic_targets = if let Some(topic) = request.topic() {
            Some(Self::fetch_topic_broker_targets(admin, topic).await?)
        } else {
            None
        };

        let mut targets = match (cluster_targets, topic_targets) {
            (Some(mut cluster_targets), Some(topic_targets)) => {
                cluster_targets.retain(|addr| topic_targets.iter().any(|topic_addr| topic_addr == addr));
                cluster_targets
            }
            (Some(cluster_targets), None) => cluster_targets,
            (None, Some(topic_targets)) => topic_targets,
            (None, None) => Vec::new(),
        };
        targets.sort();
        targets.dedup();
        Ok(targets)
    }

    async fn fetch_all_broker_targets(admin: &DefaultMQAdminExt) -> RocketMQResult<Vec<CheetahString>> {
        let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
            RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
        })?;
        let mut targets = Vec::new();
        if let Some(broker_addr_table) = cluster_info.broker_addr_table {
            for broker_data in broker_addr_table.values() {
                targets.extend(broker_data.broker_addrs().values().cloned());
            }
        }
        targets.sort();
        targets.dedup();
        Ok(targets)
    }

    async fn fetch_topic_broker_targets(
        admin: &DefaultMQAdminExt,
        topic: &CheetahString,
    ) -> RocketMQResult<Vec<CheetahString>> {
        let route_data = admin
            .examine_topic_route_info(topic.clone())
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "BrokerService: failed to examine topic route info for {}: {}",
                    topic, error
                ))
            })?
            .ok_or_else(|| {
                RocketMQError::Internal(format!("BrokerService: topic {} route info is unavailable", topic))
            })?;

        let mut targets = Vec::new();
        for broker_data in route_data.broker_datas {
            targets.extend(broker_data.broker_addrs().values().cloned());
        }
        targets.sort();
        targets.dedup();
        if targets.is_empty() {
            return Err(RocketMQError::Internal(format!(
                "BrokerService: topic {} has no broker targets",
                topic
            )));
        }
        Ok(targets)
    }

    async fn resolve_commit_log_read_ahead_targets(
        admin: &DefaultMQAdminExt,
        request: &CommitLogReadAheadRequest,
    ) -> RocketMQResult<Vec<(BrokerConfigSectionTarget, CheetahString)>> {
        match request.target() {
            BrokerTarget::BrokerAddr(addr) => Ok(vec![(BrokerConfigSectionTarget::Broker(addr.clone()), addr.clone())]),
            BrokerTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
                })?;
                let master_and_slave_map =
                    BrokerAddressResolver::fetch_master_and_slave_distinguish(&cluster_info, cluster_name.as_str())?;
                let mut sorted_masters: Vec<_> = master_and_slave_map.keys().cloned().collect();
                sorted_masters.sort();

                let mut targets = Vec::new();
                for master_addr in sorted_masters {
                    let slave_addrs = master_and_slave_map.get(&master_addr).cloned().unwrap_or_default();
                    if master_addr.as_str() != BrokerAddressResolver::NO_MASTER_PLACEHOLDER {
                        targets.push((
                            BrokerConfigSectionTarget::Master(master_addr.clone()),
                            master_addr.clone(),
                        ));
                    }
                    let mut sorted_slaves = slave_addrs;
                    sorted_slaves.sort();
                    for slave_addr in sorted_slaves {
                        targets.push((
                            BrokerConfigSectionTarget::Slave {
                                master_addr: master_addr.clone(),
                                slave_addr: slave_addr.clone(),
                            },
                            slave_addr,
                        ));
                    }
                }

                if targets.is_empty() {
                    return Err(RocketMQError::Internal(format!(
                        "BrokerService: cluster {} has no broker address",
                        cluster_name
                    )));
                }
                Ok(targets)
            }
        }
    }

    async fn apply_commit_log_read_ahead_to_target(
        admin: &DefaultMQAdminExt,
        request: &CommitLogReadAheadRequest,
        target: BrokerConfigSectionTarget,
        broker_addr: CheetahString,
    ) -> RocketMQResult<CommitLogReadAheadSection> {
        let current_config = admin.get_broker_config(broker_addr.clone()).await.map_err(|error| {
            RocketMQError::Internal(format!(
                "BrokerService: failed to get broker config for {}: {}",
                broker_addr, error
            ))
        })?;
        let size_key_for_update = resolve_read_ahead_size_key(request, &current_config)?;

        if request.show_only() || !request.has_updates() {
            return Ok(CommitLogReadAheadSection {
                target,
                broker_addr,
                current_config,
                updated_config: None,
                size_key_for_update,
                applied: false,
            });
        }

        let mut properties = HashMap::new();
        if let Some(mode) = request.mode() {
            properties.insert(
                CheetahString::from_static_str(COMMIT_LOG_READ_AHEAD_MODE_KEY),
                CheetahString::from_static_str(mode.config_value()),
            );
            properties.insert(
                CheetahString::from_static_str(DATA_READ_AHEAD_ENABLE_KEY),
                CheetahString::from(mode.read_ahead_enabled().to_string()),
            );
        }
        if let Some(read_ahead_size) = request.read_ahead_size() {
            let size_key = size_key_for_update
                .as_ref()
                .cloned()
                .unwrap_or_else(|| CheetahString::from_static_str(DEFAULT_READ_AHEAD_SIZE_KEY));
            properties.insert(size_key, CheetahString::from(read_ahead_size.to_string()));
        }

        admin
            .update_broker_config(broker_addr.clone(), properties)
            .await
            .map_err(|error| {
                RocketMQError::Internal(format!(
                    "BrokerService: failed to update broker {}: {}",
                    broker_addr, error
                ))
            })?;
        let updated_config = admin.get_broker_config(broker_addr.clone()).await.map_err(|error| {
            RocketMQError::Internal(format!(
                "BrokerService: failed to fetch updated broker config for {}: {}",
                broker_addr, error
            ))
        })?;

        Ok(CommitLogReadAheadSection {
            target,
            broker_addr,
            current_config,
            updated_config: Some(updated_config),
            size_key_for_update,
            applied: true,
        })
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

fn resolve_read_ahead_size_key(
    request: &CommitLogReadAheadRequest,
    config: &HashMap<CheetahString, CheetahString>,
) -> RocketMQResult<Option<CheetahString>> {
    if let Some(size_key) = request.read_ahead_size_key() {
        return Ok(Some(size_key.clone()));
    }
    if request.read_ahead_size().is_none() {
        return Ok(None);
    }
    for candidate in KNOWN_READ_AHEAD_SIZE_KEYS {
        if config.keys().any(|key| key.as_str() == candidate) {
            return Ok(Some(CheetahString::from(candidate)));
        }
    }
    Ok(Some(CheetahString::from_static_str(DEFAULT_READ_AHEAD_SIZE_KEY)))
}

fn build_broker_consume_stats_result(source: ConsumeStatsList, diff_level: i64) -> BrokerConsumeStatsResult {
    let mut rows = Vec::new();
    for group_map in &source.consume_stats_list {
        for (group, consume_stats_array) in group_map {
            for consume_stats in consume_stats_array {
                for (mq, offset_wrapper) in &consume_stats.offset_table {
                    let diff = offset_wrapper.get_broker_offset() - offset_wrapper.get_consumer_offset();
                    if diff < diff_level {
                        continue;
                    }

                    rows.push(BrokerConsumeStatsRow {
                        topic: mq.topic().clone(),
                        group: group.clone(),
                        broker_name: mq.broker_name().clone(),
                        queue_id: mq.queue_id(),
                        broker_offset: offset_wrapper.get_broker_offset(),
                        consumer_offset: offset_wrapper.get_consumer_offset(),
                        diff,
                        last_timestamp: offset_wrapper.get_last_timestamp(),
                    });
                }
            }
        }
    }
    rows.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then_with(|| left.group.cmp(&right.group))
            .then_with(|| left.broker_name.cmp(&right.broker_name))
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });

    BrokerConsumeStatsResult {
        broker_addr: source.broker_addr,
        total_diff: source.total_diff,
        total_inflight_diff: source.total_inflight_diff,
        rows,
    }
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

    #[test]
    fn build_broker_consume_stats_result_filters_and_sorts_rows() {
        use rocketmq_common::common::message::message_queue::MessageQueue;
        use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
        use rocketmq_remoting::protocol::admin::consume_stats_list::ConsumeStatsList;
        use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;

        let mut included = OffsetWrapper::new();
        included.set_broker_offset(120);
        included.set_consumer_offset(80);
        included.set_last_timestamp(1_700_000_000_000);

        let mut below_threshold = OffsetWrapper::new();
        below_threshold.set_broker_offset(95);
        below_threshold.set_consumer_offset(90);
        below_threshold.set_last_timestamp(1_700_000_000_000);

        let mut offset_table = HashMap::new();
        offset_table.insert(MessageQueue::from_parts("TopicB", "broker-b", 1), included);
        offset_table.insert(MessageQueue::from_parts("TopicA", "broker-a", 0), below_threshold);

        let mut group_stats = HashMap::new();
        group_stats.insert(
            CheetahString::from("GroupA"),
            vec![ConsumeStats {
                offset_table,
                consume_tps: 0.0,
            }],
        );

        let source = ConsumeStatsList {
            consume_stats_list: vec![group_stats],
            broker_addr: Some(CheetahString::from("127.0.0.1:10911")),
            total_diff: 45,
            total_inflight_diff: 0,
        };

        let result = build_broker_consume_stats_result(source, 10);

        assert_eq!(result.total_diff, 45);
        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row.topic.as_str(), "TopicB");
        assert_eq!(row.group.as_str(), "GroupA");
        assert_eq!(row.broker_name.as_str(), "broker-b");
        assert_eq!(row.queue_id, 1);
        assert_eq!(row.broker_offset, 120);
        assert_eq!(row.consumer_offset, 80);
        assert_eq!(row.diff, 40);
        assert_eq!(row.last_timestamp, 1_700_000_000_000);
    }
}
