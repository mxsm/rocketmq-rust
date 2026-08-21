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

use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;

use cheetah_string::CheetahString;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_runtime::BudgetCapacity;
use rocketmq_runtime::BudgetConfigError;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::MemoryLimitSource;
use rocketmq_runtime::ProcessMemoryLimit;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_store::FlushDiskType;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreType;

use super::broker_config::BrokerConfig;
use super::error::BrokerConfigError;
use super::error::ConfigSection;

#[derive(Clone, Debug)]
pub struct IdentityConfig {
    broker_name: CheetahString,
    cluster_name: CheetahString,
    broker_id: u64,
}

impl IdentityConfig {
    #[must_use]
    pub fn broker_name(&self) -> &str {
        self.broker_name.as_str()
    }

    #[must_use]
    pub fn cluster_name(&self) -> &str {
        self.cluster_name.as_str()
    }

    #[must_use]
    pub const fn broker_id(&self) -> u64 {
        self.broker_id
    }
}

#[derive(Clone, Debug)]
pub struct NetworkConfig {
    advertised_address: CheetahString,
    bind_address: IpAddr,
    listen_port: u16,
    fast_listen_port: u16,
    name_server_addresses: Vec<CheetahString>,
}

impl NetworkConfig {
    #[must_use]
    pub fn advertised_address(&self) -> &str {
        self.advertised_address.as_str()
    }

    #[must_use]
    pub const fn bind_address(&self) -> IpAddr {
        self.bind_address
    }

    #[must_use]
    pub const fn listen_port(&self) -> u16 {
        self.listen_port
    }

    #[must_use]
    pub const fn fast_listen_port(&self) -> u16 {
        self.fast_listen_port
    }

    #[must_use]
    pub fn name_server_addresses(&self) -> &[CheetahString] {
        &self.name_server_addresses
    }
}

#[derive(Clone, Debug)]
pub struct HighAvailabilityConfig {
    listen_address: IpAddr,
    listen_port: u16,
    broker_role: BrokerRole,
    controller_mode: bool,
}

impl HighAvailabilityConfig {
    #[must_use]
    pub const fn listen_address(&self) -> IpAddr {
        self.listen_address
    }

    #[must_use]
    pub const fn listen_port(&self) -> u16 {
        self.listen_port
    }

    #[must_use]
    pub const fn broker_role(&self) -> BrokerRole {
        self.broker_role
    }

    #[must_use]
    pub const fn controller_mode(&self) -> bool {
        self.controller_mode
    }
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    broker_root: PathBuf,
    store_root: PathBuf,
    commit_log_paths: Vec<PathBuf>,
    store_type: StoreType,
}

impl StorageConfig {
    #[must_use]
    pub fn broker_root(&self) -> &std::path::Path {
        &self.broker_root
    }

    #[must_use]
    pub fn store_root(&self) -> &std::path::Path {
        &self.store_root
    }

    #[must_use]
    pub fn commit_log_paths(&self) -> &[PathBuf] {
        &self.commit_log_paths
    }

    #[must_use]
    pub const fn store_type(&self) -> StoreType {
        self.store_type
    }
}

#[derive(Clone, Debug)]
pub struct SecurityConfig {
    authentication_enabled: bool,
    authorization_enabled: bool,
    maintenance_enabled: bool,
    tls_enabled: bool,
}

impl SecurityConfig {
    #[must_use]
    pub const fn authentication_enabled(&self) -> bool {
        self.authentication_enabled
    }

    #[must_use]
    pub const fn authorization_enabled(&self) -> bool {
        self.authorization_enabled
    }

    #[must_use]
    pub const fn maintenance_enabled(&self) -> bool {
        self.maintenance_enabled
    }

    #[must_use]
    pub const fn tls_enabled(&self) -> bool {
        self.tls_enabled
    }
}

#[derive(Clone, Debug)]
pub struct ResourceConfig {
    max_lite_subscriptions: u64,
    max_client_events: i32,
    max_pop_polling_requests: u64,
    compaction_threads: usize,
    process_memory_limit: ProcessMemoryLimit,
    managed_memory_bytes: u64,
    control_reserve_bytes: u64,
}

impl ResourceConfig {
    #[must_use]
    pub const fn max_lite_subscriptions(&self) -> u64 {
        self.max_lite_subscriptions
    }

    #[must_use]
    pub const fn max_client_events(&self) -> i32 {
        self.max_client_events
    }

    #[must_use]
    pub const fn max_pop_polling_requests(&self) -> u64 {
        self.max_pop_polling_requests
    }

    #[must_use]
    pub const fn compaction_threads(&self) -> usize {
        self.compaction_threads
    }

    #[must_use]
    pub const fn process_memory_limit_bytes(&self) -> u64 {
        self.process_memory_limit.bytes()
    }

    #[must_use]
    pub const fn process_memory_limit_source(&self) -> MemoryLimitSource {
        self.process_memory_limit.source()
    }

    #[must_use]
    pub const fn managed_memory_bytes(&self) -> u64 {
        self.managed_memory_bytes
    }

    #[must_use]
    pub const fn control_reserve_bytes(&self) -> u64 {
        self.control_reserve_bytes
    }

    pub fn budget_tree(&self) -> Result<ResourceBudgetTree, BudgetConfigError> {
        // A Lite subscription can contribute one pending event and one tracked
        // client-access entry. Both consume separate child permits.
        let item_limit = self
            .max_lite_subscriptions
            .saturating_mul(2)
            .saturating_add(self.max_pop_polling_requests)
            .saturating_add(65_536);
        let count = usize::try_from(item_limit).unwrap_or(usize::MAX);
        let bytes = usize::try_from(self.managed_memory_bytes).unwrap_or(usize::MAX);
        let reserve_count = 64.min(count.saturating_sub(1));
        let reserve_bytes = usize::try_from(self.control_reserve_bytes)
            .unwrap_or(usize::MAX)
            .min(bytes.saturating_sub(1));
        ResourceBudgetTree::new(
            "broker",
            BudgetLimit::new(count, bytes, FullPolicy::Reject)
                .with_control_reserve(BudgetCapacity::new(reserve_count, reserve_bytes)),
        )
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedConfigSections {
    identity: IdentityConfig,
    network: NetworkConfig,
    high_availability: HighAvailabilityConfig,
    storage: StorageConfig,
    security: SecurityConfig,
    resources: ResourceConfig,
}

impl ValidatedConfigSections {
    pub(crate) fn validate(broker: &BrokerConfig, store: &MessageStoreConfig) -> Result<Self, BrokerConfigError> {
        let identity = validate_identity(broker, store)?;
        let network = validate_network(broker)?;
        let high_availability = validate_high_availability(broker, store, &network, &identity)?;
        let storage = validate_storage(broker, store)?;
        let security = validate_security(broker)?;
        let resources = validate_resources(broker, store)?;

        Ok(Self {
            identity,
            network,
            high_availability,
            storage,
            security,
            resources,
        })
    }

    #[must_use]
    pub fn identity(&self) -> &IdentityConfig {
        &self.identity
    }

    #[must_use]
    pub fn network(&self) -> &NetworkConfig {
        &self.network
    }

    #[must_use]
    pub fn high_availability(&self) -> &HighAvailabilityConfig {
        &self.high_availability
    }

    #[must_use]
    pub fn storage(&self) -> &StorageConfig {
        &self.storage
    }

    #[must_use]
    pub fn security(&self) -> &SecurityConfig {
        &self.security
    }

    #[must_use]
    pub fn resources(&self) -> &ResourceConfig {
        &self.resources
    }
}

fn validate_identity(broker: &BrokerConfig, store: &MessageStoreConfig) -> Result<IdentityConfig, BrokerConfigError> {
    let broker_name = broker.broker_identity.broker_name.trim();
    if broker_name.is_empty() {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Identity,
            "broker.brokerIdentity.brokerName",
            "must not be blank",
        ));
    }
    let cluster_name = broker.broker_identity.broker_cluster_name.trim();
    if cluster_name.is_empty() {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Identity,
            "broker.brokerIdentity.brokerClusterName",
            "must not be blank",
        ));
    }

    let broker_id = broker.broker_identity.broker_id;
    if !broker.enable_controller_mode {
        match store.broker_role {
            BrokerRole::AsyncMaster | BrokerRole::SyncMaster if broker_id != 0 => {
                return Err(BrokerConfigError::invalid(
                    ConfigSection::Identity,
                    "broker.brokerIdentity.brokerId",
                    "master brokers must use brokerId 0 when controller mode is disabled",
                ));
            }
            BrokerRole::Slave if broker_id == 0 => {
                return Err(BrokerConfigError::invalid(
                    ConfigSection::Identity,
                    "broker.brokerIdentity.brokerId",
                    "slave brokers must use a non-zero brokerId when controller mode is disabled",
                ));
            }
            _ => {}
        }
    }

    Ok(IdentityConfig {
        broker_name: broker.broker_identity.broker_name.clone(),
        cluster_name: broker.broker_identity.broker_cluster_name.clone(),
        broker_id,
    })
}

fn validate_network(broker: &BrokerConfig) -> Result<NetworkConfig, BrokerConfigError> {
    validate_host(ConfigSection::Network, "broker.brokerIp1", broker.broker_ip1.as_str())?;
    if let Some(address) = broker.broker_ip2.as_ref() {
        validate_host(ConfigSection::Network, "broker.brokerIp2", address.as_str())?;
    }
    let bind_address = broker
        .broker_server_config
        .bind_address
        .parse::<IpAddr>()
        .map_err(|error| {
            BrokerConfigError::invalid(
                ConfigSection::Network,
                "broker.brokerServerConfig.bindAddress",
                format!("must be an IP address: {error}"),
            )
        })?;
    let listen_port = validated_port(ConfigSection::Network, "broker.listenPort", broker.listen_port)?;
    let fast_listen_port = listen_port.checked_sub(2).filter(|port| *port > 0).ok_or_else(|| {
        BrokerConfigError::invalid(
            ConfigSection::Network,
            "broker.listenPort",
            "must leave room for the fast remoting listener",
        )
    })?;

    let mut name_server_addresses = Vec::new();
    if let Some(addresses) = broker.namesrv_addr.as_ref() {
        for address in addresses
            .split_char(';')
            .map(str::trim)
            .filter(|address| !address.is_empty())
        {
            validate_endpoint(ConfigSection::Network, "broker.namesrvAddr", address)?;
            name_server_addresses.push(address.into());
        }
    }

    Ok(NetworkConfig {
        advertised_address: broker.broker_ip1.clone(),
        bind_address,
        listen_port,
        fast_listen_port,
        name_server_addresses,
    })
}

fn validate_high_availability(
    broker: &BrokerConfig,
    store: &MessageStoreConfig,
    network: &NetworkConfig,
    identity: &IdentityConfig,
) -> Result<HighAvailabilityConfig, BrokerConfigError> {
    if store.min_in_sync_replicas == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::HighAvailability,
            "store.minInSyncReplicas",
            "must be at least 1",
        ));
    }
    let in_sync_replicas = usize::try_from(store.in_sync_replicas).map_err(|_| {
        BrokerConfigError::invalid(
            ConfigSection::HighAvailability,
            "store.inSyncReplicas",
            "must be positive",
        )
    })?;
    if store.min_in_sync_replicas > in_sync_replicas || in_sync_replicas > store.total_replicas {
        return Err(BrokerConfigError::invalid(
            ConfigSection::HighAvailability,
            "store.minInSyncReplicas",
            "must satisfy minInSyncReplicas <= inSyncReplicas <= totalReplicas",
        ));
    }
    let replica_ack_required = store.broker_role == BrokerRole::SyncMaster
        || store.all_ack_in_sync_state_set
        || store.min_in_sync_replicas > 1;
    if replica_ack_required && store.slave_timeout == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::HighAvailability,
            "store.slaveTimeout",
            "must be greater than zero when replica acknowledgement is required",
        ));
    }
    if (broker.enable_controller_mode || store.enable_auto_in_sync_replicas) && store.ha_max_time_slave_not_catchup == 0
    {
        return Err(BrokerConfigError::invalid(
            ConfigSection::HighAvailability,
            "store.haMaxTimeSlaveNotCatchup",
            "must be greater than zero when Controller or automatic in-sync replicas are enabled",
        ));
    }
    let listen_port = u32::try_from(store.ha_listen_port)
        .ok()
        .and_then(|port| validated_port(ConfigSection::HighAvailability, "store.haListenPort", port).ok())
        .ok_or_else(|| {
            BrokerConfigError::invalid(
                ConfigSection::HighAvailability,
                "store.haListenPort",
                "must be a non-zero TCP port",
            )
        })?;
    if listen_port == network.listen_port || listen_port == network.fast_listen_port {
        return Err(BrokerConfigError::invalid(
            ConfigSection::HighAvailability,
            "store.haListenPort",
            "must not conflict with a remoting listener",
        ));
    }
    if broker.enable_controller_mode {
        if broker.controller_addr.trim().is_empty() {
            return Err(BrokerConfigError::invalid(
                ConfigSection::HighAvailability,
                "broker.controllerAddr",
                "must not be blank when controller mode is enabled",
            ));
        }
        for address in broker
            .controller_addr
            .split_char(';')
            .map(str::trim)
            .filter(|address| !address.is_empty())
        {
            validate_endpoint(ConfigSection::HighAvailability, "broker.controllerAddr", address)?;
        }
    }
    if identity.broker_id == 0 && store.broker_role == BrokerRole::Slave && !broker.enable_controller_mode {
        return Err(BrokerConfigError::invalid(
            ConfigSection::HighAvailability,
            "store.brokerRole",
            "slave role conflicts with master broker identity",
        ));
    }

    Ok(HighAvailabilityConfig {
        listen_address: store.ha_listen_address,
        listen_port,
        broker_role: store.broker_role,
        controller_mode: broker.enable_controller_mode,
    })
}

fn validate_storage(broker: &BrokerConfig, store: &MessageStoreConfig) -> Result<StorageConfig, BrokerConfigError> {
    if store.enable_dledger_commit_log
        || store.enable_dleger_commit_log
        || store.store_path_dledger_commit_log.is_some()
        || store.dledger_group.is_some()
        || store.dledger_peers.is_some()
        || store.dledger_self_id.is_some()
        || store.preferred_leader_id.is_some()
    {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.enableDledgerCommitLog",
            "DLedger is intentionally unsupported; use the independent Controller HA mode instead",
        ));
    }
    if store.transient_store_pool_enable && store.transient_store_pool_size == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.transientStorePoolSize",
            "must be greater than zero when transientStorePoolEnable is true",
        ));
    }
    if store.flush_disk_type == FlushDiskType::AsyncFlush
        && (store.flush_commit_log_least_pages == 0
            || store.flush_consume_queue_least_pages == 0
            || store.flush_consume_queue_thorough_interval == 0)
    {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.flushDiskType",
            "ASYNC_FLUSH requires non-zero CommitLog and ConsumeQueue flush thresholds",
        ));
    }
    if broker.store_path_root_dir.trim().is_empty() {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "broker.storePathRootDir",
            "must not be blank",
        ));
    }
    if store.store_path_root_dir.trim().is_empty() {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.storePathRootDir",
            "must not be blank",
        ));
    }
    if store.mapped_file_size_commit_log == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.mappedFileSizeCommitLog",
            "must be greater than zero",
        ));
    }
    if store.mapped_file_size_consume_queue == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.mappedFileSizeConsumeQueue",
            "must be greater than zero",
        ));
    }
    store.timer_policy_snapshot().map_err(|error| {
        BrokerConfigError::invalid(ConfigSection::Storage, "store.timerPrecisionMs", error.to_string())
    })?;
    if store.disk_space_clean_forcibly_ratio >= store.disk_space_warning_level_ratio {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.diskSpaceCleanForciblyRatio",
            "must be lower than diskSpaceWarningLevelRatio",
        ));
    }
    if store.disk_max_used_space_ratio >= store.disk_space_clean_forcibly_ratio {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.diskMaxUsedSpaceRatio",
            "must be lower than diskSpaceCleanForciblyRatio",
        ));
    }

    let commit_log_paths = store
        .get_store_path_commit_log()
        .split(rocketmq_model::common::mix_all::MULTI_PATH_SPLITTER.as_str())
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if commit_log_paths.is_empty() {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.storePathCommitLog",
            "must resolve to at least one path",
        ));
    }

    #[cfg(feature = "tieredstore")]
    if store
        .tiered_store_config
        .as_ref()
        .is_some_and(|config| config.storage_level.enabled())
        && store.store_type != StoreType::LocalFile
    {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Storage,
            "store.storeType",
            "tiered storage requires the local-file store",
        ));
    }

    Ok(StorageConfig {
        broker_root: PathBuf::from(broker.store_path_root_dir.as_str()),
        store_root: PathBuf::from(store.store_path_root_dir.as_str()),
        commit_log_paths,
        store_type: store.store_type,
    })
}

fn validate_security(broker: &BrokerConfig) -> Result<SecurityConfig, BrokerConfigError> {
    if broker.authorization_enabled && !broker.authentication_enabled {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Security,
            "broker.authorizationEnabled",
            "requires authenticationEnabled=true",
        ));
    }
    if (broker.authentication_enabled || broker.authorization_enabled) && broker.auth_config_path.trim().is_empty() {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Security,
            "broker.authConfigPath",
            "must not be blank when broker authentication or authorization is enabled",
        ));
    }
    if broker.maintenance_enabled {
        if !broker.authentication_enabled || !broker.authorization_enabled {
            return Err(BrokerConfigError::invalid(
                ConfigSection::Security,
                "broker.maintenanceEnabled",
                "requires authenticationEnabled=true and authorizationEnabled=true",
            ));
        }
        if broker.acl_file.trim().is_empty()
            || broker.maintenance_policy_path.trim().is_empty()
            || broker.maintenance_policy_version == 0
            || broker.maintenance_checkpoint_root.trim().is_empty()
        {
            return Err(BrokerConfigError::invalid(
                ConfigSection::Security,
                "broker.maintenancePolicy",
                "requires aclFile, maintenancePolicyPath, maintenanceCheckpointRoot, and a non-zero \
                 maintenancePolicyVersion",
            ));
        }
        let checkpoint_root = PathBuf::from(broker.maintenance_checkpoint_root.as_str());
        let store_root = PathBuf::from(broker.store_path_root_dir.as_str());
        if checkpoint_root == store_root
            || checkpoint_root.starts_with(&store_root)
            || store_root.starts_with(&checkpoint_root)
        {
            return Err(BrokerConfigError::invalid(
                ConfigSection::Security,
                "broker.maintenanceCheckpointRoot",
                "must not overlap the live Store root",
            ));
        }
        if broker.maintenance_policy_sha256.len() != 64
            || !broker
                .maintenance_policy_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(BrokerConfigError::invalid(
                ConfigSection::Security,
                "broker.maintenancePolicySha256",
                "must be 64 lowercase hexadecimal characters",
            ));
        }
    }
    let tls = &broker.broker_server_config.tls_config;
    if tls.enable && !tls.test_mode_enable {
        if tls.server.cert_path.as_deref().is_none_or(str::is_empty) {
            return Err(BrokerConfigError::invalid(
                ConfigSection::Security,
                "broker.brokerServerConfig.tlsConfig.server.certPath",
                "must be configured when TLS is enabled",
            ));
        }
        if tls.server.key_path.as_deref().is_none_or(str::is_empty) {
            return Err(BrokerConfigError::invalid(
                ConfigSection::Security,
                "broker.brokerServerConfig.tlsConfig.server.keyPath",
                "must be configured when TLS is enabled",
            ));
        }
    }

    Ok(SecurityConfig {
        authentication_enabled: broker.authentication_enabled,
        authorization_enabled: broker.authorization_enabled,
        maintenance_enabled: broker.maintenance_enabled,
        tls_enabled: tls.enable,
    })
}

fn validate_resources(broker: &BrokerConfig, store: &MessageStoreConfig) -> Result<ResourceConfig, BrokerConfigError> {
    if broker.broker_fast_failure_pending_max_count == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.brokerFastFailurePendingMaxCount",
            "must be greater than zero",
        ));
    }
    if broker.broker_fast_failure_pending_max_bytes == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.brokerFastFailurePendingMaxBytes",
            "must be greater than zero",
        ));
    }
    if broker.max_lite_subscription_count == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.maxLiteSubscriptionCount",
            "must be greater than zero",
        ));
    }
    if broker.max_client_event_count <= 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.maxClientEventCount",
            "must be greater than zero",
        ));
    }
    if broker.max_pop_polling_size == 0 || broker.pop_polling_map_size == 0 || broker.pop_polling_size == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.popPollingCapacity",
            "maxPopPollingSize, popPollingMapSize and popPollingSize must be greater than zero",
        ));
    }
    if !(1..=1024).contains(&broker.max_message_filter_num_for_notification) {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.maxMessageFilterNumForNotification",
            "must be in [1, 1024]",
        ));
    }
    if store.compaction_thread_num == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "store.compactionThreadNum",
            "must be greater than zero",
        ));
    }
    if store.timer_get_message_thread_num == 0 || store.timer_put_message_thread_num == 0 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "store.timerThreadCount",
            "timer get and put thread counts must be greater than zero",
        ));
    }
    let max_message_size = usize::try_from(store.max_message_size).map_err(|_| {
        BrokerConfigError::invalid(ConfigSection::Resources, "store.maxMessageSize", "must be non-negative")
    })?;
    if store.timer_pipeline_queue_messages == 0
        || store.timer_pipeline_queue_bytes < max_message_size
        || store.timer_source_batch_messages == 0
        || store.timer_due_batch_messages == 0
        || store.timer_completion_gap_limit == 0
    {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "store.timerPipelineBudget",
            "timer pipeline queue bytes must fit one maximum message and all queue, batch, and gap limits must be positive",
        ));
    }
    if store.timer_retry_max_attempts == 0
        || store.timer_retry_initial_backoff_ms == 0
        || store.timer_retry_max_backoff_ms < store.timer_retry_initial_backoff_ms
    {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "store.timerRetryPolicy",
            "timer retry attempts and initial backoff must be positive and max backoff must not be smaller",
        ));
    }

    let process_memory_limit = if broker.process_memory_limit_bytes == 0 {
        ProcessMemoryLimit::detect()
    } else {
        ProcessMemoryLimit::configured(broker.process_memory_limit_bytes)
    }
    .map_err(|error| {
        BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.processMemoryLimitBytes",
            error.to_string(),
        )
    })?;
    let managed_memory_bytes = process_memory_limit.fraction(1, 4).map_err(|error| {
        BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.processMemoryLimitBytes",
            error.to_string(),
        )
    })?;
    if managed_memory_bytes < 1024 * 1024 {
        return Err(BrokerConfigError::invalid(
            ConfigSection::Resources,
            "broker.processMemoryLimitBytes",
            "must provide at least 4 MiB so the managed queue budget is at least 1 MiB",
        ));
    }
    let control_reserve_bytes = (managed_memory_bytes / 20).max(1);

    Ok(ResourceConfig {
        max_lite_subscriptions: broker.max_lite_subscription_count,
        max_client_events: broker.max_client_event_count,
        max_pop_polling_requests: broker.max_pop_polling_size,
        compaction_threads: store.compaction_thread_num,
        process_memory_limit,
        managed_memory_bytes,
        control_reserve_bytes,
    })
}

fn validated_port(section: ConfigSection, field: &'static str, port: u32) -> Result<u16, BrokerConfigError> {
    let port = u16::try_from(port).map_err(|_| BrokerConfigError::invalid(section, field, "must fit a TCP port"))?;
    if port == 0 {
        return Err(BrokerConfigError::invalid(section, field, "must be greater than zero"));
    }
    Ok(port)
}

fn validate_host(section: ConfigSection, field: &'static str, host: &str) -> Result<(), BrokerConfigError> {
    let trimmed = host.trim();
    if trimmed != host {
        return Err(BrokerConfigError::invalid(
            section,
            field,
            "must not contain surrounding whitespace",
        ));
    }
    let host = trimmed.trim_end_matches('.');
    if host.is_empty() {
        return Err(BrokerConfigError::invalid(section, field, "must not be blank"));
    }
    if host.parse::<IpAddr>().is_ok() {
        return Ok(());
    }
    let valid_dns_name = host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
    if !valid_dns_name {
        return Err(BrokerConfigError::invalid(
            section,
            field,
            "must be one valid IP address or DNS name",
        ));
    }
    Ok(())
}

fn validate_endpoint(section: ConfigSection, field: &'static str, endpoint: &str) -> Result<(), BrokerConfigError> {
    if endpoint.parse::<SocketAddr>().is_ok() {
        return Ok(());
    }
    let Some((host, port)) = endpoint.rsplit_once(':') else {
        return Err(BrokerConfigError::invalid(
            section,
            field,
            format!("endpoint `{endpoint}` must use host:port syntax"),
        ));
    };
    validate_host(section, field, host.trim_matches(['[', ']']))?;
    let port = port.parse::<u16>().map_err(|error| {
        BrokerConfigError::invalid(
            section,
            field,
            format!("endpoint `{endpoint}` has an invalid port: {error}"),
        )
    })?;
    if port == 0 {
        return Err(BrokerConfigError::invalid(
            section,
            field,
            format!("endpoint `{endpoint}` must use a non-zero port"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_validation_rejects_unsupported_timer_precision() {
        let broker = BrokerConfig::default();
        let store = MessageStoreConfig {
            timer_precision_ms: 0,
            ..MessageStoreConfig::default()
        };

        let error = validate_storage(&broker, &store).expect_err("zero timer precision must fail startup");

        assert!(error.to_string().contains("store.timerPrecisionMs"));
    }

    #[test]
    fn resource_validation_rejects_timer_queue_that_cannot_hold_one_message() {
        let broker = BrokerConfig::default();
        let store = MessageStoreConfig {
            max_message_size: 1_024,
            timer_pipeline_queue_bytes: 1_023,
            ..MessageStoreConfig::default()
        };

        let error = validate_resources(&broker, &store).expect_err("undersized timer queue must fail startup");

        assert!(error.to_string().contains("store.timerPipelineBudget"));
    }

    #[test]
    fn resource_validation_rejects_zero_fast_failure_pending_count() {
        let broker = BrokerConfig {
            broker_fast_failure_pending_max_count: 0,
            ..BrokerConfig::default()
        };

        let error = validate_resources(&broker, &MessageStoreConfig::default())
            .expect_err("zero fast-failure pending count must fail startup");

        assert!(error.to_string().contains("broker.brokerFastFailurePendingMaxCount"));
    }

    #[test]
    fn resource_validation_rejects_zero_fast_failure_pending_bytes() {
        let broker = BrokerConfig {
            broker_fast_failure_pending_max_bytes: 0,
            ..BrokerConfig::default()
        };

        let error = validate_resources(&broker, &MessageStoreConfig::default())
            .expect_err("zero fast-failure pending bytes must fail startup");

        assert!(error.to_string().contains("broker.brokerFastFailurePendingMaxBytes"));
    }

    #[test]
    fn resource_validation_rejects_notification_filter_scan_outside_bounds() {
        for invalid in [0, 1025] {
            let broker = BrokerConfig {
                max_message_filter_num_for_notification: invalid,
                ..BrokerConfig::default()
            };

            let error = validate_resources(&broker, &MessageStoreConfig::default())
                .expect_err("notification filter scan limit outside [1, 1024] must fail startup");

            assert!(error.to_string().contains("broker.maxMessageFilterNumForNotification"));
        }
    }
}
