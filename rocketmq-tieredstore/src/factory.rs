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

//! Startup composition for compile-time Tiered Store provider factories.

use rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder;
use rocketmq_runtime::TaskGroup;
use rocketmq_store_api::StoreError;

use crate::config::TieredStoreConfig;
use crate::provider::PosixProvider;
use crate::provider::TieredProviderCapability;
use crate::provider::TieredProviderDescriptor;
use crate::provider::TieredProviderPersistence;
use crate::provider::TieredStoreProviderFactory;
use crate::store::TieredStore;

const INDEX_PROVIDER_CAPABILITIES: [TieredProviderCapability; 4] = [
    TieredProviderCapability::AtomicWrite,
    TieredProviderCapability::AtomicRename,
    TieredProviderCapability::PrefixListing,
    TieredProviderCapability::PrefixDelete,
];

/// Startup boundary that validates and constructs a Tiered Store provider.
pub struct TieredStoreFactory;

/// Validated Tiered Store configuration capability.
///
/// The plan is produced without opening a provider or touching the filesystem.
pub struct TieredStoreOpenPlan {
    config: TieredStoreConfig,
    descriptor: Option<TieredProviderDescriptor>,
}

impl std::fmt::Debug for TieredStoreOpenPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TieredStoreOpenPlan { redacted: true }")
    }
}

impl TieredStoreOpenPlan {
    fn try_new(config: TieredStoreConfig, descriptor: TieredProviderDescriptor) -> Option<Self> {
        valid_config(&config, Some(descriptor)).then_some(Self {
            config,
            descriptor: Some(descriptor),
        })
    }

    /// Validates configuration for a caller-supplied provider without I/O.
    ///
    /// Returns `None` when Tiered Store is disabled or the deterministic
    /// configuration is invalid. Provider construction and filesystem work are
    /// deferred until the plan is consumed.
    pub fn try_for_direct_provider(config: TieredStoreConfig) -> Option<Self> {
        valid_config(&config, None).then_some(Self {
            config,
            descriptor: None,
        })
    }

    pub(crate) const fn config(&self) -> &TieredStoreConfig {
        &self.config
    }

    pub(crate) fn into_parts(self) -> (TieredStoreConfig, Option<TieredProviderDescriptor>) {
        (self.config, self.descriptor)
    }
}

/// Factory-bound capability for opening one validated Tiered Store provider.
///
/// The capability retains the exact factory whose declaration validated the raw
/// configuration, so a planned open cannot substitute a different provider.
pub struct TieredProviderOpenPlan<F> {
    store: TieredStoreOpenPlan,
    factory: F,
}

impl<F> std::fmt::Debug for TieredProviderOpenPlan<F> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TieredProviderOpenPlan { redacted: true }")
    }
}

impl<F> TieredProviderOpenPlan<F>
where
    F: TieredStoreProviderFactory,
{
    /// Validates raw configuration and binds it to the supplied factory without I/O.
    ///
    /// Returns `None` when Tiered Store is disabled or when the configuration and
    /// provider declaration are not a supported startup combination.
    pub fn try_new(config: TieredStoreConfig, factory: F) -> Option<Self> {
        let store = TieredStoreOpenPlan::try_new(config, factory.descriptor())?;
        Some(Self { store, factory })
    }

    pub(crate) fn into_parts(self) -> (TieredStoreOpenPlan, F) {
        (self.store, self.factory)
    }
}

impl TieredStoreFactory {
    /// Opens a Tiered Store with a compile-time injected provider factory.
    ///
    /// Returns `Ok(None)` when Tiered Store is disabled or the deterministic
    /// configuration and provider declaration are invalid or unsupported.
    ///
    /// # Errors
    ///
    /// Returns an operational error when provider construction or Store
    /// composition fails.
    pub fn open<F>(
        config: TieredStoreConfig,
        factory: F,
        parent_task_group: TaskGroup,
    ) -> Result<Option<TieredStore<F::Provider>>, StoreError>
    where
        F: TieredStoreProviderFactory,
    {
        Self::open_with_metrics(config, factory, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    /// Opens a Tiered Store with an explicit metrics recorder.
    ///
    /// Returns `Ok(None)` when Tiered Store is disabled or the deterministic
    /// configuration and provider declaration are invalid or unsupported.
    ///
    /// # Errors
    ///
    /// Returns an operational error when provider construction or Store
    /// composition fails.
    pub fn open_with_metrics<F>(
        config: TieredStoreConfig,
        factory: F,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Option<TieredStore<F::Provider>>, StoreError>
    where
        F: TieredStoreProviderFactory,
    {
        let Some(plan) = TieredProviderOpenPlan::try_new(config, factory) else {
            return Ok(None);
        };
        Self::open_planned_with_metrics(plan, metrics, parent_task_group).map(Some)
    }

    /// Opens a provider from a prevalidated configuration capability.
    ///
    /// # Errors
    ///
    /// Returns an error only when provider construction or Store composition fails.
    pub fn open_planned<F>(
        plan: TieredProviderOpenPlan<F>,
        parent_task_group: TaskGroup,
    ) -> Result<TieredStore<F::Provider>, StoreError>
    where
        F: TieredStoreProviderFactory,
    {
        Self::open_planned_with_metrics(plan, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    /// Opens a provider from a prevalidated configuration capability with metrics.
    ///
    /// # Errors
    ///
    /// Returns an error only when provider construction or Store composition fails.
    pub fn open_planned_with_metrics<F>(
        plan: TieredProviderOpenPlan<F>,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<TieredStore<F::Provider>, StoreError>
    where
        F: TieredStoreProviderFactory,
    {
        let (store_plan, factory) = plan.into_parts();
        let provider = factory.create(&store_plan)?;
        TieredStore::with_provider_planned_and_metrics(store_plan, provider, metrics, parent_task_group)
    }
}

fn valid_config(config: &TieredStoreConfig, descriptor: Option<TieredProviderDescriptor>) -> bool {
    if !config.storage_level.enabled()
        || config.store_path_root_dir.as_os_str().is_empty()
        || config.commit_log_segment_size == 0
        || config.consume_queue_segment_size == 0
        || config.index_file_max_hash_slot_num == 0
        || config.index_file_max_index_num == 0
        || config.max_pending_tasks == 0
        || config.max_pending_bytes == 0
        || config.retry_ledger_max_entries == 0
        || config.retry_ledger_max_bytes == 0
        || config.retry_backoff_initial.is_zero()
        || config.retry_backoff_max < config.retry_backoff_initial
        || config.source_wal_segment_size == 0
        || config.read_ahead_cache_max_bytes == 0
        || config.read_ahead_message_count == 0
        || config.read_ahead_message_size == 0
        || config.metadata_provider != "json"
    {
        return false;
    }
    let Some(descriptor) = descriptor else {
        return true;
    };
    if descriptor.id() == "posix" && !PosixProvider::root_is_valid(&config.store_path_root_dir) {
        return false;
    }
    if descriptor.id().is_empty() {
        return false;
    }
    if descriptor.config_version() == 0 {
        return false;
    }
    if let TieredProviderPersistence::Stable { format, version } = descriptor.persistence() {
        if format.is_empty() || version == 0 {
            return false;
        }
    }
    if config.backend_provider != descriptor.id() {
        return false;
    }
    let missing = INDEX_PROVIDER_CAPABILITIES
        .into_iter()
        .filter(|capability| !descriptor.capabilities().supports(*capability))
        .collect::<Vec<_>>();
    missing.is_empty()
}
