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

use rocketmq_runtime::TaskGroup;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::dispatcher::DefaultTieredDispatcher;
use crate::dispatcher::TieredDispatcher;
use crate::factory::TieredProviderOpenPlan;
use crate::factory::TieredStoreFactory;
use crate::factory::TieredStoreOpenPlan;
use crate::fetcher::DefaultTieredMessageFetcher;
use crate::file::TieredFlatFileStore;
use crate::lifecycle::TieredLifecycle;
use crate::metadata::JsonMetadataStore;
use crate::metadata::TieredMetadataStore;
use crate::provider::BuiltinTieredStoreProviderFactory;
use crate::provider::ProviderKind;
use crate::provider::TieredProviderDescriptor;
use crate::provider::TieredStoreProvider;
use crate::service::CommitLogRecoverService;
use crate::service::TieredServiceSet;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetrics;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder;

pub struct TieredStore<P = ProviderKind>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    metadata_store: Arc<JsonMetadataStore>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
    dispatcher: Arc<DefaultTieredDispatcher<P>>,
    fetcher: Arc<DefaultTieredMessageFetcher<P>>,
    metrics: Arc<TieredStoreMetrics>,
    services: TieredServiceSet<P>,
    shutdown: CancellationToken,
    provider_descriptor: Option<TieredProviderDescriptor>,
}

impl TieredStore<ProviderKind> {
    /// Validates raw configuration and opens a built-in Tiered Store provider.
    ///
    /// Returns `Ok(None)` for disabled, unsupported, or invalid configuration.
    ///
    /// # Errors
    ///
    /// Returns an error only for operational provider or Store composition failure.
    pub fn new(config: TieredStoreConfig, parent_task_group: TaskGroup) -> Result<Option<Self>, StoreError> {
        Self::new_with_metrics(config, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    /// Validates raw configuration and opens a built-in Tiered Store provider
    /// with metrics.
    ///
    /// Returns `Ok(None)` when Tiered Store is disabled, the configured
    /// provider is unsupported, or deterministic configuration validation
    /// fails.
    ///
    /// # Errors
    ///
    /// Returns an operational storage error when provider construction or
    /// Store composition fails.
    pub fn new_with_metrics(
        config: TieredStoreConfig,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Option<Self>, StoreError> {
        let Some(factory) = BuiltinTieredStoreProviderFactory::select(&config) else {
            return Ok(None);
        };
        TieredStoreFactory::open_with_metrics(config, factory, metrics, parent_task_group)
    }

    /// Opens a built-in provider from a validated plan.
    ///
    /// # Errors
    ///
    /// Returns an error only for operational provider or Store composition failure.
    pub fn new_planned(
        plan: TieredProviderOpenPlan<BuiltinTieredStoreProviderFactory>,
        parent_task_group: TaskGroup,
    ) -> Result<Self, StoreError> {
        Self::new_planned_with_metrics(plan, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    /// Opens a built-in provider from a validated plan with metrics.
    ///
    /// # Errors
    ///
    /// Returns an error only for operational provider or Store composition failure.
    pub fn new_planned_with_metrics(
        plan: TieredProviderOpenPlan<BuiltinTieredStoreProviderFactory>,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Self, StoreError> {
        TieredStoreFactory::open_planned_with_metrics(plan, metrics, parent_task_group)
    }
}

impl<P> TieredStore<P>
where
    P: TieredStoreProvider,
{
    /// Opens a caller-supplied provider from raw configuration.
    ///
    /// Returns `Ok(None)` when Tiered Store is disabled or the deterministic
    /// configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns an error only when operational Store composition fails.
    pub fn with_provider(
        config: TieredStoreConfig,
        provider: P,
        parent_task_group: TaskGroup,
    ) -> Result<Option<Self>, StoreError> {
        Self::with_provider_and_metrics(config, provider, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    /// Opens a caller-supplied provider from raw configuration with metrics.
    ///
    /// Returns `Ok(None)` when Tiered Store is disabled or the deterministic
    /// configuration is invalid.
    ///
    /// # Errors
    ///
    /// Returns an error only when operational Store composition fails.
    pub fn with_provider_and_metrics(
        config: TieredStoreConfig,
        provider: P,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Option<Self>, StoreError> {
        let Some(plan) = TieredStoreOpenPlan::try_for_direct_provider(config) else {
            return Ok(None);
        };
        Self::with_provider_planned_and_metrics(plan, provider, metrics, parent_task_group).map(Some)
    }

    /// Opens a caller-supplied provider from a validated capability.
    ///
    /// # Errors
    ///
    /// Returns an error only when operational Store composition fails.
    pub fn with_provider_planned(
        plan: TieredStoreOpenPlan,
        provider: P,
        parent_task_group: TaskGroup,
    ) -> Result<Self, StoreError> {
        Self::with_provider_planned_and_metrics(plan, provider, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    /// Opens a caller-supplied provider from a validated capability with metrics.
    ///
    /// # Errors
    ///
    /// Returns an error only when operational Store composition fails.
    pub fn with_provider_planned_and_metrics(
        plan: TieredStoreOpenPlan,
        provider: P,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Self, StoreError> {
        let (config, descriptor) = plan.into_parts();
        Self::build(config, provider, descriptor, metrics, parent_task_group)
    }

    fn build(
        config: TieredStoreConfig,
        provider: P,
        provider_descriptor: Option<TieredProviderDescriptor>,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Self, StoreError> {
        let config = Arc::new(config);
        let shutdown = CancellationToken::new();
        let metadata_store = Arc::new(JsonMetadataStore::new_with_provider_descriptor(
            config.clone(),
            provider_descriptor,
        ));
        let metrics = Arc::new(TieredStoreMetrics::new(metrics));
        let flat_file_store = Arc::new(TieredFlatFileStore::new_with_metrics(
            config.clone(),
            metadata_store.clone(),
            provider,
            metrics.clone(),
        ));
        let dispatcher = Arc::new(DefaultTieredDispatcher::new_with_metrics(
            config.clone(),
            flat_file_store.clone(),
            shutdown.child_token(),
            metrics.clone(),
            parent_task_group.clone(),
        ));
        let fetcher = Arc::new(DefaultTieredMessageFetcher::new_with_metrics(
            config.clone(),
            flat_file_store.clone(),
            metrics.clone(),
        ));

        Ok(Self {
            config,
            metadata_store,
            flat_file_store,
            dispatcher,
            fetcher,
            metrics,
            services: TieredServiceSet::new(parent_task_group),
            shutdown,
            provider_descriptor,
        })
    }

    pub fn config(&self) -> &TieredStoreConfig {
        self.config.as_ref()
    }

    pub fn dispatcher(&self) -> Arc<DefaultTieredDispatcher<P>> {
        self.dispatcher.clone()
    }

    pub fn fetcher(&self) -> Arc<DefaultTieredMessageFetcher<P>> {
        self.fetcher.clone()
    }

    pub fn metrics(&self) -> Arc<TieredStoreMetrics> {
        self.metrics.clone()
    }

    /// Returns the declaration supplied by the startup provider factory.
    ///
    /// Stores created through a direct-provider constructor do not have a
    /// registered descriptor.
    pub const fn provider_descriptor(&self) -> Option<TieredProviderDescriptor> {
        self.provider_descriptor
    }
}

impl<P> TieredLifecycle for TieredStore<P>
where
    P: TieredStoreProvider,
{
    async fn load(&self) -> Result<(), StoreError> {
        let recover_service = CommitLogRecoverService::new(self.metadata_store.clone(), self.flat_file_store.clone());
        recover_service.recover().await?;
        self.dispatcher.load_progress().await
    }

    async fn start(&self) -> Result<(), StoreError> {
        self.dispatcher.start().await?;
        self.services
            .start_cleanup(
                self.config.clone(),
                self.flat_file_store.clone(),
                self.shutdown.child_token(),
            )
            .await
    }

    async fn shutdown(&self) -> Result<(), StoreError> {
        self.shutdown.cancel();
        self.dispatcher.shutdown().await?;
        self.services.shutdown().await?;
        self.flat_file_store.shutdown().await
    }

    async fn destroy(&self) -> Result<(), StoreError> {
        self.flat_file_store.destroy().await?;
        self.metadata_store.destroy().await?;
        self.dispatcher.destroy_progress().await
    }
}

impl<P> rocketmq_store_api::StoreLifecycle for TieredStore<P>
where
    P: TieredStoreProvider,
{
    async fn load(&mut self) -> Result<bool, StoreError> {
        TieredLifecycle::load(self)
            .await
            .map_err(|source| tiered_lifecycle_error(StoreOperation::Load, source))?;
        Ok(true)
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        TieredLifecycle::start(self)
            .await
            .map_err(|source| tiered_lifecycle_error(StoreOperation::Start, source))
    }

    async fn shutdown(&mut self) -> Result<(), StoreError> {
        TieredLifecycle::shutdown(self)
            .await
            .map_err(|source| tiered_lifecycle_error(StoreOperation::Shutdown, source))
    }
}

fn tiered_lifecycle_error(operation: StoreOperation, source: StoreError) -> StoreError {
    let _ = operation;
    source
}

#[cfg(test)]
mod store_api_tests {
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_store_api::StoreError;
    use rocketmq_store_api::StoreLifecycle;

    use super::*;

    #[tokio::test]
    async fn tiered_store_implements_backend_neutral_lifecycle() {
        let temp_dir = tempfile::tempdir().expect("create tiered lifecycle temp dir");
        let context = RuntimeContext::from_current("tiered-store-api-test");
        let mut store = TieredStore::new(
            TieredStoreConfig {
                backend_provider: "memory".to_owned(),
                store_path_root_dir: temp_dir.path().join("tiered-lifecycle"),
                ..TieredStoreConfig::default()
            },
            context.root_group().clone(),
        )
        .expect("construct tiered store")
        .expect("valid tiered store configuration");

        assert!(StoreLifecycle::load(&mut store).await.expect("load tiered store"));
        StoreLifecycle::start(&mut store).await.expect("start tiered store");
        StoreLifecycle::shutdown(&mut store)
            .await
            .expect("shutdown tiered store");
    }

    #[test]
    fn tiered_lifecycle_mapping_retains_each_operation_and_typed_source() {
        for operation in [StoreOperation::Load, StoreOperation::Start, StoreOperation::Shutdown] {
            let original = StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, operation)
                .in_component(rocketmq_store_api::StoreComponent::TieredStore)
                .with_source(std::io::Error::other("private remote response"));
            let mapped_error = tiered_lifecycle_error(operation, original);

            assert_eq!(&rocketmq_error::STORAGE_INTERNAL_FAILURE, mapped_error.descriptor());
            assert_eq!(operation, mapped_error.operation());
            assert_eq!(
                rocketmq_store_api::StoreComponent::TieredStore,
                mapped_error.component()
            );
            assert!(std::error::Error::source(&mapped_error)
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .is_some());
            assert!(mapped_error
                .public_view()
                .expect("valid public view")
                .fields()
                .next()
                .is_none());
            assert!(!format!("{mapped_error:?}").contains("private remote response"));
        }
    }
}
