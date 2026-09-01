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

use rocketmq_error::RocketMQError;
use rocketmq_runtime::TaskGroup;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::dispatcher::DefaultTieredDispatcher;
use crate::dispatcher::TieredDispatcher;
use crate::factory::TieredStoreFactory;
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
    pub fn new(config: TieredStoreConfig, parent_task_group: TaskGroup) -> Result<Self, RocketMQError> {
        Self::new_with_metrics(config, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    pub fn new_with_metrics(
        config: TieredStoreConfig,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Self, RocketMQError> {
        let factory = BuiltinTieredStoreProviderFactory::select(&config)?;
        TieredStoreFactory::open_with_metrics(config, factory, metrics, parent_task_group)
    }
}

impl<P> TieredStore<P>
where
    P: TieredStoreProvider,
{
    pub fn with_provider(
        config: TieredStoreConfig,
        provider: P,
        parent_task_group: TaskGroup,
    ) -> Result<Self, RocketMQError> {
        Self::with_provider_and_metrics(config, provider, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    pub fn with_provider_and_metrics(
        config: TieredStoreConfig,
        provider: P,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Self, RocketMQError> {
        Self::build(config, provider, None, metrics, parent_task_group)
    }

    pub(crate) fn with_provider_descriptor_and_metrics(
        config: TieredStoreConfig,
        provider: P,
        descriptor: TieredProviderDescriptor,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Self, RocketMQError> {
        Self::build(config, provider, Some(descriptor), metrics, parent_task_group)
    }

    fn build(
        config: TieredStoreConfig,
        provider: P,
        provider_descriptor: Option<TieredProviderDescriptor>,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<Self, RocketMQError> {
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
    async fn load(&self) -> Result<(), RocketMQError> {
        let recover_service = CommitLogRecoverService::new(self.metadata_store.clone(), self.flat_file_store.clone());
        recover_service.recover().await?;
        self.dispatcher.load_progress().await
    }

    async fn start(&self) -> Result<(), RocketMQError> {
        self.dispatcher.start().await?;
        self.services
            .start_cleanup(
                self.config.clone(),
                self.flat_file_store.clone(),
                self.shutdown.child_token(),
            )
            .await
    }

    async fn shutdown(&self) -> Result<(), RocketMQError> {
        self.shutdown.cancel();
        self.dispatcher.shutdown().await?;
        self.services.shutdown().await?;
        self.flat_file_store.shutdown().await
    }

    async fn destroy(&self) -> Result<(), RocketMQError> {
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

fn tiered_lifecycle_error(operation: StoreOperation, source: RocketMQError) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, operation)
        .in_component(StoreComponent::TieredStore)
        .with_source(source)
}

#[cfg(test)]
mod store_api_tests {
    use rocketmq_error::RocketMQError;
    use rocketmq_runtime::RuntimeContext;
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
        .expect("create tiered store");

        assert!(StoreLifecycle::load(&mut store).await.expect("load tiered store"));
        StoreLifecycle::start(&mut store).await.expect("start tiered store");
        StoreLifecycle::shutdown(&mut store)
            .await
            .expect("shutdown tiered store");
    }

    #[test]
    fn tiered_lifecycle_mapping_retains_each_operation_and_typed_source() {
        for operation in [StoreOperation::Load, StoreOperation::Start, StoreOperation::Shutdown] {
            let error = tiered_lifecycle_error(
                operation,
                RocketMQError::internal("tiered-test", std::io::Error::other("private remote response")),
            );

            assert_eq!(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, error.descriptor());
            assert_eq!(operation, error.operation());
            assert_eq!(StoreComponent::TieredStore, error.component());
            assert!(std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<RocketMQError>())
                .is_some());
            assert!(error
                .public_view()
                .expect("valid public view")
                .fields()
                .next()
                .is_none());
            assert!(!format!("{error:?}").contains("private remote response"));
        }
    }
}
