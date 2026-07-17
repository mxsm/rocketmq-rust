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
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::dispatcher::DefaultTieredDispatcher;
use crate::dispatcher::TieredDispatcher;
use crate::fetcher::DefaultTieredMessageFetcher;
use crate::file::TieredFlatFileStore;
use crate::lifecycle::TieredLifecycle;
use crate::metadata::JsonMetadataStore;
use crate::metadata::TieredMetadataStore;
use crate::provider::ProviderKind;
use crate::provider::TieredStoreProvider;
use crate::service::CommitLogRecoverService;
use crate::service::TieredServiceSet;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetrics;

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
}

impl TieredStore<ProviderKind> {
    pub fn new(config: TieredStoreConfig) -> Result<Self, RocketMQError> {
        let provider = ProviderKind::from_config(&config)?;
        Self::with_provider(config, provider)
    }
}

impl<P> TieredStore<P>
where
    P: TieredStoreProvider,
{
    pub fn with_provider(config: TieredStoreConfig, provider: P) -> Result<Self, RocketMQError> {
        let config = Arc::new(config);
        let shutdown = CancellationToken::new();
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let metrics = Arc::new(TieredStoreMetrics::default());
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            metadata_store.clone(),
            provider,
        ));
        let dispatcher = Arc::new(DefaultTieredDispatcher::new_with_metrics(
            config.clone(),
            flat_file_store.clone(),
            shutdown.child_token(),
            metrics.clone(),
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
            services: TieredServiceSet::new(),
            shutdown,
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
    type Error = RocketMQError;

    async fn load(&mut self) -> Result<bool, Self::Error> {
        TieredLifecycle::load(self).await?;
        Ok(true)
    }

    async fn start(&mut self) -> Result<(), Self::Error> {
        TieredLifecycle::start(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        TieredLifecycle::shutdown(self).await
    }
}

#[cfg(test)]
mod store_api_tests {
    use rocketmq_store_api::StoreLifecycle;

    use super::*;

    #[tokio::test]
    async fn tiered_store_implements_backend_neutral_lifecycle() {
        let temp_dir = tempfile::tempdir().expect("create tiered lifecycle temp dir");
        let mut store = TieredStore::new(TieredStoreConfig {
            backend_provider: "memory".to_owned(),
            store_path_root_dir: temp_dir.path().join("tiered-lifecycle"),
            ..TieredStoreConfig::default()
        })
        .expect("create tiered store");

        assert!(StoreLifecycle::load(&mut store).await.expect("load tiered store"));
        StoreLifecycle::start(&mut store).await.expect("start tiered store");
        StoreLifecycle::shutdown(&mut store)
            .await
            .expect("shutdown tiered store");
    }
}
