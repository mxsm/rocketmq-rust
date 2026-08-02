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

use rocketmq_error::RocketMQError;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetricsRecorder;
use rocketmq_runtime::TaskGroup;

use crate::config::TieredStoreConfig;
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

impl TieredStoreFactory {
    /// Opens a Tiered Store with a compile-time injected provider factory.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider declaration is incompatible with the
    /// selected configuration or provider construction fails.
    pub fn open<F>(
        config: TieredStoreConfig,
        factory: F,
        parent_task_group: TaskGroup,
    ) -> Result<TieredStore<F::Provider>, RocketMQError>
    where
        F: TieredStoreProviderFactory,
    {
        Self::open_with_metrics(config, factory, TieredStoreMetricsRecorder::noop(), parent_task_group)
    }

    /// Opens a Tiered Store with an explicit metrics recorder.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider declaration is incompatible with the
    /// selected configuration or provider construction fails.
    pub fn open_with_metrics<F>(
        config: TieredStoreConfig,
        factory: F,
        metrics: TieredStoreMetricsRecorder,
        parent_task_group: TaskGroup,
    ) -> Result<TieredStore<F::Provider>, RocketMQError>
    where
        F: TieredStoreProviderFactory,
    {
        let descriptor = factory.descriptor();
        validate_provider(&config, descriptor)?;
        let provider = factory.create(&config)?;
        TieredStore::with_provider_descriptor_and_metrics(config, provider, descriptor, metrics, parent_task_group)
    }
}

fn validate_provider(config: &TieredStoreConfig, descriptor: TieredProviderDescriptor) -> Result<(), RocketMQError> {
    if descriptor.id().is_empty() {
        return Err(RocketMQError::illegal_argument(
            "tiered provider factory id must not be empty",
        ));
    }
    if descriptor.config_version() == 0 {
        return Err(RocketMQError::illegal_argument(format!(
            "tiered provider {} config version must be non-zero",
            descriptor.id()
        )));
    }
    if let TieredProviderPersistence::Stable { format, version } = descriptor.persistence() {
        if format.is_empty() || version == 0 {
            return Err(RocketMQError::illegal_argument(format!(
                "tiered provider {} persistence format and version must be stable",
                descriptor.id()
            )));
        }
    }
    if config.backend_provider != descriptor.id() {
        return Err(RocketMQError::illegal_argument(format!(
            "configured tiered backend provider {} does not match factory {}",
            config.backend_provider,
            descriptor.id()
        )));
    }
    let missing = INDEX_PROVIDER_CAPABILITIES
        .into_iter()
        .filter(|capability| !descriptor.capabilities().supports(*capability))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(RocketMQError::illegal_argument(format!(
            "tiered provider {} is missing index-recovery capabilities: {missing:?}",
            descriptor.id()
        )));
    }
    Ok(())
}
