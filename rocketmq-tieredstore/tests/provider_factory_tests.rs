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

use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_runtime::RuntimeContext;
use rocketmq_store_api::StoreError;
use rocketmq_tieredstore::MemoryProvider;
use rocketmq_tieredstore::MemoryProviderFactory;
use rocketmq_tieredstore::PosixProviderFactory;
use rocketmq_tieredstore::TieredProviderCapabilities;
use rocketmq_tieredstore::TieredProviderCapability;
use rocketmq_tieredstore::TieredProviderDescriptor;
use rocketmq_tieredstore::TieredProviderOpenPlan;
use rocketmq_tieredstore::TieredProviderPersistence;
use rocketmq_tieredstore::TieredStore;
use rocketmq_tieredstore::TieredStoreConfig;
use rocketmq_tieredstore::TieredStoreFactory;
use rocketmq_tieredstore::TieredStoreOpenPlan;
use rocketmq_tieredstore::TieredStoreProviderFactory;

#[derive(Clone)]
struct CountingMemoryFactory {
    calls: Arc<AtomicUsize>,
    descriptor: TieredProviderDescriptor,
}

impl CountingMemoryFactory {
    fn new(descriptor: TieredProviderDescriptor) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            descriptor,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl TieredStoreProviderFactory for CountingMemoryFactory {
    type Provider = MemoryProvider;

    fn descriptor(&self) -> TieredProviderDescriptor {
        self.descriptor
    }

    fn create(&self, _plan: &TieredStoreOpenPlan) -> Result<Self::Provider, StoreError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(MemoryProvider::default())
    }
}

fn indexed_memory_descriptor(id: &'static str) -> TieredProviderDescriptor {
    TieredProviderDescriptor::new(
        id,
        1,
        TieredProviderPersistence::Ephemeral,
        TieredProviderCapabilities::EMPTY
            .with(TieredProviderCapability::AtomicWrite)
            .with(TieredProviderCapability::AtomicRename)
            .with(TieredProviderCapability::PrefixListing)
            .with(TieredProviderCapability::PrefixDelete),
    )
}

fn assert_concrete_memory_store(_store: &TieredStore<MemoryProvider>) {}

#[tokio::test]
async fn custom_factory_is_resolved_once_at_startup() {
    let context = RuntimeContext::from_current("tiered-provider-factory-test");
    let factory = CountingMemoryFactory::new(indexed_memory_descriptor("test-memory"));
    let config = TieredStoreConfig {
        backend_provider: "test-memory".to_owned(),
        ..TieredStoreConfig::default()
    };

    let store = TieredStoreFactory::open(config, factory.clone(), context.root_group().clone())
        .expect("custom provider factory should open the tiered store")
        .expect("valid custom provider configuration");

    assert_concrete_memory_store(&store);
    assert_eq!(factory.calls(), 1);
    assert_eq!(store.provider_descriptor(), Some(factory.descriptor()));
    let _ = store.config();
    assert_eq!(factory.calls(), 1, "hot-path access must not consult the factory");
}

#[tokio::test]
async fn provider_id_mismatch_is_rejected_before_construction() {
    let context = RuntimeContext::from_current("tiered-provider-id-test");
    let factory = CountingMemoryFactory::new(indexed_memory_descriptor("test-memory"));
    let config = TieredStoreConfig {
        backend_provider: "other-provider".to_owned(),
        ..TieredStoreConfig::default()
    };

    let store = TieredStoreFactory::open(config, factory.clone(), context.root_group().clone())
        .expect("validation must not produce an operational error");

    assert!(store.is_none());
    assert_eq!(factory.calls(), 0);
}

#[tokio::test]
async fn indexed_store_requires_provider_publication_capabilities() {
    let context = RuntimeContext::from_current("tiered-provider-capability-test");
    let descriptor = TieredProviderDescriptor::new(
        "minimal-memory",
        1,
        TieredProviderPersistence::Ephemeral,
        TieredProviderCapabilities::EMPTY,
    );
    let factory = CountingMemoryFactory::new(descriptor);
    let config = TieredStoreConfig {
        backend_provider: "minimal-memory".to_owned(),
        message_index_enable: true,
        ..TieredStoreConfig::default()
    };

    let store = TieredStoreFactory::open(config, factory.clone(), context.root_group().clone())
        .expect("validation must not produce an operational error");

    assert!(store.is_none());
    assert_eq!(factory.calls(), 0);
}

#[tokio::test]
async fn non_indexed_store_still_requires_provider_publication_capabilities() {
    let context = RuntimeContext::from_current("tiered-provider-base-contract-test");
    let descriptor = TieredProviderDescriptor::new(
        "minimal-memory",
        1,
        TieredProviderPersistence::Ephemeral,
        TieredProviderCapabilities::EMPTY,
    );
    let factory = CountingMemoryFactory::new(descriptor);
    let config = TieredStoreConfig {
        backend_provider: "minimal-memory".to_owned(),
        message_index_enable: false,
        ..TieredStoreConfig::default()
    };

    let store = TieredStoreFactory::open(config, factory.clone(), context.root_group().clone())
        .expect("validation must not produce an operational error");

    assert!(store.is_none());
    assert_eq!(factory.calls(), 0);
}

#[tokio::test]
async fn posix_factory_rejects_an_empty_provider_root_at_startup() {
    let context = RuntimeContext::from_current("tiered-provider-empty-root-test");
    let config = TieredStoreConfig {
        backend_provider: "posix".to_owned(),
        store_path_root_dir: PathBuf::new(),
        ..TieredStoreConfig::default()
    };

    let store = TieredStoreFactory::open(config, PosixProviderFactory, context.root_group().clone())
        .expect("validation must not produce an operational error");

    assert!(store.is_none());
}

#[test]
fn open_plan_debug_redacts_raw_configuration() {
    let config = TieredStoreConfig {
        backend_provider: "secret-provider-id".to_owned(),
        store_path_root_dir: PathBuf::from("sensitive-provider-root"),
        ..TieredStoreConfig::default()
    };
    let factory = CountingMemoryFactory::new(indexed_memory_descriptor("secret-provider-id"));
    let plan = TieredProviderOpenPlan::try_new(config, factory).expect("valid plan");

    let debug = format!("{plan:?}");
    assert!(!debug.contains("secret-provider-id"));
    assert!(!debug.contains("sensitive-provider-root"));
}

#[test]
fn provider_plan_rejects_cross_factory_configuration() {
    let config = TieredStoreConfig {
        backend_provider: "posix".to_owned(),
        ..TieredStoreConfig::default()
    };

    assert!(TieredProviderOpenPlan::try_new(config, MemoryProviderFactory).is_none());
}

#[test]
fn custom_configuration_cannot_bind_a_builtin_factory() {
    let config = TieredStoreConfig {
        backend_provider: "custom-memory".to_owned(),
        ..TieredStoreConfig::default()
    };

    assert!(TieredProviderOpenPlan::try_new(config, MemoryProviderFactory).is_none());
}

#[tokio::test]
async fn planned_open_consumes_the_factory_that_validated_the_plan() {
    let context = RuntimeContext::from_current("tiered-provider-bound-plan-test");
    let factory = CountingMemoryFactory::new(indexed_memory_descriptor("bound-memory"));
    let config = TieredStoreConfig {
        backend_provider: "bound-memory".to_owned(),
        ..TieredStoreConfig::default()
    };
    let plan = TieredProviderOpenPlan::try_new(config, factory.clone()).expect("valid bound plan");

    let store =
        TieredStoreFactory::open_planned(plan, context.root_group().clone()).expect("bound provider should open");

    assert_concrete_memory_store(&store);
    assert_eq!(factory.calls(), 1);
    assert_eq!(store.provider_descriptor(), Some(factory.descriptor()));
}

#[tokio::test]
async fn direct_provider_open_consumes_a_public_validated_plan() {
    let context = RuntimeContext::from_current("tiered-direct-provider-plan-test");
    let config = TieredStoreConfig {
        store_path_root_dir: PathBuf::from("sensitive-direct-provider-root-canary"),
        ..TieredStoreConfig::default()
    };
    let plan = TieredStoreOpenPlan::try_for_direct_provider(config).expect("valid direct-provider plan");
    assert!(!format!("{plan:?}").contains("sensitive-direct-provider-root-canary"));

    let store = TieredStore::with_provider_planned(plan, MemoryProvider::default(), context.root_group().clone())
        .expect("validated direct provider should compose");

    assert_concrete_memory_store(&store);
    assert_eq!(store.provider_descriptor(), None);
}
