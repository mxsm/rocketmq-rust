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

//! Compile-time provider factories resolved during Tiered Store startup.

use rocketmq_store_api::StoreError;

use crate::factory::TieredStoreOpenPlan;
use crate::provider::MemoryProvider;
use crate::provider::PosixProvider;
use crate::provider::ProviderKind;
use crate::provider::TieredStoreProvider;

/// Optional provider operation declared at the startup composition boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u32)]
pub enum TieredProviderCapability {
    /// Publishes a small object through atomic replacement.
    AtomicWrite = 1 << 0,
    /// Atomically renames a file or prefix.
    AtomicRename = 1 << 1,
    /// Lists all objects rooted at a prefix.
    PrefixListing = 1 << 2,
    /// Deletes all objects rooted at a prefix.
    PrefixDelete = 1 << 3,
    /// Exposes an explicit durability synchronization boundary.
    DurableSync = 1 << 4,
}

/// Set of optional operations implemented by a Tiered Store provider.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TieredProviderCapabilities(u32);

impl TieredProviderCapabilities {
    /// No optional operations beyond the required segment read/write API.
    pub const EMPTY: Self = Self(0);

    /// Returns a declaration that additionally includes `capability`.
    pub const fn with(self, capability: TieredProviderCapability) -> Self {
        Self(self.0 | capability as u32)
    }

    /// Returns whether the provider declares `capability`.
    pub const fn supports(self, capability: TieredProviderCapability) -> bool {
        self.0 & capability as u32 != 0
    }
}

/// Persisted-data compatibility promised by a provider implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TieredProviderPersistence {
    /// Data is process-local and is not expected to survive restart.
    Ephemeral,
    /// Data uses a stable provider-owned format.
    Stable {
        /// Stable identifier for the persisted format family.
        format: &'static str,
        /// Current compatible format version.
        version: u32,
    },
}

/// Startup declaration for one compile-time Tiered Store provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieredProviderDescriptor {
    id: &'static str,
    config_version: u32,
    persistence: TieredProviderPersistence,
    capabilities: TieredProviderCapabilities,
}

impl TieredProviderDescriptor {
    /// Creates a provider declaration validated when the factory is opened.
    pub const fn new(
        id: &'static str,
        config_version: u32,
        persistence: TieredProviderPersistence,
        capabilities: TieredProviderCapabilities,
    ) -> Self {
        Self {
            id,
            config_version,
            persistence,
            capabilities,
        }
    }

    /// Provider identifier selected by the `backend_provider` field on
    /// [`crate::config::TieredStoreConfig`].
    pub const fn id(self) -> &'static str {
        self.id
    }

    /// Version of the provider-owned configuration contract.
    pub const fn config_version(self) -> u32 {
        self.config_version
    }

    /// Provider persistence compatibility declaration.
    pub const fn persistence(self) -> TieredProviderPersistence {
        self.persistence
    }

    /// Optional operations implemented by the provider.
    pub const fn capabilities(self) -> TieredProviderCapabilities {
        self.capabilities
    }
}

/// Compile-time injection point that constructs a provider once during startup.
pub trait TieredStoreProviderFactory: Send + Sync {
    /// Concrete provider retained by Tiered Store hot paths.
    type Provider: TieredStoreProvider;

    /// Returns the provider's startup and persistence declaration.
    fn descriptor(&self) -> TieredProviderDescriptor;

    /// Constructs the configured provider.
    ///
    /// # Errors
    ///
    /// Returns an error when provider-owned configuration or resources cannot
    /// be established.
    fn create(&self, plan: &TieredStoreOpenPlan) -> Result<Self::Provider, StoreError>;
}

/// Factory for the built-in POSIX provider.
#[derive(Debug, Clone, Copy, Default)]
pub struct PosixProviderFactory;

impl TieredStoreProviderFactory for PosixProviderFactory {
    type Provider = PosixProvider;

    fn descriptor(&self) -> TieredProviderDescriptor {
        TieredProviderDescriptor::new(
            "posix",
            1,
            TieredProviderPersistence::Stable {
                format: "rocketmq-tiered-posix",
                version: 1,
            },
            indexed_provider_capabilities().with(TieredProviderCapability::DurableSync),
        )
    }

    fn create(&self, plan: &TieredStoreOpenPlan) -> Result<Self::Provider, StoreError> {
        Ok(PosixProvider::new(plan.config().store_path_root_dir.clone()))
    }
}

/// Factory for the built-in in-memory provider.
#[derive(Debug, Clone, Copy, Default)]
pub struct MemoryProviderFactory;

impl TieredStoreProviderFactory for MemoryProviderFactory {
    type Provider = MemoryProvider;

    fn descriptor(&self) -> TieredProviderDescriptor {
        TieredProviderDescriptor::new(
            "memory",
            1,
            TieredProviderPersistence::Ephemeral,
            indexed_provider_capabilities(),
        )
    }

    fn create(&self, _plan: &TieredStoreOpenPlan) -> Result<Self::Provider, StoreError> {
        Ok(MemoryProvider::default())
    }
}

/// Startup-selected factory for stock POSIX and in-memory providers.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum BuiltinTieredStoreProviderFactory {
    /// Built-in POSIX provider factory.
    Posix(PosixProviderFactory),
    /// Built-in in-memory provider factory.
    Memory(MemoryProviderFactory),
}

impl BuiltinTieredStoreProviderFactory {
    /// Selects a stock factory from startup configuration.
    ///
    /// Returns `None` when the configured provider is not built in.
    pub fn select(config: &crate::config::TieredStoreConfig) -> Option<Self> {
        match config.backend_provider.as_str() {
            "posix" => Some(Self::Posix(PosixProviderFactory)),
            "memory" => Some(Self::Memory(MemoryProviderFactory)),
            _ => None,
        }
    }
}

impl TieredStoreProviderFactory for BuiltinTieredStoreProviderFactory {
    type Provider = ProviderKind;

    fn descriptor(&self) -> TieredProviderDescriptor {
        match self {
            Self::Posix(factory) => factory.descriptor(),
            Self::Memory(factory) => factory.descriptor(),
        }
    }

    fn create(&self, plan: &TieredStoreOpenPlan) -> Result<Self::Provider, StoreError> {
        match self {
            Self::Posix(factory) => factory.create(plan).map(ProviderKind::Posix),
            Self::Memory(factory) => factory.create(plan).map(ProviderKind::Memory),
        }
    }
}

const fn indexed_provider_capabilities() -> TieredProviderCapabilities {
    TieredProviderCapabilities::EMPTY
        .with(TieredProviderCapability::AtomicWrite)
        .with(TieredProviderCapability::AtomicRename)
        .with(TieredProviderCapability::PrefixListing)
        .with(TieredProviderCapability::PrefixDelete)
}
