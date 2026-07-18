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

//! Explicit registry and local development adapters for secret providers.

mod encrypted_file;
mod environment;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use rocketmq_security_api::SecretProvider;
use rocketmq_security_api::SecretProviderError;
use rocketmq_security_api::SecretProviderId;

pub use encrypted_file::EncryptedFileSecretProvider;
pub use environment::EnvironmentSecretProvider;

/// Explicit, composition-root-owned provider registry. It deliberately has no global singleton.
#[derive(Default)]
pub struct SecretProviderRegistry {
    providers: BTreeMap<SecretProviderId, Arc<dyn SecretProvider>>,
}

impl SecretProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one injected provider and rejects identifier replacement.
    ///
    /// # Errors
    ///
    /// Returns [`SecretProviderError::DuplicateProvider`] when the identifier is already present.
    pub fn register(&mut self, provider: Arc<dyn SecretProvider>) -> Result<(), SecretProviderError> {
        let id = provider.id().clone();
        if self.providers.contains_key(&id) {
            return Err(SecretProviderError::DuplicateProvider);
        }
        self.providers.insert(id, provider);
        Ok(())
    }

    /// Resolves an explicitly selected provider without falling back to another implementation.
    ///
    /// # Errors
    ///
    /// Returns [`SecretProviderError::ProviderNotRegistered`] when no exact identifier is present.
    pub fn provider(&self, id: &SecretProviderId) -> Result<&Arc<dyn SecretProvider>, SecretProviderError> {
        self.providers.get(id).ok_or(SecretProviderError::ProviderNotRegistered)
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl fmt::Debug for SecretProviderRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretProviderRegistry")
            .field("provider_ids", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}
