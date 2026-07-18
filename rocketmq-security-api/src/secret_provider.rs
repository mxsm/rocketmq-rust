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

//! Runtime-neutral contracts for injected secret providers.

use std::fmt;

use cheetah_string::CheetahString;
use thiserror::Error;
use zeroize::Zeroize;

/// Stable identifier for an explicitly registered secret provider.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretProviderId(CheetahString);

impl SecretProviderId {
    /// Validates and creates a provider identifier.
    pub fn new(value: impl Into<CheetahString>) -> Result<Self, SecretIdentifierError> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SecretProviderId").field(&self.0).finish()
    }
}

/// Validated logical secret name. The name cannot contain path separators or traversal segments.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretName(CheetahString);

impl SecretName {
    /// Validates and creates a secret name.
    pub fn new(value: impl Into<CheetahString>) -> Result<Self, SecretIdentifierError> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretName([REDACTED])")
    }
}

/// Validation failure for provider identifiers and logical secret names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SecretIdentifierError {
    #[error("secret identifier is empty")]
    Empty,
    #[error("secret identifier exceeds 128 bytes")]
    TooLong,
    #[error("secret identifier contains unsupported characters")]
    UnsupportedCharacter,
}

fn validate_identifier(value: &str) -> Result<(), SecretIdentifierError> {
    if value.is_empty() {
        return Err(SecretIdentifierError::Empty);
    }
    if value.len() > 128 {
        return Err(SecretIdentifierError::TooLong);
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || value == "."
        || value == ".."
        || value.starts_with('.')
    {
        return Err(SecretIdentifierError::UnsupportedCharacter);
    }
    Ok(())
}

/// Sensitive byte material that is redacted and zeroized when dropped.
pub struct SecretMaterial(Vec<u8>);

impl SecretMaterial {
    /// Creates non-empty secret material.
    pub fn new(value: Vec<u8>) -> Result<Self, SecretMaterialError> {
        if value.is_empty() {
            return Err(SecretMaterialError::Empty);
        }
        Ok(Self(value))
    }

    /// Borrows the material for the shortest practical operation scope.
    pub fn expose_secret(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl Zeroize for SecretMaterial {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl Drop for SecretMaterial {
    fn drop(&mut self) {
        self.zeroize();
    }
}

/// Failure to construct valid secret material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SecretMaterialError {
    #[error("secret material is empty")]
    Empty,
}

/// Version assigned by a provider supporting optimistic updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretVersion(u64);

impl SecretVersion {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Secret value returned together with its optional provider version.
pub struct VersionedSecret {
    material: SecretMaterial,
    version: Option<SecretVersion>,
}

impl VersionedSecret {
    pub fn new(material: SecretMaterial, version: Option<SecretVersion>) -> Self {
        Self { material, version }
    }

    pub fn material(&self) -> &SecretMaterial {
        &self.material
    }

    pub fn into_material(self) -> SecretMaterial {
        self.material
    }

    pub const fn version(&self) -> Option<SecretVersion> {
        self.version
    }
}

impl fmt::Debug for VersionedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionedSecret")
            .field("material", &"[REDACTED]")
            .field("version", &self.version)
            .finish()
    }
}

/// Mutation support declared by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretAccess {
    ReadOnly,
    ReadWrite,
}

/// Storage class declared by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretPersistence {
    ProcessEnvironment,
    EncryptedLocalStorage,
    ExternalService,
}

/// Concurrency contract declared by a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretVersioning {
    Unversioned,
    Optimistic,
}

/// Typed capabilities exposed before a provider is selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretProviderCapabilities {
    access: SecretAccess,
    persistence: SecretPersistence,
    versioning: SecretVersioning,
}

impl SecretProviderCapabilities {
    pub const fn new(access: SecretAccess, persistence: SecretPersistence, versioning: SecretVersioning) -> Self {
        Self {
            access,
            persistence,
            versioning,
        }
    }

    pub const fn access(self) -> SecretAccess {
        self.access
    }

    pub const fn persistence(self) -> SecretPersistence {
        self.persistence
    }

    pub const fn versioning(self) -> SecretVersioning {
        self.versioning
    }
}

/// Fail-closed error surface shared by provider implementations and registries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SecretProviderError {
    #[error("secret provider is not registered")]
    ProviderNotRegistered,
    #[error("secret provider is already registered")]
    DuplicateProvider,
    #[error("secret material was not found")]
    NotFound,
    #[error("secret provider is read-only")]
    ReadOnly,
    #[error("secret material is invalid")]
    InvalidMaterial,
    #[error("secret provider configuration is invalid")]
    InvalidConfiguration,
    #[error("secret storage permissions are not owner-only")]
    InsecurePermissions,
    #[error("secret version conflict")]
    VersionConflict,
    #[error("secret envelope is invalid or authentication failed")]
    InvalidEnvelope,
    #[error("secret provider is unavailable")]
    Unavailable,
    #[error("secret provider is unsupported on this platform")]
    UnsupportedPlatform,
}

/// Synchronous, runtime-neutral boundary implemented by injected providers.
pub trait SecretProvider: Send + Sync {
    fn id(&self) -> &SecretProviderId;

    fn capabilities(&self) -> SecretProviderCapabilities;

    /// Reads a secret without exposing provider implementation types.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`SecretProviderError`] when the material cannot be obtained safely.
    fn read(&self, name: &SecretName) -> Result<VersionedSecret, SecretProviderError>;

    /// Creates or atomically updates a secret. `None` is create-only; an existing secret requires
    /// its current version to prevent lost updates.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`SecretProviderError`] when persistence or version validation fails.
    fn write(
        &self,
        name: &SecretName,
        material: SecretMaterial,
        expected_version: Option<SecretVersion>,
    ) -> Result<SecretVersion, SecretProviderError>;
}

#[cfg(test)]
mod tests {
    use zeroize::Zeroize;

    use super::*;

    #[test]
    fn identifiers_reject_traversal_and_separators() {
        for value in ["", ".", "..", ".hidden", "a/b", "a\\b", "a:b", "a b"] {
            assert!(SecretName::new(value).is_err(), "accepted {value:?}");
        }
        assert_eq!(
            SecretName::new("broker-admin_key.1").unwrap().as_str(),
            "broker-admin_key.1"
        );
    }

    #[test]
    fn material_and_name_debug_are_redacted() {
        let material = SecretMaterial::new(b"visible-only-through-explicit-access".to_vec()).unwrap();
        let name = SecretName::new("cluster-private-key").unwrap();
        assert_eq!(format!("{material:?}"), "[REDACTED]");
        assert_eq!(format!("{name:?}"), "SecretName([REDACTED])");
    }

    #[test]
    fn explicit_zeroize_clears_material() {
        let mut material = SecretMaterial::new(vec![7; 32]).unwrap();
        material.zeroize();
        assert!(material.is_empty());
    }
}
