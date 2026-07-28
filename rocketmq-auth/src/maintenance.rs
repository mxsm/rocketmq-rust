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

//! Maintenance-policy loading adapter.
//!
//! Runtime-neutral policy validation and authorization live in
//! [`rocketmq_security_api::maintenance`]. This module owns only external I/O,
//! path confinement, JSON decoding, and SHA-256 pin verification. The public
//! re-exports preserve the former `rocketmq_auth` paths for one compatibility
//! cycle.

use std::fs;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

pub use rocketmq_security_api::maintenance::MaintenanceAuthorizationContext;
pub use rocketmq_security_api::maintenance::MaintenanceAuthorizationError;
pub use rocketmq_security_api::maintenance::MaintenanceAuthorizationGrant;
pub use rocketmq_security_api::maintenance::MaintenanceAuthorizer;
pub use rocketmq_security_api::maintenance::MaintenanceCapability;
pub use rocketmq_security_api::maintenance::MaintenancePolicy;
pub use rocketmq_security_api::maintenance::MaintenancePrincipalBinding;
pub use rocketmq_security_api::maintenance::MaintenanceRequestClass;
pub use rocketmq_security_api::maintenance::MaintenanceResourceBudget;
pub use rocketmq_security_api::maintenance::MaintenanceRole;
pub use rocketmq_security_api::maintenance::MaintenanceRoleGrant;
pub use rocketmq_security_api::maintenance::MAINTENANCE_POLICY_SCHEMA_VERSION;
use rocketmq_security_api::MaintenancePolicyError as MaintenanceContractError;
use rocketmq_security_api::ValidatedMaintenancePolicy;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

/// Immutable reference used to pin a policy by path, version, and SHA-256.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct MaintenancePolicyReference {
    /// Absolute path or path relative to the service configuration root.
    pub path: PathBuf,
    /// Expected policy version.
    pub version: u64,
    /// Expected lowercase SHA-256 of the exact policy bytes.
    pub sha256: String,
}

impl MaintenancePolicyReference {
    /// Loads and validates a policy relative to `configuration_root`.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the reference is unsafe, the file cannot be
    /// read, the digest or version does not match, JSON decoding fails, or the
    /// policy is not fail closed.
    pub fn load_from(
        &self,
        configuration_root: impl AsRef<Path>,
    ) -> Result<LoadedMaintenancePolicy, MaintenancePolicyError> {
        validate_sha256(&self.sha256)?;
        if self.version == 0 {
            return Err(MaintenancePolicyError::InvalidReference(
                "policy reference version must be greater than zero".to_string(),
            ));
        }
        if self.path.as_os_str().is_empty() {
            return Err(MaintenancePolicyError::InvalidReference(
                "policy reference path is empty".to_string(),
            ));
        }
        if self.path.is_relative()
            && self.path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(MaintenancePolicyError::InvalidReference(
                "relative policy path cannot escape the configuration root".to_string(),
            ));
        }

        let resolved_path = if self.path.is_absolute() {
            self.path.clone()
        } else {
            configuration_root.as_ref().join(&self.path)
        };
        let bytes = fs::read(&resolved_path).map_err(|source| MaintenancePolicyError::Read {
            path: resolved_path.clone(),
            source,
        })?;
        let actual_sha256 = hex::encode(Sha256::digest(&bytes));
        if actual_sha256 != self.sha256 {
            return Err(MaintenancePolicyError::DigestMismatch {
                expected: self.sha256.clone(),
                actual: actual_sha256,
            });
        }

        let policy: MaintenancePolicy =
            serde_json::from_slice(&bytes).map_err(|source| MaintenancePolicyError::Decode {
                path: resolved_path.clone(),
                source,
            })?;
        let policy = policy.into_validated()?;
        if policy.policy().policy_version != self.version {
            return Err(MaintenancePolicyError::VersionMismatch {
                expected: self.version,
                actual: policy.policy().policy_version,
            });
        }

        Ok(LoadedMaintenancePolicy {
            policy,
            reference: self.clone(),
            resolved_path,
        })
    }
}

/// A policy whose bytes, version, and semantic invariants were validated.
#[derive(Clone, Debug)]
pub struct LoadedMaintenancePolicy {
    policy: ValidatedMaintenancePolicy,
    reference: MaintenancePolicyReference,
    resolved_path: PathBuf,
}

impl LoadedMaintenancePolicy {
    /// Returns the validated policy.
    pub const fn policy(&self) -> &MaintenancePolicy {
        self.policy.policy()
    }

    /// Returns the immutable policy reference.
    pub const fn reference(&self) -> &MaintenancePolicyReference {
        &self.reference
    }

    /// Returns the resolved file used during validation.
    pub fn resolved_path(&self) -> &Path {
        &self.resolved_path
    }
}

impl From<LoadedMaintenancePolicy> for ValidatedMaintenancePolicy {
    fn from(loaded: LoadedMaintenancePolicy) -> Self {
        loaded.policy
    }
}

/// Policy reference, loading, pinning, or semantic validation failure.
#[derive(Debug, Error)]
pub enum MaintenancePolicyError {
    #[error("invalid maintenance policy reference: {0}")]
    InvalidReference(String),
    #[error("failed to read maintenance policy {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to decode maintenance policy {path}: {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("maintenance policy SHA-256 mismatch: expected {expected}, actual {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("maintenance policy version mismatch: expected {expected}, actual {actual}")]
    VersionMismatch { expected: u64, actual: u64 },
    #[error(transparent)]
    Contract(#[from] MaintenanceContractError),
}

fn validate_sha256(value: &str) -> Result<(), MaintenancePolicyError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(MaintenancePolicyError::InvalidReference(
            "policy SHA-256 must be 64 lowercase hexadecimal characters".to_string(),
        ))
    }
}
