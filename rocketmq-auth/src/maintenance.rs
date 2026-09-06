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
//! policy contract remains owned by `rocketmq-security-api`.

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use rocketmq_security_api::maintenance::MaintenancePolicy;
use rocketmq_security_api::ValidatedMaintenancePolicy;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use crate::AuthFailureKind;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;

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
    pub fn load_from(&self, configuration_root: impl AsRef<Path>) -> AuthServiceResult<LoadedMaintenancePolicy> {
        self.load_from_inner(configuration_root)
            .map_err(|source| AuthServiceError::with_source(AuthOperation::MaintainService, source.kind(), source))
    }

    fn load_from_inner(
        &self,
        configuration_root: impl AsRef<Path>,
    ) -> Result<LoadedMaintenancePolicy, MaintenancePolicyLoadError> {
        validate_sha256(&self.sha256)?;
        if self.version == 0 {
            return Err(MaintenancePolicyLoadError::InvalidReference(
                "policy reference version must be greater than zero".to_string(),
            ));
        }
        if self.path.as_os_str().is_empty() {
            return Err(MaintenancePolicyLoadError::InvalidReference(
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
            return Err(MaintenancePolicyLoadError::InvalidReference(
                "relative policy path cannot escape the configuration root".to_string(),
            ));
        }

        let resolved_path = if self.path.is_absolute() {
            self.path.clone()
        } else {
            configuration_root.as_ref().join(&self.path)
        };
        let bytes = fs::read(&resolved_path).map_err(|source| MaintenancePolicyLoadError::Read {
            path: resolved_path.clone(),
            source,
        })?;
        let actual_sha256 = hex::encode(Sha256::digest(&bytes));
        if actual_sha256 != self.sha256 {
            return Err(MaintenancePolicyLoadError::DigestMismatch {
                expected: self.sha256.clone(),
                actual: actual_sha256,
            });
        }

        let policy: MaintenancePolicy =
            serde_json::from_slice(&bytes).map_err(|source| MaintenancePolicyLoadError::Decode {
                path: resolved_path.clone(),
                source,
            })?;
        let policy = policy
            .into_validated()
            .map_err(|source| MaintenancePolicyLoadError::Contract(Box::new(source)))?;
        if policy.policy().policy_version != self.version {
            return Err(MaintenancePolicyLoadError::VersionMismatch {
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

#[derive(Debug)]
enum MaintenancePolicyLoadError {
    InvalidReference(String),
    Read { path: PathBuf, source: std::io::Error },
    Decode { path: PathBuf, source: serde_json::Error },
    DigestMismatch { expected: String, actual: String },
    VersionMismatch { expected: u64, actual: u64 },
    Contract(Box<dyn Error + Send + Sync + 'static>),
}

impl MaintenancePolicyLoadError {
    const fn kind(&self) -> AuthFailureKind {
        match self {
            Self::InvalidReference(_) | Self::Contract(_) => AuthFailureKind::InvalidConfiguration,
            Self::Read { .. } => AuthFailureKind::Unavailable,
            Self::Decode { .. } | Self::DigestMismatch { .. } | Self::VersionMismatch { .. } => {
                AuthFailureKind::InvalidData
            }
        }
    }
}

impl fmt::Display for MaintenancePolicyLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReference(reason) => write!(formatter, "invalid maintenance policy reference: {reason}"),
            Self::Read { path, source } => write!(formatter, "failed to read maintenance policy {path:?}: {source}"),
            Self::Decode { path, source } => {
                write!(formatter, "failed to decode maintenance policy {path:?}: {source}")
            }
            Self::DigestMismatch { expected, actual } => {
                write!(
                    formatter,
                    "maintenance policy digest mismatch: expected {expected}, actual {actual}"
                )
            }
            Self::VersionMismatch { expected, actual } => {
                write!(
                    formatter,
                    "maintenance policy version mismatch: expected {expected}, actual {actual}"
                )
            }
            Self::Contract(source) => write!(formatter, "maintenance policy contract violation: {source}"),
        }
    }
}

impl Error for MaintenancePolicyLoadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Decode { source, .. } => Some(source),
            Self::Contract(source) => Some(source.as_ref()),
            Self::InvalidReference(_) | Self::DigestMismatch { .. } | Self::VersionMismatch { .. } => None,
        }
    }
}

fn validate_sha256(value: &str) -> Result<(), MaintenancePolicyLoadError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(MaintenancePolicyLoadError::InvalidReference(
            "policy SHA-256 must be 64 lowercase hexadecimal characters".to_string(),
        ))
    }
}
