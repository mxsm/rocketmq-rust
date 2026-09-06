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

use std::collections::BTreeMap;
use std::fmt;

use rocketmq_security_api::SecretAccess;
use rocketmq_security_api::SecretMaterial;
use rocketmq_security_api::SecretName;
use rocketmq_security_api::SecretPersistence;
use rocketmq_security_api::SecretProvider;
use rocketmq_security_api::SecretProviderCapabilities;
use rocketmq_security_api::SecretProviderId;
use rocketmq_security_api::SecretVersion;
use rocketmq_security_api::SecretVersioning;
use rocketmq_security_api::SecurityOperation;
use rocketmq_security_api::SecurityProviderError;
use rocketmq_security_api::SecurityProviderFailure;
use rocketmq_security_api::VersionedSecret;

use crate::AuthFailureKind;
use crate::AuthOperation;
use crate::AuthServiceError;
use crate::AuthServiceResult;

/// Read-only development adapter that maps logical names to an explicit environment allowlist.
pub struct EnvironmentSecretProvider {
    id: SecretProviderId,
    variables: BTreeMap<SecretName, String>,
}

impl EnvironmentSecretProvider {
    /// Creates an adapter from an explicit logical-name to environment-variable mapping.
    ///
    /// # Errors
    ///
    /// Returns a redacted authentication-service error for invalid or duplicate mappings.
    pub fn new(
        id: SecretProviderId,
        mappings: impl IntoIterator<Item = (SecretName, String)>,
    ) -> AuthServiceResult<Self> {
        let mut variables = BTreeMap::new();
        for (name, variable) in mappings {
            if variable.is_empty() || variable.contains(['\0', '=']) || variables.insert(name, variable).is_some() {
                return Err(AuthServiceError::new(
                    AuthOperation::LoadSecret,
                    AuthFailureKind::InvalidConfiguration,
                ));
            }
        }
        if variables.is_empty() {
            return Err(AuthServiceError::new(
                AuthOperation::LoadSecret,
                AuthFailureKind::InvalidConfiguration,
            ));
        }
        Ok(Self { id, variables })
    }
}

impl fmt::Debug for EnvironmentSecretProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentSecretProvider")
            .field("id", &self.id)
            .field("mapped_name_count", &self.variables.len())
            .finish()
    }
}

impl SecretProvider for EnvironmentSecretProvider {
    fn id(&self) -> &SecretProviderId {
        &self.id
    }

    fn capabilities(&self) -> SecretProviderCapabilities {
        SecretProviderCapabilities::new(
            SecretAccess::ReadOnly,
            SecretPersistence::ProcessEnvironment,
            SecretVersioning::Unversioned,
        )
    }

    fn read(&self, name: &SecretName) -> Result<VersionedSecret, SecurityProviderError> {
        let variable = self.variables.get(name).ok_or_else(|| {
            SecurityProviderError::new(SecurityProviderFailure::NotFound, SecurityOperation::ReadSecret)
        })?;
        let value = match std::env::var(variable) {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => {
                return Err(SecurityProviderError::new(
                    SecurityProviderFailure::NotFound,
                    SecurityOperation::ReadSecret,
                ));
            }
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(SecurityProviderError::new(
                    SecurityProviderFailure::InvalidData,
                    SecurityOperation::ReadSecret,
                ));
            }
        };
        let material = SecretMaterial::new(value.into_bytes()).map_err(|source| {
            SecurityProviderError::caused_by(
                SecurityProviderFailure::InvalidData,
                SecurityOperation::ReadSecret,
                source,
            )
        })?;
        Ok(VersionedSecret::new(material, None))
    }

    fn write(
        &self,
        _name: &SecretName,
        _material: SecretMaterial,
        _expected_version: Option<SecretVersion>,
    ) -> Result<SecretVersion, SecurityProviderError> {
        Err(SecurityProviderError::new(
            SecurityProviderFailure::Unsupported,
            SecurityOperation::WriteSecret,
        ))
    }
}
