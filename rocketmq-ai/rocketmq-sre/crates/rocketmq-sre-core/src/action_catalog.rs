// Copyright 2026 The RocketMQ Rust Authors
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
use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::DescriptorStatus;
use rocketmq_sre_contracts::DescriptorVersion;
use rocketmq_sre_contracts::ExecutionAction;

/// Closed, versioned catalog used by planning and execution validation.
#[derive(Clone, Debug, Default)]
pub struct ActionCatalog {
    descriptors: BTreeMap<(ExecutionAction, String), ActionDescriptor>,
}

/// Fail-closed Action Catalog error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActionCatalogError {
    UnknownAction(String),
    UnknownVersion { action: String, version: String },
    InvalidDescriptor { action: String, reason: String },
    ExecutionUnsupported(String),
}

impl fmt::Display for ActionCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownAction(action) => write!(formatter, "unknown execution action `{action}`"),
            Self::UnknownVersion { action, version } => {
                write!(formatter, "unknown descriptor version `{action}@{version}`")
            }
            Self::InvalidDescriptor { action, reason } => {
                write!(formatter, "invalid descriptor `{action}`: {reason}")
            }
            Self::ExecutionUnsupported(action) => {
                write!(formatter, "action `{action}` has no registered execution handler")
            }
        }
    }
}

impl Error for ActionCatalogError {}

impl ActionCatalog {
    /// Registers one exact R1/R2 descriptor version.
    ///
    /// # Errors
    ///
    /// Rejects unknown/R3 actions, inactive descriptors, version duplicates,
    /// and contradictory execution flags.
    pub fn register(&mut self, descriptor: ActionDescriptor) -> Result<(), ActionCatalogError> {
        let action = ExecutionAction::from_id(&descriptor.id)
            .ok_or_else(|| ActionCatalogError::UnknownAction(descriptor.id.clone()))?;
        let parsed_version =
            DescriptorVersion::parse(&descriptor.version).map_err(|error| ActionCatalogError::InvalidDescriptor {
                action: descriptor.id.clone(),
                reason: format!("version must be semantic: {error}"),
            })?;
        if parsed_version.major != 1 {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "Phase 3 action ids accept only descriptor major 1".to_owned(),
            });
        }
        if !matches!(descriptor.risk, ActionRisk::R1 | ActionRisk::R2) {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "execution catalog accepts only R1 or R2".to_owned(),
            });
        }
        if descriptor.status != DescriptorStatus::Active {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "descriptor must be active".to_owned(),
            });
        }
        if descriptor.execution_supported && descriptor.plan_only {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "plan-only descriptor cannot advertise execution support".to_owned(),
            });
        }
        if !descriptor.supported_versions.iter().any(|version| {
            version.family == "rocketmq-sre.action-plan" && version.major == 1 && version.required_features.is_empty()
        }) {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "descriptor must support rocketmq-sre.action-plan major 1".to_owned(),
            });
        }
        if descriptor.parameter_schema.get("type").and_then(|value| value.as_str()) != Some("object")
            || descriptor
                .parameter_schema
                .get("additionalProperties")
                .and_then(|value| value.as_bool())
                != Some(false)
        {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "parameter schema must be a closed JSON object".to_owned(),
            });
        }
        if descriptor.preconditions.is_empty()
            || (descriptor.verification.resource_conditions.is_empty()
                && descriptor.verification.technical_slis.is_empty())
            || descriptor.verification.stable_window_seconds == 0
            || descriptor.verification.max_wait_seconds < descriptor.verification.stable_window_seconds
            || descriptor.timeout_seconds == 0
            || descriptor.forbidden_fields.is_empty()
            || (descriptor.compensation.mode != CompensationMode::NotAvailable
                && descriptor.compensation.timeout_seconds == 0)
        {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "preconditions, bounded verification, timeout, compensation, and forbidden fields are required"
                    .to_owned(),
            });
        }
        let key = (action, descriptor.version.clone());
        if self.descriptors.contains_key(&key) {
            return Err(ActionCatalogError::InvalidDescriptor {
                action: descriptor.id,
                reason: "descriptor version already exists".to_owned(),
            });
        }
        self.descriptors.insert(key, descriptor);
        Ok(())
    }

    /// Resolves one exact action/version pair for planning.
    ///
    /// # Errors
    ///
    /// Rejects unknown actions or versions.
    pub fn descriptor(&self, action: ExecutionAction, version: &str) -> Result<&ActionDescriptor, ActionCatalogError> {
        self.descriptors
            .get(&(action, version.to_owned()))
            .ok_or_else(|| ActionCatalogError::UnknownVersion {
                action: action.id().to_owned(),
                version: version.to_owned(),
            })
    }

    /// Resolves an executable handler contract.
    ///
    /// # Errors
    ///
    /// Rejects plan-only or not-yet-implemented actions.
    pub fn executable_descriptor(
        &self,
        action: ExecutionAction,
        version: &str,
    ) -> Result<&ActionDescriptor, ActionCatalogError> {
        let descriptor = self.descriptor(action, version)?;
        if descriptor.plan_only || !descriptor.execution_supported {
            return Err(ActionCatalogError::ExecutionUnsupported(action.id().to_owned()));
        }
        Ok(descriptor)
    }

    /// Returns the number of registered action versions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.descriptors.len()
    }

    /// Returns whether the catalog is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }
}
