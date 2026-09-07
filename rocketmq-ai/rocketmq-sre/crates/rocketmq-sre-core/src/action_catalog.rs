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
use std::fmt;

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::CompensationMode;
use rocketmq_sre_contracts::DescriptorStatus;
use rocketmq_sre_contracts::DescriptorVersion;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::PublicErrorCode;
use rocketmq_sre_contracts::SreContractError;

/// Closed result for catalog validation and lookup outcomes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ActionCatalogRejection {
    InvalidDescriptor,
    DuplicateDescriptor,
    DescriptorNotFound,
    ExecutionDisabled,
}

impl fmt::Display for ActionCatalogRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SRE operation was rejected")
    }
}

impl fmt::Debug for ActionCatalogRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl From<ActionCatalogRejection> for SreContractError {
    fn from(rejection: ActionCatalogRejection) -> Self {
        let code = match rejection {
            ActionCatalogRejection::InvalidDescriptor | ActionCatalogRejection::DuplicateDescriptor => {
                PublicErrorCode::InvalidDescriptor
            }
            ActionCatalogRejection::DescriptorNotFound => PublicErrorCode::DescriptorNotFound,
            ActionCatalogRejection::ExecutionDisabled => PublicErrorCode::ExecutionDisabled,
        };
        Self::new(code)
    }
}

/// Closed, versioned catalog used by planning and execution validation.
#[derive(Clone, Debug, Default)]
pub struct ActionCatalog {
    descriptors: BTreeMap<(ExecutionAction, String), ActionDescriptor>,
}

impl ActionCatalog {
    /// Registers one exact R1/R2 descriptor version.
    ///
    /// # Errors
    ///
    /// Rejects unknown/R3 actions, inactive descriptors, version duplicates,
    /// and contradictory execution flags.
    pub fn register(&mut self, descriptor: ActionDescriptor) -> Result<(), ActionCatalogRejection> {
        let action = ExecutionAction::from_id(&descriptor.id).ok_or(ActionCatalogRejection::DescriptorNotFound)?;
        let parsed_version =
            DescriptorVersion::parse(&descriptor.version).map_err(|_| ActionCatalogRejection::InvalidDescriptor)?;
        if parsed_version.major != 1 {
            return Err(ActionCatalogRejection::InvalidDescriptor);
        }
        if !matches!(descriptor.risk, ActionRisk::R1 | ActionRisk::R2) {
            return Err(ActionCatalogRejection::InvalidDescriptor);
        }
        if descriptor.status != DescriptorStatus::Active {
            return Err(ActionCatalogRejection::InvalidDescriptor);
        }
        if descriptor.execution_supported && descriptor.plan_only {
            return Err(ActionCatalogRejection::InvalidDescriptor);
        }
        if !descriptor.supported_versions.iter().any(|version| {
            version.family == "rocketmq-sre.action-plan" && version.major == 1 && version.required_features.is_empty()
        }) {
            return Err(ActionCatalogRejection::InvalidDescriptor);
        }
        if descriptor.parameter_schema.get("type").and_then(|value| value.as_str()) != Some("object")
            || descriptor
                .parameter_schema
                .get("additionalProperties")
                .and_then(|value| value.as_bool())
                != Some(false)
        {
            return Err(ActionCatalogRejection::InvalidDescriptor);
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
            return Err(ActionCatalogRejection::InvalidDescriptor);
        }
        let key = (action, descriptor.version.clone());
        if self.descriptors.contains_key(&key) {
            return Err(ActionCatalogRejection::DuplicateDescriptor);
        }
        self.descriptors.insert(key, descriptor);
        Ok(())
    }

    /// Resolves one exact action/version pair for planning.
    ///
    /// # Errors
    ///
    /// Rejects unknown actions or versions.
    pub fn descriptor(
        &self,
        action: ExecutionAction,
        version: &str,
    ) -> Result<&ActionDescriptor, ActionCatalogRejection> {
        self.descriptors
            .get(&(action, version.to_owned()))
            .ok_or(ActionCatalogRejection::DescriptorNotFound)
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
    ) -> Result<&ActionDescriptor, ActionCatalogRejection> {
        let descriptor = self.descriptor(action, version)?;
        if descriptor.plan_only || !descriptor.execution_supported {
            return Err(ActionCatalogRejection::ExecutionDisabled);
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
