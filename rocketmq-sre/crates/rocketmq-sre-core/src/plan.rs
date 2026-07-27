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

use std::error::Error;
use std::fmt;

use rocketmq_sre_contracts::ActionPlan;
use rocketmq_sre_contracts::ActionPlanDraft;
use rocketmq_sre_contracts::ContractError;

use crate::ActionCatalog;
use crate::ActionCatalogError;

/// Stateless deterministic plan validator and sealer.
pub struct PlanService<'a> {
    catalog: &'a ActionCatalog,
}

/// Plan construction error.
#[derive(Debug)]
pub enum PlanError {
    Contract(ContractError),
    Catalog(ActionCatalogError),
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => error.fmt(formatter),
            Self::Catalog(error) => error.fmt(formatter),
        }
    }
}

impl Error for PlanError {}

impl From<ContractError> for PlanError {
    fn from(value: ContractError) -> Self {
        Self::Contract(value)
    }
}

impl From<ActionCatalogError> for PlanError {
    fn from(value: ActionCatalogError) -> Self {
        Self::Catalog(value)
    }
}

impl<'a> PlanService<'a> {
    /// Creates a service bound to one immutable catalog snapshot.
    #[must_use]
    pub const fn new(catalog: &'a ActionCatalog) -> Self {
        Self { catalog }
    }

    /// Validates every step against an exact descriptor and seals the plan.
    ///
    /// # Errors
    ///
    /// Rejects rules-only diagnoses and unknown action/version pairs.
    pub fn seal(&self, draft: ActionPlanDraft) -> Result<ActionPlan, PlanError> {
        for step in &draft.steps {
            let descriptor = self.catalog.descriptor(step.action, &step.descriptor_version)?;
            if step.max_impact != descriptor.max_impact
                || step.verification != descriptor.verification
                || step.compensation != descriptor.compensation
            {
                return Err(ActionCatalogError::InvalidDescriptor {
                    action: step.action.id().to_owned(),
                    reason: "plan step policy fields must match the exact descriptor version".to_owned(),
                }
                .into());
            }
        }
        Ok(ActionPlan::seal(draft)?)
    }
}
