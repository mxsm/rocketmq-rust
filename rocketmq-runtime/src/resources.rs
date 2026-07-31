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

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::resource_budget::BudgetLimit;
use crate::resource_budget::FullPolicy;
use crate::resource_budget::ProcessMemoryLimit;
use crate::resource_budget::ResourceBudget;
use crate::resource_budget::ResourceBudgetTree;

/// Process-wide resource capabilities owned by [`crate::RuntimeOwner`].
///
/// Clones share the same budget tree. Components derive narrower child
/// budgets from [`Self::process_budget`] instead of detecting process limits
/// or creating independent roots.
#[derive(Debug, Clone)]
pub struct RuntimeResources {
    memory_limit: ProcessMemoryLimit,
    process_budget: ResourceBudget,
}

impl RuntimeResources {
    pub(crate) fn from_memory_limit(memory_limit: ProcessMemoryLimit) -> RuntimeResult<Self> {
        let managed_bytes = usize::try_from(memory_limit.bytes()).unwrap_or(usize::MAX);
        let process_budget = ResourceBudgetTree::new(
            "process",
            BudgetLimit::new(usize::MAX, managed_bytes, FullPolicy::Reject),
        )
        .map_err(|error| RuntimeError::InvalidConfig(format!("invalid process resource budget: {error}")))?
        .root();
        Ok(Self {
            memory_limit,
            process_budget,
        })
    }

    /// Returns the detected or explicitly configured process memory limit.
    #[must_use]
    pub const fn memory_limit(&self) -> ProcessMemoryLimit {
        self.memory_limit
    }

    /// Returns the shared process resource-budget root.
    #[must_use]
    pub fn process_budget(&self) -> ResourceBudget {
        self.process_budget.clone()
    }
}
