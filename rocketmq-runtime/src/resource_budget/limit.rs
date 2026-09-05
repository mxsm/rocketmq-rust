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

use std::time::Duration;

use crate::RuntimeContractViolation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Identifies the full policy state.
pub enum FullPolicy {
    /// Represents the reject case.
    Reject,
    /// Waits for capacity until the caller-provided absolute deadline.
    WaitUntilDeadline,
    /// Represents the coalesce latest case.
    CoalesceLatest,
    /// Represents the drop stale case.
    DropStale,
    /// Represents the close slow consumer case.
    CloseSlowConsumer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Identifies the budget class state.
pub enum BudgetClass {
    /// Represents the data case.
    Data,
    /// Represents the control case.
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Identifies the budget dimension state.
pub enum BudgetDimension {
    /// Represents the count case.
    Count,
    /// Represents the bytes case.
    Bytes,
    /// Represents the rate case.
    Rate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents rate limit.
pub struct RateLimit {
    /// The permits per second value.
    pub permits_per_second: u64,
    /// The burst value.
    pub burst: u64,
}

impl RateLimit {
    #[must_use]
    /// Creates a new `RateLimit`.
    pub const fn new(permits_per_second: u64, burst: u64) -> Self {
        Self {
            permits_per_second,
            burst,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents budget capacity.
pub struct BudgetCapacity {
    /// The count value.
    pub count: usize,
    /// The bytes value.
    pub bytes: usize,
    /// The rate value.
    pub rate: Option<RateLimit>,
}

impl BudgetCapacity {
    #[must_use]
    /// Creates a new `BudgetCapacity`.
    pub const fn new(count: usize, bytes: usize) -> Self {
        Self {
            count,
            bytes,
            rate: None,
        }
    }

    #[must_use]
    /// Sets rate and returns the updated value.
    pub const fn with_rate(mut self, rate: RateLimit) -> Self {
        self.rate = Some(rate);
        self
    }
}

impl Default for BudgetCapacity {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents budget limit.
pub struct BudgetLimit {
    /// The capacity value.
    pub capacity: BudgetCapacity,
    /// The control reserve value.
    pub control_reserve: BudgetCapacity,
    /// The max age value.
    pub max_age: Option<Duration>,
    /// The full policy value.
    pub full_policy: FullPolicy,
}

impl BudgetLimit {
    #[must_use]
    /// Creates a new `BudgetLimit`.
    pub const fn new(count: usize, bytes: usize, full_policy: FullPolicy) -> Self {
        Self {
            capacity: BudgetCapacity::new(count, bytes),
            control_reserve: BudgetCapacity::new(0, 0),
            max_age: None,
            full_policy,
        }
    }

    #[must_use]
    /// Sets rate and returns the updated value.
    pub const fn with_rate(mut self, rate: RateLimit) -> Self {
        self.capacity.rate = Some(rate);
        self
    }

    #[must_use]
    /// Sets control reserve and returns the updated value.
    pub const fn with_control_reserve(mut self, reserve: BudgetCapacity) -> Self {
        self.control_reserve = reserve;
        self
    }

    #[must_use]
    /// Sets max age and returns the updated value.
    pub const fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    pub(crate) fn validate(self, path: &str) -> Result<(), RuntimeContractViolation> {
        if self.capacity.count == 0 {
            return Err(RuntimeContractViolation::ZeroBudgetCapacity {
                dimension: BudgetDimension::Count,
            });
        }
        if self.capacity.bytes == 0 {
            return Err(RuntimeContractViolation::ZeroBudgetCapacity {
                dimension: BudgetDimension::Bytes,
            });
        }
        validate_rate(path, self.capacity.rate)?;
        validate_rate(path, self.control_reserve.rate)?;
        if self.control_reserve.count > self.capacity.count {
            return Err(RuntimeContractViolation::ReserveExceedsBudgetCapacity {
                dimension: BudgetDimension::Count,
            });
        }
        if self.control_reserve.bytes > self.capacity.bytes {
            return Err(RuntimeContractViolation::ReserveExceedsBudgetCapacity {
                dimension: BudgetDimension::Bytes,
            });
        }
        match (self.capacity.rate, self.control_reserve.rate) {
            (Some(capacity), Some(reserve))
                if reserve.permits_per_second > capacity.permits_per_second || reserve.burst > capacity.burst =>
            {
                Err(RuntimeContractViolation::ReserveExceedsBudgetCapacity {
                    dimension: BudgetDimension::Rate,
                })
            }
            (None, Some(_)) => Err(RuntimeContractViolation::ReserveWithoutBudgetCapacity {
                dimension: BudgetDimension::Rate,
            }),
            _ if self.max_age == Some(Duration::ZERO) => Err(RuntimeContractViolation::ZeroBudgetMaxAge),
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_child(self, parent: Self, path: &str) -> Result<(), RuntimeContractViolation> {
        self.validate(path)?;
        if self.capacity.count > parent.capacity.count {
            return Err(RuntimeContractViolation::ChildBudgetExceedsParent {
                dimension: BudgetDimension::Count,
            });
        }
        if self.capacity.bytes > parent.capacity.bytes {
            return Err(RuntimeContractViolation::ChildBudgetExceedsParent {
                dimension: BudgetDimension::Bytes,
            });
        }
        match (self.capacity.rate, parent.capacity.rate) {
            (Some(child), Some(parent))
                if child.permits_per_second > parent.permits_per_second || child.burst > parent.burst =>
            {
                return Err(RuntimeContractViolation::ChildBudgetExceedsParent {
                    dimension: BudgetDimension::Rate,
                });
            }
            (None, Some(_)) => {
                return Err(RuntimeContractViolation::ChildBudgetExceedsParent {
                    dimension: BudgetDimension::Rate,
                });
            }
            _ => {}
        }
        match (self.max_age, parent.max_age) {
            (Some(child), Some(parent)) if child > parent => {
                Err(RuntimeContractViolation::ChildBudgetMaxAgeExceedsParent)
            }
            (None, Some(_)) => Err(RuntimeContractViolation::ChildBudgetMaxAgeExceedsParent),
            _ => Ok(()),
        }
    }
}

fn validate_rate(_path: &str, rate: Option<RateLimit>) -> Result<(), RuntimeContractViolation> {
    if let Some(rate) = rate {
        if rate.permits_per_second == 0 || rate.burst == 0 {
            return Err(RuntimeContractViolation::ZeroBudgetRate);
        }
    }
    Ok(())
}
