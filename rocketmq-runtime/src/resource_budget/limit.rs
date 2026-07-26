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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullPolicy {
    Reject,
    CoalesceLatest,
    DropStale,
    CloseSlowConsumer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetClass {
    Data,
    Control,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDimension {
    Count,
    Bytes,
    Rate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit {
    pub permits_per_second: u64,
    pub burst: u64,
}

impl RateLimit {
    #[must_use]
    pub const fn new(permits_per_second: u64, burst: u64) -> Self {
        Self {
            permits_per_second,
            burst,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetCapacity {
    pub count: usize,
    pub bytes: usize,
    pub rate: Option<RateLimit>,
}

impl BudgetCapacity {
    #[must_use]
    pub const fn new(count: usize, bytes: usize) -> Self {
        Self {
            count,
            bytes,
            rate: None,
        }
    }

    #[must_use]
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
pub struct BudgetLimit {
    pub capacity: BudgetCapacity,
    pub control_reserve: BudgetCapacity,
    pub max_age: Option<Duration>,
    pub full_policy: FullPolicy,
}

impl BudgetLimit {
    #[must_use]
    pub const fn new(count: usize, bytes: usize, full_policy: FullPolicy) -> Self {
        Self {
            capacity: BudgetCapacity::new(count, bytes),
            control_reserve: BudgetCapacity::new(0, 0),
            max_age: None,
            full_policy,
        }
    }

    #[must_use]
    pub const fn with_rate(mut self, rate: RateLimit) -> Self {
        self.capacity.rate = Some(rate);
        self
    }

    #[must_use]
    pub const fn with_control_reserve(mut self, reserve: BudgetCapacity) -> Self {
        self.control_reserve = reserve;
        self
    }

    #[must_use]
    pub const fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    pub(crate) fn validate(self, path: &str) -> Result<(), BudgetConfigError> {
        if self.capacity.count == 0 {
            return Err(BudgetConfigError::ZeroCapacity {
                path: path.to_owned(),
                dimension: BudgetDimension::Count,
            });
        }
        if self.capacity.bytes == 0 {
            return Err(BudgetConfigError::ZeroCapacity {
                path: path.to_owned(),
                dimension: BudgetDimension::Bytes,
            });
        }
        validate_rate(path, self.capacity.rate)?;
        validate_rate(path, self.control_reserve.rate)?;
        if self.control_reserve.count > self.capacity.count {
            return Err(BudgetConfigError::ReserveExceedsCapacity {
                path: path.to_owned(),
                dimension: BudgetDimension::Count,
            });
        }
        if self.control_reserve.bytes > self.capacity.bytes {
            return Err(BudgetConfigError::ReserveExceedsCapacity {
                path: path.to_owned(),
                dimension: BudgetDimension::Bytes,
            });
        }
        match (self.capacity.rate, self.control_reserve.rate) {
            (Some(capacity), Some(reserve))
                if reserve.permits_per_second > capacity.permits_per_second || reserve.burst > capacity.burst =>
            {
                Err(BudgetConfigError::ReserveExceedsCapacity {
                    path: path.to_owned(),
                    dimension: BudgetDimension::Rate,
                })
            }
            (None, Some(_)) => Err(BudgetConfigError::ReserveWithoutCapacity {
                path: path.to_owned(),
                dimension: BudgetDimension::Rate,
            }),
            _ if self.max_age == Some(Duration::ZERO) => Err(BudgetConfigError::ZeroMaxAge { path: path.to_owned() }),
            _ => Ok(()),
        }
    }

    pub(crate) fn validate_child(self, parent: Self, path: &str) -> Result<(), BudgetConfigError> {
        self.validate(path)?;
        if self.capacity.count > parent.capacity.count {
            return Err(BudgetConfigError::ChildExceedsParent {
                path: path.to_owned(),
                dimension: BudgetDimension::Count,
            });
        }
        if self.capacity.bytes > parent.capacity.bytes {
            return Err(BudgetConfigError::ChildExceedsParent {
                path: path.to_owned(),
                dimension: BudgetDimension::Bytes,
            });
        }
        match (self.capacity.rate, parent.capacity.rate) {
            (Some(child), Some(parent))
                if child.permits_per_second > parent.permits_per_second || child.burst > parent.burst =>
            {
                return Err(BudgetConfigError::ChildExceedsParent {
                    path: path.to_owned(),
                    dimension: BudgetDimension::Rate,
                });
            }
            (None, Some(_)) => {
                return Err(BudgetConfigError::ChildExceedsParent {
                    path: path.to_owned(),
                    dimension: BudgetDimension::Rate,
                });
            }
            _ => {}
        }
        match (self.max_age, parent.max_age) {
            (Some(child), Some(parent)) if child > parent => {
                Err(BudgetConfigError::ChildMaxAgeExceedsParent { path: path.to_owned() })
            }
            (None, Some(_)) => Err(BudgetConfigError::ChildMaxAgeExceedsParent { path: path.to_owned() }),
            _ => Ok(()),
        }
    }
}

fn validate_rate(path: &str, rate: Option<RateLimit>) -> Result<(), BudgetConfigError> {
    if let Some(rate) = rate {
        if rate.permits_per_second == 0 || rate.burst == 0 {
            return Err(BudgetConfigError::ZeroRate { path: path.to_owned() });
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BudgetConfigError {
    #[error("resource budget name must not be blank")]
    EmptyName,
    #[error("resource budget name must not contain '/'")]
    InvalidName,
    #[error("resource budget {path} has zero {dimension:?} capacity")]
    ZeroCapacity { path: String, dimension: BudgetDimension },
    #[error("resource budget {path} has a zero rate or burst")]
    ZeroRate { path: String },
    #[error("resource budget {path} has zero maximum age")]
    ZeroMaxAge { path: String },
    #[error("resource budget {path} {dimension:?} reserve exceeds its capacity")]
    ReserveExceedsCapacity { path: String, dimension: BudgetDimension },
    #[error("resource budget {path} defines a {dimension:?} reserve without a parent capacity")]
    ReserveWithoutCapacity { path: String, dimension: BudgetDimension },
    #[error("resource budget {path} {dimension:?} capacity exceeds its parent")]
    ChildExceedsParent { path: String, dimension: BudgetDimension },
    #[error("resource budget {path} maximum age exceeds its parent")]
    ChildMaxAgeExceedsParent { path: String },
}
