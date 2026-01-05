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

//! Filter Service Provider Interface (SPI)
//!
//! This module defines the core trait for message filtering in RocketMQ.
//! Filters are responsible for evaluating expressions against messages to
//! determine whether they should be delivered to consumers.
//!
//! # Design
//!
//! The `Filter` trait provides a pluggable architecture for different filter
//! implementations (SQL92, Tag-based, etc.). Each filter type:
//! - Compiles expression strings into executable expression objects
//! - Identifies its type through a unique type identifier
//! - Supports thread-safe concurrent filtering operations
//!
//! # Thread Safety
//!
//! All filter implementations must be `Send + Sync` to support concurrent
//! message filtering across multiple threads in the broker.

use std::fmt;

use crate::expression::Expression;

/// Error type for filter compilation and evaluation failures.
///
/// This error wraps various failure scenarios during filter operations:
/// - Expression parsing errors
/// - Type conversion failures
/// - Invalid expression syntax
#[derive(Debug, Clone)]
pub struct FilterError {
    /// Human-readable error message
    message: String,
}

impl FilterError {
    /// Creates a new filter error with the given message.
    ///
    /// # Arguments
    ///
    /// * `message` - Error description
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Gets the error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilterError: {}", self.message)
    }
}

impl std::error::Error for FilterError {}

/// Core trait for message filter implementations.
///
/// This trait defines the service provider interface (SPI) for pluggable
/// filter implementations. Each filter type (SQL92, Tag, etc.) must implement
/// this trait to participate in the message filtering pipeline.
///
/// # Type Parameters
///
/// Implementations must be `Send + Sync` to support multi-threaded filtering.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_filter::filter::{Filter, FilterError};
/// use rocketmq_filter::expression::Expression;
///
/// struct CustomFilter;
///
/// impl Filter for CustomFilter {
///     fn compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterError> {
///         // Parse and compile the expression
///         Ok(Box::new(MyExpression::parse(expr)?))
///     }
///
///     fn of_type(&self) -> &str {
///         "CUSTOM"
///     }
/// }
/// ```
///
/// # Thread Safety
///
/// Filter instances are typically wrapped in `Arc` and shared across threads.
/// Implementations should be stateless or use interior mutability with
/// appropriate synchronization.
pub trait Filter: Send + Sync + fmt::Debug {
    /// Compiles an expression string into an executable expression object.
    ///
    /// This method parses the input string according to the filter's syntax
    /// rules and produces an `Expression` that can be evaluated against messages.
    ///
    /// # Arguments
    ///
    /// * `expr` - The expression string to compile (e.g., "age > 18 AND region = 'US'")
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn Expression>)` - Successfully compiled expression
    /// * `Err(FilterError)` - Compilation failed due to syntax or semantic errors
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let filter = SqlFilter::new();
    /// let expr = filter.compile("price > 100 AND category = 'electronics'")?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `FilterError` if:
    /// - Expression syntax is invalid
    /// - Referenced properties don't exist
    /// - Type constraints are violated
    fn compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterError>;

    /// Returns the unique type identifier for this filter.
    ///
    /// The type identifier distinguishes different filter implementations
    /// and is used for filter registration and lookup in the factory.
    ///
    /// # Returns
    ///
    /// A string slice identifying the filter type. Common values:
    /// - `"SQL92"` - SQL-92 expression filter
    /// - `"TAG"` - Tag-based filter
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let filter = SqlFilter::new();
    /// assert_eq!(filter.of_type(), "SQL92");
    /// ```
    fn of_type(&self) -> &str;
}

/// Type alias for compatibility with Java naming conventions.
///
/// In the Java implementation, this is called `FilterSpi`.
/// This alias allows using either name in Rust code.
pub type FilterSpi = dyn Filter;
