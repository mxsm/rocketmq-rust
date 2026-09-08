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

use rocketmq_error::FilterCompileError;

use crate::expression::Expression;

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
/// use rocketmq_filter::filter::{Filter, FilterFactory};
///
/// let filter = FilterFactory::instance().get("SQL92").unwrap();
/// let expression = filter.try_compile("age > 18")?;
/// ```
///
/// # Thread Safety
///
/// Filter instances are typically wrapped in `Arc` and shared across threads.
/// Implementations should be stateless or use interior mutability with
/// appropriate synchronization.
pub trait Filter: Send + Sync + fmt::Debug {
    /// Compiles an expression with structured, redaction-safe failure details.
    ///
    /// Implementations must return a typed failure without retaining submitted
    /// expression text in the error.
    ///
    /// # Errors
    ///
    /// Returns [`FilterCompileError`] when the expression cannot be compiled.
    fn try_compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterCompileError>;

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
