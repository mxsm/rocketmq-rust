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
use rocketmq_error::FilterCompileErrorKind;
use rocketmq_error::FilterCompileStage;

use crate::expression::Expression;

/// Deprecated local string error retained for 1.x filter compilation compatibility.
///
/// New filter compilation paths return [`FilterCompileError`], whose stable
/// metadata is safe to expose at service boundaries. This compatibility type
/// remains available throughout 1.x; any future removal requires a complete
/// release cycle, an explicit 2.0 breaking window, and individual reviewed
/// post-freeze approvals for every affected frozen public item.
#[deprecated(since = "1.0.0", note = "use Filter::try_compile and FilterCompileError")]
#[derive(Debug, Clone)]
pub struct FilterError {
    /// Human-readable error message
    message: String,
}

#[allow(
    deprecated,
    reason = "This inherent implementation preserves the legacy string-error compatibility API."
)]
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

#[allow(
    deprecated,
    reason = "The legacy string-error Display implementation is retained for compatibility."
)]
impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilterError: {}", self.message)
    }
}

#[allow(
    deprecated,
    reason = "The legacy string-error Error implementation is retained for compatibility."
)]
impl std::error::Error for FilterError {}

/// Core trait for message filter implementations.
///
/// This trait defines the service provider interface (SPI) for pluggable
/// filter implementations. Each filter type (SQL92, Tag, etc.) must implement
/// this trait to participate in the message filtering pipeline.
///
/// New callers should use [`Self::try_compile`]. The deprecated 1.x
/// [`Self::compile`] facade remains available for compatibility and is not
/// removed or authorized for removal by this API documentation.
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
    /// Compiles an expression string through the deprecated 1.x compatibility facade.
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
    /// * `Err(FilterError)` - Legacy string-only compilation failure
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let filter = SqlFilter::new();
    /// let expr = filter.try_compile("price > 100 AND category = 'electronics'")?;
    /// ```
    ///
    /// # Errors
    ///
    /// New code should call [`Self::try_compile`] to preserve structured failure
    /// metadata. This wrapper remains available throughout 1.x and is not
    /// authorized for removal by this documentation.
    #[deprecated(since = "1.0.0", note = "use Filter::try_compile and FilterCompileError")]
    #[allow(
        deprecated,
        reason = "The legacy string-error trait method is retained so existing external filters continue to compile."
    )]
    fn compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterError>;

    /// Compiles an expression with structured, redaction-safe failure details.
    ///
    /// Filters that only implement the legacy [`Self::compile`] method continue
    /// to work. Their failures are classified as
    /// [`FilterCompileErrorKind::LegacyAdapter`] during the compatibility stage.
    /// Future removal of that adapter requires a complete release cycle, an
    /// explicit 2.0 breaking window, and individual reviewed post-freeze
    /// approvals for every affected frozen public item.
    fn try_compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterCompileError> {
        #[allow(
            deprecated,
            reason = "This default method is the narrow compatibility adapter for legacy Filter implementations."
        )]
        self.compile(expr).map_err(|_| {
            FilterCompileError::new(
                FilterCompileErrorKind::LegacyAdapter,
                FilterCompileStage::Compatibility,
                None,
            )
        })
    }

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
