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

//! SQL-92 Expression Filter Implementation
//!
//! This module provides the SQL-92 filter implementation for RocketMQ message filtering.
//! It supports standard SQL-92 syntax for property-based filtering of messages.
//!
//! # Supported Syntax
//!
//! The SQL-92 filter supports:
//! - Comparison operators: `=`, `!=`, `>`, `>=`, `<`, `<=`
//! - Logical operators: `AND`, `OR`, `NOT`
//! - Property references: Column names map to message properties
//! - String literals: Enclosed in single quotes
//! - Numeric literals: Integer and floating-point numbers
//! - Boolean literals: `TRUE`, `FALSE`
//!
//! # Examples
//!
//! ```rust,ignore
//! use rocketmq_filter::filter::{SqlFilter, Filter};
//!
//! let filter = SqlFilter::new();
//! let expr = filter.compile("age > 18 AND region = 'US'")?;
//! ```

use rocketmq_common::common::filter::expression_type::ExpressionType;

use crate::expression::Expression;
use crate::filter::filter_spi::Filter;
use crate::filter::filter_spi::FilterError;
use crate::filter::sql_runtime;

/// SQL-92 expression filter implementation.
///
/// `SqlFilter` provides SQL-92 compliant expression filtering for RocketMQ messages.
/// It compiles SQL expressions into executable expression trees that can be
/// evaluated against message properties.
///
/// # Type Identifier
///
/// This filter uses the type identifier `"SQL92"` for registration in the filter factory.
///
/// # Thread Safety
///
/// `SqlFilter` is stateless and can be safely shared across threads when wrapped in `Arc`.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_filter::filter::{SqlFilter, Filter};
/// use std::sync::Arc;
///
/// let filter: Arc<dyn Filter> = Arc::new(SqlFilter::new());
/// let expr = filter.compile("price > 100 AND category = 'electronics'")?;
/// ```
///
/// # Performance
///
/// Expression compilation is performed once and the resulting expression tree
/// can be reused for multiple message evaluations. For best performance,
/// compile expressions once and cache them.
#[derive(Debug, Clone, Default)]
pub struct SqlFilter;

impl SqlFilter {
    /// Creates a new SQL-92 filter instance.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use rocketmq_filter::filter::SqlFilter;
    ///
    /// let filter = SqlFilter::new();
    /// ```
    pub fn new() -> Self {
        Self
    }
}

impl Filter for SqlFilter {
    fn compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterError> {
        sql_runtime::compile_expression(expr)
    }

    fn of_type(&self) -> &str {
        ExpressionType::SQL92
    }
}

#[cfg(test)]
mod tests {
    use ahash::RandomState;
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::expression::MessageEvaluationContext;
    use crate::expression::Value;

    #[test]
    fn test_sql_filter_of_type() {
        let filter = SqlFilter::new();
        assert_eq!(filter.of_type(), "SQL92");
    }

    #[test]
    fn test_sql_filter_default() {
        let filter = SqlFilter;
        assert_eq!(filter.of_type(), "SQL92");
    }

    #[test]
    fn test_sql_filter_clone() {
        let filter = SqlFilter::new();
        let cloned = filter.clone();
        assert_eq!(filter.of_type(), cloned.of_type());
    }

    #[test]
    fn test_sql_filter_compile_and_evaluate() {
        let filter = SqlFilter::new();
        let expression = filter
            .compile("color = 'blue' AND retries >= 3")
            .expect("SQL92 expression should compile");

        let mut properties = HashMap::with_hasher(RandomState::default());
        properties.insert(CheetahString::from_slice("color"), CheetahString::from_slice("blue"));
        properties.insert(CheetahString::from_slice("retries"), CheetahString::from_slice("3"));
        let context = MessageEvaluationContext::from_properties(properties);

        assert_eq!(expression.evaluate(&context).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_sql_filter_rejects_invalid_expression() {
        let filter = SqlFilter::new();

        assert!(filter.compile("color = ").is_err());
    }
}
