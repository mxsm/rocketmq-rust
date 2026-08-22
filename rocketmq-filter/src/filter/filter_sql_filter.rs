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
//! let expr = filter.try_compile("age > 18 AND region = 'US'")?;
//! ```

use rocketmq_error::FilterCompileError;
use rocketmq_model::common::filter::expression_type::ExpressionType;

use crate::expression::Expression;
use crate::filter::filter_spi::Filter;
#[allow(
    deprecated,
    reason = "SqlFilter's deprecated compile wrapper preserves the legacy error type."
)]
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
/// let expr = filter.try_compile("price > 100 AND category = 'electronics'")?;
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
    #[allow(
        deprecated,
        reason = "This compatibility wrapper preserves the deprecated Filter::compile API."
    )]
    fn compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterError> {
        self.try_compile(expr).map_err(sql_runtime::legacy_projection)
    }

    fn try_compile(&self, expr: &str) -> Result<Box<dyn Expression>, FilterCompileError> {
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
    #[allow(
        deprecated,
        reason = "This test verifies the deprecated compile compatibility wrapper."
    )]
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
    #[allow(
        deprecated,
        reason = "This test verifies the deprecated compile compatibility wrapper."
    )]
    fn test_sql_filter_rejects_invalid_expression() {
        let filter = SqlFilter::new();

        assert!(filter.compile("color = ").is_err());
    }

    #[test]
    fn deterministic_equivalent_expressions_match_for_all_contexts() {
        const SEED: u64 = 0x524d_5146_494c_5445;
        let filter = SqlFilter::new();
        let left = filter
            .try_compile("score >= 10 AND color = 'blue'")
            .expect("left expression");
        let right = filter
            .try_compile("color = 'blue' AND score >= 10")
            .expect("right expression");
        let complement = filter
            .try_compile("NOT (score >= 10 AND color = 'blue')")
            .expect("complement expression");
        let mut state = SEED;

        for case in 0..24 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let score = (state % 20).to_string();
            let color = if state & 1 == 0 { "blue" } else { "red" };
            let mut properties = HashMap::with_hasher(RandomState::default());
            properties.insert(
                CheetahString::from_static_str("score"),
                CheetahString::from_string(score),
            );
            properties.insert(
                CheetahString::from_static_str("color"),
                CheetahString::from_slice(color),
            );
            let context = MessageEvaluationContext::from_properties(properties);
            let left_value = left
                .evaluate(&context)
                .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} left failed: {error}"));
            let right_value = right
                .evaluate(&context)
                .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} right failed: {error}"));
            let complement_value = complement
                .evaluate(&context)
                .unwrap_or_else(|error| panic!("seed={SEED:#018x} case={case} complement failed: {error}"));

            assert_eq!(left_value, right_value, "seed={SEED:#018x} case={case}");
            assert_eq!(
                complement_value,
                Value::Boolean(!left_value.as_bool()),
                "seed={SEED:#018x} case={case}"
            );
        }
    }
}
