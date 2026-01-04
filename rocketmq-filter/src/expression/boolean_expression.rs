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

use std::fmt;

use crate::expression::evaluation_context::EvaluationContext;
use crate::expression::Expression;

/// Trait for boolean expression evaluation in message filtering.
///
/// This trait represents boolean expressions that can be evaluated against a context
/// to produce a boolean result. It is the foundation of RocketMQ's SQL92 filter and
/// custom expression evaluators.
///
/// # Design Philosophy
///
/// - **Deterministic**: Same input always produces same output
/// - **Non-panicking**: Never panics; edge cases return `false`
/// - **Object-safe**: Can be used as `Box<dyn BooleanExpression>` or `Arc<dyn BooleanExpression>`
/// - **Thread-safe**: Can be safely shared across threads when wrapped in `Arc`
///
/// # Edge Case Handling
///
/// Following Java RocketMQ semantics:
/// - Missing variables in context → return `false`
/// - Type mismatch in comparison → return `false`
/// - Invalid operations → return `false`
/// - Null/None values → return `false`
///
/// # Thread Safety
///
/// Implementations should be thread-safe for read-only operations. The trait requires
/// `Send + Sync` to enable safe concurrent evaluation across multiple threads.
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::{BooleanExpression, MessageEvaluationContext};
///
/// // Custom expression implementation
/// struct PropertyEqualsExpression {
///     property_name: String,
///     expected_value: String,
/// }
///
/// impl BooleanExpression for PropertyEqualsExpression {
///     fn matches(&self, context: &dyn EvaluationContext) -> bool {
///         context
///             .get(&self.property_name)
///             .map(|v| v.as_str() == self.expected_value.as_str())
///             .unwrap_or(false)
///     }
/// }
///
/// // Usage
/// let expr = PropertyEqualsExpression {
///     property_name: "region".to_string(),
///     expected_value: "us-west".to_string(),
/// };
///
/// let mut context = MessageEvaluationContext::new();
/// context.put("region", "us-west");
///
/// assert!(expr.matches(&context)); // true
/// ```
///
/// # Compatibility
///
/// This trait is semantically equivalent to the Java interface:
///
/// ```java
/// public interface BooleanExpression extends Expression {
///     boolean matches(EvaluationContext context) throws Exception;
/// }
/// ```
///
/// Key differences from Java version:
/// - No checked exceptions; errors are handled by returning `false`
/// - Explicit `Send + Sync` bounds for thread safety
/// - Immutable `&self` receiver for better Rust ergonomics
pub trait BooleanExpression: Send + Sync + Expression {
    /// Evaluates the boolean expression against the provided context.
    ///
    /// # Arguments
    ///
    /// * `context` - The evaluation context containing message properties and variables
    ///
    /// # Returns
    ///
    /// Returns `true` if the expression evaluates to true, `false` otherwise.
    ///
    /// # Guarantees
    ///
    /// - **Non-panicking**: This method will never panic
    /// - **Deterministic**: Same context produces same result
    /// - **Idempotent**: Can be called multiple times safely
    ///
    /// # Error Handling
    ///
    /// Unlike the Java version which throws `Exception`, this method returns `false`
    /// for any error conditions:
    /// - Variable not found in context
    /// - Type conversion failures
    /// - Invalid operations (e.g., comparing incompatible types)
    /// - Any other evaluation errors
    ///
    /// # Example
    ///
    /// ```ignore
    /// let expr = SomeExpression::new();
    /// let context = MessageEvaluationContext::new();
    ///
    /// if expr.matches(&context) {
    ///     println!("Expression matched!");
    /// }
    /// ```
    fn matches(&self, context: &dyn EvaluationContext) -> bool;
}

/// Always returns `true` regardless of context.
///
/// This is a trivial implementation useful for testing, debugging,
/// or as a default "accept all" filter.
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::{BooleanExpression, AlwaysTrueExpression};
///
/// let expr = AlwaysTrueExpression;
/// let context = MessageEvaluationContext::new();
///
/// assert!(expr.matches(&context));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AlwaysTrueExpression;

impl BooleanExpression for AlwaysTrueExpression {
    #[inline]
    fn matches(&self, _context: &dyn EvaluationContext) -> bool {
        true
    }
}

impl Expression for AlwaysTrueExpression {
    fn evaluate(
        &self,
        _context: &dyn EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(true))
    }
}

impl fmt::Display for AlwaysTrueExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TRUE")
    }
}

/// Always returns `false` regardless of context.
///
/// This is a trivial implementation useful for testing, debugging,
/// or as a default "reject all" filter.
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::{BooleanExpression, AlwaysFalseExpression};
///
/// let expr = AlwaysFalseExpression;
/// let context = MessageEvaluationContext::new();
///
/// assert!(!expr.matches(&context));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AlwaysFalseExpression;

impl BooleanExpression for AlwaysFalseExpression {
    #[inline]
    fn matches(&self, _context: &dyn EvaluationContext) -> bool {
        false
    }
}

impl Expression for AlwaysFalseExpression {
    fn evaluate(
        &self,
        _context: &dyn EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(false))
    }
}

impl fmt::Display for AlwaysFalseExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FALSE")
    }
}

/// Checks if a property in the context equals a specific value.
///
/// Returns `false` if:
/// - The property does not exist in the context
/// - The property value does not match the expected value
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::{PropertyEqualsExpression, MessageEvaluationContext};
///
/// let expr = PropertyEqualsExpression::new("region", "us-west");
///
/// let mut context = MessageEvaluationContext::new();
/// context.put("region", "us-west");
///
/// assert!(expr.matches(&context));
///
/// context.put("region", "eu-central");
/// assert!(!expr.matches(&context));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PropertyEqualsExpression {
    property_name: String,
    expected_value: String,
}

impl PropertyEqualsExpression {
    /// Creates a new property equals expression.
    ///
    /// # Arguments
    ///
    /// * `property_name` - The name of the property to check
    /// * `expected_value` - The expected value of the property
    ///
    /// # Returns
    ///
    /// A new `PropertyEqualsExpression` instance.
    pub fn new(property_name: impl Into<String>, expected_value: impl Into<String>) -> Self {
        Self {
            property_name: property_name.into(),
            expected_value: expected_value.into(),
        }
    }

    /// Gets the property name being checked.
    #[inline]
    pub fn property_name(&self) -> &str {
        &self.property_name
    }

    /// Gets the expected value.
    #[inline]
    pub fn expected_value(&self) -> &str {
        &self.expected_value
    }
}

impl BooleanExpression for PropertyEqualsExpression {
    fn matches(&self, context: &dyn EvaluationContext) -> bool {
        context
            .get(&self.property_name)
            .map(|v| v.as_str() == self.expected_value.as_str())
            .unwrap_or(false)
    }
}

impl Expression for PropertyEqualsExpression {
    fn evaluate(
        &self,
        context: &dyn EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(self.matches(context)))
    }
}

impl fmt::Display for PropertyEqualsExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} = '{}')", self.property_name, self.expected_value)
    }
}

/// Logical AND expression that returns `true` only if both operands evaluate to `true`.
///
/// # Short-circuit Evaluation
///
/// This implementation uses short-circuit evaluation: if the left operand
/// evaluates to `false`, the right operand is not evaluated.
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::{AndExpression, PropertyEqualsExpression};
///
/// let left = PropertyEqualsExpression::new("region", "us-west");
/// let right = PropertyEqualsExpression::new("env", "production");
/// let expr = AndExpression::new(Box::new(left), Box::new(right));
///
/// // Only matches if both conditions are true
/// ```
pub struct AndExpression {
    left: Box<dyn BooleanExpression>,
    right: Box<dyn BooleanExpression>,
}

impl AndExpression {
    /// Creates a new AND expression.
    ///
    /// # Arguments
    ///
    /// * `left` - The left operand
    /// * `right` - The right operand
    pub fn new(left: Box<dyn BooleanExpression>, right: Box<dyn BooleanExpression>) -> Self {
        Self { left, right }
    }
}

impl BooleanExpression for AndExpression {
    fn matches(&self, context: &dyn EvaluationContext) -> bool {
        self.left.matches(context) && self.right.matches(context)
    }
}

impl Expression for AndExpression {
    fn evaluate(
        &self,
        context: &dyn EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(self.matches(context)))
    }
}

impl fmt::Display for AndExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

/// Logical OR expression that returns `true` if either operand evaluates to `true`.
///
/// # Short-circuit Evaluation
///
/// This implementation uses short-circuit evaluation: if the left operand
/// evaluates to `true`, the right operand is not evaluated.
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::{OrExpression, PropertyEqualsExpression};
///
/// let left = PropertyEqualsExpression::new("region", "us-west");
/// let right = PropertyEqualsExpression::new("region", "us-east");
/// let expr = OrExpression::new(Box::new(left), Box::new(right));
///
/// // Matches if either condition is true
/// ```
pub struct OrExpression {
    left: Box<dyn BooleanExpression>,
    right: Box<dyn BooleanExpression>,
}

impl OrExpression {
    /// Creates a new OR expression.
    ///
    /// # Arguments
    ///
    /// * `left` - The left operand
    /// * `right` - The right operand
    pub fn new(left: Box<dyn BooleanExpression>, right: Box<dyn BooleanExpression>) -> Self {
        Self { left, right }
    }
}

impl BooleanExpression for OrExpression {
    fn matches(&self, context: &dyn EvaluationContext) -> bool {
        self.left.matches(context) || self.right.matches(context)
    }
}

impl Expression for OrExpression {
    fn evaluate(
        &self,
        context: &dyn EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(self.matches(context)))
    }
}

impl fmt::Display for OrExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

/// Logical NOT expression that inverts the result of its operand.
///
/// # Example
///
/// ```ignore
/// use rocketmq_filter::expression::{NotExpression, PropertyEqualsExpression};
///
/// let inner = PropertyEqualsExpression::new("env", "test");
/// let expr = NotExpression::new(Box::new(inner));
///
/// // Matches if the inner expression is false
/// ```
pub struct NotExpression {
    operand: Box<dyn BooleanExpression>,
}

impl NotExpression {
    /// Creates a new NOT expression.
    ///
    /// # Arguments
    ///
    /// * `operand` - The expression to negate
    pub fn new(operand: Box<dyn BooleanExpression>) -> Self {
        Self { operand }
    }
}

impl BooleanExpression for NotExpression {
    fn matches(&self, context: &dyn EvaluationContext) -> bool {
        !self.operand.matches(context)
    }
}

impl Expression for NotExpression {
    fn evaluate(
        &self,
        context: &dyn EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        Ok(Box::new(self.matches(context)))
    }
}

impl fmt::Display for NotExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT {}", self.operand)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::MessageEvaluationContext;

    #[test]
    fn test_always_true_expression() {
        let expr = AlwaysTrueExpression;
        let context = MessageEvaluationContext::new();
        assert!(expr.matches(&context));
    }

    #[test]
    fn test_always_false_expression() {
        let expr = AlwaysFalseExpression;
        let context = MessageEvaluationContext::new();
        assert!(!expr.matches(&context));
    }

    #[test]
    fn test_property_equals_expression_match() {
        let expr = PropertyEqualsExpression::new("region", "us-west");
        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-west");

        assert!(expr.matches(&context));
    }

    #[test]
    fn test_property_equals_expression_no_match() {
        let expr = PropertyEqualsExpression::new("region", "us-west");
        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-east");

        assert!(!expr.matches(&context));
    }

    #[test]
    fn test_property_equals_expression_missing_property() {
        let expr = PropertyEqualsExpression::new("region", "us-west");
        let context = MessageEvaluationContext::new();

        // Missing property should return false
        assert!(!expr.matches(&context));
    }

    #[test]
    fn test_and_expression_both_true() {
        let left = PropertyEqualsExpression::new("region", "us-west");
        let right = PropertyEqualsExpression::new("env", "production");
        let expr = AndExpression::new(Box::new(left), Box::new(right));

        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-west");
        context.put("env", "production");

        assert!(expr.matches(&context));
    }

    #[test]
    fn test_and_expression_left_false() {
        let left = PropertyEqualsExpression::new("region", "us-west");
        let right = PropertyEqualsExpression::new("env", "production");
        let expr = AndExpression::new(Box::new(left), Box::new(right));

        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-east");
        context.put("env", "production");

        assert!(!expr.matches(&context));
    }

    #[test]
    fn test_and_expression_right_false() {
        let left = PropertyEqualsExpression::new("region", "us-west");
        let right = PropertyEqualsExpression::new("env", "production");
        let expr = AndExpression::new(Box::new(left), Box::new(right));

        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-west");
        context.put("env", "test");

        assert!(!expr.matches(&context));
    }

    #[test]
    fn test_or_expression_both_true() {
        let left = PropertyEqualsExpression::new("region", "us-west");
        let right = PropertyEqualsExpression::new("region", "us-east");
        let expr = OrExpression::new(Box::new(left), Box::new(right));

        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-west");

        assert!(expr.matches(&context));
    }

    #[test]
    fn test_or_expression_left_true() {
        let left = PropertyEqualsExpression::new("region", "us-west");
        let right = PropertyEqualsExpression::new("region", "us-east");
        let expr = OrExpression::new(Box::new(left), Box::new(right));

        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-west");

        assert!(expr.matches(&context));
    }

    #[test]
    fn test_or_expression_right_true() {
        let left = PropertyEqualsExpression::new("region", "us-west");
        let right = PropertyEqualsExpression::new("region", "us-east");
        let expr = OrExpression::new(Box::new(left), Box::new(right));

        let mut context = MessageEvaluationContext::new();
        context.put("region", "us-east");

        assert!(expr.matches(&context));
    }

    #[test]
    fn test_or_expression_both_false() {
        let left = PropertyEqualsExpression::new("region", "us-west");
        let right = PropertyEqualsExpression::new("region", "us-east");
        let expr = OrExpression::new(Box::new(left), Box::new(right));

        let mut context = MessageEvaluationContext::new();
        context.put("region", "eu-central");

        assert!(!expr.matches(&context));
    }

    #[test]
    fn test_not_expression_true() {
        let inner = AlwaysFalseExpression;
        let expr = NotExpression::new(Box::new(inner));

        let context = MessageEvaluationContext::new();
        assert!(expr.matches(&context));
    }

    #[test]
    fn test_not_expression_false() {
        let inner = AlwaysTrueExpression;
        let expr = NotExpression::new(Box::new(inner));

        let context = MessageEvaluationContext::new();
        assert!(!expr.matches(&context));
    }

    #[test]
    fn test_complex_expression() {
        // (region == "us-west" AND env == "production") OR (region == "us-east" AND env == "test")
        let left_and = AndExpression::new(
            Box::new(PropertyEqualsExpression::new("region", "us-west")),
            Box::new(PropertyEqualsExpression::new("env", "production")),
        );

        let right_and = AndExpression::new(
            Box::new(PropertyEqualsExpression::new("region", "us-east")),
            Box::new(PropertyEqualsExpression::new("env", "test")),
        );

        let expr = OrExpression::new(Box::new(left_and), Box::new(right_and));

        // Test case 1: us-west + production -> true
        let mut context1 = MessageEvaluationContext::new();
        context1.put("region", "us-west");
        context1.put("env", "production");
        assert!(expr.matches(&context1));

        // Test case 2: us-east + test -> true
        let mut context2 = MessageEvaluationContext::new();
        context2.put("region", "us-east");
        context2.put("env", "test");
        assert!(expr.matches(&context2));

        // Test case 3: us-west + test -> false
        let mut context3 = MessageEvaluationContext::new();
        context3.put("region", "us-west");
        context3.put("env", "test");
        assert!(!expr.matches(&context3));
    }

    #[test]
    fn test_trait_object_usage() {
        let expr: Box<dyn BooleanExpression> = Box::new(AlwaysTrueExpression);
        let context = MessageEvaluationContext::new();
        assert!(expr.matches(&context));
    }

    #[test]
    fn test_send_sync_bounds() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlwaysTrueExpression>();
        assert_send_sync::<AlwaysFalseExpression>();
        assert_send_sync::<PropertyEqualsExpression>();
    }
}
