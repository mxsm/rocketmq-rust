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

//! Binary Expression Trait for Message Filtering
//!
//! This module provides the `BinaryExpression` trait, which represents expressions that
//! operate on two operands (left and right). Binary expressions are fundamental building
//! blocks for constructing complex filter expressions in RocketMQ's message filtering system.
//!
//! # Overview
//!
//! Binary expressions combine two sub-expressions using an operator (represented by a symbol).
//! Common examples include:
//! - Logical operations: AND, OR
//! - Comparison operations: =, !=, <, >, <=, >=
//! - Arithmetic operations: +, -, *, /
//!
//! # Design
//!
//! The trait extends the base `Expression` trait and adds:
//! - `left()`: Access to the left operand expression
//! - `right()`: Access to the right operand expression
//! - `get_expression_symbol()`: The operator symbol (e.g., "AND", "=", "+")
//!
//! # Thread Safety
//!
//! All binary expressions must implement `Send + Sync` to support concurrent evaluation
//! in multi-threaded message filtering scenarios.
//!
//! # Examples
//!
//! ```rust,ignore
//! use rocketmq_filter::expression::{BinaryExpression, Expression, EvaluationContext};
//! use cheetah_string::CheetahString;
//!
//! // Example implementation of a simple comparison expression
//! struct EqualsExpression {
//!     left: Box<dyn Expression>,
//!     right: Box<dyn Expression>,
//!     symbol: CheetahString,
//! }
//!
//! impl BinaryExpression for EqualsExpression {
//!     fn left(&self) -> &Box<dyn Expression> {
//!         &self.left
//!     }
//!
//!     fn right(&self) -> &Box<dyn Expression> {
//!         &self.right
//!     }
//!
//!     fn get_expression_symbol(&self) -> Option<&CheetahString> {
//!         Some(&self.symbol)
//!     }
//! }
//!
//! impl Expression for EqualsExpression {
//!     fn evaluate(&self, context: &dyn EvaluationContext) -> Result<Box<dyn Expression>> {
//!         let left_value = self.left.evaluate(context)?;
//!         let right_value = self.right.evaluate(context)?;
//!         // Compare values and return boolean result
//!         // ...
//!     }
//! }
//! ```
//!
//! # Java Compatibility
//!
//! This trait corresponds to the `BinaryExpression` interface in the original Java
//! implementation of Apache RocketMQ. The Rust version maintains the same semantics
//! while adapting to Rust's type system and ownership model.
//!
//! # See Also
//!
//! - [`Expression`]: Base trait for all expressions
//! - [`BooleanExpression`]: Trait for boolean-valued expressions
//! - [`EvaluationContext`]: Context for variable lookup during evaluation

use crate::expression::Expression;
use cheetah_string::CheetahString;

/// Trait representing binary expressions that operate on two operands.
///
/// A binary expression combines two sub-expressions (left and right) using an operator.
/// This trait provides access to both operands and the operator symbol.
///
/// # Type Parameters
///
/// This trait extends `Expression` and requires `Send + Sync` for thread-safe evaluation.
///
/// # Methods
///
/// - [`left()`](BinaryExpression::left): Returns a reference to the left operand expression
/// - [`right()`](BinaryExpression::right): Returns a reference to the right operand expression
/// - [`get_expression_symbol()`](BinaryExpression::get_expression_symbol): Returns the operator
///   symbol
///
/// # Implementation Notes
///
/// Implementors should:
/// 1. Store left and right operands as `Box<dyn Expression>`
/// 2. Store the operator symbol as `CheetahString` for efficiency
/// 3. Implement the base `Expression::evaluate()` method to perform the binary operation
/// 4. Ensure thread safety for concurrent evaluation scenarios
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_filter::expression::BinaryExpression;
///
/// // Creating a binary expression tree
/// let expr = AndExpression::new(
///     Box::new(PropertyExpression::new("type")),
///     Box::new(ConstantExpression::new("order"))
/// );
///
/// // Access operands
/// let left_operand = expr.left();
/// let right_operand = expr.right();
/// let operator = expr.get_expression_symbol(); // Returns Some("AND")
/// ```
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync` to support concurrent message filtering
/// in multi-threaded RocketMQ broker environments.
#[allow(dead_code)]
pub trait BinaryExpression: Expression + Send + Sync {
    /// Returns a reference to the left operand expression.
    ///
    /// The left operand is the first expression in the binary operation.
    /// For example, in `A AND B`, `A` is the left operand.
    ///
    /// # Returns
    ///
    /// A reference to the boxed left operand expression.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let expr = AndExpression::new(left_expr, right_expr);
    /// let left = expr.left();
    /// // Evaluate or inspect the left operand
    /// ```
    fn left(&self) -> &dyn Expression;

    /// Returns a reference to the right operand expression.
    ///
    /// The right operand is the second expression in the binary operation.
    /// For example, in `A AND B`, `B` is the right operand.
    ///
    /// # Returns
    ///
    /// A reference to the boxed right operand expression.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let expr = AndExpression::new(left_expr, right_expr);
    /// let right = expr.right();
    /// // Evaluate or inspect the right operand
    /// ```
    fn right(&self) -> &dyn Expression;

    /// Returns the operator symbol for this binary expression.
    ///
    /// The symbol represents the operation performed between the left and right operands.
    /// Common symbols include:
    /// - Logical: "AND", "OR"
    /// - Comparison: "=", "!=", "<", ">", "<=", ">="
    /// - Arithmetic: "+", "-", "*", "/"
    ///
    /// # Returns
    ///
    /// - `Some(&CheetahString)`: The operator symbol if available
    /// - `None`: If the expression doesn't have a defined symbol
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let and_expr = AndExpression::new(left, right);
    /// assert_eq!(and_expr.get_expression_symbol(), Some(&CheetahString::from("AND")));
    ///
    /// let equals_expr = EqualsExpression::new(left, right);
    /// assert_eq!(equals_expr.get_expression_symbol(), Some(&CheetahString::from("=")));
    /// ```
    ///
    /// # Implementation Notes
    ///
    /// Some binary expressions might not have a meaningful symbol representation,
    /// in which case implementations should return `None`.
    fn get_expression_symbol(&self) -> Option<&CheetahString>;
}
