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
//! # See Also
//!
//! - [`Expression`]: Base trait for all expressions
//! - [`BooleanExpression`]: Trait for boolean-valued expressions
//! - [`EvaluationContext`]: Context for variable lookup during evaluation

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

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

    /// Sets the left operand expression.
    ///
    /// # Arguments
    ///
    /// * `expression` - The new left operand expression
    fn set_left(&mut self, expression: Box<dyn Expression>);

    /// Sets the right operand expression.
    ///
    /// # Arguments
    ///
    /// * `expression` - The new right operand expression
    fn set_right(&mut self, expression: Box<dyn Expression>);
}

/// Base implementation of a binary expression.
///
/// `BaseBinaryExpression` provides a concrete implementation of the `BinaryExpression` trait
/// with standard functionality for managing left and right operands, as well as the operator
/// symbol.
///
/// # Fields
///
/// - `left`: The left operand expression
/// - `right`: The right operand expression
/// - `symbol`: The operator symbol (e.g., "AND", "=", "+")
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_filter::expression::{BaseBinaryExpression, Expression};
/// use cheetah_string::CheetahString;
///
/// // Create a custom binary expression by extending BaseBinaryExpression
/// struct AddExpression {
///     base: BaseBinaryExpression,
/// }
///
/// impl AddExpression {
///     fn new(left: Box<dyn Expression>, right: Box<dyn Expression>) -> Self {
///         Self {
///             base: BaseBinaryExpression::new(
///                 left,
///                 right,
///                 CheetahString::from("+")
///             ),
///         }
///     }
/// }
/// ```
///
/// # Thread Safety
///
/// This struct implements `Send + Sync` for concurrent evaluation scenarios.
///
/// # Display Format
///
/// The `Display` implementation formats the expression as: `(left symbol right)`
/// For example: `(a + b)` or `(price > 100)`
pub struct BaseBinaryExpression {
    /// The left operand expression
    left: Box<dyn Expression>,
    /// The right operand expression
    right: Box<dyn Expression>,
    /// The operator symbol
    symbol: CheetahString,
}

impl BaseBinaryExpression {
    /// Creates a new `BaseBinaryExpression`.
    ///
    /// # Arguments
    ///
    /// * `left` - The left operand expression
    /// * `right` - The right operand expression
    /// * `symbol` - The operator symbol (e.g., "AND", "=", "+")
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use rocketmq_filter::expression::BaseBinaryExpression;
    /// use cheetah_string::CheetahString;
    ///
    /// let expr = BaseBinaryExpression::new(
    ///     Box::new(left_expr),
    ///     Box::new(right_expr),
    ///     CheetahString::from("=")
    /// );
    /// ```
    pub fn new(left: Box<dyn Expression>, right: Box<dyn Expression>, symbol: CheetahString) -> Self {
        Self { left, right, symbol }
    }
}

impl BinaryExpression for BaseBinaryExpression {
    fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }

    fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }

    fn get_expression_symbol(&self) -> Option<&CheetahString> {
        Some(&self.symbol)
    }

    fn set_left(&mut self, expression: Box<dyn Expression>) {
        self.left = expression;
    }

    fn set_right(&mut self, expression: Box<dyn Expression>) {
        self.right = expression;
    }
}

impl fmt::Display for BaseBinaryExpression {
    /// Formats the binary expression as: `(left symbol right)`
    ///
    /// # Examples
    ///
    /// For an expression with left="a", symbol="+", right="b":
    /// Output: `(a + b)`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.symbol, self.right)
    }
}

impl Hash for BaseBinaryExpression {
    /// Computes the hash based on the string representation.
    ///
    /// This ensures that expressions with the same structure and values
    /// produce the same hash.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_string().hash(state);
    }
}

impl PartialEq for BaseBinaryExpression {
    /// Compares two binary expressions for equality.
    ///
    /// Two expressions are equal if their string representations are identical.
    /// This means they must have the same left operand, symbol, and right operand.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let expr1 = BaseBinaryExpression::new(left1, right1, CheetahString::from("+"));
    /// let expr2 = BaseBinaryExpression::new(left2, right2, CheetahString::from("+"));
    /// assert_eq!(expr1 == expr2, expr1.to_string() == expr2.to_string());
    /// ```
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl Eq for BaseBinaryExpression {}

impl fmt::Debug for BaseBinaryExpression {
    /// Formats the binary expression for debugging.
    ///
    /// Uses the same format as Display: `(left symbol right)`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Expression for BaseBinaryExpression {
    /// Evaluates the binary expression.
    ///
    /// This default implementation should be overridden by concrete expression types
    /// to provide specific evaluation logic.
    ///
    /// # Arguments
    ///
    /// * `context` - The evaluation context containing variable bindings
    ///
    /// # Returns
    ///
    /// The result of evaluating this expression
    fn evaluate(
        &self,
        _context: &dyn crate::expression::EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        // Default implementation - should be overridden in derived types
        // For now, return the expression itself as a boxed Any
        Ok(Box::new(self.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::MessageEvaluationContext;

    #[test]
    fn test_base_binary_expression_new() {
        use crate::expression::AlwaysTrueExpression;

        let left = Box::new(AlwaysTrueExpression) as Box<dyn Expression>;
        let right = Box::new(AlwaysTrueExpression) as Box<dyn Expression>;
        let expr = BaseBinaryExpression::new(left, right, CheetahString::from("+"));

        assert_eq!(expr.get_expression_symbol(), Some(&CheetahString::from("+")));
    }

    #[test]
    fn test_base_binary_expression_display() {
        use crate::expression::AlwaysTrueExpression;

        let left = Box::new(AlwaysTrueExpression) as Box<dyn Expression>;
        let right = Box::new(AlwaysTrueExpression) as Box<dyn Expression>;
        let expr = BaseBinaryExpression::new(left, right, CheetahString::from("+"));

        assert_eq!(expr.to_string(), "(TRUE + TRUE)");
    }

    #[test]
    fn test_base_binary_expression_equality() {
        use crate::expression::AlwaysFalseExpression;
        use crate::expression::AlwaysTrueExpression;

        let expr1 = BaseBinaryExpression::new(
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            Box::new(AlwaysFalseExpression) as Box<dyn Expression>,
            CheetahString::from("+"),
        );

        let expr2 = BaseBinaryExpression::new(
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            Box::new(AlwaysFalseExpression) as Box<dyn Expression>,
            CheetahString::from("+"),
        );

        assert_eq!(expr1, expr2);
    }

    #[test]
    fn test_base_binary_expression_set_left_right() {
        use crate::expression::AlwaysFalseExpression;
        use crate::expression::AlwaysTrueExpression;

        let mut expr = BaseBinaryExpression::new(
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            CheetahString::from("+"),
        );

        assert_eq!(expr.to_string(), "(TRUE + TRUE)");

        expr.set_left(Box::new(AlwaysFalseExpression) as Box<dyn Expression>);
        assert_eq!(expr.to_string(), "(FALSE + TRUE)");

        expr.set_right(Box::new(AlwaysFalseExpression) as Box<dyn Expression>);
        assert_eq!(expr.to_string(), "(FALSE + FALSE)");
    }

    #[test]
    fn test_base_binary_expression_evaluate() {
        use crate::expression::AlwaysTrueExpression;

        let expr = BaseBinaryExpression::new(
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            CheetahString::from("+"),
        );

        let context = MessageEvaluationContext::new();
        let result = expr.evaluate(&context);
        assert!(result.is_ok());

        let value = result.unwrap();
        let string_value = value.downcast_ref::<String>().unwrap();
        assert_eq!(string_value, "(TRUE + TRUE)");
    }
}
