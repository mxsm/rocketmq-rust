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
//! use rocketmq_filter::expression::{BinaryExpression, Expression, EvaluationContext, Value, EvaluationError};
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
//!     fn evaluate(&self, context: &dyn EvaluationContext) -> Result<Value, EvaluationError> {
//!         let left_value = self.left.evaluate(context)?;
//!         let right_value = self.right.evaluate(context)?;
//!         // Compare values and return boolean result
//!         Ok(Value::Boolean(left_value == right_value))
//!     }
//! }
//! ```
//!
//! # See Also
//!
//! - [`crate::expression::Expression`]: Base trait for all expressions
//! - [`crate::expression::BooleanExpression`]: Trait for boolean-valued expressions
//! - [`crate::expression::EvaluationContext`]: Context for variable lookup during evaluation

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;

use crate::expression::Expression;
use cheetah_string::CheetahString;

/// Trait representing binary expressions that operate on two operands.
///
/// A binary expression combines two sub-expressions (left and right) using an operator.
/// Implementations must be `Send + Sync` for thread-safe evaluation.
pub trait BinaryExpression: Expression + Send + Sync {
    /// Returns a reference to the left operand expression.
    fn left(&self) -> &dyn Expression;

    /// Returns a reference to the right operand expression.
    fn right(&self) -> &dyn Expression;

    /// Returns the operator symbol for this binary expression.
    ///
    /// Returns `None` if the expression does not have a defined symbol.
    fn get_expression_symbol(&self) -> Option<&CheetahString>;

    /// Sets the left operand expression.
    fn set_left(&mut self, expression: Box<dyn Expression>);

    /// Sets the right operand expression.
    fn set_right(&mut self, expression: Box<dyn Expression>);
}

/// Base implementation of a binary expression.
///
/// Provides concrete storage for left and right operands along with an operator symbol.
/// The `Display` implementation formats expressions as `(left symbol right)`.
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.symbol, self.right)
    }
}

impl Hash for BaseBinaryExpression {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_string().hash(state);
    }
}

impl PartialEq for BaseBinaryExpression {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl Eq for BaseBinaryExpression {}

impl fmt::Debug for BaseBinaryExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Expression for BaseBinaryExpression {
    fn evaluate(
        &self,
        _context: &dyn crate::expression::EvaluationContext,
    ) -> Result<crate::expression::Value, crate::expression::EvaluationError> {
        Ok(crate::expression::Value::String(cheetah_string::CheetahString::from(
            self.to_string(),
        )))
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
        use crate::expression::Value;

        let expr = BaseBinaryExpression::new(
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            Box::new(AlwaysTrueExpression) as Box<dyn Expression>,
            CheetahString::from("+"),
        );

        let context = MessageEvaluationContext::new();
        let result = expr.evaluate(&context);
        assert!(result.is_ok());

        let value = result.unwrap();
        match value {
            Value::String(s) => assert_eq!(s.as_str(), "(TRUE + TRUE)"),
            _ => panic!("Expected Value::String, got {:?}", value),
        }
    }
}
