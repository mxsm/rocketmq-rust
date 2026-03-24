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

pub mod binary_expression;
pub mod boolean_expression;
pub mod empty_evaluation_context;
pub mod evaluation_context;

use cheetah_string::CheetahString;
use std::fmt;

pub use binary_expression::BaseBinaryExpression;
pub use binary_expression::BinaryExpression;
pub use boolean_expression::AlwaysFalseExpression;
pub use boolean_expression::AlwaysTrueExpression;
pub use boolean_expression::AndExpression;
pub use boolean_expression::BooleanExpression;
pub use boolean_expression::NotExpression;
pub use boolean_expression::OrExpression;
pub use boolean_expression::PropertyEqualsExpression;
pub use empty_evaluation_context::EmptyEvaluationContext;
pub use evaluation_context::EvaluationContext;
pub use evaluation_context::MessageEvaluationContext;

/// Value types that can be returned from expression evaluation.
///
/// This enum represents all possible expression result values. Each variant
/// corresponds to a supported data type in the filter expression system.
///
/// # Examples
///
/// ```
/// use cheetah_string::CheetahString;
/// use rocketmq_filter::expression::Value;
///
/// let bool_value = Value::Boolean(true);
/// let string_value = Value::String(CheetahString::from("test"));
/// let number_value = Value::Long(42);
///
/// // Pattern matching for type-safe access
/// match bool_value {
///     Value::Boolean(b) => println!("Boolean: {}", b),
///     _ => println!("Not a boolean"),
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Boolean value (true/false)
    Boolean(bool),
    /// String value using efficient CheetahString
    String(CheetahString),
    /// 64-bit signed integer
    Long(i64),
    /// 64-bit floating point number
    Double(f64),
    /// Null/absent value
    Null,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Boolean(b) => write!(f, "{}", b),
            Value::String(s) => write!(f, "{}", s),
            Value::Long(l) => write!(f, "{}", l),
            Value::Double(d) => write!(f, "{}", d),
            Value::Null => write!(f, "null"),
        }
    }
}

impl Value {
    /// Converts the value to a boolean for conditional evaluation.
    ///
    /// # Conversion Rules
    ///
    /// - `Boolean(b)` → `b`
    /// - `String(s)` → `!s.is_empty()`
    /// - `Long(n)` → `n != 0`
    /// - `Double(d)` → `d != 0.0`
    /// - `Null` → `false`
    ///
    /// # Examples
    ///
    /// ```
    /// use rocketmq_filter::expression::Value;
    ///
    /// assert_eq!(Value::Boolean(true).as_bool(), true);
    /// assert_eq!(Value::Long(0).as_bool(), false);
    /// assert_eq!(Value::Null.as_bool(), false);
    /// ```
    #[inline]
    pub fn as_bool(&self) -> bool {
        match self {
            Value::Boolean(b) => *b,
            Value::String(s) => !s.is_empty(),
            Value::Long(n) => *n != 0,
            Value::Double(d) => *d != 0.0,
            Value::Null => false,
        }
    }

    /// Checks if the value is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Error type for expression evaluation failures.
///
/// Each variant represents a specific failure mode that may occur during
/// expression evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum EvaluationError {
    /// Variable not found in evaluation context
    VariableNotFound(CheetahString),
    /// Type mismatch during operation (expected, actual)
    TypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    /// Invalid operation (e.g., division by zero)
    InvalidOperation(CheetahString),
    /// Other evaluation errors
    Other(CheetahString),
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvaluationError::VariableNotFound(name) => {
                write!(f, "Variable not found: {}", name)
            }
            EvaluationError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            EvaluationError::InvalidOperation(msg) => {
                write!(f, "Invalid operation: {}", msg)
            }
            EvaluationError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for EvaluationError {}

/// Core trait for all expression types in the RocketMQ filter system.
///
/// Implementations can be evaluated within an evaluation context to produce a value.
/// This trait serves as the foundation for SQL92 filtering and custom expression evaluation.
///
/// Evaluation returns a `Result<Value, EvaluationError>` containing either the computed
/// value or an error describing the failure mode.
///
/// All implementations must be `Send + Sync` to support concurrent message filtering
/// in multi-threaded broker environments.
///
/// # Examples
///
/// ```ignore
/// use rocketmq_filter::expression::{Expression, Value, EvaluationContext};
///
/// struct ConstantExpression(Value);
///
/// impl Expression for ConstantExpression {
///     fn evaluate(&self, _context: &dyn EvaluationContext) -> Result<Value, EvaluationError> {
///         Ok(self.0.clone())
///     }
/// }
///
/// impl fmt::Display for ConstantExpression {
///     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
///         write!(f, "{}", self.0)
///     }
/// }
/// ```
pub trait Expression: Send + Sync + fmt::Display {
    /// Evaluates the expression within the given context.
    ///
    /// # Errors
    ///
    /// Returns an error if a required variable is missing from the context, if type
    /// constraints are violated, or if an invalid operation is attempted.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let expr = SomeExpression::new();
    /// let context = MessageEvaluationContext::new();
    ///
    /// match expr.evaluate(&context) {
    ///     Ok(Value::Boolean(true)) => println!("Expression is true"),
    ///     Ok(value) => println!("Result: {}", value),
    ///     Err(e) => eprintln!("Evaluation failed: {}", e),
    /// }
    /// ```
    fn evaluate(&self, context: &dyn EvaluationContext) -> Result<Value, EvaluationError>;
}
