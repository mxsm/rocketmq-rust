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

use std::error::Error;

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

pub trait Expression: Send + Sync + std::fmt::Display {
    /// Calculate expression result with context
    ///
    /// # Arguments
    ///
    /// * `context` - Context of evaluation
    ///
    /// # Returns
    ///
    /// The value of this expression
    fn evaluate(
        &self,
        context: &dyn EvaluationContext,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, Box<dyn Error + Send + Sync + 'static>>;
}
