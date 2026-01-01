// Copyright 2025-2026 The RocketMQ Rust Authors
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

pub mod evaluation_context;

use std::error::Error;

use crate::expression::evaluation_context::EvaluationContext;

pub trait Expression {
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
