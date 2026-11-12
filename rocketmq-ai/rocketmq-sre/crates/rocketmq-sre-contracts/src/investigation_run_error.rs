// Copyright 2026 The RocketMQ Rust Authors
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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Sanitized failures for the independent investigation-run contract.
///
/// These codes do not change the established Incident or execution error enum.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case", deny_unknown_fields)]
pub enum InvestigationRunFailure {
    InvalidInput,
    Unauthorized,
    ScopeMismatch,
    PackNotAllowed,
    InvalidStateTransition,
    RevisionConflict {
        expected: crate::InvestigationRevision,
        actual: crate::InvestigationRevision,
    },
    IdempotencyConflict,
    BudgetExceeded,
    DeadlineExceeded,
    StepIdentityConflict,
    StepAlreadyCompleted,
    StepInFlight,
    StepNotFound,
    AttemptMismatch,
    ResultConflict,
    InvalidAssessment,
    CounterExhausted,
}

impl std::fmt::Display for InvestigationRunFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("investigation run contract was rejected")
    }
}

impl std::error::Error for InvestigationRunFailure {}

pub(crate) fn valid_run_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

pub(crate) fn valid_run_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}

pub(crate) fn unique_run_ids<T: Ord>(values: &[T], maximum: usize) -> bool {
    values.len() <= maximum && values.iter().collect::<std::collections::BTreeSet<_>>().len() == values.len()
}
