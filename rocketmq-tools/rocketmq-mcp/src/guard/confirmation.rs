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

use rmcp::model::JsonObject;
use serde_json::Value;

use crate::config::SecurityConfig;
use crate::guard::GuardError;
use crate::guard::RiskLevel;

pub(crate) fn check_confirmation(
    security: &SecurityConfig,
    risk_level: RiskLevel,
    arguments: &JsonObject,
) -> Result<(), GuardError> {
    if !security.require_confirmation || !matches!(risk_level, RiskLevel::Change) {
        return Ok(());
    }

    if has_non_empty_string(arguments, "confirm_token") {
        return Ok(());
    }

    Err(GuardError::ConfirmationRequired(
        "change tools require a non-empty confirm_token".to_string(),
    ))
}

fn has_non_empty_string(arguments: &JsonObject, key: &str) -> bool {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}
