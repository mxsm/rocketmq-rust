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
    if !matches!(risk_level, RiskLevel::Change) {
        return Ok(());
    }

    if is_dry_run_change(arguments) {
        return Ok(());
    }

    if has_non_empty_string(arguments, "confirm_token") {
        return Ok(());
    }

    if !security.require_confirmation && !is_apply_change(arguments) {
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

fn is_dry_run_change(arguments: &JsonObject) -> bool {
    arguments
        .get("mode")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| matches!(value.as_str(), "dry_run" | "dry-run" | "dryrun"))
}

fn is_apply_change(arguments: &JsonObject) -> bool {
    arguments
        .get("mode")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .is_some_and(|value| value == "apply")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_dry_run_does_not_require_confirm_token() {
        let arguments = serde_json::json!({
            "cluster": "local-dev",
            "mode": "dry_run",
        })
        .as_object()
        .unwrap()
        .clone();

        check_confirmation(&security(true), RiskLevel::Change, &arguments).unwrap();
    }

    #[test]
    fn change_apply_requires_confirm_token() {
        let arguments = serde_json::json!({
            "cluster": "local-dev",
            "mode": "apply",
        })
        .as_object()
        .unwrap()
        .clone();

        let err = check_confirmation(&security(true), RiskLevel::Change, &arguments).unwrap_err();

        assert!(err.to_string().contains("confirm_token"));
    }

    #[test]
    fn change_apply_requires_confirm_token_even_when_confirmation_is_disabled() {
        let arguments = serde_json::json!({
            "cluster": "local-dev",
            "mode": "apply",
        })
        .as_object()
        .unwrap()
        .clone();

        let err = check_confirmation(&security(false), RiskLevel::Change, &arguments).unwrap_err();

        assert!(err.to_string().contains("confirm_token"));
    }

    fn security(require_confirmation: bool) -> SecurityConfig {
        SecurityConfig {
            profile: "operator".to_string(),
            allow_dangerous_tools: true,
            require_confirmation,
            sanitize_output: true,
            rate_limit_per_minute: 60,
        }
    }
}
