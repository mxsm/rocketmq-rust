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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ModelProfileId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// Operator-governed lifecycle of one configured model profile.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ModelProfileLifecycleState {
    Draft,
    Certified,
    Promoted,
    Quarantined,
    Retired,
}

impl ModelProfileLifecycleState {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Certified => "certified",
            Self::Promoted => "promoted",
            Self::Quarantined => "quarantined",
            Self::Retired => "retired",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, &'static str> {
        match value {
            "draft" => Ok(Self::Draft),
            "certified" => Ok(Self::Certified),
            "promoted" => Ok(Self::Promoted),
            "quarantined" => Ok(Self::Quarantined),
            "retired" => Ok(Self::Retired),
            _ => Err("stored model profile lifecycle state is invalid"),
        }
    }

    pub(super) const fn permits_operator_transition_to(self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::Draft, Self::Certified | Self::Quarantined | Self::Retired)
                | (Self::Certified, Self::Promoted | Self::Quarantined | Self::Retired)
                | (Self::Promoted, Self::Quarantined | Self::Retired)
                | (Self::Quarantined, Self::Certified | Self::Retired)
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelProfileLifecycleTransitionRequest {
    pub(crate) target_state: ModelProfileLifecycleState,
    pub(crate) expected_revision: u64,
    pub(crate) rollback_profile_id: Option<ModelProfileId>,
    pub(crate) reason_code: String,
    pub(crate) operator_confirmed: bool,
}

impl ModelProfileLifecycleTransitionRequest {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        if self.expected_revision == 0 {
            return Err("expected_revision must be greater than zero");
        }
        if !self.operator_confirmed {
            return Err("operator_confirmed must be true");
        }
        validate_reason_code(&self.reason_code)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelProfileRollbackRequest {
    pub(crate) expected_revision: u64,
    pub(crate) reason_code: String,
    pub(crate) operator_confirmed: bool,
}

impl ModelProfileRollbackRequest {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        if self.expected_revision == 0 {
            return Err("expected_revision must be greater than zero");
        }
        if !self.operator_confirmed {
            return Err("operator_confirmed must be true");
        }
        validate_reason_code(&self.reason_code)
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProviderSmokeResultView {
    pub(crate) id: uuid::Uuid,
    pub(crate) profile_id: ModelProfileId,
    pub(crate) connectivity_ok: bool,
    pub(crate) structured_output_ok: bool,
    pub(crate) tool_arguments_ok: bool,
    pub(crate) evidence_citation_ok: bool,
    pub(crate) overall_ok: bool,
    pub(crate) latency_ms: Option<u64>,
    pub(crate) failure_codes: Vec<String>,
    pub(crate) result_snapshot: Value,
    pub(crate) observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ModelProfileLifecycleView {
    pub(crate) profile_id: ModelProfileId,
    pub(crate) profile_name: String,
    pub(crate) provider_family: String,
    pub(crate) model_family: String,
    pub(crate) model_revision: String,
    pub(crate) state: ModelProfileLifecycleState,
    pub(crate) revision: u64,
    pub(crate) rollback_profile_id: Option<ModelProfileId>,
    pub(crate) reason_code: String,
    pub(crate) operator_confirmed: bool,
    pub(crate) updated_by: String,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) latest_smoke: Option<ProviderSmokeResultView>,
    pub(crate) automation_eligible: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ModelProfileLifecyclePage {
    pub(crate) schema_version: &'static str,
    pub(crate) items: Vec<ModelProfileLifecycleView>,
    pub(crate) observed_at: DateTime<Utc>,
}

fn validate_reason_code(value: &str) -> Result<(), &'static str> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err("reason_code must contain 1-128 safe identifier characters");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_is_terminal_and_quarantine_requires_recertification() {
        assert!(
            ModelProfileLifecycleState::Promoted
                .permits_operator_transition_to(ModelProfileLifecycleState::Quarantined)
        );
        assert!(
            ModelProfileLifecycleState::Quarantined
                .permits_operator_transition_to(ModelProfileLifecycleState::Certified)
        );
        assert!(
            !ModelProfileLifecycleState::Quarantined
                .permits_operator_transition_to(ModelProfileLifecycleState::Promoted)
        );
        assert!(!ModelProfileLifecycleState::Retired.permits_operator_transition_to(ModelProfileLifecycleState::Draft));
    }

    #[test]
    fn transition_requires_confirmation_revision_and_safe_reason() {
        let request = ModelProfileLifecycleTransitionRequest {
            target_state: ModelProfileLifecycleState::Certified,
            expected_revision: 1,
            rollback_profile_id: None,
            reason_code: "smoke.certified".to_owned(),
            operator_confirmed: true,
        };
        assert_eq!(request.validate(), Ok(()));

        let mut invalid = request;
        invalid.reason_code = "contains whitespace".to_owned();
        assert!(invalid.validate().is_err());
        invalid.reason_code = "valid".to_owned();
        invalid.operator_confirmed = false;
        assert!(invalid.validate().is_err());
    }
}
