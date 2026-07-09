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

use crate::config::SecurityConfig;
use crate::guard::GuardError;
use crate::guard::RiskLevel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SecurityRole {
    ReadOnly,
    Diagnose,
    Operator,
}

impl SecurityRole {
    pub(crate) fn from_profile(profile: &str) -> Result<Self, GuardError> {
        match profile.trim().to_ascii_lowercase().as_str() {
            "read_only" | "readonly" | "read-only" => Ok(Self::ReadOnly),
            "diagnose" | "diagnostic" => Ok(Self::Diagnose),
            "operator" => Ok(Self::Operator),
            other => Err(GuardError::InvalidArgument(format!(
                "unknown security profile `{other}`"
            ))),
        }
    }

    fn allows(self, risk_level: RiskLevel) -> bool {
        match self {
            Self::ReadOnly => matches!(risk_level, RiskLevel::ReadOnly),
            Self::Diagnose => matches!(risk_level, RiskLevel::ReadOnly | RiskLevel::Diagnose),
            Self::Operator => matches!(
                risk_level,
                RiskLevel::ReadOnly | RiskLevel::Diagnose | RiskLevel::Change
            ),
        }
    }
}

pub(crate) fn check_rbac(security: &SecurityConfig, risk_level: RiskLevel) -> Result<(), GuardError> {
    let role = SecurityRole::from_profile(&security.profile)?;
    if role.allows(risk_level) {
        return Ok(());
    }

    Err(GuardError::PermissionDenied(format!(
        "profile `{}` cannot call {risk_level} tools",
        security.profile
    )))
}
