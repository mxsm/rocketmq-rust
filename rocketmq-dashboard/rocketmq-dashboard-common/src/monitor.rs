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

//! Persisted consumer-group threshold configuration; no evaluation or notification engine.

use serde::{Deserialize, Serialize};

use crate::{ConsumerIdentity, EnvironmentId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MonitorThresholds {
    pub min_count: u32,
    pub max_diff_total: u64,
}

impl MonitorThresholds {
    /// Parses the same nonnegative integer ranges as the Web rule contract.
    ///
    /// # Errors
    /// Rejects missing, signed, fractional, or out-of-range values.
    pub fn parse(min_count: &str, max_diff_total: &str) -> Result<Self, MonitorValidationError> {
        if [min_count, max_diff_total]
            .iter()
            .any(|value| value.trim().is_empty() || !value.trim().bytes().all(|byte| byte.is_ascii_digit()))
        {
            return Err(MonitorValidationError::Thresholds);
        }
        let thresholds = Self {
            min_count: min_count
                .trim()
                .parse()
                .map_err(|_| MonitorValidationError::Thresholds)?,
            max_diff_total: max_diff_total
                .trim()
                .parse()
                .map_err(|_| MonitorValidationError::Thresholds)?,
        };
        thresholds.validate()?;
        Ok(thresholds)
    }

    /// # Errors
    /// Rejects values beyond the signed Web persistence contract's ranges.
    pub fn validate(self) -> Result<(), MonitorValidationError> {
        if self.min_count > i32::MAX as u32 || self.max_diff_total > i64::MAX as u64 {
            return Err(MonitorValidationError::Thresholds);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MonitorRuleStatus {
    Ready,
    NeedsThresholds,
    NeedsEnvironmentAndThresholds,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitorRuleView {
    pub environment_id: Option<EnvironmentId>,
    /// Retained solely to identify a legacy entry during explicit assignment.
    pub legacy_id: String,
    pub consumer_group: String,
    pub enabled: bool,
    pub thresholds: Option<MonitorThresholds>,
    pub revision: u64,
    pub updated_at_epoch_ms: Option<u64>,
    pub status: MonitorRuleStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitorRulesSnapshot {
    pub environment_id: EnvironmentId,
    pub configuration_revision: u64,
    pub rules: Vec<MonitorRuleView>,
    pub unassigned: Vec<MonitorRuleView>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MonitorRuleChange {
    Save {
        thresholds: MonitorThresholds,
        enabled: bool,
        /// Assignment is explicit; reading a legacy rule never associates it with an environment.
        adopt_legacy_id: Option<String>,
    },
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitorRuleMutation {
    pub environment_id: EnvironmentId,
    pub consumer_group: String,
    /// Zero requires absence. Existing and migrated entries use their observed positive revision.
    pub expected_revision: u64,
    pub change: MonitorRuleChange,
}

impl MonitorRuleMutation {
    /// # Errors
    /// Rejects invalid Group names, thresholds, and create/edit revision combinations.
    pub fn validate(&self) -> Result<(), MonitorValidationError> {
        let group = ConsumerIdentity::parse(&self.consumer_group).map_err(|_| MonitorValidationError::Group)?;
        if group.as_str() != self.consumer_group {
            return Err(MonitorValidationError::Group);
        }
        match &self.change {
            MonitorRuleChange::Save {
                thresholds,
                adopt_legacy_id,
                ..
            } => {
                thresholds.validate()?;
                if adopt_legacy_id.as_ref().is_some_and(|id| id.is_empty())
                    || adopt_legacy_id.is_some() && self.expected_revision == 0
                {
                    return Err(MonitorValidationError::Revision);
                }
            }
            MonitorRuleChange::Delete if self.expected_revision == 0 => return Err(MonitorValidationError::Revision),
            MonitorRuleChange::Delete => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MonitorMutationOutcome {
    Saved(MonitorRuleView),
    Deleted,
    Conflict { latest: Option<MonitorRuleView> },
}

#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum MonitorValidationError {
    #[error("Consumer Group must be a valid group name of at most 255 bytes.")]
    Group,
    #[error("Min Count and Max Diff Total must be nonnegative integers within their supported ranges.")]
    Thresholds,
    #[error("Reload the rule revision before editing, assigning, or deleting it.")]
    Revision,
    #[error("Multiple legacy entries share this Group; no rule was changed.")]
    AmbiguousLegacy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_thresholds_reject_negative_fractional_and_out_of_range_values() {
        assert_eq!(
            MonitorThresholds::parse("0", "0").unwrap(),
            MonitorThresholds {
                min_count: 0,
                max_diff_total: 0
            }
        );
        for (minimum, maximum) in [
            ("-1", "1"),
            ("+1", "1"),
            ("1", "-1"),
            ("1.5", "0"),
            ("", "0"),
            ("2147483648", "0"),
            ("0", "9223372036854775808"),
        ] {
            assert_eq!(
                MonitorThresholds::parse(minimum, maximum),
                Err(MonitorValidationError::Thresholds)
            );
        }
        assert!(serde_json::from_str::<MonitorThresholds>(r#"{"minCount":-1,"maxDiffTotal":1}"#).is_err());
    }
}
