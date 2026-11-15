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

use std::collections::BTreeSet;
use std::fmt;
use std::io::Read;
use std::path::Path;

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ExecutionAction;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::VerificationSpec;
use rocketmq_sre_contracts::VerificationWindowPolicy;
use rocketmq_sre_contracts::canonical_sha256;
use serde::Deserialize;
use serde::Serialize;

use crate::ExecutorActionRegistry;
use crate::ExecutorError;

const SCHEMA: &str = "rocketmq-sre.execution-verification-profiles.v1";
const MAX_BYTES: usize = 128 * 1024;
const MAX_RULES: usize = 128;

/// Operator-selected observation profiles. These rules grant no write authority
/// and cannot prove a physical target's declared software version.
#[derive(Clone, Default)]
pub struct VerificationProfiles {
    rules: Vec<Rule>,
}

impl fmt::Debug for VerificationProfiles {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerificationProfiles")
            .field("rule_count", &self.rules.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    tenant_id: TenantId,
    cluster_id: ClusterId,
    action: ExecutionAction,
    descriptor_version: String,
    target: String,
    declared_target_version: String,
    policy_revision: String,
    max_source_age_seconds: u64,
    max_sample_gap_seconds: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema_version: String,
    rules: Vec<Rule>,
}

impl VerificationProfiles {
    /// Parses closed bounded configuration without returning raw private rules.
    ///
    /// # Errors
    /// Rejects oversized documents, duplicate/ambiguous rules, unknown fields,
    /// non-executable exact descriptors, wildcards and unbounded age/gap limits.
    pub fn from_json(bytes: &[u8]) -> Result<Self, ExecutorError> {
        if bytes.len() > MAX_BYTES {
            return Err(ExecutorError::Configuration);
        }
        let document: Document = serde_json::from_slice(bytes).map_err(ExecutorError::configuration_source)?;
        if document.schema_version != SCHEMA || document.rules.len() > MAX_RULES {
            return Err(ExecutorError::Configuration);
        }
        let registry = ExecutorActionRegistry::embedded()?;
        let mut identities = BTreeSet::new();
        for rule in &document.rules {
            if rule.tenant_id.as_uuid().is_nil()
                || rule.cluster_id.as_uuid().is_nil()
                || !literal(&rule.descriptor_version, 64)
                || !literal(&rule.target, 1024)
                || !literal(&rule.declared_target_version, 128)
                || !literal(&rule.policy_revision, 64)
                || !(1..=300).contains(&rule.max_source_age_seconds)
                || !(1..=300).contains(&rule.max_sample_gap_seconds)
                || registry
                    .verification_spec(rule.action, &rule.descriptor_version)
                    .is_err()
                || !identities.insert((
                    rule.tenant_id,
                    rule.cluster_id,
                    rule.action,
                    rule.descriptor_version.as_str(),
                    rule.target.as_str(),
                ))
            {
                return Err(ExecutorError::Configuration);
            }
        }
        Ok(Self { rules: document.rules })
    }

    pub(crate) fn load(path: &Path) -> Result<Self, ExecutorError> {
        let file = std::fs::File::open(path).map_err(ExecutorError::configuration_source)?;
        let metadata = file.metadata().map_err(ExecutorError::configuration_source)?;
        if !metadata.is_file() || metadata.len() > MAX_BYTES as u64 {
            return Err(ExecutorError::Configuration);
        }
        let mut bytes = Vec::new();
        file.take((MAX_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(ExecutorError::configuration_source)?;
        Self::from_json(&bytes)
    }

    pub(crate) fn resolve(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        action: ExecutionAction,
        descriptor_version: &str,
        target: &str,
        spec: &VerificationSpec,
    ) -> Result<Option<VerificationWindowPolicy>, ExecutorError> {
        let Some(rule) = self.rules.iter().find(|rule| {
            rule.tenant_id == tenant_id
                && rule.cluster_id == cluster_id
                && rule.action == action
                && rule.descriptor_version == descriptor_version
                && rule.target == target
        }) else {
            return Ok(None);
        };
        if spec.stable_window_seconds == 0
            || spec.stable_window_seconds > spec.max_wait_seconds
            || spec.max_wait_seconds > 86_400
        {
            return Err(ExecutorError::Configuration);
        }
        let policy_digest = canonical_sha256(&serde_json::json!({
            "schema_version": SCHEMA,
            "rule": rule,
            "verification": spec,
        }))
        .map_err(ExecutorError::configuration_source)?;
        Ok(Some(VerificationWindowPolicy {
            policy_digest,
            policy_revision: rule.policy_revision.clone(),
            declared_target_version: rule.declared_target_version.clone(),
            stable_window_seconds: spec.stable_window_seconds,
            max_wait_seconds: spec.max_wait_seconds,
            max_source_age_seconds: rule.max_source_age_seconds,
            max_sample_gap_seconds: rule.max_sample_gap_seconds,
        }))
    }
}

fn literal(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.is_ascii()
        && !value.contains(['*', '?'])
        && !value.chars().any(char::is_control)
        && value.trim() == value
}

#[cfg(test)]
mod tests;
