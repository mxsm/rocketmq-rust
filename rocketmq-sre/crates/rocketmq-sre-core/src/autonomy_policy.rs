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

use rocketmq_sre_contracts::ActionDescriptor;
use rocketmq_sre_contracts::ActionRisk;
use rocketmq_sre_contracts::AutonomyCohortId;
use rocketmq_sre_contracts::AutonomyPolicyDefinition;
use rocketmq_sre_contracts::AutonomyQualificationCohort;
use rocketmq_sre_contracts::AutonomyQualificationLevel;
use rocketmq_sre_contracts::ContractError;
use rocketmq_sre_contracts::SreTimestamp;
use rocketmq_sre_contracts::canonical_sha256;
use rocketmq_sre_contracts::is_sha256_digest;

/// Stable actual model identity used by two-level autonomy cohorts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActualModelIdentity {
    pub profile: String,
    pub model_family: String,
    pub model_revision: String,
}

impl ActualModelIdentity {
    /// Returns the normalized identity hash used by cohort keys.
    ///
    /// # Errors
    ///
    /// Rejects empty, control-character, or unbounded identity fields.
    pub fn identity_hash(&self) -> Result<String, ContractError> {
        let profile = bounded(&self.profile, 128)?;
        let family = bounded(&self.model_family, 128)?.to_ascii_lowercase();
        let revision = bounded(&self.model_revision, 128)?;
        canonical_sha256(&(profile, family, revision))
    }
}

/// Validates policy-to-descriptor binding and derives qualification cohorts.
pub struct AutonomyPolicy;

impl AutonomyPolicy {
    /// Validates one immutable policy against the exact descriptor snapshot.
    ///
    /// # Errors
    ///
    /// Rejects non-R1, planning-only, mismatched, disabled, or malformed
    /// policies. Execution support is deliberately not required for Shadow.
    pub fn validate(
        policy: &AutonomyPolicyDefinition,
        descriptor: &ActionDescriptor,
    ) -> Result<(), ContractError> {
        policy.validate()?;
        if descriptor.id != policy.action.id()
            || descriptor.version != policy.action_version
            || descriptor.risk != ActionRisk::R1
            || descriptor.plan_only
            || !is_sha256_digest(&policy.descriptor_digest)
        {
            return Err(ContractError::InvalidDescriptor {
                reason: "autonomy policy must bind one exact, non-plan-only R1 descriptor".to_owned(),
            });
        }
        Ok(())
    }

    /// Builds a Shadow qualification cohort without a Critic identity.
    ///
    /// # Errors
    ///
    /// Returns canonical hashing or identity validation failures.
    pub fn shadow_cohort(
        policy: &AutonomyPolicyDefinition,
        primary: &ActualModelIdentity,
        created_at: SreTimestamp,
    ) -> Result<AutonomyQualificationCohort, ContractError> {
        let primary_hash = primary.identity_hash()?;
        let cohort_hash = shadow_hash(policy, &primary_hash)?;
        Ok(AutonomyQualificationCohort {
            id: AutonomyCohortId::new(),
            level: AutonomyQualificationLevel::Shadow,
            tenant_id: policy.tenant_id,
            cluster_id: policy.cluster_id,
            action: policy.action,
            action_version: policy.action_version.clone(),
            policy_definition_version: policy.definition_version,
            descriptor_digest: policy.descriptor_digest.clone(),
            diagnostic_pack_id: policy.diagnostic_pack_id.clone(),
            diagnostic_pack_version: policy.diagnostic_pack_version.clone(),
            primary_actual_model_identity_hash: primary_hash,
            critic_actual_model_identity_hash: None,
            cohort_hash,
            created_at,
        })
    }

    /// Builds an Autonomous qualification cohort from the Shadow key plus an
    /// actually invoked heterogeneous Critic identity.
    ///
    /// # Errors
    ///
    /// Rejects same-family main/Critic identities and invalid identity fields.
    pub fn autonomous_cohort(
        policy: &AutonomyPolicyDefinition,
        primary: &ActualModelIdentity,
        critic: &ActualModelIdentity,
        created_at: SreTimestamp,
    ) -> Result<AutonomyQualificationCohort, ContractError> {
        let primary_family = bounded(&primary.model_family, 128)?.to_ascii_lowercase();
        let critic_family = bounded(&critic.model_family, 128)?.to_ascii_lowercase();
        if primary_family == critic_family {
            return Err(ContractError::InvalidDescriptor {
                reason: "autonomy Critic must use a different normalized model family".to_owned(),
            });
        }
        let primary_hash = primary.identity_hash()?;
        let critic_hash = critic.identity_hash()?;
        let shadow_hash = shadow_hash(policy, &primary_hash)?;
        let cohort_hash = canonical_sha256(&("autonomous", shadow_hash, &critic_hash))?;
        Ok(AutonomyQualificationCohort {
            id: AutonomyCohortId::new(),
            level: AutonomyQualificationLevel::Autonomous,
            tenant_id: policy.tenant_id,
            cluster_id: policy.cluster_id,
            action: policy.action,
            action_version: policy.action_version.clone(),
            policy_definition_version: policy.definition_version,
            descriptor_digest: policy.descriptor_digest.clone(),
            diagnostic_pack_id: policy.diagnostic_pack_id.clone(),
            diagnostic_pack_version: policy.diagnostic_pack_version.clone(),
            primary_actual_model_identity_hash: primary_hash,
            critic_actual_model_identity_hash: Some(critic_hash),
            cohort_hash,
            created_at,
        })
    }
}

fn shadow_hash(policy: &AutonomyPolicyDefinition, primary_hash: &str) -> Result<String, ContractError> {
    canonical_sha256(&(
        "shadow",
        policy.tenant_id,
        policy.cluster_id,
        policy.action,
        policy.action_version.as_str(),
        policy.definition_version,
        policy.descriptor_digest.as_str(),
        policy.diagnostic_pack_id.as_str(),
        policy.diagnostic_pack_version.as_str(),
        primary_hash,
    ))
}

fn bounded(value: &str, maximum: usize) -> Result<&str, ContractError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > maximum || value.chars().any(char::is_control) {
        return Err(ContractError::InvalidDescriptor {
            reason: "actual model identity must be bounded plain text".to_owned(),
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::ActionRisk;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::CompensationMode;
    use rocketmq_sre_contracts::CompensationSpec;
    use rocketmq_sre_contracts::Deprecation;
    use rocketmq_sre_contracts::DescriptorStatus;
    use rocketmq_sre_contracts::ExecutionAction;
    use rocketmq_sre_contracts::ImpactScope;
    use rocketmq_sre_contracts::SchemaVersion;
    use rocketmq_sre_contracts::TenantId;
    use rocketmq_sre_contracts::VerificationSpec;

    use super::*;

    fn digest(value: char) -> String {
        format!("sha256:{}", value.to_string().repeat(64))
    }

    fn policy() -> AutonomyPolicyDefinition {
        AutonomyPolicyDefinition {
            id: rocketmq_sre_contracts::AutonomyPolicyId::new(),
            definition_version: 1,
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            action: ExecutionAction::ObservabilityLoggerLevelTtl,
            action_version: "1.0.0".to_owned(),
            descriptor_digest: digest('a'),
            diagnostic_pack_id: "runtime-diagnostics".to_owned(),
            diagnostic_pack_version: "1.0.0".to_owned(),
            owner: "messaging-observability".to_owned(),
            minimum_evidence_freshness_seconds: 60,
            required_evidence_sources: vec!["prometheus".to_owned()],
            min_shadow_samples: 20,
            min_supervised_successes: 5,
            observation_window_days: 7,
            max_unresolved_unknown: 0,
            max_recent_rollbacks: 0,
            max_executions_per_hour: 2,
            cooldown_seconds: 900,
            max_concurrent_executions: 1,
            stable_window_seconds: 300,
            created_at: chrono::Utc::now(),
        }
    }

    fn descriptor() -> ActionDescriptor {
        ActionDescriptor {
            id: ExecutionAction::ObservabilityLoggerLevelTtl.id().to_owned(),
            version: "1.0.0".to_owned(),
            owner: "messaging-observability".to_owned(),
            supported_versions: vec![SchemaVersion::new("rocketmq-sre.action-plan", 1, 0)],
            required_capabilities: ["runtime.typed-logger-control".to_owned()].into_iter().collect(),
            config_schema: serde_json::json!({"type": "object"}),
            parameter_schema: serde_json::json!({"type": "object"}),
            status: DescriptorStatus::Active,
            deprecation: None::<Deprecation>,
            risk: ActionRisk::R1,
            execution_supported: false,
            preconditions: vec!["runtime_error_budget_available".to_owned()],
            max_impact: ImpactScope::SingleResource,
            verification: VerificationSpec {
                resource_conditions: vec!["logger_level_applied".to_owned()],
                technical_slis: vec!["runtime_error_ratio".to_owned()],
                stable_window_seconds: 30,
                max_wait_seconds: 120,
            },
            timeout_seconds: 120,
            compensation: CompensationSpec {
                mode: CompensationMode::Automatic,
                required_before_fields: vec!["previous_level".to_owned()],
                timeout_seconds: 60,
            },
            forbidden_fields: Default::default(),
            plan_only: false,
        }
    }

    fn identity(profile: &str, family: &str) -> ActualModelIdentity {
        ActualModelIdentity {
            profile: profile.to_owned(),
            model_family: family.to_owned(),
            model_revision: "2026-07".to_owned(),
        }
    }

    #[test]
    fn shadow_can_qualify_before_execution_is_enabled() {
        let policy = policy();
        assert!(AutonomyPolicy::validate(&policy, &descriptor()).is_ok());
        let cohort = AutonomyPolicy::shadow_cohort(
            &policy,
            &identity("primary", "deepseek"),
            chrono::Utc::now(),
        )
        .expect("shadow cohort");
        assert_eq!(cohort.level, AutonomyQualificationLevel::Shadow);
        assert!(cohort.critic_actual_model_identity_hash.is_none());
    }

    #[test]
    fn same_family_alias_cannot_create_autonomous_cohort() {
        let policy = policy();
        assert!(
            AutonomyPolicy::autonomous_cohort(
                &policy,
                &identity("primary-cn", "DeepSeek"),
                &identity("critic-us", "deepseek"),
                chrono::Utc::now(),
            )
            .is_err()
        );
    }

    #[test]
    fn critic_fallback_identity_creates_a_new_autonomous_cohort() {
        let policy = policy();
        let primary = identity("primary", "deepseek");
        let first =
            AutonomyPolicy::autonomous_cohort(&policy, &primary, &identity("critic-a", "glm"), chrono::Utc::now())
                .expect("first cohort");
        let fallback =
            AutonomyPolicy::autonomous_cohort(&policy, &primary, &identity("critic-b", "kimi"), chrono::Utc::now())
                .expect("fallback cohort");
        assert_ne!(first.cohort_hash, fallback.cohort_hash);
    }
}
