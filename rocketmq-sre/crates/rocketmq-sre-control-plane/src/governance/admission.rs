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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::GovernanceAccessPath;
use rocketmq_sre_contracts::GovernanceAdmission;
use rocketmq_sre_contracts::GovernanceAdmissionId;
use rocketmq_sre_contracts::GovernanceLifecycleState;
use rocketmq_sre_contracts::GovernanceObjectKind;
use rocketmq_sre_contracts::GovernanceSignaturePayload;
use rocketmq_sre_contracts::GovernanceVersion;
use rocketmq_sre_contracts::GovernanceVersionId;
use rocketmq_sre_contracts::TenantId;

use super::repository::GovernanceRepository;
use super::signer::GovernanceSigner;
use crate::ControlPlaneError;
use crate::PostgresRepository;

const MAX_GOVERNANCE_DEPENDENCIES: usize = 128;

pub(crate) struct GovernanceRequirement<'a> {
    pub(crate) kind: GovernanceObjectKind,
    pub(crate) logical_key: &'a str,
    pub(crate) version: &'a str,
}

#[derive(Clone)]
pub(crate) struct GovernanceAdmissionGuard {
    repository: GovernanceRepository,
    signer: GovernanceSigner,
}

impl GovernanceAdmissionGuard {
    pub(crate) fn new(
        repository: PostgresRepository,
        signing_key: impl AsRef<[u8]>,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            repository: GovernanceRepository::new(repository.pool),
            signer: GovernanceSigner::new(signing_key)?,
        })
    }

    pub(crate) async fn evaluate(
        &self,
        tenant_id: TenantId,
        cluster_id: Option<ClusterId>,
        access_path: GovernanceAccessPath,
        required_version_ids: &[GovernanceVersionId],
        now: DateTime<Utc>,
    ) -> Result<GovernanceAdmission, ControlPlaneError> {
        let mut versions = Vec::with_capacity(required_version_ids.len());
        let mut initial_reasons = BTreeSet::new();
        if required_version_ids.is_empty() {
            initial_reasons.insert("governance_version_required".to_owned());
        }
        for id in required_version_ids {
            match self.repository.get_version(tenant_id, *id).await {
                Ok(version) => versions.push(version),
                Err(ControlPlaneError::NotFound) => {
                    initial_reasons.insert("governance_version_unknown".to_owned());
                }
                Err(error) => return Err(error),
            }
        }
        self.evaluate_versions(
            tenant_id,
            cluster_id,
            access_path,
            versions,
            initial_reasons,
            now,
        )
        .await
    }

    pub(crate) async fn ensure_high_privilege_overrides(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        requirements: &[GovernanceRequirement<'_>],
        now: DateTime<Utc>,
    ) -> Result<(), ControlPlaneError> {
        let mut versions = Vec::new();
        let mut reasons = BTreeSet::new();
        for requirement in requirements {
            let configured = self
                .repository
                .governance_override(
                    tenant_id,
                    requirement.kind,
                    requirement.logical_key,
                    requirement.version,
                )
                .await?;
            if !configured.artifact_present {
                continue;
            }
            match configured.version {
                Some(version) => versions.push(version),
                None => {
                    reasons.insert("governance_version_unknown".to_owned());
                }
            }
        }
        let decision = self
            .evaluate_versions(
                tenant_id,
                Some(cluster_id),
                GovernanceAccessPath::HighPrivilege,
                versions,
                reasons,
                now,
            )
            .await?;
        if decision.allowed {
            Ok(())
        } else {
            Err(ControlPlaneError::forbidden(
                "governance_admission_denied",
                format!(
                    "high-privilege execution was denied by governance: {}",
                    decision.reason_codes.join(",")
                ),
            ))
        }
    }

    async fn evaluate_versions(
        &self,
        tenant_id: TenantId,
        cluster_id: Option<ClusterId>,
        access_path: GovernanceAccessPath,
        initial_versions: Vec<GovernanceVersion>,
        mut reasons: BTreeSet<String>,
        now: DateTime<Utc>,
    ) -> Result<GovernanceAdmission, ControlPlaneError> {
        let mut hard_denied = !reasons.is_empty();
        let mut queue = initial_versions;
        let mut seen = BTreeSet::new();
        let mut required_version_ids = Vec::new();
        while let Some(version) = queue.pop() {
            if !seen.insert(version.id) {
                continue;
            }
            if seen.len() > MAX_GOVERNANCE_DEPENDENCIES {
                reasons.insert("governance_dependency_limit_exceeded".to_owned());
                hard_denied = true;
                break;
            }
            required_version_ids.push(version.id);
            if version.tenant_id != tenant_id {
                reasons.insert("governance_tenant_mismatch".to_owned());
                hard_denied = true;
                continue;
            }
            match access_path {
                GovernanceAccessPath::HighPrivilege => {
                    self.assess_high_privilege(&version, now, &mut reasons);
                }
                GovernanceAccessPath::ReadOnly => {
                    hard_denied |= self.assess_read_only(&version, now, &mut reasons);
                }
            }
            for dependency in &version.dependencies {
                let configured = self
                    .repository
                    .governance_override(tenant_id, dependency.kind, &dependency.logical_key, &dependency.version)
                    .await?;
                match configured.version {
                    Some(version) => queue.push(version),
                    None => {
                        reasons.insert("governance_dependency_missing".to_owned());
                        if access_path == GovernanceAccessPath::HighPrivilege {
                            hard_denied = true;
                        }
                    }
                }
            }
        }
        if access_path == GovernanceAccessPath::HighPrivilege && !reasons.is_empty() {
            hard_denied = true;
        }
        required_version_ids.sort_unstable();
        let allowed = !hard_denied;
        let degraded = allowed && access_path == GovernanceAccessPath::ReadOnly && !reasons.is_empty();
        self.repository
            .record_admission(&GovernanceAdmission {
                id: GovernanceAdmissionId::new(),
                tenant_id,
                cluster_id,
                access_path,
                required_version_ids,
                allowed,
                degraded,
                reason_codes: reasons.into_iter().collect(),
                evaluated_at: now,
            })
            .await
    }

    fn assess_high_privilege(
        &self,
        version: &GovernanceVersion,
        now: DateTime<Utc>,
        reasons: &mut BTreeSet<String>,
    ) {
        if version.state != GovernanceLifecycleState::Active {
            reasons.insert(state_reason(version.state).to_owned());
        }
        if version.review_due_at <= now {
            reasons.insert("governance_review_overdue".to_owned());
        }
        if version.expires_at.is_some_and(|expires_at| expires_at <= now) {
            reasons.insert("governance_version_expired".to_owned());
        }
        let signature_valid = version.signature.as_ref().is_some_and(|signature| {
            self.signer
                .verify(&GovernanceSignaturePayload::from(version), signature)
                .is_ok()
        });
        if !signature_valid {
            reasons.insert("governance_signature_invalid".to_owned());
        }
    }

    fn assess_read_only(
        &self,
        version: &GovernanceVersion,
        now: DateTime<Utc>,
        reasons: &mut BTreeSet<String>,
    ) -> bool {
        if matches!(
            version.state,
            GovernanceLifecycleState::Quarantined | GovernanceLifecycleState::Retired
        ) {
            reasons.insert(state_reason(version.state).to_owned());
            return true;
        }
        if version.state != GovernanceLifecycleState::Active {
            reasons.insert(state_reason(version.state).to_owned());
        }
        if version.review_due_at <= now {
            reasons.insert("governance_review_overdue".to_owned());
        }
        if version.expires_at.is_some_and(|expires_at| expires_at <= now) {
            reasons.insert("governance_version_expired".to_owned());
        }
        if version.state == GovernanceLifecycleState::Active {
            let signature_valid = version.signature.as_ref().is_some_and(|signature| {
                self.signer
                    .verify(&GovernanceSignaturePayload::from(version), signature)
                    .is_ok()
            });
            if !signature_valid {
                reasons.insert("governance_signature_invalid".to_owned());
            }
        }
        false
    }
}

const fn state_reason(state: GovernanceLifecycleState) -> &'static str {
    match state {
        GovernanceLifecycleState::Draft => "governance_version_draft",
        GovernanceLifecycleState::Review => "governance_version_in_review",
        GovernanceLifecycleState::Active => "governance_version_active",
        GovernanceLifecycleState::Deprecated => "governance_version_deprecated",
        GovernanceLifecycleState::Quarantined => "governance_version_quarantined",
        GovernanceLifecycleState::Retired => "governance_version_retired",
    }
}
