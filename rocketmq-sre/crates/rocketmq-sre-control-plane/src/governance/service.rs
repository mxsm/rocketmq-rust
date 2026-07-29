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

use chrono::Utc;
use rocketmq_sre_contracts::GovernanceAccessPath;
use rocketmq_sre_contracts::GovernanceActorKind;
use rocketmq_sre_contracts::GovernanceArtifact;
use rocketmq_sre_contracts::GovernanceArtifactId;
use rocketmq_sre_contracts::GovernanceEvent;
use rocketmq_sre_contracts::GovernanceEventId;
use rocketmq_sre_contracts::GovernanceImpact;
use rocketmq_sre_contracts::GovernanceLifecycleState;
use rocketmq_sre_contracts::GovernanceSignaturePayload;
use rocketmq_sre_contracts::GovernanceVersion;
use rocketmq_sre_contracts::GovernanceVersionId;
use rocketmq_sre_contracts::is_sha256_digest;

use super::admission::GovernanceAdmissionGuard;
use super::model::CreateGovernanceArtifactRequest;
use super::model::CreateGovernanceVersionRequest;
use super::model::EvaluateGovernanceAdmissionRequest;
use super::model::GOVERNANCE_API_SCHEMA_VERSION;
use super::model::GovernanceAdmissionView;
use super::model::GovernanceArtifactPage;
use super::model::GovernanceArtifactQuery;
use super::model::GovernanceAuditExport;
use super::model::GovernanceAuditQuery;
use super::model::GovernanceComplianceReport;
use super::model::GovernanceImpactPage;
use super::model::GovernanceImpactQuery;
use super::model::GovernanceVersionPage;
use super::model::GovernanceVersionQuery;
use super::model::RecordGovernanceImpactRequest;
use super::model::TransitionGovernanceVersionRequest;
use super::repository::GovernanceRepository;
use super::repository::human_event;
use super::signer::GovernanceSigner;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const MAX_COMPONENTS: usize = 64;
const MAX_DEPENDENCIES: usize = 64;

#[derive(Clone)]
pub(crate) struct GovernanceService {
    repository: GovernanceRepository,
    signer: GovernanceSigner,
    admission: GovernanceAdmissionGuard,
}

impl GovernanceService {
    pub(crate) fn new(
        repository: PostgresRepository,
        signing_key: impl AsRef<[u8]>,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            repository: GovernanceRepository::new(repository.pool.clone()),
            signer: GovernanceSigner::new(signing_key.as_ref())?,
            admission: GovernanceAdmissionGuard::new(repository, signing_key)?,
        })
    }

    pub(crate) async fn create_artifact(
        &self,
        auth: &AuthContext,
        request: &CreateGovernanceArtifactRequest,
    ) -> Result<GovernanceArtifact, ControlPlaneError> {
        require_governance_human(auth)?;
        validate_text("governance logical key", &request.logical_key, 256)?;
        validate_text("governance owner", &request.owner, 256)?;
        validate_text("governance reviewer", &request.reviewer, 256)?;
        if request.owner != auth.subject {
            return Err(ControlPlaneError::forbidden(
                "governance_owner_mismatch",
                "the authenticated human must own a newly governed artifact",
            ));
        }
        if request.owner == request.reviewer {
            return Err(ControlPlaneError::validation(
                "governance_separation_required",
                "governance owner and reviewer must be different humans",
            ));
        }
        let now = Utc::now();
        self.repository
            .create_artifact(&GovernanceArtifact {
                id: GovernanceArtifactId::new(),
                tenant_id: auth.tenant_id,
                kind: request.kind,
                logical_key: request.logical_key.trim().to_owned(),
                owner: request.owner.trim().to_owned(),
                reviewer: request.reviewer.trim().to_owned(),
                current_version_id: None,
                created_at: now,
                updated_at: now,
            })
            .await
    }

    pub(crate) async fn artifacts(
        &self,
        auth: &AuthContext,
        query: &GovernanceArtifactQuery,
    ) -> Result<GovernanceArtifactPage, ControlPlaneError> {
        require_read(auth)?;
        let (items, truncated) = self.repository.list_artifacts(auth.tenant_id, query).await?;
        Ok(GovernanceArtifactPage {
            schema_version: GOVERNANCE_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn create_version(
        &self,
        auth: &AuthContext,
        artifact_id: GovernanceArtifactId,
        request: &CreateGovernanceVersionRequest,
    ) -> Result<GovernanceVersion, ControlPlaneError> {
        require_candidate_author(auth)?;
        validate_version_request(request)?;
        let artifact = self.repository.get_artifact(auth.tenant_id, artifact_id).await?;
        if !is_model_actor(auth) && artifact.owner != auth.subject {
            return Err(ControlPlaneError::forbidden(
                "governance_owner_mismatch",
                "only the artifact owner or a model candidate author can create a draft version",
            ));
        }
        if let Some(rollback_id) = request.rollback_version_id {
            let rollback = self.repository.get_version(auth.tenant_id, rollback_id).await?;
            if rollback.artifact_id != artifact.id {
                return Err(ControlPlaneError::validation(
                    "governance_version_mismatch",
                    "rollback version belongs to a different governed artifact",
                ));
            }
        }
        let now = Utc::now();
        let version = GovernanceVersion {
            id: GovernanceVersionId::new(),
            artifact_id,
            tenant_id: auth.tenant_id,
            version: request.version.trim().to_owned(),
            content_digest: request.content_digest.clone(),
            signature: None,
            state: GovernanceLifecycleState::Draft,
            applicable_components: request.applicable_components.clone(),
            applicable_version_range: request.applicable_version_range.trim().to_owned(),
            dependencies: request.dependencies.clone(),
            review_due_at: request.review_due_at,
            expires_at: request.expires_at,
            replacement_version_id: None,
            rollback_version_id: request.rollback_version_id,
            created_by: auth.subject.clone(),
            created_at: now,
            updated_at: now,
        };
        let event = GovernanceEvent {
            id: GovernanceEventId::new(),
            tenant_id: auth.tenant_id,
            artifact_id,
            version_id: version.id,
            from_state: None,
            to_state: GovernanceLifecycleState::Draft,
            actor: auth.subject.clone(),
            actor_kind: if is_model_actor(auth) {
                GovernanceActorKind::Model
            } else {
                GovernanceActorKind::Human
            },
            reason: "Governed version candidate created".to_owned(),
            occurred_at: now,
        };
        self.repository.create_version(&version, &event).await
    }

    pub(crate) async fn versions(
        &self,
        auth: &AuthContext,
        artifact_id: GovernanceArtifactId,
        query: &GovernanceVersionQuery,
    ) -> Result<GovernanceVersionPage, ControlPlaneError> {
        require_read(auth)?;
        self.repository.get_artifact(auth.tenant_id, artifact_id).await?;
        let (items, truncated) = self
            .repository
            .list_versions(auth.tenant_id, artifact_id, query)
            .await?;
        Ok(GovernanceVersionPage {
            schema_version: GOVERNANCE_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn transition_version(
        &self,
        auth: &AuthContext,
        version_id: GovernanceVersionId,
        request: &TransitionGovernanceVersionRequest,
    ) -> Result<GovernanceVersion, ControlPlaneError> {
        require_governance_human(auth)?;
        validate_text("governance transition reason", &request.reason, 2_048)?;
        let current = self.repository.get_version(auth.tenant_id, version_id).await?;
        let artifact = self
            .repository
            .get_artifact(auth.tenant_id, current.artifact_id)
            .await?;
        if !current.state.permits_transition_to(request.state) {
            return Err(ControlPlaneError::conflict_code(
                "invalid_governance_transition",
                "the requested governance lifecycle transition is not allowed",
            ));
        }
        require_transition_actor(auth, &artifact, current.state, request.state)?;
        validate_related_version(
            &self.repository,
            auth,
            current.artifact_id,
            request.replacement_version_id,
            "replacement",
        )
        .await?;
        validate_related_version(
            &self.repository,
            auth,
            current.artifact_id,
            request.rollback_version_id,
            "rollback",
        )
        .await?;
        let now = Utc::now();
        let signature = if request.state == GovernanceLifecycleState::Active {
            if current.review_due_at <= now || current.expires_at.is_some_and(|expires_at| expires_at <= now) {
                return Err(ControlPlaneError::conflict_code(
                    "governance_version_expired",
                    "overdue or expired governance versions cannot be activated",
                ));
            }
            self.admission
                .ensure_dependencies_active(auth.tenant_id, &current.dependencies, now)
                .await?;
            Some(self.signer.sign(&GovernanceSignaturePayload::from(&current))?)
        } else {
            None
        };
        self.repository
            .transition_version(
                &current,
                signature.as_ref(),
                request.replacement_version_id,
                request.rollback_version_id.or(current.rollback_version_id),
                &human_event(&current, request.state, &auth.subject, request.reason.trim(), now),
            )
            .await
    }

    pub(crate) async fn record_impact(
        &self,
        auth: &AuthContext,
        version_id: GovernanceVersionId,
        request: &RecordGovernanceImpactRequest,
    ) -> Result<GovernanceImpact, ControlPlaneError> {
        require_governance_human(auth)?;
        require_cluster(auth, request.cluster_id)?;
        validate_text("governance impact reference", &request.reference_id, 256)?;
        validate_text("governance impact label", &request.label, 512)?;
        if let Some(cluster_id) = request.cluster_id
            && !self.repository.cluster_in_tenant(auth.tenant_id, cluster_id).await?
        {
            return Err(ControlPlaneError::forbidden(
                "tenant_mismatch",
                "governance impact cluster does not belong to the authenticated tenant",
            ));
        }
        self.repository.get_version(auth.tenant_id, version_id).await?;
        self.repository
            .record_impact(&GovernanceImpact {
                version_id,
                tenant_id: auth.tenant_id,
                cluster_id: request.cluster_id,
                kind: request.kind,
                reference_id: request.reference_id.trim().to_owned(),
                label: request.label.trim().to_owned(),
                observed_at: Utc::now(),
            })
            .await
    }

    pub(crate) async fn impacts(
        &self,
        auth: &AuthContext,
        version_id: GovernanceVersionId,
        query: &GovernanceImpactQuery,
    ) -> Result<GovernanceImpactPage, ControlPlaneError> {
        require_read(auth)?;
        require_cluster(auth, query.cluster_id)?;
        self.repository.get_version(auth.tenant_id, version_id).await?;
        let (items, truncated) = self
            .repository
            .list_impacts(auth.tenant_id, version_id, query)
            .await?;
        Ok(GovernanceImpactPage {
            schema_version: GOVERNANCE_API_SCHEMA_VERSION,
            items,
            truncated,
        })
    }

    pub(crate) async fn evaluate_admission(
        &self,
        auth: &AuthContext,
        request: &EvaluateGovernanceAdmissionRequest,
    ) -> Result<GovernanceAdmissionView, ControlPlaneError> {
        require_read(auth)?;
        require_cluster(auth, request.cluster_id)?;
        if request.access_path == GovernanceAccessPath::HighPrivilege {
            require_governance_human(auth)?;
        }
        Ok(GovernanceAdmissionView {
            schema_version: GOVERNANCE_API_SCHEMA_VERSION,
            decision: self
                .admission
                .evaluate(
                    auth.tenant_id,
                    request.cluster_id,
                    request.access_path,
                    &request.required_version_ids,
                    Utc::now(),
                )
                .await?,
        })
    }

    pub(crate) async fn audit_export(
        &self,
        auth: &AuthContext,
        query: &GovernanceAuditQuery,
    ) -> Result<GovernanceAuditExport, ControlPlaneError> {
        require_read(auth)?;
        if query.from.zip(query.to).is_some_and(|(from, to)| from >= to) {
            return Err(ControlPlaneError::validation(
                "invalid_governance_audit_window",
                "governance audit end must be after its start",
            ));
        }
        self.repository.audit_export(auth.tenant_id, query).await
    }

    pub(crate) async fn compliance(
        &self,
        auth: &AuthContext,
    ) -> Result<GovernanceComplianceReport, ControlPlaneError> {
        require_read(auth)?;
        self.repository.compliance_report(auth.tenant_id, Utc::now()).await
    }
}

async fn validate_related_version(
    repository: &GovernanceRepository,
    auth: &AuthContext,
    artifact_id: GovernanceArtifactId,
    version_id: Option<GovernanceVersionId>,
    relation: &str,
) -> Result<(), ControlPlaneError> {
    if let Some(version_id) = version_id {
        let version = repository.get_version(auth.tenant_id, version_id).await?;
        if version.artifact_id != artifact_id {
            return Err(ControlPlaneError::validation(
                "governance_version_mismatch",
                format!("{relation} version belongs to a different governed artifact"),
            ));
        }
    }
    Ok(())
}

fn validate_version_request(request: &CreateGovernanceVersionRequest) -> Result<(), ControlPlaneError> {
    validate_text("governance version", &request.version, 128)?;
    validate_text(
        "applicable RocketMQ version range",
        &request.applicable_version_range,
        256,
    )?;
    if !is_sha256_digest(&request.content_digest) {
        return Err(ControlPlaneError::validation(
            "invalid_governance_digest",
            "governed content must use a SHA-256 digest",
        ));
    }
    if request.applicable_components.len() > MAX_COMPONENTS || request.dependencies.len() > MAX_DEPENDENCIES {
        return Err(ControlPlaneError::validation(
            "invalid_governance_version",
            "governance components or dependencies exceed the supported bound",
        ));
    }
    for component in &request.applicable_components {
        validate_text("applicable component", component, 128)?;
    }
    for dependency in &request.dependencies {
        validate_text("dependency logical key", &dependency.logical_key, 256)?;
        validate_text("dependency version", &dependency.version, 128)?;
    }
    let now = Utc::now();
    if request.review_due_at <= now || request.expires_at.is_some_and(|expires_at| expires_at <= now) {
        return Err(ControlPlaneError::validation(
            "invalid_governance_expiry",
            "review due and expiry must be in the future",
        ));
    }
    Ok(())
}

fn require_transition_actor(
    auth: &AuthContext,
    artifact: &GovernanceArtifact,
    current: GovernanceLifecycleState,
    next: GovernanceLifecycleState,
) -> Result<(), ControlPlaneError> {
    if is_model_actor(auth) {
        return Err(ControlPlaneError::forbidden(
            "model_governance_transition_forbidden",
            "model identities cannot activate, publish, quarantine, deprecate, or retire governed versions",
        ));
    }
    let owner_transition = matches!(
        (current, next),
        (GovernanceLifecycleState::Draft, GovernanceLifecycleState::Review)
            | (GovernanceLifecycleState::Review, GovernanceLifecycleState::Draft)
    );
    let expected = if owner_transition {
        artifact.owner.as_str()
    } else {
        artifact.reviewer.as_str()
    };
    if auth.subject == expected {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "governance_reviewer_required",
            "the lifecycle transition requires the configured owner or reviewer",
        ))
    }
}

fn require_candidate_author(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if is_model_actor(auth) || is_governance_human(auth) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "governance version candidates require a model-service or governance role",
        ))
    }
}

fn require_governance_human(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if is_model_actor(auth) {
        return Err(ControlPlaneError::forbidden(
            "model_governance_transition_forbidden",
            "model identities cannot perform governance lifecycle operations",
        ));
    }
    if is_governance_human(auth) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "governance lifecycle operations require an operator or model-governance role",
        ))
    }
}

fn is_governance_human(auth: &AuthContext) -> bool {
    auth.roles.contains("operator") || auth.roles.contains("model-governance")
}

fn is_model_actor(auth: &AuthContext) -> bool {
    auth.roles.contains("model_service")
        || auth.roles.contains("provider_service")
        || auth.subject.starts_with("model:")
        || auth.subject.starts_with("provider:")
}

fn require_read(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "diagnose" | "operator" | "approver" | "model-governance" | "rocketmq:diagnose"
        )
    }) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "governance reads require diagnose or governance access",
        ))
    }
}

fn require_cluster(
    auth: &AuthContext,
    cluster_id: Option<rocketmq_sre_contracts::ClusterId>,
) -> Result<(), ControlPlaneError> {
    if cluster_id.is_none_or(|cluster_id| auth.clusters.contains(&cluster_id)) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "the authenticated identity cannot access this governance cluster",
        ))
    }
}

fn validate_text(name: &str, value: &str, max: usize) -> Result<(), ControlPlaneError> {
    let value = value.trim();
    if value.is_empty() || value.len() > max {
        return Err(ControlPlaneError::validation(
            "invalid_governance_request",
            format!("{name} must contain between 1 and {max} bytes"),
        ));
    }
    Ok(())
}
