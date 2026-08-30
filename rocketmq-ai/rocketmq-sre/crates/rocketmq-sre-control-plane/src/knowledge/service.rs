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

use chrono::Utc;
use rocketmq_sre_contracts::KnowledgeItem;
use rocketmq_sre_contracts::KnowledgeItemId;
use rocketmq_sre_contracts::KnowledgeReviewStatus;
use semver::VersionReq;
use sha2::Digest;
use sha2::Sha256;

use super::chunk::chunk_markdown;
use super::model::ImportKnowledgeRequest;
use super::model::KnowledgeFeedbackRequest;
use super::model::KnowledgeImport;
use super::model::KnowledgeImportResult;
use super::model::KnowledgeListQuery;
use super::model::KnowledgePage;
use super::model::KnowledgeReviewRequest;
use super::model::KnowledgeSearchPage;
use super::model::KnowledgeSearchQuery;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const MAX_MARKDOWN_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct KnowledgeService {
    repository: PostgresRepository,
}

impl KnowledgeService {
    pub(crate) fn new(repository: PostgresRepository) -> Self {
        Self { repository }
    }

    pub(crate) async fn import(
        &self,
        auth: &AuthContext,
        request: ImportKnowledgeRequest,
    ) -> Result<KnowledgeImportResult, ControlPlaneError> {
        validate_import(&request)?;
        let chunks = chunk_markdown(&request.markdown)?;
        let now = Utc::now();
        let item = KnowledgeItem {
            id: KnowledgeItemId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: request.cluster_id,
            title: request.title.trim().to_owned(),
            component: request.component.trim().to_owned(),
            rocketmq_version_range: request.rocketmq_version_range.trim().to_owned(),
            source_uri: request.source_uri.trim().to_owned(),
            source_version: request.source_version.trim().to_owned(),
            valid_from: request.valid_from,
            valid_until: request.valid_until,
            owner: request.owner.trim().to_owned(),
            review_status: request.review_status,
            review_due_at: request.review_due_at,
            sensitivity: sensitivity_name(request.sensitivity).to_owned(),
            content_hash: format!(
                "sha256:{}",
                rocketmq_sre_contracts::encode_lower_hex(Sha256::digest(request.markdown.as_bytes()))
            ),
            conflict: false,
            created_at: now,
            updated_at: now,
        };
        self.repository
            .import_knowledge(auth, KnowledgeImport { item, chunks })
            .await
    }

    pub(crate) async fn item(
        &self,
        auth: &AuthContext,
        id: KnowledgeItemId,
    ) -> Result<KnowledgeItem, ControlPlaneError> {
        self.repository.knowledge_item(auth, id).await
    }

    pub(crate) async fn list(
        &self,
        auth: &AuthContext,
        query: &KnowledgeListQuery,
    ) -> Result<KnowledgePage, ControlPlaneError> {
        self.repository.list_knowledge(auth, query).await
    }

    pub(crate) async fn search(
        &self,
        auth: &AuthContext,
        query: &KnowledgeSearchQuery,
    ) -> Result<KnowledgeSearchPage, ControlPlaneError> {
        if query.q.trim().is_empty() || query.q.len() > 500 {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "knowledge query must contain between 1 and 500 bytes",
            ));
        }
        self.repository.search_knowledge(auth, query).await
    }

    pub(crate) async fn review(
        &self,
        auth: &AuthContext,
        id: KnowledgeItemId,
        request: &KnowledgeReviewRequest,
    ) -> Result<KnowledgeItem, ControlPlaneError> {
        if request.reason.trim().is_empty() || request.reason.len() > 2_000 {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "knowledge review reason must contain between 1 and 2000 bytes",
            ));
        }
        let current = self.repository.knowledge_item(auth, id).await?;
        validate_transition(current.review_status, request.status, request.human_confirmed)?;
        self.repository
            .transition_knowledge(auth, current, request.status, request.reason.trim())
            .await
    }

    pub(crate) async fn feedback(
        &self,
        auth: &AuthContext,
        id: KnowledgeItemId,
        request: &KnowledgeFeedbackRequest,
    ) -> Result<KnowledgeItem, ControlPlaneError> {
        if request.comment.as_ref().is_some_and(|comment| comment.len() > 2_000) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "knowledge feedback comment exceeds 2000 bytes",
            ));
        }
        self.repository.record_knowledge_feedback(auth, id, request).await
    }
}

fn validate_import(request: &ImportKnowledgeRequest) -> Result<(), ControlPlaneError> {
    for (name, value, max) in [
        ("title", request.title.as_str(), 300),
        ("component", request.component.as_str(), 100),
        ("source URI", request.source_uri.as_str(), 2_000),
        ("source version", request.source_version.as_str(), 200),
        ("owner", request.owner.as_str(), 200),
    ] {
        if value.trim().is_empty() || value.len() > max {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                format!("knowledge {name} must contain between 1 and {max} bytes"),
            ));
        }
    }
    VersionReq::parse(request.rocketmq_version_range.trim()).map_err(|_| {
        ControlPlaneError::validation(
            "invalid_request",
            "knowledge RocketMQ version range must be a semantic version requirement",
        )
    })?;
    if request.markdown.trim().is_empty() || request.markdown.len() > MAX_MARKDOWN_BYTES {
        return Err(ControlPlaneError::validation(
            "output_too_large",
            "knowledge Markdown must contain at most 1048576 bytes",
        ));
    }
    let lowercase_markdown = request.markdown.to_ascii_lowercase();
    if lowercase_markdown.contains("-----begin private key-----")
        || lowercase_markdown.contains("-----begin rsa private key-----")
        || lowercase_markdown.contains("authorization: bearer ")
    {
        return Err(ControlPlaneError::validation(
            "sensitive_content_rejected",
            "knowledge Markdown contains forbidden credential material",
        ));
    }
    if request.review_due_at <= Utc::now() {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "knowledge review due date must be in the future",
        ));
    }
    if request
        .valid_from
        .zip(request.valid_until)
        .is_some_and(|(from, until)| until <= from)
    {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "knowledge validity end must be later than its start",
        ));
    }
    if request.review_status == KnowledgeReviewStatus::Validated && (!request.human_validated || request.ai_generated) {
        return Err(ControlPlaneError::validation(
            "human_validation_required",
            "AI-generated or unconfirmed knowledge cannot be imported as validated",
        ));
    }
    Ok(())
}

fn validate_transition(
    current: KnowledgeReviewStatus,
    next: KnowledgeReviewStatus,
    human_confirmed: bool,
) -> Result<(), ControlPlaneError> {
    let allowed = matches!(
        (current, next),
        (KnowledgeReviewStatus::Draft, KnowledgeReviewStatus::InReview)
            | (KnowledgeReviewStatus::InReview, KnowledgeReviewStatus::Validated)
            | (
                KnowledgeReviewStatus::Draft | KnowledgeReviewStatus::InReview | KnowledgeReviewStatus::Validated,
                KnowledgeReviewStatus::Deprecated | KnowledgeReviewStatus::Expired
            )
    );
    if !allowed {
        return Err(ControlPlaneError::validation(
            "invalid_state_transition",
            "knowledge review lifecycle transition is not allowed",
        ));
    }
    if next == KnowledgeReviewStatus::Validated && !human_confirmed {
        return Err(ControlPlaneError::validation(
            "human_validation_required",
            "knowledge validation requires explicit human confirmation",
        ));
    }
    Ok(())
}

fn sensitivity_name(value: rocketmq_sre_contracts::Sensitivity) -> &'static str {
    match value {
        rocketmq_sre_contracts::Sensitivity::Public => "public",
        rocketmq_sre_contracts::Sensitivity::Internal => "internal",
        rocketmq_sre_contracts::Sensitivity::Confidential => "confidential",
        rocketmq_sre_contracts::Sensitivity::Restricted => "restricted",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_requires_human_confirmation() {
        let error = validate_transition(KnowledgeReviewStatus::InReview, KnowledgeReviewStatus::Validated, false)
            .expect_err("validation should require human confirmation");
        assert!(matches!(
            error,
            ControlPlaneError::Validation {
                code: "human_validation_required",
                ..
            }
        ));
    }

    #[test]
    fn terminal_knowledge_cannot_return_to_review() {
        assert!(
            validate_transition(KnowledgeReviewStatus::Deprecated, KnowledgeReviewStatus::InReview, true,).is_err()
        );
    }
}
