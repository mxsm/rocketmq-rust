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
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::ModelProfileId;

use super::ModelGatewayService;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::models::lifecycle::ModelProfileLifecyclePage;
use crate::models::lifecycle::ModelProfileLifecycleTransitionRequest;
use crate::models::lifecycle::ModelProfileLifecycleView;
use crate::models::lifecycle::ModelProfileRollbackRequest;

impl ModelGatewayService {
    pub(crate) async fn profile_lifecycles(
        &self,
        auth: &AuthContext,
    ) -> Result<ModelProfileLifecyclePage, ControlPlaneError> {
        self.configured_profiles(auth).await?;
        Ok(ModelProfileLifecyclePage {
            schema_version: "rocketmq-sre.model-profile-lifecycle.v1",
            items: self.repository.model_profile_lifecycles(auth.tenant_id).await?,
            observed_at: Utc::now(),
        })
    }

    pub(crate) async fn profile_lifecycle(
        &self,
        auth: &AuthContext,
        profile_id: ModelProfileId,
    ) -> Result<ModelProfileLifecycleView, ControlPlaneError> {
        self.configured_profiles(auth).await?;
        self.repository
            .model_profile_lifecycle(auth.tenant_id, profile_id)
            .await
    }

    pub(crate) async fn transition_profile_lifecycle(
        &self,
        auth: &AuthContext,
        profile_id: ModelProfileId,
        request: &ModelProfileLifecycleTransitionRequest,
        correlation_id: CorrelationId,
    ) -> Result<ModelProfileLifecycleView, ControlPlaneError> {
        require_model_governance(auth)?;
        request
            .validate()
            .map_err(|detail| ControlPlaneError::validation("invalid_model_lifecycle_transition", detail))?;
        self.configured_profiles(auth).await?;
        self.repository
            .transition_model_profile_lifecycle(auth.tenant_id, profile_id, request, &auth.subject, correlation_id)
            .await?;
        self.repository
            .model_profile_lifecycle(auth.tenant_id, profile_id)
            .await
    }

    pub(crate) async fn rollback_profile(
        &self,
        auth: &AuthContext,
        profile_id: ModelProfileId,
        request: &ModelProfileRollbackRequest,
        correlation_id: CorrelationId,
    ) -> Result<ModelProfileLifecycleView, ControlPlaneError> {
        require_model_governance(auth)?;
        request
            .validate()
            .map_err(|detail| ControlPlaneError::validation("invalid_model_rollback", detail))?;
        self.configured_profiles(auth).await?;
        let active_profile_id = self
            .repository
            .rollback_model_profile(
                auth.tenant_id,
                profile_id,
                request.expected_revision,
                &request.reason_code,
                &auth.subject,
                correlation_id,
            )
            .await?;
        self.repository
            .model_profile_lifecycle(auth.tenant_id, active_profile_id)
            .await
    }
}

pub(super) fn require_model_governance(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth
        .roles
        .iter()
        .any(|role| matches!(role.as_str(), "model-governance" | "sre-admin"))
    {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "model_governance_role_required",
            "model profile lifecycle changes require the model-governance role",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use rocketmq_sre_contracts::TenantId;

    use super::*;

    fn auth(roles: &[&str]) -> AuthContext {
        AuthContext {
            tenant_id: TenantId::new(),
            subject: "model-owner".to_owned(),
            clusters: BTreeSet::new(),
            roles: roles.iter().map(|role| (*role).to_owned()).collect(),
        }
    }

    #[test]
    fn model_lifecycle_changes_require_dedicated_governance_role() {
        assert!(require_model_governance(&auth(&["operator"])).is_err());
        assert!(require_model_governance(&auth(&["model-governance"])).is_ok());
        assert!(require_model_governance(&auth(&["sre-admin"])).is_ok());
    }
}
