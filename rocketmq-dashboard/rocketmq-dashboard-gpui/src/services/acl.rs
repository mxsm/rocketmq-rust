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

use super::{AppServices, ServiceFuture, audit::MutationAudit};
use crate::{
    infrastructure::admin_provider::{GpuiAdminProvider, ProviderFailure},
    state::{UiError, UiErrorCode},
};
use rocketmq_dashboard_common::*;
use std::sync::Arc;

#[cfg(test)]
#[path = "acl_test_support.rs"]
pub(crate) mod test_support;
#[cfg(test)]
#[path = "acl_tests.rs"]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AclRequest {
    pub revision: u64,
    pub epoch: u64,
    pub scope: AclScope,
}
pub(crate) trait AclBackend: Send + Sync {
    fn users(&self, request: AclRequest) -> ServiceFuture<'_, Result<AclRead<AclUser>, UiError>>;
    fn policies(&self, request: AclRequest) -> ServiceFuture<'_, Result<AclRead<AclPolicy>, UiError>>;
    fn mutate(&self, revision: u64, command: AclCommand) -> ServiceFuture<'_, Result<AclWriteResult, UiError>>;
}
pub(super) struct RealAclBackend {
    provider: Option<Arc<GpuiAdminProvider>>,
}
impl RealAclBackend {
    pub(super) fn new(provider: Arc<GpuiAdminProvider>) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
        })
    }
    pub(super) fn unavailable() -> Arc<Self> {
        Arc::new(Self { provider: None })
    }
    fn provider(&self) -> Result<&Arc<GpuiAdminProvider>, UiError> {
        self.provider.as_ref().ok_or_else(|| {
            UiError::new(
                "ACL capability is unavailable in this configuration.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )
        })
    }
}
impl AclBackend for RealAclBackend {
    fn users(&self, request: AclRequest) -> ServiceFuture<'_, Result<AclRead<AclUser>, UiError>> {
        Box::pin(async move {
            self.provider()?
                .acl_users(request.revision, request.scope)
                .await
                .map_err(provider_error)
        })
    }
    fn policies(&self, request: AclRequest) -> ServiceFuture<'_, Result<AclRead<AclPolicy>, UiError>> {
        Box::pin(async move {
            self.provider()?
                .acl_policies(request.revision, request.scope)
                .await
                .map_err(provider_error)
        })
    }
    fn mutate(&self, revision: u64, command: AclCommand) -> ServiceFuture<'_, Result<AclWriteResult, UiError>> {
        Box::pin(async move {
            self.provider()?
                .mutate_acl(revision, command)
                .await
                .map_err(provider_error)
        })
    }
}
fn provider_error(error: ProviderFailure) -> UiError {
    let invalid = error.is_invalid_request();
    let denied = error.acl_access_denied()
        || error.code() == crate::infrastructure::admin_provider::ProviderFailureCode::Authentication;
    let unavailable = error.code() == crate::infrastructure::admin_provider::ProviderFailureCode::NotConfigured;
    let summary = if denied {
        "ACL access was denied or the configured Admin credentials are unavailable."
    } else if unavailable {
        "Select and connect a NameServer before accessing ACL data."
    } else if invalid {
        "ACL scope or record changed. Reload inventory and explicitly confirm scope again."
    } else {
        "ACL operation is unavailable or unconfirmed. Verify Broker state before another attempt."
    };
    let code = if denied {
        UiErrorCode::Authentication
    } else if unavailable {
        UiErrorCode::CapabilityUnavailable
    } else if invalid {
        UiErrorCode::Validation
    } else {
        UiErrorCode::Connection
    };
    match error.into_operational() {
        Some(source) => UiError::caused_by(summary, code, false, source),
        None => UiError::new(summary, code, false),
    }
}
impl AppServices {
    pub(crate) async fn acl_users(&self, request: AclRequest) -> Result<AclRead<AclUser>, UiError> {
        self.require_acl_access(request.revision)?;
        let backend = self.acl.clone();
        match &self.runtime_bridge {
            Some(runtime) => {
                runtime
                    .run("gpui-service-acl-users", async move { backend.users(request).await })
                    .await
            }
            None => backend.users(request).await,
        }
    }
    pub(crate) async fn acl_policies(&self, request: AclRequest) -> Result<AclRead<AclPolicy>, UiError> {
        self.require_acl_access(request.revision)?;
        let backend = self.acl.clone();
        match &self.runtime_bridge {
            Some(runtime) => {
                runtime
                    .run(
                        "gpui-service-acl-policies",
                        async move { backend.policies(request).await },
                    )
                    .await
            }
            None => backend.policies(request).await,
        }
    }
    pub(crate) async fn mutate_acl(&self, revision: u64, command: AclCommand) -> Result<AclWriteResult, UiError> {
        command
            .validate()
            .map_err(|_| UiError::new("Invalid ACL command.", UiErrorCode::Validation, false))?;
        self.require_acl_access(revision)?;
        let action = match &command.change {
            AclChange::SaveUser { create: true, .. } => AuditAction::AclUserCreated,
            AclChange::SaveUser { create: false, .. } => AuditAction::AclUserUpdated,
            AclChange::DeleteUser { .. } => AuditAction::AclUserDeleted,
            AclChange::SavePolicy { create: true, .. } => AuditAction::AclPolicyCreated,
            AclChange::SavePolicy { create: false, .. } => AuditAction::AclPolicyUpdated,
            AclChange::DeletePolicyEntry { .. } => AuditAction::AclPolicyDeleted,
        };
        let operation = self.accept_operation(
            revision,
            action,
            OperationResource::AclSubject(command.change.subject().into()),
        )?;
        let backend = self.acl.clone();
        self.run_mutation("gpui-service-acl-write", operation, async move {
            backend.mutate(revision, command).await
        })
        .await
    }
    #[cfg(test)]
    pub(crate) fn with_acl_backend(mut self, backend: Arc<dyn AclBackend>) -> Self {
        self.acl = backend;
        self
    }
}
impl MutationAudit for AclWriteResult {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        let applied = self
            .targets
            .iter()
            .filter(|target| target.state == AclWriteState::Applied)
            .count();
        let unknown = self
            .targets
            .iter()
            .filter(|target| target.state == AclWriteState::Unknown)
            .count();
        let outcome = if applied == self.targets.len() && applied > 0 {
            AuditOutcome::Succeeded
        } else if applied > 0 {
            AuditOutcome::Partial
        } else if unknown > 0 {
            AuditOutcome::Unknown
        } else {
            AuditOutcome::Rejected
        };
        (
            outcome,
            AuditFacts {
                applied_targets: applied as u32,
                unconfirmed_targets: unknown as u32,
                failure_code: if unknown > 0 {
                    Some(AuditFailureCode::Unknown)
                } else {
                    None
                },
                ..Default::default()
            },
        )
    }
}
