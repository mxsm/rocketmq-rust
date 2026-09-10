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

use super::{
    AclManager,
    service::validate_scope,
    types::{AclScope, validate_username},
};
use crate::error::{DashboardError, DashboardResult};
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::dashboard::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PolicyEntry {
    pub(crate) resources: Vec<String>,
    pub(crate) actions: Vec<String>,
    pub(crate) source_ips: Vec<String>,
    pub(crate) decision: PolicyDecision,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum PolicyDecision {
    Allow,
    Deny,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PolicyDraft {
    pub(crate) policy_type: DashboardAclPolicyType,
    pub(crate) entries: Vec<PolicyEntry>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PolicyChange {
    pub(crate) scope: AclScope,
    pub(crate) subject: String,
    pub(crate) policies: Vec<PolicyDraft>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PolicyDelete {
    pub(crate) scope: AclScope,
    pub(crate) subject: String,
    pub(crate) policy_type: DashboardAclPolicyType,
    pub(crate) resource: String,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PolicyView {
    pub(crate) subject: Option<String>,
    pub(crate) policy_type: Option<String>,
    pub(crate) entries: Vec<PolicyEntryView>,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PolicyEntryView {
    pub(crate) resource: Option<String>,
    pub(crate) actions: Vec<String>,
    pub(crate) source_ips: Vec<String>,
    pub(crate) decision: Option<String>,
}
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PolicyOperation {
    Create,
    Update,
    Delete,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PolicyResult {
    pub(crate) scope: AclScope,
    pub(crate) subject: String,
    pub(crate) operation: PolicyOperation,
    pub(crate) success: bool,
    pub(crate) policies: Option<Vec<PolicyView>>,
    pub(crate) read_back_error: Option<&'static str>,
}
impl crate::audit::types::AuditReceipt for PolicyResult {
    fn summary(&self) -> crate::audit::types::Summary {
        crate::audit::types::Summary::count(usize::from(self.success), usize::from(!self.success))
    }
}
impl PolicyChange {
    pub(crate) fn to_core(&self) -> DashboardResult<DashboardAclPolicyMutationRequest> {
        self.scope.validate()?;
        validate_username(&self.subject)?;
        if self.policies.is_empty() {
            return Err(DashboardError::Validation("Provide at least one ACL policy.".into()));
        }
        let mut identities = std::collections::HashSet::new();
        let mut policies = Vec::new();
        for policy in &self.policies {
            if policy.entries.is_empty() {
                return Err(DashboardError::Validation("Policy entries cannot be empty.".into()));
            }
            let mut entries = Vec::new();
            for entry in &policy.entries {
                if entry.resources.is_empty()
                    || entry.actions.is_empty()
                    || entry
                        .resources
                        .iter()
                        .chain(&entry.actions)
                        .any(|value| value.trim().is_empty() || value != value.trim())
                {
                    return Err(DashboardError::Validation(
                        "Every entry requires explicit resources and actions.".into(),
                    ));
                }
                for resource in &entry.resources {
                    if !identities.insert((policy.policy_type.as_str(), resource)) {
                        return Err(DashboardError::Validation("Duplicate policy resource identity.".into()));
                    }
                }
                entries.push(DashboardAclPolicyMutationEntry {
                    resources: entry.resources.clone(),
                    actions: entry.actions.clone(),
                    source_ips: entry.source_ips.clone(),
                    decision: match entry.decision {
                        PolicyDecision::Allow => "Allow",
                        PolicyDecision::Deny => "Deny",
                    }
                    .into(),
                });
            }
            policies.push(DashboardAclPolicyMutation {
                policy_type: policy.policy_type.as_str().into(),
                entries,
            });
        }
        Ok(DashboardAclPolicyMutationRequest {
            selector: self.scope.selector(),
            subject: self.subject.clone(),
            policies,
        })
    }
}
impl AclManager {
    pub(crate) async fn list_policies(&self, scope: AclScope) -> DashboardResult<Vec<PolicyView>> {
        scope.validate()?;
        let mut slot = self.session.lock().await;
        self.ensure(&mut slot).await?;
        let session = slot
            .as_ref()
            .ok_or(DashboardError::Internal("ACL session was not initialized"))?;
        validate_scope(&session.admin, &scope).await?;
        list_policies(&session.admin, &scope).await
    }
    pub(crate) async fn change_policy(
        &self,
        request: PolicyChange,
        operation: PolicyOperation,
    ) -> DashboardResult<PolicyResult> {
        request.to_core()?;
        let mut slot = self.session.lock().await;
        self.ensure(&mut slot).await?;
        let session = slot
            .as_ref()
            .ok_or(DashboardError::Internal("ACL session was not initialized"))?;
        validate_scope(&session.admin, &request.scope).await?;
        let core = request.to_core()?;
        match operation {
            PolicyOperation::Create => {
                session.admin.dashboard_create_acl_policy(&core).await?;
            }
            PolicyOperation::Update => {
                session.admin.dashboard_update_acl_policy(&core).await?;
            }
            PolicyOperation::Delete => return Err(DashboardError::Internal("Policy change cannot delete a resource")),
        }
        Ok(read_result(&session.admin, request.scope, request.subject, operation).await)
    }
    pub(crate) async fn delete_policy(&self, request: PolicyDelete) -> DashboardResult<PolicyResult> {
        let core = delete_request(&request)?;
        let mut slot = self.session.lock().await;
        self.ensure(&mut slot).await?;
        let session = slot
            .as_ref()
            .ok_or(DashboardError::Internal("ACL session was not initialized"))?;
        validate_scope(&session.admin, &request.scope).await?;
        let policies = list_policies(&session.admin, &request.scope).await?;
        validate_delete_identity(&policies, &request)?;
        session.admin.dashboard_delete_acl_entry(&core).await?;
        Ok(read_result(&session.admin, request.scope, request.subject, PolicyOperation::Delete).await)
    }
}
fn delete_request(request: &PolicyDelete) -> DashboardResult<DashboardAclEntryDeleteRequest> {
    request.scope.validate()?;
    validate_username(&request.subject)?;
    let core = DashboardAclEntryDeleteRequest {
        selector: request.scope.selector(),
        subject: request.subject.clone(),
        policy_type: request.policy_type,
        resource: request.resource.clone(),
    };
    core.validate()?;
    Ok(core)
}
fn validate_delete_identity(policies: &[PolicyView], request: &PolicyDelete) -> DashboardResult<()> {
    let count = policies
        .iter()
        .filter(|policy| {
            policy.subject.as_deref() == Some(request.subject.as_str())
                && policy
                    .policy_type
                    .as_deref()
                    .is_some_and(|kind| kind.eq_ignore_ascii_case(request.policy_type.as_str()))
        })
        .flat_map(|policy| &policy.entries)
        .filter(|entry| entry.resource.as_deref() == Some(request.resource.as_str()))
        .count();
    if count != 1 {
        return Err(DashboardError::Validation(
            "The exact ACL policy resource is missing or ambiguous. Refresh policies before deleting.".into(),
        ));
    }
    Ok(())
}
async fn list_policies(admin: &AdminSession, scope: &AclScope) -> DashboardResult<Vec<PolicyView>> {
    let policies = admin
        .dashboard_list_acl_policies(&DashboardAclQuery {
            selector: scope.selector(),
            ..Default::default()
        })
        .await?;
    if policies
        .iter()
        .any(|policy| policy.broker_name != scope.broker_name || policy.broker_addr != scope.broker_addr)
    {
        return Err(DashboardError::Validation(
            "ACL Broker identity changed during the query.".into(),
        ));
    }
    Ok(policies
        .into_iter()
        .map(|policy| PolicyView {
            subject: policy.subject,
            policy_type: policy.policy_type,
            entries: policy
                .entries
                .into_iter()
                .map(|entry| PolicyEntryView {
                    resource: entry.resource,
                    actions: entry.actions,
                    source_ips: entry.source_ips,
                    decision: entry.decision,
                })
                .collect(),
        })
        .collect())
}
async fn read_result(
    admin: &AdminSession,
    scope: AclScope,
    subject: String,
    operation: PolicyOperation,
) -> PolicyResult {
    let (policies, read_back_error) = match list_policies(admin, &scope).await {
        Ok(policies) => (Some(policies), None),
        Err(_) => (
            None,
            Some("ACL write was acknowledged, but policies could not be read back. Refresh before another operation."),
        ),
    };
    PolicyResult {
        scope,
        subject,
        operation,
        success: true,
        policies,
        read_back_error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope() -> AclScope {
        AclScope {
            cluster_name: "cluster".into(),
            broker_name: "broker-a".into(),
            broker_addr: "localhost:10911".into(),
        }
    }
    fn entry(resource: &str) -> PolicyEntryView {
        PolicyEntryView {
            resource: Some(resource.into()),
            actions: vec!["Pub".into()],
            source_ips: vec![],
            decision: Some("Allow".into()),
        }
    }
    #[test]
    fn deletion_uses_subject_policy_type_and_resource_instead_of_row_position() {
        let policies = vec![
            PolicyView {
                subject: Some("User:alice".into()),
                policy_type: Some("Custom".into()),
                entries: vec![entry("Topic:a"), entry("Topic:b")],
            },
            PolicyView {
                subject: Some("User:alice".into()),
                policy_type: Some("Default".into()),
                entries: vec![entry("Topic:a")],
            },
        ];
        let mut request = PolicyDelete {
            scope: scope(),
            subject: "User:alice".into(),
            policy_type: DashboardAclPolicyType::Default,
            resource: "Topic:a".into(),
        };
        validate_delete_identity(&policies, &request).unwrap();
        assert_eq!(
            delete_request(&request).unwrap().policy_type,
            DashboardAclPolicyType::Default
        );
        request.resource = "Topic:b".into();
        assert!(validate_delete_identity(&policies, &request).is_err());
        request.policy_type = DashboardAclPolicyType::Custom;
        validate_delete_identity(&policies, &request).unwrap();
        request.resource.clear();
        assert!(delete_request(&request).is_err());
    }
    #[test]
    fn policy_conversion_preserves_multiple_resources_and_rejects_duplicate_identity() {
        let mut request = PolicyChange {
            scope: scope(),
            subject: "User:alice".into(),
            policies: vec![PolicyDraft {
                policy_type: DashboardAclPolicyType::Custom,
                entries: vec![PolicyEntry {
                    resources: vec!["Topic:a".into(), "Topic:b".into()],
                    actions: vec!["Pub".into()],
                    source_ips: vec!["127.0.0.1".into()],
                    decision: PolicyDecision::Deny,
                }],
            }],
        };
        let core = request.to_core().unwrap();
        assert_eq!(core.policies[0].entries[0].resources, ["Topic:a", "Topic:b"]);
        assert_eq!(core.policies[0].entries[0].decision, "Deny");
        request.policies[0].entries[0].resources.push("Topic:a".into());
        assert!(request.to_core().is_err());
    }

    #[test]
    #[ignore = "requires the isolated deploy/dev/acl Docker fixture"]
    fn local_acl_policy_typed_delete_preserves_other_policy_and_resource() {
        use rocketmq_admin_core::client_adapter::{AdminBuilder, ClientRuntime, ClientRuntimeConfig, TelemetryHandle};
        use rocketmq_admin_core::core::security::AdminCredentials;
        use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("tauri-acl-policy-smoke"))
            .unwrap()
            .build()
            .unwrap();
        let runtime = ClientRuntime::try_new(
            owner.root_context().component("client"),
            ClientRuntimeConfig::default(),
            TelemetryHandle::noop(),
        )
        .unwrap();
        let result: DashboardResult<()> = owner.block_on(async {
            let mut admin = AdminBuilder::new(runtime.clone())
                .namesrv_addr("127.0.0.1:29876")
                .vip_channel_enabled(false)
                .credentials(AdminCredentials::try_new("tauri-dev-admin", "tauri-dev-secret", None).unwrap())
                .admin_group(format!("tauri-policy-{}", uuid::Uuid::new_v4()))
                .build_and_start()
                .await
                .unwrap();
            let scope = AclScope {
                cluster_name: "TauriAclDebugCluster".into(),
                broker_name: "tauri-acl-broker".into(),
                broker_addr: "127.0.0.1:22911".into(),
            };
            let username = format!("tauri-policy-{}", uuid::Uuid::new_v4().simple());
            let subject = format!("User:{username}");
            let checked: DashboardResult<()> = async {
                admin
                    .dashboard_create_acl_user(&DashboardAclUserMutationRequest {
                        selector: scope.selector(),
                        username: username.clone(),
                        password: "public-fixture-secret".into(),
                        user_type: "normal".into(),
                        user_status: None,
                    })
                    .await?;
                let mut request = PolicyChange {
                    scope: scope.clone(),
                    subject: subject.clone(),
                    policies: vec![
                        PolicyDraft {
                            policy_type: DashboardAclPolicyType::Custom,
                            entries: vec![PolicyEntry {
                                resources: vec!["Topic:tauri-policy-a".into(), "Topic:tauri-policy-b".into()],
                                actions: vec!["Pub".into()],
                                source_ips: vec![],
                                decision: PolicyDecision::Allow,
                            }],
                        },
                        PolicyDraft {
                            policy_type: DashboardAclPolicyType::Default,
                            entries: vec![PolicyEntry {
                                resources: vec!["Topic:tauri-policy-a".into()],
                                actions: vec!["Sub".into()],
                                source_ips: vec![],
                                decision: PolicyDecision::Allow,
                            }],
                        },
                    ],
                };
                admin.dashboard_create_acl_policy(&request.to_core()?).await?;
                request.policies.truncate(1);
                request.policies[0].entries[0].resources.truncate(1);
                request.policies[0].entries[0].decision = PolicyDecision::Deny;
                admin.dashboard_update_acl_policy(&request.to_core()?).await?;
                let deletion = PolicyDelete {
                    scope: scope.clone(),
                    subject: subject.clone(),
                    policy_type: DashboardAclPolicyType::Default,
                    resource: "Topic:tauri-policy-a".into(),
                };
                let before = list_policies(&admin, &scope).await?;
                validate_delete_identity(&before, &deletion)?;
                admin.dashboard_delete_acl_entry(&delete_request(&deletion)?).await?;
                let after = list_policies(&admin, &scope).await?;
                let custom = after
                    .iter()
                    .find(|policy| {
                        policy.subject.as_deref() == Some(subject.as_str())
                            && policy.policy_type.as_deref() == Some("Custom")
                    })
                    .ok_or(DashboardError::Internal("Custom policy disappeared"))?;
                if custom.entries.len() != 2
                    || !custom.entries.iter().any(|entry| {
                        entry.resource.as_deref() == Some("Topic:tauri-policy-a")
                            && entry.decision.as_deref() == Some("Deny")
                    })
                {
                    return Err(DashboardError::Internal("Policy update lost another resource"));
                }
                if after.iter().any(|policy| {
                    policy.subject.as_deref() == Some(subject.as_str())
                        && policy.policy_type.as_deref() == Some("Default")
                        && policy
                            .entries
                            .iter()
                            .any(|entry| entry.resource.as_deref() == Some("Topic:tauri-policy-a"))
                }) {
                    return Err(DashboardError::Internal("Default resource was not deleted"));
                }
                Ok(())
            }
            .await;
            let removed_acl = admin.dashboard_delete_acl_policy(&scope.selector(), &subject, "").await;
            let removed_user = admin.dashboard_delete_acl_user(&scope.selector(), &username).await;
            admin.shutdown().await;
            runtime.shutdown().await;
            checked?;
            removed_acl?;
            removed_user?;
            Ok(())
        });
        owner.shutdown_runtime_blocking().unwrap();
        result.unwrap();
    }
}
