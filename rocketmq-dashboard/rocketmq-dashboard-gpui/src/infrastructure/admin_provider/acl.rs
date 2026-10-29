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

use super::{GpuiAdminProvider, ProviderFailure, mutation_for_revision, query_for_revision, select_admin};
use rocketmq_admin_core::core::{
    AdminError,
    acl_workspace::{AclEntryWrite, AclExactWrite, AclReadKind, AclRemoteFailure, classify_acl_failure},
};
use rocketmq_dashboard_common::*;
use std::sync::Arc;

#[path = "acl_decode.rs"]
mod decode;

impl ProviderFailure {
    pub(crate) fn acl_access_denied(&self) -> bool {
        read_failure(self) == AclFailure::AccessDenied
    }
}

impl GpuiAdminProvider {
    pub async fn acl_users(
        self: &Arc<Self>,
        revision: u64,
        scope: AclScope,
    ) -> Result<AclRead<AclUser>, ProviderFailure> {
        let this = self.clone();
        self.run_owned("gpui-acl-users", move |cancellation| async move {
            this.require_acl_scope(revision)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            verify_scope(session, &scope, cancellation.clone()).await?;
            let mut result = AclRead::default();
            for target in scope.targets() {
                match select_admin(
                    cancellation.clone(),
                    session.acl_documents(&target.address, AclReadKind::Users),
                )
                .await
                {
                    Ok(values) => {
                        let values = values.into_values();
                        result.truncated |= values.len() > ACL_ROW_LIMIT;
                        result.items.extend(decode::users(target, values));
                    }
                    Err(error) => result.failures.push(AclTargetFailure {
                        target: target.clone(),
                        failure: read_failure(&error),
                    }),
                }
            }
            result.truncated |= result.items.len() > ACL_ROW_LIMIT;
            result.items.truncate(ACL_ROW_LIMIT);
            Ok(result)
        })
        .await
    }
    pub async fn acl_policies(
        self: &Arc<Self>,
        revision: u64,
        scope: AclScope,
    ) -> Result<AclRead<AclPolicy>, ProviderFailure> {
        let this = self.clone();
        self.run_owned("gpui-acl-policies", move |cancellation| async move {
            this.require_acl_scope(revision)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            verify_scope(session, &scope, cancellation.clone()).await?;
            let mut result = AclRead::default();
            for target in scope.targets() {
                match select_admin(
                    cancellation.clone(),
                    session.acl_documents(&target.address, AclReadKind::Policies),
                )
                .await
                {
                    Ok(values) => {
                        let values = values.into_values();
                        result.truncated |= values.len() > ACL_ROW_LIMIT;
                        result.items.extend(decode::policies(target, values));
                    }
                    Err(error) => result.failures.push(AclTargetFailure {
                        target: target.clone(),
                        failure: read_failure(&error),
                    }),
                }
            }
            result.truncated |= result.items.len() > ACL_ROW_LIMIT;
            result.items.truncate(ACL_ROW_LIMIT);
            Ok(result)
        })
        .await
    }
    pub async fn mutate_acl(
        self: &Arc<Self>,
        revision: u64,
        command: AclCommand,
    ) -> Result<AclWriteResult, ProviderFailure> {
        command
            .validate()
            .map_err(|_| ProviderFailure::invalid_request("The ACL command is invalid."))?;
        let this = self.clone();
        self.run_owned("gpui-acl-write", move |cancellation| async move {
            this.require_acl_scope(revision)?;
            let read_guard = this.query_session.read().await;
            let query = query_for_revision(&read_guard, revision)?;
            verify_scope(query, &command.scope, cancellation.clone()).await?;
            let mut write_guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut write_guard, revision, cancellation.clone())
                .await?;
            let mutation = mutation_for_revision(&mut write_guard, revision)?;
            let mut targets = Vec::new();
            for target in &command.targets {
                let preflight = preflight(query, target, &command.change, cancellation.clone()).await;
                let state = match preflight {
                    Ok(()) => match select_admin(
                        cancellation.clone(),
                        mutation.apply_acl(&target.address, &wire(&command.change)),
                    )
                    .await
                    {
                        Ok(()) => AclWriteState::Applied,
                        Err(error) => match remote_failure(&error) {
                            AclRemoteFailure::AccessDenied => AclWriteState::Rejected(AclFailure::AccessDenied),
                            AclRemoteFailure::Invalid => AclWriteState::Rejected(AclFailure::Invalid),
                            AclRemoteFailure::Unsupported => AclWriteState::Rejected(AclFailure::Unavailable),
                            AclRemoteFailure::Unconfirmed => AclWriteState::Unknown,
                        },
                    },
                    Err(failure) => AclWriteState::Rejected(failure),
                };
                targets.push(AclWriteTarget {
                    target: target.clone(),
                    state,
                });
            }
            Ok(AclWriteResult { targets })
        })
        .await
    }
    fn require_acl_scope(&self, revision: u64) -> Result<(), ProviderFailure> {
        if self.snapshot_for_revision(revision)?.scope != ConnectionScope::NameServer {
            return Err(ProviderFailure::invalid_request(
                "ACL administration requires NameServer Direct scope.",
            ));
        }
        Ok(())
    }
}

async fn verify_scope(
    session: &dyn crate::infrastructure::admin_session::DashboardQuerySession,
    scope: &AclScope,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<(), ProviderFailure> {
    let inventory = select_admin(cancellation, session.list_brokers()).await?;
    let identities = inventory
        .items
        .into_iter()
        .map(|row| BrokerIdentity {
            cluster: row.cluster_name,
            broker_name: row.broker_name,
            broker_id: row.broker_id,
            address: row.address,
        })
        .collect::<Vec<_>>();
    scope
        .verify(&identities)
        .map_err(|_| ProviderFailure::invalid_request("The confirmed ACL scope changed. Select and confirm it again."))
}
fn read_failure(error: &ProviderFailure) -> AclFailure {
    match remote_failure(error) {
        AclRemoteFailure::AccessDenied => AclFailure::AccessDenied,
        AclRemoteFailure::Invalid => AclFailure::Invalid,
        AclRemoteFailure::Unsupported | AclRemoteFailure::Unconfirmed => AclFailure::Unavailable,
    }
}
fn remote_failure(error: &ProviderFailure) -> AclRemoteFailure {
    use std::error::Error as StdError;
    let operational = error.clone().into_operational();
    let mut source = operational.as_ref().map(|source| source as &(dyn StdError + 'static));
    while let Some(current) = source {
        if let Some(admin) = current.downcast_ref::<AdminError>() {
            return classify_acl_failure(admin);
        }
        source = current.source();
    }
    AclRemoteFailure::Unconfirmed
}
async fn preflight(
    session: &dyn crate::infrastructure::admin_session::DashboardQuerySession,
    target: &BrokerIdentity,
    change: &AclChange,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<(), AclFailure> {
    let kind = match change {
        AclChange::SaveUser { .. } | AclChange::DeleteUser { .. } => AclReadKind::Users,
        _ => AclReadKind::Policies,
    };
    let values = select_admin(cancellation, session.acl_documents(&target.address, kind))
        .await
        .map_err(|error| read_failure(&error))?
        .into_values();
    if values.len() > ACL_ROW_LIMIT {
        return Err(AclFailure::Invalid);
    }
    match change {
        AclChange::SaveUser { create, username, .. } => {
            let users = decode::users(target, values);
            let user = users.iter().find(|user| user.username == *username);
            if (*create && user.is_some()) || (!*create && user.is_none_or(|user| user.read_only)) {
                return Err(AclFailure::Invalid);
            }
        }
        AclChange::DeleteUser { username } => {
            if !decode::users(target, values)
                .iter()
                .any(|user| user.username == *username && !user.read_only)
            {
                return Err(AclFailure::Invalid);
            }
        }
        AclChange::SavePolicy {
            create,
            subject,
            policy_type,
            entry,
        } => {
            let policies = decode::policies(target, values);
            let policy = policies
                .iter()
                .find(|policy| policy.subject == *subject && policy.policy_type == *policy_type);
            if policies
                .iter()
                .any(|policy| policy.subject == *subject && policy.read_only)
                || (*create
                    && policy.is_some_and(|policy| {
                        policy
                            .entries
                            .iter()
                            .any(|existing| existing.resource == entry.resource)
                    }))
                || (!*create
                    && !policy.is_some_and(|policy| {
                        policy
                            .entries
                            .iter()
                            .any(|existing| existing.resource == entry.resource)
                    }))
            {
                return Err(AclFailure::Invalid);
            }
        }
        AclChange::DeletePolicyEntry {
            subject,
            policy_type,
            resource,
        } => {
            if !decode::policies(target, values).iter().any(|policy| {
                !policy.read_only
                    && policy.subject == *subject
                    && policy.policy_type == *policy_type
                    && policy.entries.iter().any(|entry| entry.resource == *resource)
            }) {
                return Err(AclFailure::Invalid);
            }
        }
    }
    Ok(())
}
fn wire(change: &AclChange) -> AclExactWrite {
    match change {
        AclChange::SaveUser {
            create,
            username,
            password,
            user_type,
            status,
        } => AclExactWrite::User {
            create: *create,
            username: username.clone(),
            password: password.as_ref().map(|value| value.expose().to_owned()),
            user_type: user_type.clone(),
            status: status.clone(),
        },
        AclChange::DeleteUser { username } => AclExactWrite::DeleteUser {
            username: username.clone(),
        },
        AclChange::SavePolicy {
            create,
            subject,
            policy_type,
            entry,
        } => AclExactWrite::Policy {
            create: *create,
            subject: subject.clone(),
            policy_type: policy_type.clone(),
            entry: AclEntryWrite {
                resource: entry.resource.clone(),
                actions: entry.actions.clone(),
                source_ips: entry.source_ips.clone(),
                decision: entry.decision.clone(),
            },
        },
        AclChange::DeletePolicyEntry {
            subject,
            policy_type,
            resource,
        } => AclExactWrite::DeleteEntry {
            subject: subject.clone(),
            policy_type: policy_type.clone(),
            resource: resource.clone(),
        },
    }
}
