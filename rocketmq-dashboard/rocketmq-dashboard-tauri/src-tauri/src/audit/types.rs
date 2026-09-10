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

use crate::auth::{SessionState, types::AuthSessionResponse};
use crate::error::{CommandError, CommandErrorCategory};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
pub(crate) enum AuditAction {
    Login,
    Logout,
    ChangePassword,
    RevokeSessions,
    AddNameServer,
    SwitchNameServer,
    DeleteNameServer,
    ReplaceNameServers,
    UpdateVip,
    UpdateTls,
    AddProxy,
    SwitchProxy,
    DeleteProxy,
    UpsertTopic,
    DeleteTopic,
    DeleteTopicByBroker,
    ResetOffset,
    SkipMessages,
    SendMessage,
    CreateAclPolicy,
    UpdateAclPolicy,
    DeleteAclPolicy,
    CreateAclUser,
    UpdateAclUser,
    DeleteAclUser,
    UpdateBrokerConfig,
    UpsertConsumer,
    DeleteConsumer,
    ConsumeDirectly,
    ResendDlq,
    BatchResendDlq,
}

impl AuditAction {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Login => "auth.login",
            Self::Logout => "auth.logout",
            Self::ChangePassword => "auth.change_password",
            Self::RevokeSessions => "auth.revoke_sessions",
            Self::AddNameServer => "nameserver.add",
            Self::SwitchNameServer => "nameserver.switch",
            Self::DeleteNameServer => "nameserver.delete",
            Self::ReplaceNameServers => "nameserver.replace",
            Self::UpdateVip => "connection.vip",
            Self::UpdateTls => "connection.tls",
            Self::AddProxy => "proxy.add",
            Self::SwitchProxy => "proxy.switch",
            Self::DeleteProxy => "proxy.delete",
            Self::UpsertTopic => "topic.upsert",
            Self::DeleteTopic => "topic.delete",
            Self::DeleteTopicByBroker => "topic.delete_broker",
            Self::ResetOffset => "consumer.reset_offset",
            Self::SkipMessages => "consumer.skip_messages",
            Self::SendMessage => "message.send",
            Self::CreateAclPolicy => "acl.create_policy",
            Self::UpdateAclPolicy => "acl.update_policy",
            Self::DeleteAclPolicy => "acl.delete_policy",
            Self::CreateAclUser => "acl.create_user",
            Self::UpdateAclUser => "acl.update_user",
            Self::DeleteAclUser => "acl.delete_user",
            Self::UpdateBrokerConfig => "broker.update_config",
            Self::UpsertConsumer => "consumer.upsert",
            Self::DeleteConsumer => "consumer.delete",
            Self::ConsumeDirectly => "message.consume_directly",
            Self::ResendDlq => "dlq.resend",
            Self::BatchResendDlq => "dlq.batch_resend",
        }
    }
    pub(crate) fn resource_type(self) -> &'static str {
        match self {
            Self::Login | Self::Logout | Self::ChangePassword | Self::RevokeSessions => "account",
            Self::AddNameServer
            | Self::SwitchNameServer
            | Self::DeleteNameServer
            | Self::ReplaceNameServers
            | Self::UpdateVip
            | Self::UpdateTls
            | Self::AddProxy
            | Self::SwitchProxy
            | Self::DeleteProxy => "connection",
            Self::UpsertTopic | Self::DeleteTopic | Self::DeleteTopicByBroker | Self::SendMessage => "topic",
            Self::CreateAclPolicy | Self::UpdateAclPolicy | Self::DeleteAclPolicy => "acl_policy",
            Self::CreateAclUser | Self::UpdateAclUser | Self::DeleteAclUser => "acl_user",
            Self::UpdateBrokerConfig => "broker",
            Self::ResetOffset
            | Self::SkipMessages
            | Self::UpsertConsumer
            | Self::DeleteConsumer
            | Self::ConsumeDirectly
            | Self::ResendDlq
            | Self::BatchResendDlq => "consumer_group",
        }
    }
    pub(crate) fn remote(self) -> bool {
        matches!(
            self,
            Self::UpsertTopic
                | Self::DeleteTopic
                | Self::DeleteTopicByBroker
                | Self::ResetOffset
                | Self::SkipMessages
                | Self::SendMessage
                | Self::CreateAclPolicy
                | Self::UpdateAclPolicy
                | Self::DeleteAclPolicy
                | Self::CreateAclUser
                | Self::UpdateAclUser
                | Self::DeleteAclUser
                | Self::UpdateBrokerConfig
                | Self::UpsertConsumer
                | Self::DeleteConsumer
                | Self::ConsumeDirectly
                | Self::ResendDlq
                | Self::BatchResendDlq
        )
    }
}

pub(crate) enum AuditAccess {
    Login,
    Account { sessions: SessionState, token: String },
    Dashboard { sessions: SessionState, token: String },
}

impl AuditAccess {
    pub(crate) fn dashboard(sessions: &SessionState, token: String) -> Self {
        Self::Dashboard {
            sessions: sessions.clone(),
            token,
        }
    }
    pub(crate) fn account(sessions: &SessionState, token: String) -> Self {
        Self::Account {
            sessions: sessions.clone(),
            token,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Success,
    Rejected,
    Failed,
    Partial,
    Unknown,
}
impl Outcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Partial => "partial",
            Self::Unknown => "unknown",
        }
    }
}

/// Only explicit receipt metadata can be persisted. Request bodies and credentials have no field here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditDetail {
    pub(crate) result_unknown: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) success_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_count: Option<usize>,
}

#[derive(Debug)]
pub(crate) struct Summary {
    pub(crate) outcome: Outcome,
    pub(crate) detail: AuditDetail,
}
impl Summary {
    pub(crate) fn count(success: usize, failure: usize) -> Self {
        Self {
            outcome: if failure == 0 {
                Outcome::Success
            } else if success == 0 {
                Outcome::Failed
            } else {
                Outcome::Partial
            },
            detail: AuditDetail {
                result_unknown: false,
                error_code: None,
                success_count: Some(success),
                failure_count: Some(failure),
            },
        }
    }
    pub(crate) fn error(error: &CommandError, remote: bool) -> Self {
        let outcome = match error.category {
            CommandErrorCategory::Authentication | CommandErrorCategory::Validation => Outcome::Rejected,
            _ if remote => Outcome::Unknown,
            _ => Outcome::Failed,
        };
        Self {
            outcome,
            detail: AuditDetail {
                result_unknown: outcome == Outcome::Unknown,
                error_code: Some(error.code.into()),
                success_count: None,
                failure_count: None,
            },
        }
    }
}

pub(crate) trait AuditReceipt {
    fn summary(&self) -> Summary;
    fn authenticated_actor(&self) -> Option<String> {
        None
    }
}
macro_rules! successful_receipt { ($($ty:ty),+ $(,)?) => { $(impl AuditReceipt for $ty { fn summary(&self) -> Summary { Summary::count(1, 0) } })+ }; }
successful_receipt!(
    crate::auth::types::CommonResponse,
    crate::auth::types::RevokeSessionsResponse,
    rocketmq_dashboard_common::NameServerMutationResult,
    rocketmq_dashboard_common::ProxyMutationResult
);
impl AuditReceipt for AuthSessionResponse {
    fn summary(&self) -> Summary {
        Summary::count(1, 0)
    }
    fn authenticated_actor(&self) -> Option<String> {
        Some(self.current_user.username.clone())
    }
}
impl AuditReceipt for crate::topic::types::TopicMutationResult {
    fn summary(&self) -> Summary {
        Summary::count(usize::from(self.success), usize::from(!self.success))
    }
}
impl AuditReceipt for crate::topic::types::TopicSendMessageResult {
    fn summary(&self) -> Summary {
        let ok = self.success;
        Summary::count(usize::from(ok), usize::from(!ok))
    }
}
impl AuditReceipt for crate::message::types::MessageResendResult {
    fn summary(&self) -> Summary {
        Summary::count(usize::from(self.success), usize::from(!self.success))
    }
}
impl AuditReceipt for crate::message::types::MessageBatchResendResponse {
    fn summary(&self) -> Summary {
        Summary::count(self.success_count, self.failure_count)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Audited<T> {
    #[serde(flatten)]
    pub(crate) result: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) audit_warning: Option<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditEvent {
    pub(crate) event_id: String,
    pub(crate) request_id: String,
    pub(crate) actor: Option<String>,
    pub(crate) action: String,
    pub(crate) resource_type: String,
    pub(crate) resource_name: Option<String>,
    pub(crate) environment_id: Option<String>,
    pub(crate) outcome: String,
    pub(crate) detail: AuditDetail,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditQuery {
    pub(crate) from_ms: Option<i64>,
    pub(crate) to_ms: Option<i64>,
    pub(crate) actor: Option<String>,
    pub(crate) action: Option<String>,
    pub(crate) outcome: Option<Outcome>,
    pub(crate) environment_id: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) cursor: Option<String>,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuditPage {
    pub(crate) items: Vec<AuditEvent>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone)]
pub(crate) struct AuditContext {
    pub(crate) actor: Option<String>,
    pub(crate) environment: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    pub(crate) event_id: String,
    pub(crate) request_id: String,
    pub(crate) action: AuditAction,
    pub(crate) resource_name: Option<String>,
}
impl AuditContext {
    pub(crate) fn event(&self, actor: Option<String>, summary: Summary) -> AuditEvent {
        AuditEvent {
            event_id: self.event_id.clone(),
            request_id: self.request_id.clone(),
            actor,
            action: self.action.name().into(),
            resource_type: self.action.resource_type().into(),
            resource_name: self.resource_name.clone(),
            environment_id: self
                .environment
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            outcome: summary.outcome.as_str().into(),
            detail: summary.detail,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
    pub(crate) fn set_environment(&self, environment: Option<String>) -> crate::error::DashboardResult<()> {
        *self
            .environment
            .lock()
            .map_err(|_| crate::error::DashboardError::Internal("audit scope poisoned"))? = environment;
        Ok(())
    }
    pub(crate) fn record_local_success(&self, connection: &rusqlite::Connection) -> crate::error::DashboardResult<()> {
        let actor = self
            .actor
            .as_deref()
            .ok_or(crate::error::DashboardError::Unauthenticated)?;
        self.record_success(connection, actor)
    }
    /// Call inside the mutation's transaction so a successful account change always has its audit record.
    pub(crate) fn record_success(
        &self,
        connection: &rusqlite::Connection,
        actor: &str,
    ) -> crate::error::DashboardResult<()> {
        super::db::insert(connection, &self.event(Some(actor.into()), Summary::count(1, 0)))
    }
}

impl AuditReceipt for crate::topic::batch::TopicBatchResult {
    fn summary(&self) -> Summary {
        let success = self.targets.iter().filter(|target| target.success).count()
            + usize::from(self.order_config.as_ref().is_some_and(|result| result.success));
        let failure = self.targets.iter().filter(|target| !target.success).count()
            + usize::from(self.order_config.as_ref().is_some_and(|result| !result.success));
        Summary::count(success, failure.max(usize::from(!self.success)))
    }
}

impl AuditReceipt for crate::consumer::types::ConsumerMutationResult {
    fn summary(&self) -> Summary {
        let successes = self.targets.iter().filter(|target| target.success).count();
        Summary::count(
            successes,
            (self.targets.len() - successes).max(usize::from(!self.success)),
        )
    }
}
