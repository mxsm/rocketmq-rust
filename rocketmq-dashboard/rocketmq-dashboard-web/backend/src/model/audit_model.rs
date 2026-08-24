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

use crate::model::EnvironmentId;
use crate::model::StorageBackend;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// The actor category recorded in a dashboard audit event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditActorKind {
    Admin,
    LocalOperator,
    System,
}

impl AuditActorKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::LocalOperator => "local_operator",
            Self::System => "system",
        }
    }

    pub fn parse(code: &str) -> Option<Self> {
        match code {
            "admin" => Some(Self::Admin),
            "local_operator" => Some(Self::LocalOperator),
            "system" => Some(Self::System),
            _ => None,
        }
    }
}

/// A bounded, non-secret identity attached to a request and audit event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditActor {
    pub kind: AuditActorKind,
    pub username: Option<String>,
}

impl AuditActor {
    pub fn admin(username: impl Into<String>) -> Self {
        Self {
            kind: AuditActorKind::Admin,
            username: Some(username.into()),
        }
    }

    pub const fn local_operator() -> Self {
        Self {
            kind: AuditActorKind::LocalOperator,
            username: None,
        }
    }

    pub const fn system() -> Self {
        Self {
            kind: AuditActorKind::System,
            username: None,
        }
    }

    pub const fn is_administrator(&self) -> bool {
        matches!(self.kind, AuditActorKind::Admin | AuditActorKind::LocalOperator)
    }

    pub fn stable_name(&self) -> &str {
        self.username.as_deref().unwrap_or_else(|| self.kind.code())
    }
}

/// Stable operation names accepted by the audit repository. Keeping this
/// finite prevents a handler from accidentally persisting an arbitrary URL,
/// header, or client-controlled string as an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    SessionCreate,
    SessionRevokeCurrent,
    SessionRevokeAll,
    ConfigNameserverReplace,
    ConfigNameserverAdd,
    ConfigNameserverSwitch,
    ConfigNameserverDelete,
    ConfigVipSet,
    ConfigTlsSet,
    ConfigProxyAdd,
    ConfigProxySwitch,
    ConfigProxyDelete,
    MonitorUpsert,
    MonitorDelete,
    TopicCreate,
    TopicUpdate,
    TopicDelete,
    TopicDeleteFromBroker,
    TopicTestMessageSend,
    TopicConsumerOffsetReset,
    TopicConsumerOffsetSkip,
    ConsumerCreate,
    ConsumerUpdate,
    ConsumerDelete,
    ConsumerOffsetReset,
    BrokerConfigUpdate,
    MessageResend,
    MessageDlqResend,
    AclUserCreate,
    AclUserUpdate,
    AclUserDelete,
    AclPolicyCreate,
    AclPolicyUpdate,
    AclPolicyDelete,
    DlqExport,
}

impl AuditAction {
    pub const fn code(self) -> &'static str {
        match self {
            Self::SessionCreate => "session.create",
            Self::SessionRevokeCurrent => "session.revoke_current",
            Self::SessionRevokeAll => "session.revoke_all",
            Self::ConfigNameserverReplace => "config.nameservers.replace",
            Self::ConfigNameserverAdd => "config.nameservers.add",
            Self::ConfigNameserverSwitch => "config.nameservers.switch",
            Self::ConfigNameserverDelete => "config.nameservers.delete",
            Self::ConfigVipSet => "config.vip.set",
            Self::ConfigTlsSet => "config.tls.set",
            Self::ConfigProxyAdd => "config.proxies.add",
            Self::ConfigProxySwitch => "config.proxies.switch",
            Self::ConfigProxyDelete => "config.proxies.delete",
            Self::MonitorUpsert => "monitor.upsert",
            Self::MonitorDelete => "monitor.delete",
            Self::TopicCreate => "topic.create",
            Self::TopicUpdate => "topic.update",
            Self::TopicDelete => "topic.delete",
            Self::TopicDeleteFromBroker => "topic.delete_from_broker",
            Self::TopicTestMessageSend => "topic.test_message.send",
            Self::TopicConsumerOffsetReset => "topic.consumer_offset.reset",
            Self::TopicConsumerOffsetSkip => "topic.consumer_offset.skip",
            Self::ConsumerCreate => "consumer.create",
            Self::ConsumerUpdate => "consumer.update",
            Self::ConsumerDelete => "consumer.delete",
            Self::ConsumerOffsetReset => "consumer.offset.reset",
            Self::BrokerConfigUpdate => "broker.config.update",
            Self::MessageResend => "message.resend",
            Self::MessageDlqResend => "message.dlq.resend",
            Self::AclUserCreate => "acl.user.create",
            Self::AclUserUpdate => "acl.user.update",
            Self::AclUserDelete => "acl.user.delete",
            Self::AclPolicyCreate => "acl.policy.create",
            Self::AclPolicyUpdate => "acl.policy.update",
            Self::AclPolicyDelete => "acl.policy.delete",
            Self::DlqExport => "dlq.export",
        }
    }

    pub fn parse(code: &str) -> Option<Self> {
        [
            Self::SessionCreate,
            Self::SessionRevokeCurrent,
            Self::SessionRevokeAll,
            Self::ConfigNameserverReplace,
            Self::ConfigNameserverAdd,
            Self::ConfigNameserverSwitch,
            Self::ConfigNameserverDelete,
            Self::ConfigVipSet,
            Self::ConfigTlsSet,
            Self::ConfigProxyAdd,
            Self::ConfigProxySwitch,
            Self::ConfigProxyDelete,
            Self::MonitorUpsert,
            Self::MonitorDelete,
            Self::TopicCreate,
            Self::TopicUpdate,
            Self::TopicDelete,
            Self::TopicDeleteFromBroker,
            Self::TopicTestMessageSend,
            Self::TopicConsumerOffsetReset,
            Self::TopicConsumerOffsetSkip,
            Self::ConsumerCreate,
            Self::ConsumerUpdate,
            Self::ConsumerDelete,
            Self::ConsumerOffsetReset,
            Self::BrokerConfigUpdate,
            Self::MessageResend,
            Self::MessageDlqResend,
            Self::AclUserCreate,
            Self::AclUserUpdate,
            Self::AclUserDelete,
            Self::AclPolicyCreate,
            Self::AclPolicyUpdate,
            Self::AclPolicyDelete,
            Self::DlqExport,
        ]
        .into_iter()
        .find(|action| action.code() == code)
    }
}

/// Audited resource categories. Resource values are validated and truncated
/// by the service before they cross the persistence boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResourceType {
    Session,
    Environment,
    Nameserver,
    Proxy,
    Monitor,
    Topic,
    Consumer,
    Broker,
    Message,
    AclUser,
    AclPolicy,
    Dlq,
}

impl AuditResourceType {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Environment => "environment",
            Self::Nameserver => "nameserver",
            Self::Proxy => "proxy",
            Self::Monitor => "monitor",
            Self::Topic => "topic",
            Self::Consumer => "consumer",
            Self::Broker => "broker",
            Self::Message => "message",
            Self::AclUser => "acl_user",
            Self::AclPolicy => "acl_policy",
            Self::Dlq => "dlq",
        }
    }

    pub fn parse(code: &str) -> Option<Self> {
        [
            Self::Session,
            Self::Environment,
            Self::Nameserver,
            Self::Proxy,
            Self::Monitor,
            Self::Topic,
            Self::Consumer,
            Self::Broker,
            Self::Message,
            Self::AclUser,
            Self::AclPolicy,
            Self::Dlq,
        ]
        .into_iter()
        .find(|resource| resource.code() == code)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Succeeded,
    Rejected,
    Failed,
}

impl AuditOutcome {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }

    pub fn parse(code: &str) -> Option<Self> {
        match code {
            "succeeded" => Some(Self::Succeeded),
            "rejected" => Some(Self::Rejected),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// Domain event persisted by all storage engines. It intentionally contains
/// only a safe, redacted detail projection, never a request DTO or error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    pub event_id: String,
    pub request_id: String,
    pub actor: AuditActor,
    pub action: AuditAction,
    pub resource_type: AuditResourceType,
    pub resource_name: Option<String>,
    pub environment_id: Option<EnvironmentId>,
    pub outcome: AuditOutcome,
    pub detail: Option<Value>,
    pub created_at_ms: i64,
}

/// A public audit projection. It is deliberately separate from `AuditEvent`
/// so storage-only implementation details cannot become API fields by
/// accident.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEventView {
    pub event_id: String,
    pub request_id: String,
    pub actor: String,
    pub actor_kind: AuditActorKind,
    pub action: AuditAction,
    pub resource_type: AuditResourceType,
    pub resource_name: Option<String>,
    pub environment_id: Option<String>,
    pub outcome: AuditOutcome,
    pub detail: Option<Value>,
    pub created_at_ms: i64,
}

/// Safe operational state of the lifecycle-owned session/audit retention
/// worker. It intentionally contains no lease holder identity or error text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionAuditCleanupHealth {
    pub backend: StorageBackend,
    pub connectivity: String,
    pub role: String,
    pub last_cleanup_at_ms: Option<i64>,
    pub recent_error: Option<String>,
}

impl From<AuditEvent> for AuditEventView {
    fn from(event: AuditEvent) -> Self {
        Self {
            event_id: event.event_id,
            request_id: event.request_id,
            actor: event.actor.stable_name().to_string(),
            actor_kind: event.actor.kind,
            action: event.action,
            resource_type: event.resource_type,
            resource_name: event.resource_name,
            environment_id: event.environment_id.map(|id| id.0),
            outcome: event.outcome,
            detail: event.detail,
            created_at_ms: event.created_at_ms,
        }
    }
}
