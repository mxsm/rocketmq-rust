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

use crate::model::AuditAction;
use crate::model::AuditEvent;
use crate::model::AuditOutcome;
use crate::model::AuditResourceType;
use crate::model::AuthenticatedActor;
use crate::service::redact_audit_value;
use crate::state::AppState;
use axum::body::Body;
use axum::body::to_bytes;
use axum::extract::MatchedPath;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use serde_json::Value;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::Mutex;

/// A handler/service-produced terminal fact. It carries only the narrow,
/// action-specific public identifier and domain outcome needed for audit; it
/// is deliberately incapable of accepting a request body or header map.
#[derive(Debug, Clone)]
pub struct AuditTerminalFact {
    pub resource_name: Option<String>,
    pub environment_id: Option<crate::model::EnvironmentId>,
    pub outcome: AuditOutcome,
    pub already_persisted: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AuditTerminalFactSink(Arc<Mutex<Option<AuditTerminalFact>>>);

impl AuditTerminalFactSink {
    pub async fn record_success(
        &self,
        resource_name: Option<&str>,
        environment_id: Option<crate::model::EnvironmentId>,
    ) {
        self.record(resource_name, environment_id, AuditOutcome::Succeeded)
            .await;
    }

    /// Marks a successful operation whose persistence repository committed
    /// the matching audit event in the same storage transaction. The generic
    /// middleware must not append a duplicate terminal event afterward.
    pub async fn record_persisted_success(
        &self,
        resource_name: Option<&str>,
        environment_id: Option<crate::model::EnvironmentId>,
    ) {
        let resource_name = resource_name
            .filter(|value| is_safe_resource_segment(value))
            .map(str::to_string);
        *self.0.lock().await = Some(AuditTerminalFact {
            resource_name,
            environment_id,
            outcome: AuditOutcome::Succeeded,
            already_persisted: true,
        });
    }

    pub async fn record_rejected(
        &self,
        resource_name: Option<&str>,
        environment_id: Option<crate::model::EnvironmentId>,
    ) {
        self.record(resource_name, environment_id, AuditOutcome::Rejected).await;
    }

    pub async fn record_failed(
        &self,
        resource_name: Option<&str>,
        environment_id: Option<crate::model::EnvironmentId>,
    ) {
        self.record(resource_name, environment_id, AuditOutcome::Failed).await;
    }

    async fn record(
        &self,
        resource_name: Option<&str>,
        environment_id: Option<crate::model::EnvironmentId>,
        outcome: AuditOutcome,
    ) {
        let resource_name = resource_name
            .filter(|value| is_safe_resource_segment(value))
            .map(str::to_string);
        *self.0.lock().await = Some(AuditTerminalFact {
            resource_name,
            environment_id,
            outcome,
            already_persisted: false,
        });
    }

    async fn take(&self) -> Option<AuditTerminalFact> {
        self.0.lock().await.take()
    }
}

/// Runs a catalogued mutation and its terminal audit inside the application
/// service task group. Once admitted, disconnecting HTTP clients can only
/// drop their response receiver; they cannot cancel the RocketMQ mutation or
/// split its terminal audit from the response decision.
pub async fn audit_mutation(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let operation = request
        .extensions()
        .get::<MatchedPath>()
        .and_then(|path| mutation_for(request.method(), path.as_str()));
    let actor = request.extensions().get::<AuthenticatedActor>().cloned();
    let resource_name = operation.and_then(|(action, _)| safe_resource_name(action, request.uri().path()));
    let (Some((action, resource_type)), Some(actor)) = (operation, actor) else {
        return next.run(request).await;
    };
    let environment_id = state.published().environment.environment_id;
    let fact_sink = AuditTerminalFactSink::default();
    request.extensions_mut().insert(fact_sink.clone());
    match state
        .run_persisted_mutation("dashboard-audit-mutation", move |state| async move {
            let response = next.run(request).await;
            let fallback_outcome = match response.status().as_u16() {
                200..=299 => AuditOutcome::Succeeded,
                400..=499 => AuditOutcome::Rejected,
                _ => AuditOutcome::Failed,
            };
            let fact = fact_sink.take().await;
            if fact.as_ref().is_some_and(|fact| fact.already_persisted) {
                return Ok(response);
            }
            let event = AuditEvent {
                event_id: uuid::Uuid::now_v7().to_string(),
                request_id: actor.request_id,
                actor: actor.actor,
                action,
                resource_type,
                resource_name: fact
                    .as_ref()
                    .and_then(|fact| fact.resource_name.clone())
                    .or(resource_name),
                environment_id: fact
                    .as_ref()
                    .and_then(|fact| fact.environment_id.clone())
                    .or(Some(environment_id)),
                outcome: fact.map_or(fallback_outcome, |fact| fact.outcome),
                detail: Some(redact_audit_value(serde_json::json!({"operation": action.code()}))),
                created_at_ms: now_millis(),
            };
            if state.persistence.append_audit_event(event).await.is_err() && response.status().is_success() {
                return Ok(applied_audit_failed_response(response).await);
            }
            Ok(response)
        })
        .await
    {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

/// Creates the narrow successful-event projection that a repository can
/// commit atomically with a configuration or monitor mutation.
pub fn successful_mutation_audit_event(
    actor: &AuthenticatedActor,
    action: AuditAction,
    resource_type: AuditResourceType,
    resource_name: Option<&str>,
    environment_id: Option<crate::model::EnvironmentId>,
) -> AuditEvent {
    AuditEvent {
        event_id: uuid::Uuid::now_v7().to_string(),
        request_id: actor.request_id.clone(),
        actor: actor.actor.clone(),
        action,
        resource_type,
        resource_name: resource_name
            .filter(|value| is_safe_resource_segment(value))
            .map(str::to_string),
        environment_id,
        outcome: AuditOutcome::Succeeded,
        detail: Some(redact_audit_value(serde_json::json!({"operation": action.code()}))),
        created_at_ms: now_millis(),
    }
}

fn safe_resource_name(action: AuditAction, path: &str) -> Option<String> {
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let candidate = match action {
        AuditAction::ConfigNameserverDelete | AuditAction::ConfigProxyDelete => segments.last().copied(),
        AuditAction::MonitorDelete => segments.get(3).copied(),
        AuditAction::TopicUpdate
        | AuditAction::TopicDelete
        | AuditAction::TopicDeleteFromBroker
        | AuditAction::TopicTestMessageSend
        | AuditAction::TopicConsumerOffsetReset
        | AuditAction::TopicConsumerOffsetSkip => segments.get(2).copied(),
        AuditAction::ConsumerUpdate | AuditAction::ConsumerDelete | AuditAction::ConsumerOffsetReset => {
            segments.get(2).copied()
        }
        AuditAction::BrokerConfigUpdate | AuditAction::MessageResend => segments.get(2).copied(),
        // Access keys are credentials, so user mutations are attributable by
        // actor/action only. Request bodies are never examined here either.
        AuditAction::AclUserCreate | AuditAction::AclUserUpdate | AuditAction::AclUserDelete => None,
        AuditAction::AclPolicyUpdate | AuditAction::AclPolicyDelete => segments.get(3).copied(),
        _ => None,
    };
    candidate
        .filter(|value| is_safe_resource_segment(value))
        .map(str::to_string)
}

fn is_safe_resource_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'@'))
}

async fn applied_audit_failed_response(response: Response) -> Response {
    const WARNING: &str = "Mutation was applied, but its audit event could not be persisted";
    const MAX_AUDIT_WARNING_BODY_BYTES: usize = 1_048_576;
    let status = response.status();
    let (mut parts, body) = response.into_parts();
    parts
        .headers
        .insert("x-dashboard-audit", HeaderValue::from_static("failed"));
    let body = to_bytes(body, MAX_AUDIT_WARNING_BODY_BYTES)
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|mut value| {
            let object = value.as_object_mut()?;
            object.insert("success".to_string(), Value::Bool(true));
            object.insert("code".to_string(), Value::String("APPLIED_AUDIT_FAILED".to_string()));
            object.insert("message".to_string(), Value::String(WARNING.to_string()));
            object.insert("applied".to_string(), Value::Bool(true));
            object.insert("auditFailed".to_string(), Value::Bool(true));
            serde_json::to_vec(&value).ok()
        })
        .unwrap_or_else(|| {
            br#"{"success":true,"code":"APPLIED_AUDIT_FAILED","message":"Mutation was applied, but its audit event could not be persisted","applied":true,"auditFailed":true}"#.to_vec()
        });
    parts.status = status;
    Response::from_parts(parts, Body::from(body))
}

fn mutation_for(method: &Method, path: &str) -> Option<(AuditAction, AuditResourceType)> {
    let operation = match (method, path) {
        (&Method::PUT, "/api/config/nameservers") => {
            (AuditAction::ConfigNameserverReplace, AuditResourceType::Nameserver)
        }
        (&Method::POST, "/api/config/nameservers") => (AuditAction::ConfigNameserverAdd, AuditResourceType::Nameserver),
        (&Method::PUT, "/api/config/nameservers/current") => {
            (AuditAction::ConfigNameserverSwitch, AuditResourceType::Nameserver)
        }
        (&Method::DELETE, "/api/config/nameservers/{endpoint_id}") => {
            (AuditAction::ConfigNameserverDelete, AuditResourceType::Nameserver)
        }
        (&Method::PUT, "/api/config/vip-channel") => (AuditAction::ConfigVipSet, AuditResourceType::Environment),
        (&Method::PUT, "/api/config/tls") => (AuditAction::ConfigTlsSet, AuditResourceType::Environment),
        (&Method::POST, "/api/config/proxies") => (AuditAction::ConfigProxyAdd, AuditResourceType::Proxy),
        (&Method::PUT, "/api/config/proxies/current") => (AuditAction::ConfigProxySwitch, AuditResourceType::Proxy),
        (&Method::DELETE, "/api/config/proxies/{endpoint_id}") => {
            (AuditAction::ConfigProxyDelete, AuditResourceType::Proxy)
        }
        (&Method::POST, "/api/monitors/consumers") => (AuditAction::MonitorUpsert, AuditResourceType::Monitor),
        (&Method::DELETE, "/api/monitors/consumers/{consumer_group}") => {
            (AuditAction::MonitorDelete, AuditResourceType::Monitor)
        }
        (&Method::POST, "/api/topics") => (AuditAction::TopicCreate, AuditResourceType::Topic),
        (&Method::PUT, "/api/topics/{topic}") => (AuditAction::TopicUpdate, AuditResourceType::Topic),
        (&Method::DELETE, "/api/topics/{topic}") => (AuditAction::TopicDelete, AuditResourceType::Topic),
        (&Method::DELETE, "/api/topics/{topic}/brokers/{broker}") => {
            (AuditAction::TopicDeleteFromBroker, AuditResourceType::Topic)
        }
        (&Method::POST, "/api/topics/{topic}/test-message") => {
            (AuditAction::TopicTestMessageSend, AuditResourceType::Topic)
        }
        (&Method::POST, "/api/topics/{topic}/consumer-offset/reset") => {
            (AuditAction::TopicConsumerOffsetReset, AuditResourceType::Topic)
        }
        (&Method::POST, "/api/topics/{topic}/consumer-offset/skip") => {
            (AuditAction::TopicConsumerOffsetSkip, AuditResourceType::Topic)
        }
        (&Method::POST, "/api/consumers") => (AuditAction::ConsumerCreate, AuditResourceType::Consumer),
        (&Method::PUT, "/api/consumers/{group}") => (AuditAction::ConsumerUpdate, AuditResourceType::Consumer),
        (&Method::DELETE, "/api/consumers/{group}") => (AuditAction::ConsumerDelete, AuditResourceType::Consumer),
        (&Method::POST, "/api/consumers/{group}/reset-offset") => {
            (AuditAction::ConsumerOffsetReset, AuditResourceType::Consumer)
        }
        (&Method::PUT, "/api/brokers/{broker_name}/config") => {
            (AuditAction::BrokerConfigUpdate, AuditResourceType::Broker)
        }
        (&Method::POST, "/api/messages/{message_id}/resend") => {
            (AuditAction::MessageResend, AuditResourceType::Message)
        }
        (&Method::POST, "/api/messages/dlq/resend") => (AuditAction::MessageDlqResend, AuditResourceType::Message),
        (&Method::POST, "/api/acl/users") => (AuditAction::AclUserCreate, AuditResourceType::AclUser),
        (&Method::PUT, "/api/acl/users/{access_key}") => (AuditAction::AclUserUpdate, AuditResourceType::AclUser),
        (&Method::DELETE, "/api/acl/users/{access_key}") => (AuditAction::AclUserDelete, AuditResourceType::AclUser),
        (&Method::POST, "/api/acl/policies") => (AuditAction::AclPolicyCreate, AuditResourceType::AclPolicy),
        (&Method::PUT, "/api/acl/policies/{policy_name}") => {
            (AuditAction::AclPolicyUpdate, AuditResourceType::AclPolicy)
        }
        (&Method::DELETE, "/api/acl/policies/{policy_name}") => {
            (AuditAction::AclPolicyDelete, AuditResourceType::AclPolicy)
        }
        (&Method::GET, "/api/messages/dlq/export") => (AuditAction::DlqExport, AuditResourceType::Dlq),
        _ => return None,
    };
    Some(operation)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::AuditTerminalFactSink;
    use super::applied_audit_failed_response;
    use super::mutation_for;
    use crate::model::AuditAction;
    use crate::model::AuditOutcome;
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::Method;
    use axum::http::Response;

    #[test]
    fn mutation_catalog_covers_each_expected_route() {
        let cases = [
            (
                Method::PUT,
                "/api/config/nameservers",
                AuditAction::ConfigNameserverReplace,
            ),
            (
                Method::POST,
                "/api/config/nameservers",
                AuditAction::ConfigNameserverAdd,
            ),
            (
                Method::PUT,
                "/api/config/nameservers/current",
                AuditAction::ConfigNameserverSwitch,
            ),
            (
                Method::DELETE,
                "/api/config/nameservers/{endpoint_id}",
                AuditAction::ConfigNameserverDelete,
            ),
            (Method::PUT, "/api/config/vip-channel", AuditAction::ConfigVipSet),
            (Method::PUT, "/api/config/tls", AuditAction::ConfigTlsSet),
            (Method::POST, "/api/config/proxies", AuditAction::ConfigProxyAdd),
            (
                Method::PUT,
                "/api/config/proxies/current",
                AuditAction::ConfigProxySwitch,
            ),
            (
                Method::DELETE,
                "/api/config/proxies/{endpoint_id}",
                AuditAction::ConfigProxyDelete,
            ),
            (Method::POST, "/api/monitors/consumers", AuditAction::MonitorUpsert),
            (
                Method::DELETE,
                "/api/monitors/consumers/{consumer_group}",
                AuditAction::MonitorDelete,
            ),
            (Method::POST, "/api/topics", AuditAction::TopicCreate),
            (Method::PUT, "/api/topics/{topic}", AuditAction::TopicUpdate),
            (Method::DELETE, "/api/topics/{topic}", AuditAction::TopicDelete),
            (
                Method::DELETE,
                "/api/topics/{topic}/brokers/{broker}",
                AuditAction::TopicDeleteFromBroker,
            ),
            (
                Method::POST,
                "/api/topics/{topic}/test-message",
                AuditAction::TopicTestMessageSend,
            ),
            (
                Method::POST,
                "/api/topics/{topic}/consumer-offset/reset",
                AuditAction::TopicConsumerOffsetReset,
            ),
            (
                Method::POST,
                "/api/topics/{topic}/consumer-offset/skip",
                AuditAction::TopicConsumerOffsetSkip,
            ),
            (Method::POST, "/api/consumers", AuditAction::ConsumerCreate),
            (Method::PUT, "/api/consumers/{group}", AuditAction::ConsumerUpdate),
            (Method::DELETE, "/api/consumers/{group}", AuditAction::ConsumerDelete),
            (
                Method::POST,
                "/api/consumers/{group}/reset-offset",
                AuditAction::ConsumerOffsetReset,
            ),
            (
                Method::PUT,
                "/api/brokers/{broker_name}/config",
                AuditAction::BrokerConfigUpdate,
            ),
            (
                Method::POST,
                "/api/messages/{message_id}/resend",
                AuditAction::MessageResend,
            ),
            (Method::POST, "/api/messages/dlq/resend", AuditAction::MessageDlqResend),
            (Method::POST, "/api/acl/users", AuditAction::AclUserCreate),
            (Method::PUT, "/api/acl/users/{access_key}", AuditAction::AclUserUpdate),
            (
                Method::DELETE,
                "/api/acl/users/{access_key}",
                AuditAction::AclUserDelete,
            ),
            (Method::POST, "/api/acl/policies", AuditAction::AclPolicyCreate),
            (
                Method::PUT,
                "/api/acl/policies/{policy_name}",
                AuditAction::AclPolicyUpdate,
            ),
            (
                Method::DELETE,
                "/api/acl/policies/{policy_name}",
                AuditAction::AclPolicyDelete,
            ),
            (Method::GET, "/api/messages/dlq/export", AuditAction::DlqExport),
        ];
        assert_eq!(cases.len(), 32);
        for (method, path, expected_action) in cases {
            assert_eq!(
                mutation_for(&method, path).map(|operation| operation.0),
                Some(expected_action)
            );
        }
        assert!(mutation_for(&Method::GET, "/api/topics").is_none());
    }

    #[tokio::test]
    async fn applied_audit_failure_response_has_a_stable_non_retryable_body() {
        let response = Response::new(Body::from(r#"{"success":true,"data":{"revoked":33}}"#));
        let response = applied_audit_failed_response(response).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-dashboard-audit")
                .and_then(|value| value.to_str().ok()),
            Some("failed")
        );
        let body = to_bytes(response.into_body(), 1_024).await.expect("read response body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("parse response body");
        assert_eq!(value["success"], true);
        assert_eq!(value["code"], "APPLIED_AUDIT_FAILED");
        assert_eq!(
            value["message"],
            "Mutation was applied, but its audit event could not be persisted"
        );
        assert_eq!(value["applied"], true);
        assert_eq!(value["auditFailed"], true);
        assert_eq!(value["data"]["revoked"], 33);
    }

    #[tokio::test]
    async fn applied_audit_failure_uses_the_stable_body_when_original_body_is_too_large() {
        let response = Response::new(Body::from(vec![b'x'; 1_048_577]));
        let response = applied_audit_failed_response(response).await;
        let body = to_bytes(response.into_body(), 1_024)
            .await
            .expect("read fallback response body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("parse fallback response body");

        assert_eq!(value["success"], true);
        assert_eq!(value["code"], "APPLIED_AUDIT_FAILED");
        assert_eq!(value["applied"], true);
        assert_eq!(value["auditFailed"], true);
        assert!(value.get("data").is_none());
    }

    #[tokio::test]
    async fn applied_audit_failure_uses_the_stable_body_when_body_collection_fails() {
        let response = Response::new(Body::from_stream(futures_util::stream::once(async {
            Err::<axum::body::Bytes, std::io::Error>(std::io::Error::other("body read failed"))
        })));
        let response = applied_audit_failed_response(response).await;
        let body = to_bytes(response.into_body(), 1_024)
            .await
            .expect("read fallback response body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("parse fallback response body");

        assert_eq!(value["success"], true);
        assert_eq!(value["code"], "APPLIED_AUDIT_FAILED");
        assert_eq!(value["applied"], true);
        assert_eq!(value["auditFailed"], true);
        assert!(value.get("data").is_none());
    }

    #[tokio::test]
    async fn typed_terminal_fact_retains_only_a_safe_public_resource_name() {
        let facts = AuditTerminalFactSink::default();
        facts.record_success(Some("orders-1"), None).await;
        let fact = facts.take().await.expect("recorded fact");
        assert_eq!(fact.resource_name.as_deref(), Some("orders-1"));
        assert_eq!(fact.outcome, AuditOutcome::Succeeded);

        facts.record_failed(Some("contains space"), None).await;
        let fact = facts.take().await.expect("recorded fact");
        assert_eq!(fact.resource_name, None);
        assert_eq!(fact.outcome, AuditOutcome::Failed);
    }
}
