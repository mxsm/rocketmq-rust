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

use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::core::consumer::{
    ConsumerDiagnosticAdmin, DashboardConsumerRunningInfo, DashboardConsumerRunningInfoRequest,
};
use rocketmq_error::CanonicalCondition;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::*;
use crate::consumer::scope::ConsumerQueryScope;

// Four independently bounded sections leave space for identities and the envelope under 256 KiB.
const SECTION_BYTES: usize = 60 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DiagnosticRequest {
    pub(crate) consumer_group: String,
    pub(crate) client_id: String,
    pub(crate) scope: ConsumerQueryScope,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticStatus {
    Available,
    Offline,
    Unsupported,
    Unavailable,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DiagnosticResult {
    consumer_group: String,
    client_id: String,
    status: DiagnosticStatus,
    reason: Option<&'static str>,
    properties: Vec<Value>,
    subscriptions: Vec<Value>,
    process_queues: Vec<Value>,
    jstack: Option<String>,
    truncated: bool,
}

impl DiagnosticResult {
    fn empty(group: String, client: String, status: DiagnosticStatus, reason: &'static str) -> Self {
        Self {
            consumer_group: group,
            client_id: client,
            status,
            reason: Some(reason),
            properties: vec![],
            subscriptions: vec![],
            process_queues: vec![],
            jstack: None,
            truncated: false,
        }
    }
}

impl ConsumerManager {
    pub(crate) async fn query_diagnostic(
        &self,
        request: DiagnosticRequest,
        include_jstack: bool,
    ) -> ConsumerResult<DiagnosticResult> {
        let group = validate_consumer_group_name(&request.consumer_group)?;
        let client = request.client_id.trim().to_string();
        if client.is_empty() || client.len() > 512 || group.len() > 512 {
            return Err(ConsumerError::Validation(
                "A bounded Consumer group and client ID are required.".into(),
            ));
        }
        if matches!(request.scope, ConsumerQueryScope::Proxy { .. }) {
            return Ok(DiagnosticResult::empty(
                group,
                client,
                DiagnosticStatus::Unsupported,
                "The diagnostic admin capability cannot forward through a Proxy. Select NameServer discovery to inspect remoting clients.",
            ));
        }
        let mut session = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session).await?;
        let admin = &mut session
            .as_mut()
            .ok_or_else(|| ConsumerError::Validation("Consumer connection unavailable.".into()))?
            .admin;
        let connections = match admin
            .query_dashboard_consumer_connection(&DashboardConsumerConnectionRequest {
                consumer_group: group.clone(),
                address: None,
            })
            .await
        {
            Ok(connections) => connections,
            Err(error) => return Ok(failed(group, client, &error)),
        };
        if !connections
            .connections
            .iter()
            .any(|connection| connection.client_id == client)
        {
            return Ok(DiagnosticResult::empty(
                group,
                client,
                DiagnosticStatus::Offline,
                "This client is absent from the current Consumer connections. Refresh the connection list.",
            ));
        }
        let core_request = DashboardConsumerRunningInfoRequest::try_new(&group, &client, include_jstack, SECTION_BYTES)
            .map_err(map_admin_error)?;
        match admin.query_dashboard_consumer_running_info(&core_request).await {
            Ok(info) => project(info, include_jstack),
            Err(error) => Ok(failed(group, client, &error)),
        }
    }
}

fn failed(group: String, client: String, error: &AdminError) -> DiagnosticResult {
    let broker_code = error.diagnostic_view().ok().and_then(|view| {
        view.fields().find_map(|field| match (field.name(), field.value()) {
            ("broker_code", rocketmq_error::ViewValueRef::I64(code)) => Some(code),
            _ => None,
        })
    });
    let (status, reason) = if error.condition() == CanonicalCondition::Unimplemented || broker_code == Some(3) {
        (
            DiagnosticStatus::Unsupported,
            "The Broker or client does not support this diagnostic request.",
        )
    } else if broker_code == Some(206) || error.remoting_response_code().as_i32() == 206 {
        (DiagnosticStatus::Offline, "The Consumer client is offline.")
    } else {
        (
            DiagnosticStatus::Unavailable,
            "Diagnostic data is unavailable. Check the connection and Broker permissions.",
        )
    };
    DiagnosticResult::empty(group, client, status, reason)
}

fn bounded_entries<T: Serialize>(entries: Vec<T>, truncated: &mut bool) -> ConsumerResult<Vec<Value>> {
    let mut remaining = SECTION_BYTES - 2;
    let mut output = Vec::new();
    for entry in entries {
        let value = serde_json::to_value(entry)
            .map_err(|_| ConsumerError::Validation("Cannot represent diagnostic data.".into()))?;
        let bytes = serde_json::to_vec(&value)
            .map_err(|_| ConsumerError::Validation("Cannot encode diagnostic data.".into()))?
            .len()
            + 1;
        if bytes > remaining {
            *truncated = true;
            break;
        }
        remaining -= bytes;
        output.push(value);
    }
    Ok(output)
}

fn bounded_jstack(text: String, truncated: &mut bool) -> ConsumerResult<String> {
    // Count JSON escaping as well as UTF-8 bytes so IPC output remains bounded.
    let mut remaining = SECTION_BYTES - 2;
    let mut end = 0;
    for (index, character) in text.char_indices() {
        let bytes = serde_json::to_string(&character)
            .map_err(|_| ConsumerError::Validation("Cannot encode thread stack.".into()))?
            .len()
            - 2;
        if bytes > remaining {
            *truncated = true;
            break;
        }
        remaining -= bytes;
        end = index + character.len_utf8();
    }
    Ok(text[..end].to_string())
}

fn project(info: DashboardConsumerRunningInfo, include_jstack: bool) -> ConsumerResult<DiagnosticResult> {
    let parts = info.into_parts();
    let mut truncated = parts.truncated;
    let properties = bounded_entries(parts.properties, &mut truncated)?;
    let subscriptions = bounded_entries(parts.subscriptions, &mut truncated)?;
    let process_queues = bounded_entries(parts.process_queues, &mut truncated)?;
    let jstack = parts
        .jstack
        .filter(|_| include_jstack)
        .map(|text| bounded_jstack(text, &mut truncated))
        .transpose()?;
    let reason = (include_jstack && jstack.as_ref().is_none_or(String::is_empty))
        .then_some("The client returned no thread stack; it may not support JStack.");
    Ok(DiagnosticResult {
        consumer_group: parts.consumer_group,
        client_id: parts.client_id,
        status: DiagnosticStatus::Available,
        reason,
        properties,
        subscriptions,
        process_queues,
        jstack,
        truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_admin_core::core::consumer::DashboardConsumerConfigAttribute;

    #[test]
    fn diagnostic_projection_bounds_escaped_unicode_and_preserves_manual_stack_selection() {
        let info = DashboardConsumerRunningInfo::new(
            "group".into(),
            "client".into(),
            vec![DashboardConsumerConfigAttribute {
                key: "threads".into(),
                value: "8".into(),
            }],
            vec![],
            vec![],
            Some("\n线程".repeat(30_000)),
            false,
        );
        let result = project(info, true).unwrap();
        assert!(result.truncated);
        assert_eq!(result.status, DiagnosticStatus::Available);
        assert_eq!(result.properties[0]["value"], "8");
        assert!(serde_json::to_vec(&result).unwrap().len() < 256 * 1024);
        let info = DashboardConsumerRunningInfo::new(
            "group".into(),
            "client".into(),
            vec![],
            vec![],
            vec![],
            Some("stack".into()),
            false,
        );
        assert!(project(info, false).unwrap().jstack.is_none());
    }

    #[test]
    fn diagnostic_collection_budget_omits_large_entries_and_reports_truncation() {
        let mut truncated = false;
        let result = bounded_entries(vec!["small".to_string(), "x".repeat(SECTION_BYTES)], &mut truncated).unwrap();
        assert_eq!(result, [Value::String("small".into())]);
        assert!(truncated);
    }

    #[test]
    fn diagnostic_broker_responses_distinguish_offline_and_unsupported() {
        for (code, status) in [
            (206, DiagnosticStatus::Offline),
            (3, DiagnosticStatus::Unsupported),
            (1, DiagnosticStatus::Unavailable),
        ] {
            let error = AdminError::from_error(
                "diagnostic",
                rocketmq_error::Error::new(&rocketmq_error::BROKER_OPERATION_FAILED).with_context(
                    rocketmq_error::ErrorContext::new().with_i64(rocketmq_error::fields::BROKER_CODE, code),
                ),
            );
            assert_eq!(failed("group".into(), "client".into(), &error).status, status);
        }
    }

    #[test]
    fn diagnostic_failures_are_explicit_and_do_not_leak_backend_details() {
        let error = AdminError::from_error(
            "diagnostic",
            rocketmq_error::Error::new(&rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED),
        );
        assert_eq!(
            failed("g".into(), "c".into(), &error).status,
            DiagnosticStatus::Unsupported
        );
        let error = AdminError::backend("diagnostic", "secret-debug-detail");
        let result = failed("g".into(), "c".into(), &error);
        assert_eq!(result.status, DiagnosticStatus::Unavailable);
        assert!(!serde_json::to_string(&result).unwrap().contains("secret-debug-detail"));
    }
}
