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

use crate::grpc::service::producer::is_transaction_message;
use crate::proto::v2;
use crate::ClientSessionRegistry;
use crate::EndTransactionPlan;
use crate::EndTransactionRequest;
use crate::PreparedTransactionRegistration;
use crate::ProxyContextWithPrincipal;
use crate::ProxyError;
use crate::ProxyResult;
use crate::SendMessagePlan;
use crate::SendMessageRequest;

pub fn track_prepared_transactions<C, P>(
    sessions: &ClientSessionRegistry<C>,
    producer_group: &str,
    context: &ProxyContextWithPrincipal<P>,
    grpc_request: &v2::SendMessageRequest,
    request: &SendMessageRequest,
    plan: &SendMessagePlan,
) {
    let Some(client_id) = context.client_id() else {
        return;
    };

    for ((grpc_message, message), result) in grpc_request
        .messages
        .iter()
        .zip(request.messages.iter())
        .zip(plan.entries.iter())
    {
        if !is_transaction_message(&message.message) {
            continue;
        }
        let Some(send_result) = result.send_result.as_ref() else {
            continue;
        };
        let Some(transaction_id) = send_result.transaction_id.clone() else {
            continue;
        };
        let Some(commit_log_message_id) = send_result
            .offset_msg_id
            .clone()
            .or_else(|| send_result.msg_id.as_ref().map(ToString::to_string))
        else {
            continue;
        };
        let client_visible_message_id = send_result
            .msg_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| message.client_message_id.clone());
        sessions.track_prepared_transaction(PreparedTransactionRegistration {
            client_id: client_id.to_owned(),
            topic: message.topic.clone(),
            message_id: client_visible_message_id,
            transaction_id,
            producer_group: producer_group.to_owned(),
            transaction_state_table_offset: send_result.queue_offset,
            commit_log_message_id,
            message: grpc_message.clone(),
            orphaned_transaction_recovery_duration: grpc_message
                .system_properties
                .as_ref()
                .and_then(|system| system.orphaned_transaction_recovery_duration.as_ref())
                .and_then(proto_duration),
        });
    }
}

pub fn enrich_end_transaction_request<C, P>(
    sessions: &ClientSessionRegistry<C>,
    context: &ProxyContextWithPrincipal<P>,
    request: &mut EndTransactionRequest,
) -> ProxyResult<()> {
    let client_id = context.require_client_id()?;
    let tracked = sessions
        .prepared_transaction(client_id, request.transaction_id.as_str(), request.message_id.as_str())
        .ok_or_else(|| {
            ProxyError::invalid_transaction_id(format!(
                "transaction '{}' was not found in proxy session state",
                request.transaction_id
            ))
        })?;
    request.producer_group = Some(tracked.producer_group);
    request.transaction_state_table_offset = Some(tracked.transaction_state_table_offset);
    request.commit_log_message_id = Some(tracked.commit_log_message_id);
    Ok(())
}

pub fn reconcile_end_transaction_result<C, P>(
    sessions: &ClientSessionRegistry<C>,
    context: &ProxyContextWithPrincipal<P>,
    request: &EndTransactionRequest,
    plan: &EndTransactionPlan,
) {
    if !plan.status.is_ok() {
        return;
    }
    let Some(client_id) = context.client_id() else {
        return;
    };
    let _ =
        sessions.remove_prepared_transaction(client_id, request.transaction_id.as_str(), request.message_id.as_str());
}

fn proto_duration(duration: &prost_types::Duration) -> Option<std::time::Duration> {
    if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
        return None;
    }
    Some(std::time::Duration::new(
        u64::try_from(duration.seconds).ok()?,
        u32::try_from(duration.nanos).ok()?,
    ))
}
