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

use super::*;

pub(super) fn remaining_timeout_ms(deadline: tokio::time::Instant, original_ms: u64) -> ProxyResult<u64> {
    let remaining = deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .as_millis();
    if remaining == 0 {
        return Err(canonical::timed_out("forward_message_to_dead_letter_queue", original_ms).into());
    }
    Ok(remaining.min(u128::from(original_ms)) as u64)
}

pub(super) async fn read_original_message(
    client: &dyn ClusterClientIo,
    broker_addr: &CheetahString,
    broker_name: &CheetahString,
    request: &ForwardMessageToDeadLetterQueueRequest,
    parsed: &ParsedReceiptHandle,
    timeout_ms: u64,
) -> ProxyResult<MessageExt> {
    if parsed.queue_id < 0 || parsed.queue_offset < 0 || request.message_id.is_empty() {
        return Err(ProxyError::invalid_receipt_handle("receipt source identity is invalid"));
    }
    // A client UNIQ ID may itself be 32 hex digits. It must never be decoded
    // as a commit-log address. Resolve only the exact POP receipt position,
    // without committing an offset or changing delivery state.
    let header = PullMessageRequestHeader {
        consumer_group: request.group.to_string().into(),
        topic: parsed.topic.clone(),
        lite_topic: None,
        queue_id: parsed.queue_id,
        queue_offset: parsed.queue_offset,
        max_msg_nums: 1,
        sys_flag: PullSysFlag::build_sys_flag(false, false, true, false) as i32,
        commit_offset: 0,
        suspend_timeout_millis: 0,
        sub_version: 0,
        subscription: Some("*".into()),
        expression_type: Some(ExpressionType::TAG.into()),
        max_msg_bytes: None,
        request_source: None,
        proxy_forward_client_id: None,
        topic_request: Some(PopTopicRequestHeader {
            lo: None,
            rpc: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
        }),
    };
    let pulled = client
        .pull_response_from_broker(broker_addr.as_str(), header, timeout_ms)
        .await?;
    if pulled.pull_status() != PullStatus::Found {
        return Err(ProxyError::invalid_receipt_handle(
            "receipt source message was not observed",
        ));
    }
    let mut messages = pulled.into_messages().unwrap_or_default();
    if messages.len() != 1 {
        return Err(ProxyError::invalid_receipt_handle(
            "receipt source must identify one message",
        ));
    }
    let mut message = messages.remove(0);
    let public_id = message
        .property_ref(&MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX.into())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| message.msg_id());
    if message.topic() != &parsed.topic
        || message.queue_id() != parsed.queue_id
        || message.queue_offset() != parsed.queue_offset
        || (request.message_id != public_id.as_str() && request.message_id != message.msg_id().as_str())
    {
        return Err(ProxyError::invalid_receipt_handle(
            "receipt source does not match the requested message",
        ));
    }
    let native_id = decode_broker_message_id(message.msg_id().as_str())?;
    if message.commit_log_offset() < 0 || native_id.offset != message.commit_log_offset() {
        return Err(ProxyError::invalid_receipt_handle(
            "receipt source storage identity is invalid",
        ));
    }
    message.set_broker_name(broker_name.clone());
    message.set_reconsume_times(request.delivery_attempt.saturating_sub(1));
    Ok(message)
}
