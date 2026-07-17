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

pub(super) fn is_consumer_not_online_error(error: &RocketMQError) -> bool {
    matches!(
        error,
        RocketMQError::BrokerOperationFailed { code, .. }
            if ResponseCode::from(*code) == ResponseCode::ConsumerNotOnline
    )
}

pub(super) fn unique_producer_group(now_millis: u64, transactional: bool) -> String {
    let sequence = PRODUCER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let kind = if transactional { "tx-sender" } else { "sender" };
    format!("dashboard-topic-{kind}-{now_millis}-{sequence}")
}

pub(super) fn build_send_message(request: &TopicSendRequest) -> Message {
    let mut builder = Message::builder()
        .topic(request.topic.clone())
        .body_slice(request.message_body.as_bytes())
        .trace_switch(request.trace_enabled);
    if !request.tag.trim().is_empty() {
        builder = builder.tags(request.tag.clone());
    }
    if !request.key.trim().is_empty() {
        builder = builder.key(request.key.clone());
    }
    builder.build_unchecked()
}

pub(super) async fn send_normal_message(
    mut client_config: rocketmq_client_rust::base::client_config::ClientConfig,
    producer_group: String,
    request: &TopicSendRequest,
) -> Result<TopicSendResult, AdminError> {
    client_config.set_instance_name(producer_group.clone().into());
    let mut producer = DefaultMQProducer::builder()
        .producer_group(producer_group)
        .client_config(client_config)
        .build();
    producer
        .start()
        .await
        .map_err(|error| backend_error("start_topic_test_producer", error))?;
    let send_result = producer
        .send_with_timeout(build_send_message(request), SEND_TIMEOUT_MILLIS)
        .await;
    producer.shutdown().await;
    let send_result = send_result
        .map_err(|error| backend_error("send_topic_test_message", error))?
        .ok_or_else(|| {
            AdminError::backend(
                "send_topic_test_message",
                "broker acknowledged send without returning a result",
            )
        })?;
    Ok(map_send_result(request.topic.clone(), send_result))
}

pub(super) async fn send_transaction_message(
    mut client_config: rocketmq_client_rust::base::client_config::ClientConfig,
    producer_group: String,
    request: &TopicSendRequest,
) -> Result<TopicSendResult, AdminError> {
    client_config.set_instance_name(producer_group.clone().into());
    let mut producer = TransactionMQProducer::builder()
        .producer_group(producer_group)
        .client_config(client_config)
        .transaction_listener(CommitTransactionListener)
        .build();
    producer
        .start()
        .await
        .map_err(|error| backend_error("start_topic_transaction_test_producer", error))?;
    let send_result = producer
        .send_message_in_transaction::<(), _>(build_send_message(request), None)
        .await;
    producer.shutdown().await;
    map_transaction_send_result(
        request.topic.clone(),
        send_result.map_err(|error| backend_error("send_topic_transaction_test_message", error))?,
    )
}

pub(super) fn map_send_result(topic: String, send_result: SendResult) -> TopicSendResult {
    TopicSendResult {
        topic,
        send_status: format!("{:?}", send_result.send_status),
        message_id: send_result.msg_id.map(|message_id| message_id.to_string()),
        broker_name: send_result
            .message_queue
            .as_ref()
            .map(|queue| queue.broker_name().to_string()),
        queue_id: send_result.message_queue.as_ref().map(|queue| queue.queue_id()),
        queue_offset: send_result.queue_offset,
        transaction_id: send_result.transaction_id,
        region_id: send_result.region_id,
        local_transaction_state: None,
    }
}

pub(super) fn map_transaction_send_result(
    topic: String,
    transaction_result: TransactionSendResult,
) -> Result<TopicSendResult, AdminError> {
    let send_result = transaction_result.send_result.ok_or_else(|| {
        AdminError::backend(
            "send_topic_transaction_test_message",
            "transaction producer completed without returning a send result",
        )
    })?;
    let mut result = map_send_result(topic, send_result);
    result.local_transaction_state = transaction_result
        .local_transaction_state
        .map(|state| state.to_string());
    if let Some(local_state) = result.local_transaction_state.as_deref() {
        result.send_status = format!("{} ({local_state})", result.send_status);
    }
    Ok(result)
}
