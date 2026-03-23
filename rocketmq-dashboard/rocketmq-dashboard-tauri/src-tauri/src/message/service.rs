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

use crate::message::admin::ManagedMessageAdmin;
use crate::message::types::MessageError;
use crate::message::types::MessageResult;
use crate::message::types::MessageSummaryListResponse;
use crate::message::types::MessageSummaryView;
use crate::nameserver::NameServerRuntimeState;
use cheetah_string::CheetahString;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_dashboard_common::MessageIdQueryRequest;
use rocketmq_dashboard_common::MessageKeyQueryRequest;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct MessageManager {
    runtime: Arc<NameServerRuntimeState>,
    admin_session: Arc<Mutex<Option<ManagedMessageAdmin>>>,
}

impl MessageManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            admin_session: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn query_message_by_topic_key(
        &self,
        request: MessageKeyQueryRequest,
    ) -> MessageResult<MessageSummaryListResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.query_message_by_topic_key_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_message_by_topic_key failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_message_by_topic_key` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn query_message_by_id(
        &self,
        request: MessageIdQueryRequest,
    ) -> MessageResult<MessageSummaryListResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.query_message_by_id_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_message_by_id failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_message_by_id` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn ensure_admin_session(&self, session_slot: &mut Option<ManagedMessageAdmin>) -> MessageResult<()> {
        let generation = self.runtime.generation();
        let needs_reconnect = session_slot
            .as_ref()
            .is_none_or(|session| !session.matches_generation(generation));

        if needs_reconnect {
            self.reset_admin_session(session_slot, "refreshing message admin session")
                .await;
            let session = ManagedMessageAdmin::connect(&self.runtime).await?;
            log::info!(
                "Connected message admin session for namesrv `{}` at generation {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                session.generation
            );
            *session_slot = Some(session);
        }

        Ok(())
    }

    async fn reset_admin_session(&self, session_slot: &mut Option<ManagedMessageAdmin>, reason: &str) {
        if let Some(mut session) = session_slot.take() {
            log::info!(
                "Shutting down message admin session for namesrv `{}`: {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                reason
            );
            session.shutdown().await;
        }
    }

    fn should_reset_session<T>(result: &MessageResult<T>) -> bool {
        match result {
            Err(MessageError::RocketMQ(message)) => is_reconnect_worthy_error(message),
            _ => false,
        }
    }

    async fn query_message_by_topic_key_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: MessageKeyQueryRequest,
    ) -> MessageResult<MessageSummaryListResponse> {
        let topic = normalize_required_field("topic", request.topic)?;
        let key = normalize_required_field("key", request.key)?;

        let result = admin
            .query_message_by_key(
                None,
                CheetahString::from(topic.clone()),
                CheetahString::from(key),
                64,
                0,
                i64::MAX,
                CheetahString::from_static_str("K"),
                None,
            )
            .await
            .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

        let mut items: Vec<MessageSummaryView> = result
            .message_list()
            .iter()
            .cloned()
            .map(map_message_summary)
            .collect();
        sort_message_summaries_desc(&mut items);

        Ok(MessageSummaryListResponse {
            total: items.len(),
            items,
        })
    }

    async fn query_message_by_id_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: MessageIdQueryRequest,
    ) -> MessageResult<MessageSummaryListResponse> {
        let topic = normalize_required_field("topic", request.topic)?;
        let message_id = normalize_required_field("messageId", request.message_id)?;

        let result = admin
            .query_message_by_unique_key(
                None,
                CheetahString::from(topic),
                CheetahString::from(message_id),
                32,
                0,
                i64::MAX,
            )
            .await
            .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

        let messages = result.message_list().to_vec();
        let message = first_message_or_validation_error(messages)?;

        Ok(MessageSummaryListResponse {
            items: vec![map_message_summary(message)],
            total: 1,
        })
    }
}

fn normalize_required_field(field_name: &str, value: String) -> MessageResult<String> {
    let normalized = value.trim().to_string();
    if normalized.is_empty() {
        return Err(MessageError::Validation(format!(
            "`{field_name}` is required for message queries."
        )));
    }
    Ok(normalized)
}

fn sort_message_summaries_desc(items: &mut [MessageSummaryView]) {
    items.sort_by(|left, right| {
        right
            .store_timestamp
            .cmp(&left.store_timestamp)
            .then_with(|| left.msg_id.cmp(&right.msg_id))
    });
}

fn is_reconnect_worthy_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("connection refused")
        || normalized.contains("connection reset")
        || normalized.contains("timed out")
        || normalized.contains("timeout")
        || normalized.contains("channel inactive")
        || normalized.contains("broken pipe")
}

fn extract_keys(message: &MessageExt) -> Option<String> {
    message
        .message_inner()
        .keys()
        .map(|keys| keys.join(" "))
        .filter(|keys| !keys.is_empty())
}

fn map_message_summary(message: MessageExt) -> MessageSummaryView {
    MessageSummaryView {
        topic: message.topic().to_string(),
        msg_id: message.msg_id().to_string(),
        tags: message.get_tags().map(|value| value.to_string()),
        keys: extract_keys(&message),
        store_timestamp: message.store_timestamp(),
    }
}

fn first_message_or_validation_error(mut messages: Vec<MessageExt>) -> MessageResult<MessageExt> {
    messages.sort_by(|left, right| {
        right
            .store_timestamp()
            .cmp(&left.store_timestamp())
            .then_with(|| left.msg_id().cmp(right.msg_id()))
    });

    messages
        .into_iter()
        .next()
        .ok_or_else(|| MessageError::Validation("No message matched the current query.".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_common::common::message::message_builder::MessageBuilder;

    #[test]
    fn map_message_summary_extracts_topic_tags_keys_and_store_timestamp() {
        let message = MessageBuilder::new()
            .topic("TopicTest")
            .body_slice(b"payload")
            .tags("TagA")
            .keys(vec!["KeyA".to_string(), "KeyB".to_string()])
            .build_unchecked();

        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.set_msg_id(CheetahString::from("msg-1"));
        message_ext.set_store_timestamp(1_700_000_000_123);

        let summary = map_message_summary(message_ext);

        assert_eq!(summary.topic, "TopicTest");
        assert_eq!(summary.msg_id, "msg-1");
        assert_eq!(summary.tags.as_deref(), Some("TagA"));
        assert_eq!(summary.keys.as_deref(), Some("KeyA KeyB"));
        assert_eq!(summary.store_timestamp, 1_700_000_000_123);
    }

    #[test]
    fn first_message_or_validation_error_requires_a_match() {
        let error = first_message_or_validation_error(Vec::new()).expect_err("empty results should fail");

        assert!(error.to_string().contains("No message matched the current query"));
    }
}
