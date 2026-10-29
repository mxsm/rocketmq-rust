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

//! Allowlisted Message metadata and bounded, private detail values.

use super::{GpuiAdminProvider, ProviderFailure, mutation_for_revision, query_for_revision, select_admin};
use rocketmq_admin_core::core::{
    dashboard::{DashboardMessage, DashboardMessageQuery},
    message::{DirectConsumeRequest, MessageLookupRequest, MessageRecord},
};
use rocketmq_dashboard_common::{
    ConnectionScope, MESSAGE_KEY_MATCH_LIMIT, MessageConsumeCode, MessageConsumeCommand, MessageConsumeReceipt,
    MessageIdentity, MessageMetadataRow, MessagePageExtent, MessagePayload, MessageSearchPage, MessageSearchQuery,
    MessageTextPreview,
};
use std::sync::Arc;

impl GpuiAdminProvider {
    pub async fn query_messages(
        self: &Arc<Self>,
        revision: u64,
        query: MessageSearchQuery,
    ) -> Result<MessageSearchPage, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-message-query", move |cancellation| async move {
            require_direct(this.snapshot_for_revision(revision)?.scope)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            if let MessageSearchQuery::MessageId { topic, message_id } = &query {
                let request = MessageLookupRequest {
                    topic: topic.clone(),
                    message_id: message_id.clone(),
                };
                let record = select_admin(cancellation, session.lookup_message(&request)).await?;
                require_topic(&record.topic, topic)?;
                return Ok(MessageSearchPage {
                    rows: vec![record_metadata(&record)],
                    extent: MessagePageExtent::End,
                });
            }
            let request = query_request(&query);
            let result = select_admin(cancellation, session.query_messages(&request)).await?;
            let mut rows = Vec::new();
            let limit = match &query {
                MessageSearchQuery::TopicRange { page_size, .. } => *page_size as usize,
                _ => MESSAGE_KEY_MATCH_LIMIT,
            };
            for message in result.items.into_iter().take(limit) {
                require_topic(&message.topic, query.topic())?;
                rows.push(dashboard_metadata(&message));
                // Query payloads and properties are discarded here. Details use the real lookup API.
            }
            let extent = match query {
                MessageSearchQuery::TopicRange { page_size, .. } if rows.len() == page_size as usize => {
                    MessagePageExtent::MoreMayExist
                }
                MessageSearchQuery::Key { .. } if rows.len() == MESSAGE_KEY_MATCH_LIMIT => {
                    MessagePageExtent::KeyLimitReached
                }
                _ => MessagePageExtent::End,
            };
            Ok(MessageSearchPage { rows, extent })
        })
        .await
    }

    pub async fn message_payload(
        self: &Arc<Self>,
        revision: u64,
        identity: MessageIdentity,
    ) -> Result<MessagePayload, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-message-detail", move |cancellation| async move {
            require_direct(this.snapshot_for_revision(revision)?.scope)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let request = MessageLookupRequest {
                topic: identity.topic.clone(),
                message_id: identity.message_id.clone(),
            };
            let message = select_admin(cancellation, session.lookup_message(&request)).await?;
            require_topic(&message.topic, &identity.topic)?;
            if message.message_id != identity.message_id {
                return Err(ProviderFailure::invalid_data(
                    "Message lookup returned a different Message ID.",
                ));
            }
            Ok(MessagePayload::bounded(identity, message.body, message.properties))
        })
        .await
    }

    pub async fn consume_message(
        self: &Arc<Self>,
        revision: u64,
        command: MessageConsumeCommand,
    ) -> Result<MessageConsumeReceipt, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-message-consume", move |cancellation| async move {
            require_direct(this.snapshot_for_revision(revision)?.scope)?;
            let request = DirectConsumeRequest {
                topic: command.identity.topic,
                message_id: command.identity.message_id,
                consumer_group: command.consumer_group,
                client_id: command.client_id,
            };
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let result = select_admin(cancellation, session.consume_message_directly(&request)).await?;
            Ok(MessageConsumeReceipt {
                code: MessageConsumeCode::from_reported(result.consume_result.as_deref()),
                reported_success: result.success,
                remark_returned: result.remark.as_ref().is_some_and(|remark| !remark.trim().is_empty()),
            })
        })
        .await
    }
}

pub(super) fn require_direct(scope: ConnectionScope) -> Result<(), ProviderFailure> {
    if scope == ConnectionScope::NameServer {
        Ok(())
    } else {
        Err(ProviderFailure::invalid_request(
            "Messages require NameServer Direct scope; Proxy forwarding is unavailable.",
        ))
    }
}
pub(super) fn require_topic(actual: &str, expected: &str) -> Result<(), ProviderFailure> {
    if actual == expected {
        Ok(())
    } else {
        Err(ProviderFailure::invalid_data(
            "Message response did not belong to the requested Topic.",
        ))
    }
}

fn query_request(query: &MessageSearchQuery) -> DashboardMessageQuery {
    let mut request = DashboardMessageQuery {
        topic: Some(query.topic().into()),
        ..Default::default()
    };
    match query {
        MessageSearchQuery::TopicRange {
            begin,
            end,
            page,
            page_size,
            ..
        } => {
            request.begin = Some(*begin);
            request.end = Some(*end);
            request.page_num = Some(*page);
            request.page_size = Some(*page_size);
        }
        MessageSearchQuery::Key { key, .. } => request.key = Some(key.clone()),
        MessageSearchQuery::MessageId { message_id, .. } => request.message_id = Some(message_id.clone()),
    }
    request
}

pub(super) fn dashboard_metadata(message: &DashboardMessage) -> MessageMetadataRow {
    MessageMetadataRow {
        identity: MessageIdentity {
            topic: message.topic.clone(),
            message_id: message.message_id.clone(),
        },
        tags: message.tags.as_deref().map(MessageTextPreview::bounded),
        keys: message.keys.as_deref().map(MessageTextPreview::bounded),
        born_host: MessageTextPreview::bounded(&message.born_host),
        store_host: MessageTextPreview::bounded(&message.store_host),
        born_timestamp: message.born_timestamp,
        store_timestamp: message.store_timestamp,
        queue_id: message.queue_id,
        queue_offset: message.queue_offset,
        store_size: message.store_size,
        reconsume_times: message.reconsume_times,
        sys_flag: message.sys_flag,
        flag: message.flag,
        prepared_transaction_offset: message.prepared_transaction_offset,
    }
}
fn record_metadata(message: &MessageRecord) -> MessageMetadataRow {
    MessageMetadataRow {
        identity: MessageIdentity {
            topic: message.topic.clone(),
            message_id: message.message_id.clone(),
        },
        tags: message.tags.as_deref().map(MessageTextPreview::bounded),
        keys: message.keys.as_deref().map(MessageTextPreview::bounded),
        born_host: MessageTextPreview::bounded(&message.born_host),
        store_host: MessageTextPreview::bounded(&message.store_host),
        born_timestamp: message.born_timestamp,
        store_timestamp: message.store_timestamp,
        queue_id: message.queue_id,
        queue_offset: message.queue_offset,
        store_size: message.store_size,
        reconsume_times: message.reconsume_times,
        sys_flag: message.sys_flag,
        flag: message.flag,
        prepared_transaction_offset: message.prepared_transaction_offset,
    }
}
