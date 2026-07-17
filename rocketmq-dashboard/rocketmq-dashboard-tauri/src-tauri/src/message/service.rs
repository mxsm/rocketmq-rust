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
use crate::message::page_cache::MessagePageCache;
use crate::message::page_cache::MessagePageCacheEntry;
use crate::message::page_cache::MessagePageCacheKey;
use crate::message::page_cache::NormalizedMessagePageQuery;
use crate::message::page_cache::QueueScanState;
use crate::message::page_cache::build_page_selection;
use crate::message::page_cache::normalize_message_page_query;
use crate::message::types::DlqBatchMessageExportView;
use crate::message::types::DlqMessageExportView;
use crate::message::types::MessageBatchResendResponse;
use crate::message::types::MessageDetailView;
use crate::message::types::MessageError;
use crate::message::types::MessagePageResponse;
use crate::message::types::MessagePageView;
use crate::message::types::MessageResendResult;
use crate::message::types::MessageResult;
use crate::message::types::MessageSummaryListResponse;
use crate::message::types::MessageSummaryView;
use crate::message::types::MessageTraceConsumerGroupView;
use crate::message::types::MessageTraceDetailView;
use crate::message::types::MessageTraceNodeView;
use crate::nameserver::NameServerRuntimeState;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::Local;
use chrono::TimeZone;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::message::DirectConsumeRequest;
use rocketmq_admin_core::core::message::DirectConsumeResult;
use rocketmq_admin_core::core::message::DlqMessageLookupRequest;
use rocketmq_admin_core::core::message::MessageAdmin;
use rocketmq_admin_core::core::message::MessageDetailRecord;
use rocketmq_admin_core::core::message::MessageLookupRequest;
use rocketmq_admin_core::core::message::MessagePullStatus;
use rocketmq_admin_core::core::message::MessageQueuePlan;
use rocketmq_admin_core::core::message::MessageQueuePlanRequest;
use rocketmq_admin_core::core::message::MessageRecord;
use rocketmq_admin_core::core::message::MessageTrackRecord;
use rocketmq_admin_core::core::message::PullMessagesRequest;
use rocketmq_admin_core::core::message::QueryMessagesByKeyRequest;
use rocketmq_admin_core::core::message::TraceQueryRequest;
use rocketmq_admin_core::core::message::TraceSeed;
use rocketmq_dashboard_common::DlqBatchExportMessageRequest;
use rocketmq_dashboard_common::DlqBatchResendMessageRequest;
use rocketmq_dashboard_common::DlqMessagePageQueryRequest;
use rocketmq_dashboard_common::DlqResendMessageRequest;
use rocketmq_dashboard_common::DlqViewMessageRequest;
use rocketmq_dashboard_common::MessageDirectConsumeRequest;
use rocketmq_dashboard_common::MessageIdQueryRequest;
use rocketmq_dashboard_common::MessageKeyQueryRequest;
use rocketmq_dashboard_common::MessagePageQueryRequest;
use rocketmq_dashboard_common::MessageTraceQueryRequest;
use rocketmq_dashboard_common::ViewMessageRequest;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const PULL_BATCH_SIZE: i32 = 32;
const DLQ_GROUP_TOPIC_PREFIX: &str = "%DLQ%";

#[derive(Clone)]
pub(crate) struct MessageManager {
    runtime: Arc<NameServerRuntimeState>,
    admin_session: Arc<Mutex<Option<ManagedMessageAdmin>>>,
    page_cache: MessagePageCache,
}

impl MessageManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            admin_session: Arc::new(Mutex::new(None)),
            page_cache: MessagePageCache::new(),
        }
    }

    pub(crate) async fn shutdown(&self) {
        let mut session = self.admin_session.lock().await;
        if let Some(mut admin) = session.take() {
            admin.shutdown().await;
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

    pub(crate) async fn query_message_page_by_topic(
        &self,
        request: MessagePageQueryRequest,
    ) -> MessageResult<MessagePageResponse> {
        let query = normalize_message_page_query(request)?;
        let generation = self.runtime.generation();

        if let Some(task_id) = query.task_id.as_deref()
            && let Some(entry) = self.page_cache.get(task_id).await
            && entry.matches_request(&query, generation)
        {
            return self.query_message_page_from_cache(query, entry).await;
        }

        self.query_first_message_page(query.first_page(), generation).await
    }

    pub(crate) async fn query_dlq_message_by_consumer_group(
        &self,
        request: DlqMessagePageQueryRequest,
    ) -> MessageResult<MessagePageResponse> {
        let query = normalize_message_page_query(dlq_page_query_to_message_page_request(request)?)?;
        let generation = self.runtime.generation();

        if let Some(task_id) = query.task_id.as_deref()
            && let Some(entry) = self.page_cache.get(task_id).await
            && entry.matches_request(&query, generation)
        {
            return self.query_message_page_from_cache(query, entry).await;
        }

        self.query_first_dlq_message_page(query.first_page(), generation).await
    }

    pub(crate) async fn view_message_detail(&self, request: ViewMessageRequest) -> MessageResult<MessageDetailView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.view_message_detail_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "view_message_detail failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `view_message_detail` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn view_dlq_message_detail(
        &self,
        request: DlqViewMessageRequest,
    ) -> MessageResult<MessageDetailView> {
        self.view_message_detail(ViewMessageRequest {
            topic: build_dlq_topic(&request.consumer_group)?,
            message_id: request.message_id,
        })
        .await
    }

    pub(crate) async fn resend_dlq_message(
        &self,
        request: DlqResendMessageRequest,
    ) -> MessageResult<MessageResendResult> {
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;

        let result = {
            let session = session_guard
                .as_mut()
                .expect("message admin session should be initialized before use");
            self.resend_dlq_message_with_admin(&mut session.admin, request).await
        };

        if Self::should_reset_session(&result) {
            // The broker may have completed this mutation before the transport failed. Reconnect
            // for the next request, but never replay a potentially successful resend.
            self.reset_admin_session(&mut session_guard, "resend_dlq_message failed")
                .await;
        }
        result
    }

    pub(crate) async fn batch_resend_dlq_message(
        &self,
        request: DlqBatchResendMessageRequest,
    ) -> MessageResult<MessageBatchResendResponse> {
        let requests = normalize_batch_resend_requests(request.messages)?;
        let mut items = Vec::with_capacity(requests.len());

        for request in requests {
            let consumer_group = request.consumer_group.trim().to_string();
            let message_id = request.message_id.trim().to_string();
            let result = match self.resend_dlq_message(request).await {
                Ok(result) => result,
                Err(error) => build_failed_message_resend_result(consumer_group, message_id, error),
            };
            items.push(result);
        }

        let success_count = items.iter().filter(|item| item.success).count();
        let total = items.len();

        Ok(MessageBatchResendResponse {
            items,
            total,
            success_count,
            failure_count: total.saturating_sub(success_count),
        })
    }

    pub(crate) async fn export_dlq_message(
        &self,
        request: DlqViewMessageRequest,
    ) -> MessageResult<DlqMessageExportView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.export_dlq_message_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "export_dlq_message failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `export_dlq_message` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn batch_export_dlq_message(
        &self,
        request: DlqBatchExportMessageRequest,
    ) -> MessageResult<DlqBatchMessageExportView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.batch_export_dlq_message_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "batch_export_dlq_message failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `batch_export_dlq_message` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn consume_message_directly(
        &self,
        request: MessageDirectConsumeRequest,
    ) -> MessageResult<MessageResendResult> {
        let request = normalize_direct_consume_request(request)?;
        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;

        let result = {
            let session = session_guard
                .as_mut()
                .expect("message admin session should be initialized before use");
            self.consume_message_directly_with_admin(&mut session.admin, request)
                .await
        };

        if Self::should_reset_session(&result) {
            // Direct consume is not idempotent. Reset the broken session without replaying the
            // request because the broker may already have performed the consume operation.
            self.reset_admin_session(&mut session_guard, "consume_message_directly failed")
                .await;
        }
        result
    }

    pub(crate) async fn query_message_trace_by_id(
        &self,
        request: MessageTraceQueryRequest,
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
                self.query_message_trace_by_id_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_message_trace_by_id failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_message_trace_by_id` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn view_message_trace_detail(
        &self,
        request: MessageTraceQueryRequest,
    ) -> MessageResult<MessageTraceDetailView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.view_message_trace_detail_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "view_message_trace_detail failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `view_message_trace_detail` after reconnect: {}", error);
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
        matches!(
            result,
            Err(MessageError::Admin(error)) if error.is_retryable()
                || matches!(error, rocketmq_admin_core::core::AdminError::SessionClosed)
        )
    }

    async fn query_first_message_page(
        &self,
        query: NormalizedMessagePageQuery,
        generation: u64,
    ) -> MessageResult<MessagePageResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.query_first_message_page_with_admin(&mut session.admin, &query, generation)
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_first_message_page failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_first_message_page` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn query_first_dlq_message_page(
        &self,
        query: NormalizedMessagePageQuery,
        generation: u64,
    ) -> MessageResult<MessagePageResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.query_first_dlq_message_page_with_admin(&mut session.admin, &query, generation)
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_first_dlq_message_page failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_first_dlq_message_page` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn query_message_page_from_cache(
        &self,
        query: NormalizedMessagePageQuery,
        entry: MessagePageCacheEntry,
    ) -> MessageResult<MessagePageResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.query_message_page_from_cache_with_admin(&mut session.admin, &query, &entry)
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_message_page_from_cache failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_message_page_from_cache` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn query_first_message_page_with_admin(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
        generation: u64,
    ) -> MessageResult<MessagePageResponse> {
        let queue_states = self.build_topic_queue_states(admin, query).await?;
        self.finish_first_message_page(admin, query, generation, queue_states)
            .await
    }

    async fn finish_first_message_page(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
        generation: u64,
        mut queue_states: Vec<QueueScanState>,
    ) -> MessageResult<MessagePageResponse> {
        for queue_state in &mut queue_states {
            self.align_queue_start(admin, query, queue_state).await?;
        }
        for queue_state in &mut queue_states {
            self.align_queue_end(admin, query, queue_state).await?;
        }

        let selection = build_page_selection(&queue_states, query);
        let items = self.load_page_messages(admin, query, &selection.queue_states).await?;
        let task_id = self
            .page_cache
            .put(
                MessagePageCacheKey {
                    topic: query.topic.clone(),
                    begin: query.begin,
                    end: query.end,
                },
                generation,
                selection.total,
                queue_states,
            )
            .await;

        Ok(build_message_page_response(items, selection.total, query, &task_id))
    }

    async fn query_first_dlq_message_page_with_admin(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
        generation: u64,
    ) -> MessageResult<MessagePageResponse> {
        let plan = admin
            .message_queue_plan(&MessageQueuePlanRequest {
                topic: query.topic.clone(),
                begin: query.begin,
                end: query.end,
            })
            .await
            .map_err(MessageError::Admin)?;
        if !plan.topic_exists {
            return Ok(build_empty_message_page_response(query));
        }

        let queue_states = queue_states_from_plan(query, plan)?;
        self.finish_first_message_page(admin, query, generation, queue_states)
            .await
    }

    async fn query_message_page_from_cache_with_admin(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
        entry: &MessagePageCacheEntry,
    ) -> MessageResult<MessagePageResponse> {
        let selection = build_page_selection(&entry.queue_states, query);
        let items = self.load_page_messages(admin, query, &selection.queue_states).await?;

        Ok(build_message_page_response(items, entry.total, query, &entry.task_id))
    }

    async fn build_topic_queue_states(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
    ) -> MessageResult<Vec<QueueScanState>> {
        let plan = admin
            .message_queue_plan(&MessageQueuePlanRequest {
                topic: query.topic.clone(),
                begin: query.begin,
                end: query.end,
            })
            .await
            .map_err(MessageError::Admin)?;
        queue_states_from_plan(query, plan)
    }

    async fn align_queue_start(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
        queue_state: &mut QueueScanState,
    ) -> MessageResult<()> {
        let mut start = queue_state.start;
        let mut has_data = false;

        while start < queue_state.end {
            let batch_size = ((queue_state.end - start).min(PULL_BATCH_SIZE as i64)).max(1) as i32;
            let result = admin
                .pull_messages(&PullMessagesRequest {
                    broker_addr: queue_state.broker_addr.clone(),
                    queue: queue_state.queue_ref(),
                    offset: start,
                    max_messages: batch_size,
                })
                .await
                .map_err(MessageError::Admin)?;

            match result.status {
                MessagePullStatus::Found => {
                    let messages = result.messages;
                    if messages.is_empty() {
                        break;
                    }

                    has_data = true;
                    let mut advanced = false;
                    for message in messages {
                        if message.store_timestamp < query.begin {
                            start += 1;
                            advanced = true;
                        } else {
                            advanced = false;
                            break;
                        }
                    }

                    if !advanced {
                        break;
                    }
                }
                MessagePullStatus::NoMatchedMsg | MessagePullStatus::NoNewMsg | MessagePullStatus::OffsetIllegal => {
                    break;
                }
            }
        }

        if !has_data || start >= queue_state.end {
            queue_state.end = start;
        }
        queue_state.start = start;
        queue_state.reset_selection();

        Ok(())
    }

    async fn align_queue_end(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
        queue_state: &mut QueueScanState,
    ) -> MessageResult<()> {
        if queue_state.start >= queue_state.end {
            queue_state.end = queue_state.start;
            queue_state.reset_selection();
            return Ok(());
        }

        let mut end = queue_state.end;
        let mut pull_offset = end;
        let mut has_illegal_offset = true;

        while has_illegal_offset {
            let mut pull_size = PULL_BATCH_SIZE as i64;
            if pull_offset - pull_size > queue_state.start {
                pull_offset -= pull_size;
            } else {
                pull_offset = queue_state.start;
                pull_size = end - pull_offset;
            }

            if pull_size <= 0 {
                break;
            }

            let result = admin
                .pull_messages(&PullMessagesRequest {
                    broker_addr: queue_state.broker_addr.clone(),
                    queue: queue_state.queue_ref(),
                    offset: pull_offset,
                    max_messages: pull_size as i32,
                })
                .await
                .map_err(MessageError::Admin)?;

            match result.status {
                MessagePullStatus::Found => {
                    for message in result.messages.iter().rev() {
                        if message.store_timestamp > query.end {
                            end -= 1;
                        } else {
                            has_illegal_offset = false;
                            break;
                        }
                    }
                }
                MessagePullStatus::NoMatchedMsg | MessagePullStatus::NoNewMsg | MessagePullStatus::OffsetIllegal => {
                    has_illegal_offset = false;
                }
            }

            if pull_offset == queue_state.start {
                break;
            }
        }

        queue_state.end = end.max(queue_state.start);
        queue_state.reset_selection();

        Ok(())
    }

    async fn load_page_messages(
        &self,
        admin: &mut AdminSession,
        query: &NormalizedMessagePageQuery,
        queue_states: &[QueueScanState],
    ) -> MessageResult<Vec<MessageSummaryView>> {
        let mut items = Vec::new();

        for queue_state in queue_states {
            let mut pull_offset = queue_state.start_offset;
            let mut remaining = queue_state.selection_len();

            while remaining > 0 && pull_offset < queue_state.end_offset {
                let batch_size = ((queue_state.end_offset - pull_offset).min(PULL_BATCH_SIZE as i64)).max(1) as i32;
                let result = admin
                    .pull_messages(&PullMessagesRequest {
                        broker_addr: queue_state.broker_addr.clone(),
                        queue: queue_state.queue_ref(),
                        offset: pull_offset,
                        max_messages: batch_size,
                    })
                    .await
                    .map_err(MessageError::Admin)?;

                match result.status {
                    MessagePullStatus::Found => {
                        let messages = result.messages;
                        if messages.is_empty() {
                            break;
                        }

                        for message in messages {
                            if remaining == 0 {
                                break;
                            }
                            if message.store_timestamp < query.begin || message.store_timestamp > query.end {
                                continue;
                            }

                            items.push(map_message_summary(message));
                            remaining -= 1;
                        }

                        let next_begin_offset = result.next_begin_offset;
                        if next_begin_offset <= pull_offset {
                            break;
                        }
                        pull_offset = next_begin_offset;
                    }
                    MessagePullStatus::NoMatchedMsg
                    | MessagePullStatus::NoNewMsg
                    | MessagePullStatus::OffsetIllegal => break,
                }
            }
        }

        sort_message_summaries_desc(&mut items);
        Ok(items)
    }

    async fn query_message_by_topic_key_with_admin(
        &self,
        admin: &mut AdminSession,
        request: MessageKeyQueryRequest,
    ) -> MessageResult<MessageSummaryListResponse> {
        let topic = normalize_required_field("topic", request.topic)?;
        let key = normalize_required_field("key", request.key)?;

        let result = admin
            .query_messages_by_key(&QueryMessagesByKeyRequest {
                topic,
                key,
                max_messages: 64,
                begin: 0,
                end: i64::MAX,
            })
            .await
            .map_err(MessageError::Admin)?;

        let mut items: Vec<MessageSummaryView> = result.messages.into_iter().map(map_message_summary).collect();
        sort_message_summaries_desc(&mut items);

        Ok(MessageSummaryListResponse {
            total: items.len(),
            items,
        })
    }

    async fn query_message_by_id_with_admin(
        &self,
        admin: &mut AdminSession,
        request: MessageIdQueryRequest,
    ) -> MessageResult<MessageSummaryListResponse> {
        let message = self
            .find_message_by_id_with_admin(admin, request.topic, request.message_id)
            .await?;

        Ok(MessageSummaryListResponse {
            items: vec![map_message_summary(message)],
            total: 1,
        })
    }

    async fn view_message_detail_with_admin(
        &self,
        admin: &mut AdminSession,
        request: ViewMessageRequest,
    ) -> MessageResult<MessageDetailView> {
        let detail = admin
            .message_detail(&MessageLookupRequest {
                topic: normalize_required_field("topic", request.topic)?,
                message_id: normalize_required_field("messageId", request.message_id)?,
            })
            .await
            .map_err(MessageError::Admin)?;

        Ok(map_message_detail(detail))
    }

    async fn resend_dlq_message_with_admin(
        &self,
        admin: &mut AdminSession,
        request: DlqResendMessageRequest,
    ) -> MessageResult<MessageResendResult> {
        let consumer_group = normalize_required_field("consumerGroup", request.consumer_group)?;
        let message_id = normalize_required_field("messageId", request.message_id)?;
        let result = admin
            .resend_dlq_message(&DlqMessageLookupRequest {
                consumer_group: consumer_group.clone(),
                message_id,
            })
            .await
            .map_err(MessageError::Admin)?;
        Ok(build_message_resend_result(
            consumer_group,
            result.topic,
            result.message_id,
            result.consume,
        ))
    }

    async fn export_dlq_message_with_admin(
        &self,
        admin: &mut AdminSession,
        request: DlqViewMessageRequest,
    ) -> MessageResult<DlqMessageExportView> {
        let consumer_group = normalize_required_field("consumerGroup", request.consumer_group)?;
        let topic = build_dlq_topic(&consumer_group)?;
        let message_id = normalize_required_field("messageId", request.message_id)?;
        let message = admin
            .find_dlq_message(&DlqMessageLookupRequest {
                consumer_group,
                message_id,
            })
            .await?;
        Ok(build_dlq_export_view(topic, message))
    }

    async fn batch_export_dlq_message_with_admin(
        &self,
        admin: &mut AdminSession,
        request: DlqBatchExportMessageRequest,
    ) -> MessageResult<DlqBatchMessageExportView> {
        let requests = normalize_batch_export_requests(request.messages)?;
        let mut rows = Vec::with_capacity(requests.len());
        let mut success_count = 0usize;
        let mut failure_count = 0usize;

        for request in requests {
            let consumer_group = request.consumer_group.clone();
            let message_id = request.message_id.clone();
            let topic = build_dlq_topic(&consumer_group)?;
            match admin
                .find_dlq_message(&DlqMessageLookupRequest {
                    consumer_group,
                    message_id: message_id.clone(),
                })
                .await
            {
                Ok(message) => {
                    rows.push(build_dlq_export_row(topic, message, String::new()));
                    success_count += 1;
                }
                Err(error) => {
                    rows.push(build_failed_dlq_export_row(
                        topic,
                        message_id,
                        MessageError::Admin(error),
                    ));
                    failure_count += 1;
                }
            }
        }

        Ok(build_batch_dlq_export_view(rows, success_count, failure_count))
    }

    async fn consume_message_directly_with_admin(
        &self,
        admin: &mut AdminSession,
        request: NormalizedDirectConsumeRequest,
    ) -> MessageResult<MessageResendResult> {
        let consume_result = admin
            .consume_message_directly(&DirectConsumeRequest {
                topic: request.topic.clone(),
                consumer_group: request.consumer_group.clone(),
                message_id: request.message_id.clone(),
                client_id: request.client_id,
            })
            .await
            .map_err(MessageError::Admin)?;

        Ok(build_message_resend_result(
            request.consumer_group,
            request.topic,
            request.message_id,
            consume_result,
        ))
    }

    async fn find_message_by_id_with_admin(
        &self,
        admin: &mut AdminSession,
        topic: String,
        message_id: String,
    ) -> MessageResult<MessageRecord> {
        let topic = normalize_required_field("topic", topic)?;
        let message_id = normalize_required_field("messageId", message_id)?;
        admin
            .find_message(&MessageLookupRequest { topic, message_id })
            .await
            .map_err(MessageError::Admin)
    }

    async fn query_message_trace_by_id_with_admin(
        &self,
        admin: &mut AdminSession,
        request: MessageTraceQueryRequest,
    ) -> MessageResult<MessageSummaryListResponse> {
        let (_, message_id, seeds) = self.query_trace_seeds_with_admin(admin, request).await?;
        let summary = build_trace_summary(&message_id, seeds)?;

        Ok(MessageSummaryListResponse {
            items: vec![summary],
            total: 1,
        })
    }

    async fn view_message_trace_detail_with_admin(
        &self,
        admin: &mut AdminSession,
        request: MessageTraceQueryRequest,
    ) -> MessageResult<MessageTraceDetailView> {
        let (trace_topic, message_id, seeds) = self.query_trace_seeds_with_admin(admin, request).await?;
        build_trace_detail(&message_id, &trace_topic, seeds)
    }

    async fn query_trace_seeds_with_admin(
        &self,
        admin: &mut AdminSession,
        request: MessageTraceQueryRequest,
    ) -> MessageResult<(String, String, Vec<TraceSeed>)> {
        let message_id = normalize_required_field("messageId", request.message_id)?;
        let trace_topic = request.trace_topic.as_str().trim();
        let trace = admin
            .query_trace_data(&TraceQueryRequest {
                message_id,
                trace_topic: (!trace_topic.is_empty()).then(|| trace_topic.to_string()),
            })
            .await
            .map_err(MessageError::Admin)?;

        Ok((trace.trace_topic, trace.message_id, trace.seeds))
    }
}

fn queue_states_from_plan(
    query: &NormalizedMessagePageQuery,
    plan: MessageQueuePlan,
) -> MessageResult<Vec<QueueScanState>> {
    let queue_states = plan
        .queues
        .into_iter()
        .enumerate()
        .map(|(idx, range)| QueueScanState::new(idx, range.broker_addr, range.queue, range.start, range.end))
        .collect::<Vec<_>>();

    if queue_states.is_empty() {
        return Err(MessageError::Validation(format!(
            "No readable message queue was found for topic `{}`.",
            query.topic
        )));
    }

    Ok(queue_states)
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

fn normalize_batch_resend_requests(
    requests: Vec<DlqResendMessageRequest>,
) -> MessageResult<Vec<DlqResendMessageRequest>> {
    if requests.is_empty() {
        return Err(MessageError::Validation(
            "At least one DLQ message must be selected for batch resend.".to_string(),
        ));
    }

    requests
        .into_iter()
        .map(|request| {
            Ok(DlqResendMessageRequest {
                consumer_group: normalize_required_field("consumerGroup", request.consumer_group)?,
                message_id: normalize_required_field("messageId", request.message_id)?,
            })
        })
        .collect()
}

fn normalize_batch_export_requests(requests: Vec<DlqViewMessageRequest>) -> MessageResult<Vec<DlqViewMessageRequest>> {
    if requests.is_empty() {
        return Err(MessageError::Validation(
            "At least one DLQ message must be selected for batch export.".to_string(),
        ));
    }

    requests
        .into_iter()
        .map(|request| {
            Ok(DlqViewMessageRequest {
                consumer_group: normalize_required_field("consumerGroup", request.consumer_group)?,
                message_id: normalize_required_field("messageId", request.message_id)?,
            })
        })
        .collect()
}

fn sort_message_summaries_desc(items: &mut [MessageSummaryView]) {
    items.sort_by(|left, right| {
        right
            .store_timestamp
            .cmp(&left.store_timestamp)
            .then_with(|| left.msg_id.cmp(&right.msg_id))
    });
}

fn extract_keys(message: &MessageRecord) -> Option<String> {
    message
        .keys
        .as_deref()
        .map(str::trim)
        .filter(|keys| !keys.is_empty())
        .map(|keys| keys.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn resolve_message_summary_id(message: &MessageRecord) -> String {
    message
        .unique_message_id
        .as_deref()
        .and_then(non_empty)
        .unwrap_or_else(|| message.message_id.clone())
}

fn map_message_summary(message: MessageRecord) -> MessageSummaryView {
    let keys = extract_keys(&message);
    MessageSummaryView {
        topic: message.topic.clone(),
        msg_id: resolve_message_summary_id(&message),
        query_msg_id: message.message_id.clone(),
        tags: message.tags,
        keys,
        store_timestamp: message.store_timestamp,
    }
}

fn dlq_page_query_to_message_page_request(
    request: DlqMessagePageQueryRequest,
) -> MessageResult<MessagePageQueryRequest> {
    Ok(MessagePageQueryRequest {
        topic: build_dlq_topic(&request.consumer_group)?,
        begin: request.begin,
        end: request.end,
        page_num: request.page_num,
        page_size: request.page_size,
        task_id: request.task_id,
    })
}

fn build_dlq_topic(consumer_group: &str) -> MessageResult<String> {
    let group = consumer_group
        .trim()
        .strip_prefix(DLQ_GROUP_TOPIC_PREFIX)
        .unwrap_or(consumer_group.trim())
        .trim();
    if group.is_empty() {
        return Err(MessageError::Validation(
            "`consumerGroup` is required for DLQ queries.".to_string(),
        ));
    }

    Ok(format!("{DLQ_GROUP_TOPIC_PREFIX}{group}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedDirectConsumeRequest {
    topic: String,
    consumer_group: String,
    message_id: String,
    client_id: Option<String>,
}

fn normalize_direct_consume_request(
    request: MessageDirectConsumeRequest,
) -> MessageResult<NormalizedDirectConsumeRequest> {
    Ok(NormalizedDirectConsumeRequest {
        topic: normalize_required_field("topic", request.topic)?,
        consumer_group: normalize_required_field("consumerGroup", request.consumer_group)?,
        message_id: normalize_required_field("messageId", request.message_id)?,
        client_id: request.client_id.as_deref().and_then(non_empty),
    })
}

fn build_empty_message_page_response(query: &NormalizedMessagePageQuery) -> MessagePageResponse {
    build_message_page_response(Vec::new(), 0, query, query.task_id.as_deref().unwrap_or_default())
}

fn build_message_page_response(
    items: Vec<MessageSummaryView>,
    total: usize,
    query: &NormalizedMessagePageQuery,
    task_id: &str,
) -> MessagePageResponse {
    let total_pages = if total == 0 {
        0
    } else {
        total.div_ceil(query.page_size) as u32
    };
    let number = query.page_index as u32;
    let number_of_elements = items.len();

    MessagePageResponse {
        page: MessagePageView {
            content: items,
            number,
            size: query.page_size as u32,
            total_elements: total,
            total_pages,
            number_of_elements,
            first: number == 0,
            last: total_pages == 0 || number + 1 >= total_pages,
            empty: number_of_elements == 0,
        },
        task_id: task_id.to_string(),
    }
}

fn build_message_resend_result(
    consumer_group: String,
    topic: String,
    msg_id: String,
    consume_result: DirectConsumeResult,
) -> MessageResendResult {
    let DirectConsumeResult {
        success,
        consume_result: consume_result_text,
        remark,
    } = consume_result;
    let mut message = if success {
        format!("Direct consume succeeded for `{msg_id}` on `{topic}` in consumer group `{consumer_group}`.")
    } else {
        let status = consume_result_text.clone().unwrap_or_else(|| "UNKNOWN".to_string());
        format!("Direct consume returned {status} for `{msg_id}` on `{topic}` in consumer group `{consumer_group}`.")
    };
    if let Some(remark_text) = remark.as_deref().and_then(non_empty) {
        message.push_str(&format!(" Remark: {remark_text}."));
    }

    MessageResendResult {
        success,
        message,
        consumer_group,
        topic,
        msg_id,
        consume_result: consume_result_text,
        remark,
    }
}

fn build_failed_message_resend_result(
    consumer_group: String,
    msg_id: String,
    error: MessageError,
) -> MessageResendResult {
    MessageResendResult {
        success: false,
        message: format!("Direct consume failed for `{msg_id}` in consumer group `{consumer_group}`."),
        consumer_group,
        topic: String::new(),
        msg_id,
        consume_result: None,
        remark: Some(error.to_string()),
    }
}

fn build_dlq_export_view(request_topic: String, message: MessageRecord) -> DlqMessageExportView {
    let msg_id = message.message_id.clone();
    let row = build_dlq_export_row(request_topic.clone(), message, String::new());

    DlqMessageExportView {
        file_name: format!(
            "dlq-{}-{}.csv",
            sanitize_export_file_fragment(request_topic.trim_start_matches(DLQ_GROUP_TOPIC_PREFIX)),
            sanitize_export_file_fragment(&msg_id),
        ),
        mime_type: "text/csv;charset=utf-8".to_string(),
        content: build_dlq_export_csv(vec![row]),
    }
}

fn build_batch_dlq_export_view(
    rows: Vec<[String; 10]>,
    success_count: usize,
    failure_count: usize,
) -> DlqBatchMessageExportView {
    let timestamp = Local::now().format("%Y%m%d%H%M%S");

    DlqBatchMessageExportView {
        file_name: format!("dlqs-{timestamp}.csv"),
        mime_type: "text/csv;charset=utf-8".to_string(),
        content: build_dlq_export_csv(rows),
        total: success_count + failure_count,
        success_count,
        failure_count,
    }
}

fn build_dlq_export_csv(rows: Vec<[String; 10]>) -> String {
    let header = [
        "topic",
        "msgId",
        "bornHost",
        "bornTimestamp",
        "storeTimestamp",
        "reconsumeTimes",
        "properties",
        "messageBody",
        "bodyCRC",
        "exception",
    ];

    let mut csv_rows = Vec::with_capacity(rows.len() + 1);
    csv_rows.push(header.into_iter().map(csv_escape).collect::<Vec<_>>().join(","));
    csv_rows.extend(
        rows.into_iter()
            .map(|row| row.into_iter().map(csv_escape).collect::<Vec<_>>().join(",")),
    );
    format!("{}\n", csv_rows.join("\n"))
}

fn build_dlq_export_row(request_topic: String, message: MessageRecord, exception: String) -> [String; 10] {
    [
        request_topic,
        message.message_id.clone(),
        message.born_host.clone(),
        format_export_timestamp(message.born_timestamp),
        format_export_timestamp(message.store_timestamp),
        message.reconsume_times.to_string(),
        format_java_properties_string(&message),
        decode_export_message_body(&message),
        message.body_crc.to_string(),
        exception,
    ]
}

fn build_failed_dlq_export_row(topic: String, message_id: String, error: MessageError) -> [String; 10] {
    [
        topic,
        message_id,
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        error.to_string(),
    ]
}

fn format_export_timestamp(value: i64) -> String {
    if value <= 0 {
        return String::new();
    }

    Local
        .timestamp_millis_opt(value)
        .single()
        .map(|timestamp| timestamp.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

fn format_java_properties_string(message: &MessageRecord) -> String {
    let mut entries = message
        .properties
        .iter()
        .filter(|(key, _)| is_dashboard_property(key.as_str()))
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    entries.sort();
    format!("{{{}}}", entries.join(", "))
}

fn is_dashboard_property(key: &str) -> bool {
    key != "WAIT"
}

fn decode_export_message_body(message: &MessageRecord) -> String {
    let body = message.body.clone();

    match String::from_utf8(body.clone()) {
        Ok(text) => text,
        Err(_) => format!("base64:{}", BASE64_STANDARD.encode(body)),
    }
}

fn sanitize_export_file_fragment(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| match character {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ => character,
        })
        .collect::<String>()
        .trim()
        .to_string();

    if sanitized.is_empty() {
        "message".to_string()
    } else {
        sanitized
    }
}

fn csv_escape(value: impl AsRef<str>) -> String {
    let value = value.as_ref();
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn build_trace_summary(message_id: &str, mut seeds: Vec<TraceSeed>) -> MessageResult<MessageSummaryView> {
    if seeds.is_empty() {
        return Err(MessageError::Validation(
            "No trace information matched the current message.".to_string(),
        ));
    }

    seeds.sort_by_key(|left| left.timestamp);
    let selected = seeds
        .iter()
        .find(|seed| seed.trace_type == "Pub")
        .or_else(|| seeds.first())
        .ok_or_else(|| MessageError::Validation("No trace information matched the current message.".to_string()))?;

    Ok(MessageSummaryView {
        topic: selected.topic.clone().unwrap_or_default(),
        msg_id: message_id.to_string(),
        query_msg_id: message_id.to_string(),
        tags: selected.tags.clone(),
        keys: selected.keys.clone(),
        store_timestamp: selected.timestamp,
    })
}

fn build_trace_detail(
    message_id: &str,
    trace_topic: &str,
    mut seeds: Vec<TraceSeed>,
) -> MessageResult<MessageTraceDetailView> {
    if seeds.is_empty() {
        return Err(MessageError::Validation(
            "No trace information matched the current message.".to_string(),
        ));
    }

    seeds.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.group_name.cmp(&right.group_name))
    });

    let topic = seeds.iter().find_map(|seed| seed.topic.clone());
    let tags = seeds.iter().find_map(|seed| seed.tags.clone());
    let keys = seeds.iter().find_map(|seed| seed.keys.clone());
    let store_host = seeds.iter().find_map(|seed| non_empty(seed.store_host.as_str()));
    let producer_seed = seeds.iter().find(|seed| trace_role(seed) == "PRODUCER").cloned();

    let mut grouped_consumers: HashMap<String, Vec<MessageTraceNodeView>> = HashMap::new();
    let mut transaction_checks = Vec::new();
    let mut timeline = Vec::new();

    for seed in seeds {
        let role = trace_role(&seed).to_string();
        let node = MessageTraceNodeView {
            trace_type: seed.trace_type.clone(),
            role: role.clone(),
            group_name: seed.group_name.clone(),
            client_host: seed.client_host.clone(),
            store_host: seed.store_host.clone(),
            timestamp: seed.timestamp,
            cost_time: seed.cost_time,
            status: seed.status.clone(),
            retry_times: seed.retry_times,
            from_transaction_check: seed.from_transaction_check,
        };

        match role.as_str() {
            "CONSUMER" => {
                grouped_consumers
                    .entry(seed.group_name.clone())
                    .or_default()
                    .push(node.clone());
            }
            "TRANSACTION" => transaction_checks.push(node.clone()),
            _ => {}
        }

        timeline.push(node);
    }

    let mut consumer_groups = grouped_consumers
        .into_iter()
        .map(|(consumer_group, mut nodes)| {
            nodes.sort_by_key(|left| left.timestamp);
            MessageTraceConsumerGroupView { consumer_group, nodes }
        })
        .collect::<Vec<_>>();
    consumer_groups.sort_by_key(|left| left.consumer_group.clone());

    let min_timestamp = timeline.iter().map(|node| node.timestamp).min();
    let max_timestamp = timeline.iter().map(|node| node.timestamp).max();
    let total_span_ms = min_timestamp.zip(max_timestamp).map(|(start, end)| end - start);

    Ok(MessageTraceDetailView {
        msg_id: message_id.to_string(),
        trace_topic: trace_topic.to_string(),
        topic,
        tags,
        keys,
        store_host,
        producer_group: producer_seed.as_ref().map(|seed| seed.group_name.clone()),
        producer_client_host: producer_seed.as_ref().map(|seed| seed.client_host.clone()),
        producer_store_host: producer_seed.as_ref().map(|seed| seed.store_host.clone()),
        producer_timestamp: producer_seed.as_ref().map(|seed| seed.timestamp),
        producer_cost_time: producer_seed.as_ref().map(|seed| seed.cost_time),
        producer_status: producer_seed.as_ref().map(|seed| seed.status.clone()),
        producer_trace_type: producer_seed.as_ref().map(|seed| seed.trace_type.clone()),
        min_timestamp,
        max_timestamp,
        total_span_ms,
        timeline,
        consumer_groups,
        transaction_checks,
    })
}

fn trace_role(seed: &TraceSeed) -> &'static str {
    if seed.trace_type == "Pub" {
        "PRODUCER"
    } else if seed.trace_type == "EndTransaction" || seed.from_transaction_check {
        "TRANSACTION"
    } else {
        "CONSUMER"
    }
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn map_message_track_list(tracks: Vec<MessageTrackRecord>) -> Vec<crate::message::types::MessageTrackView> {
    tracks
        .into_iter()
        .map(|track| crate::message::types::MessageTrackView {
            consumer_group: track.consumer_group,
            track_type: track.track_type,
            exception_desc: track.exception_desc,
        })
        .collect()
}

fn map_message_detail(detail: MessageDetailRecord) -> MessageDetailView {
    let message = detail.message;
    let properties = message
        .properties
        .iter()
        .filter(|(key, _)| is_dashboard_property(key.as_str()))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    let body = message.body;
    let (body_text, body_base64) = decode_body_fields(&body);

    MessageDetailView {
        topic: message.topic,
        msg_id: message.message_id,
        born_host: Some(message.born_host),
        store_host: Some(message.store_host),
        born_timestamp: Some(message.born_timestamp),
        store_timestamp: Some(message.store_timestamp),
        queue_id: Some(message.queue_id),
        queue_offset: Some(message.queue_offset),
        store_size: Some(message.store_size),
        reconsume_times: Some(message.reconsume_times),
        body_crc: Some(message.body_crc),
        sys_flag: Some(message.sys_flag),
        flag: Some(message.flag),
        prepared_transaction_offset: Some(message.prepared_transaction_offset),
        properties,
        body_text,
        body_base64,
        message_track_list: Some(map_message_track_list(detail.tracks)),
    }
}

fn decode_body_fields(body: &[u8]) -> (Option<String>, Option<String>) {
    if body.is_empty() {
        return (Some(String::new()), None);
    }

    match String::from_utf8(body.to_vec()) {
        Ok(text) => (Some(text), None),
        Err(_) => (None, Some(BASE64_STANDARD.encode(body))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message_record(topic: &str, message_id: &str, body: &[u8]) -> MessageRecord {
        MessageRecord {
            topic: topic.to_string(),
            message_id: message_id.to_string(),
            unique_message_id: None,
            keys: None,
            tags: None,
            born_timestamp: 0,
            store_timestamp: 0,
            born_host: String::new(),
            store_host: String::new(),
            queue_id: 0,
            queue_offset: 0,
            store_size: 0,
            reconsume_times: 0,
            body_crc: 0,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
            body: body.to_vec(),
            properties: BTreeMap::new(),
        }
    }

    #[test]
    fn map_message_summary_extracts_topic_tags_keys_and_store_timestamp() {
        let mut message = message_record("TopicTest", "msg-1", b"payload");
        message.tags = Some("TagA".to_string());
        message.keys = Some("KeyA KeyB".to_string());
        message.store_timestamp = 1_700_000_000_123;

        let summary = map_message_summary(message);

        assert_eq!(summary.topic, "TopicTest");
        assert_eq!(summary.msg_id, "msg-1");
        assert_eq!(summary.query_msg_id, "msg-1");
        assert_eq!(summary.tags.as_deref(), Some("TagA"));
        assert_eq!(summary.keys.as_deref(), Some("KeyA KeyB"));
        assert_eq!(summary.store_timestamp, 1_700_000_000_123);
    }

    #[test]
    fn map_message_summary_prefers_uniq_key_property_over_offset_msg_id() {
        let mut message = message_record("TopicTest", "offset-msg-id", b"payload");
        message.unique_message_id = Some("uniq-msg-id".to_string());

        let summary = map_message_summary(message);

        assert_eq!(summary.msg_id, "uniq-msg-id");
        assert_eq!(summary.query_msg_id, "offset-msg-id");
    }

    #[test]
    fn map_message_summary_falls_back_to_offset_msg_id_when_uniq_key_is_blank() {
        let mut message = message_record("TopicTest", "offset-msg-id", b"payload");
        message.unique_message_id = Some("   ".to_string());

        let summary = map_message_summary(message);

        assert_eq!(summary.msg_id, "offset-msg-id");
        assert_eq!(summary.query_msg_id, "offset-msg-id");
    }

    #[test]
    fn build_dlq_topic_uses_rocketmq_prefix() {
        assert_eq!(
            build_dlq_topic("group-a").expect("plain group should map to dlq topic"),
            format!("{DLQ_GROUP_TOPIC_PREFIX}group-a")
        );
        assert_eq!(
            build_dlq_topic("%DLQ%group-a").expect("prefixed group should normalize"),
            format!("{DLQ_GROUP_TOPIC_PREFIX}group-a")
        );
    }

    #[test]
    fn build_empty_message_page_response_preserves_page_metadata() {
        let response = build_empty_message_page_response(&NormalizedMessagePageQuery {
            topic: "%DLQ%group-a".to_string(),
            begin: 100,
            end: 200,
            page_num: 2,
            page_index: 1,
            page_size: 20,
            task_id: Some("task-1".to_string()),
        });

        assert_eq!(response.task_id, "task-1");
        assert_eq!(response.page.number, 1);
        assert_eq!(response.page.size, 20);
        assert_eq!(response.page.total_elements, 0);
        assert_eq!(response.page.total_pages, 0);
        assert!(!response.page.first);
        assert!(response.page.last);
        assert!(response.page.empty);
    }

    #[test]
    fn map_message_detail_maps_hosts_properties_and_utf8_body() {
        let mut message = message_record("TopicTest", "msg-2", br#"{"ok":true}"#);
        message.tags = Some("TagA".to_string());
        message.keys = Some("KeyA".to_string());
        message.store_timestamp = 1_700_000_000_123;
        message.born_timestamp = 1_700_000_000_000;
        message.queue_id = 3;
        message.queue_offset = 288;
        message.store_size = 264;
        message.body_crc = 613_185_359;
        message.sys_flag = 7;
        message.store_host = "172.20.48.1:10911".to_string();
        message.born_host = "172.20.48.1:61266".to_string();
        message.properties.insert("TAGS".to_string(), "TagA".to_string());
        message.properties.insert("KEYS".to_string(), "KeyA".to_string());
        message
            .properties
            .insert("order_source".to_string(), "mobile".to_string());

        let detail = map_message_detail(MessageDetailRecord {
            message,
            tracks: Vec::new(),
        });

        assert_eq!(detail.topic, "TopicTest");
        assert_eq!(detail.msg_id, "msg-2");
        assert_eq!(detail.store_host.as_deref(), Some("172.20.48.1:10911"));
        assert_eq!(detail.born_host.as_deref(), Some("172.20.48.1:61266"));
        assert_eq!(detail.queue_id, Some(3));
        assert_eq!(detail.queue_offset, Some(288));
        assert_eq!(detail.store_size, Some(264));
        assert_eq!(detail.body_text.as_deref(), Some(r#"{"ok":true}"#));
        assert_eq!(detail.body_base64, None);
        assert_eq!(detail.properties.get("TAGS").map(String::as_str), Some("TagA"));
        assert_eq!(detail.properties.get("KEYS").map(String::as_str), Some("KeyA"));
        assert_eq!(
            detail.properties.get("order_source").map(String::as_str),
            Some("mobile")
        );
        assert!(!detail.properties.contains_key("WAIT"));
        assert_eq!(detail.message_track_list, Some(Vec::new()));
    }

    #[test]
    fn map_message_detail_uses_offset_msg_id_instead_of_uniq_key_property() {
        let mut message = message_record("TopicTest", "offset-msg-id", b"payload");
        message.unique_message_id = Some("uniq-msg-id".to_string());

        let detail = map_message_detail(MessageDetailRecord {
            message,
            tracks: Vec::new(),
        });

        assert_eq!(detail.msg_id, "offset-msg-id");
    }

    #[test]
    fn map_message_detail_falls_back_to_base64_for_binary_body() {
        let detail = map_message_detail(MessageDetailRecord {
            message: message_record("TopicTest", "msg-bin", &[0xff, 0x00, 0x41]),
            tracks: Vec::new(),
        });

        assert_eq!(detail.body_text, None);
        assert_eq!(detail.body_base64.as_deref(), Some("/wBB"));
    }

    #[test]
    fn map_message_track_list_serializes_track_types_and_exceptions() {
        let tracks = map_message_track_list(vec![MessageTrackRecord {
            consumer_group: "group-a".to_string(),
            track_type: "CONSUMED_BUT_FILTERED".to_string(),
            exception_desc: Some("CODE:213 DESC:broadcast".to_string()),
        }]);

        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].consumer_group, "group-a");
        assert_eq!(tracks[0].track_type, "CONSUMED_BUT_FILTERED");
        assert_eq!(tracks[0].exception_desc.as_deref(), Some("CODE:213 DESC:broadcast"));
    }

    #[test]
    fn build_message_resend_result_marks_non_success_as_failure() {
        let result = build_message_resend_result(
            "group-a".to_string(),
            "%RETRY%group-a".to_string(),
            "origin-msg-1".to_string(),
            DirectConsumeResult {
                success: false,
                consume_result: Some("CR_LATER".to_string()),
                remark: Some("retry later".to_string()),
            },
        );

        assert!(!result.success);
        assert_eq!(result.consume_result.as_deref(), Some("CR_LATER"));
        assert_eq!(result.remark.as_deref(), Some("retry later"));
        assert!(result.message.contains("CR_LATER"));
    }

    #[test]
    fn normalize_batch_resend_requests_rejects_empty_input() {
        let error = normalize_batch_resend_requests(Vec::new()).expect_err("empty batch resend should fail");

        assert!(
            error
                .to_string()
                .contains("At least one DLQ message must be selected for batch resend")
        );
    }

    #[test]
    fn normalize_batch_resend_requests_trims_fields() {
        let requests = normalize_batch_resend_requests(vec![DlqResendMessageRequest {
            consumer_group: " group-a ".to_string(),
            message_id: " msg-1 ".to_string(),
        }])
        .expect("batch resend requests should normalize");

        assert_eq!(
            requests,
            vec![DlqResendMessageRequest {
                consumer_group: "group-a".to_string(),
                message_id: "msg-1".to_string(),
            }]
        );
    }

    #[test]
    fn build_failed_message_resend_result_preserves_error_context() {
        let result = build_failed_message_resend_result(
            "group-a".to_string(),
            "msg-1".to_string(),
            MessageError::Validation("missing origin id".to_string()),
        );

        assert!(!result.success);
        assert_eq!(result.consumer_group, "group-a");
        assert_eq!(result.msg_id, "msg-1");
        assert_eq!(result.remark.as_deref(), Some("Validation error: missing origin id"));
    }

    #[test]
    fn normalize_batch_export_requests_rejects_empty_input() {
        let error = normalize_batch_export_requests(Vec::new()).expect_err("empty batch export should fail");

        assert!(
            error
                .to_string()
                .contains("At least one DLQ message must be selected for batch export")
        );
    }

    #[test]
    fn normalize_batch_export_requests_trims_fields() {
        let requests = normalize_batch_export_requests(vec![DlqViewMessageRequest {
            consumer_group: " group-a ".to_string(),
            message_id: " msg-1 ".to_string(),
        }])
        .expect("batch export requests should normalize");

        assert_eq!(
            requests,
            vec![DlqViewMessageRequest {
                consumer_group: "group-a".to_string(),
                message_id: "msg-1".to_string(),
            }]
        );
    }

    #[test]
    fn build_dlq_export_view_maps_java_dashboard_columns() {
        let mut message = message_record("%DLQ%group-a", "msg-1", b"payload");
        message.born_timestamp = 1_700_000_000_000;
        message.store_timestamp = 1_700_000_100_000;
        message.reconsume_times = 2;
        message.body_crc = 613_185_359;
        message.born_host = "172.20.48.1:61266".to_string();
        message.properties.insert("KEYS".to_string(), "order-1".to_string());

        let export = build_dlq_export_view("%DLQ%group-a".to_string(), message);

        assert_eq!(export.file_name, "dlq-group-a-msg-1.csv");
        assert_eq!(export.mime_type, "text/csv;charset=utf-8");
        assert!(export.content.contains("\"topic\",\"msgId\",\"bornHost\""));
        assert!(
            export
                .content
                .contains("\"%DLQ%group-a\",\"msg-1\",\"172.20.48.1:61266\"")
        );
        assert!(export.content.contains("\"{KEYS=order-1}\""));
        assert!(export.content.contains("\"payload\""));
        assert!(export.content.contains("\"613185359\""));
    }

    #[test]
    fn build_dlq_export_view_falls_back_to_base64_for_binary_body() {
        let export = build_dlq_export_view(
            "%DLQ%group-a".to_string(),
            message_record("%DLQ%group-a", "msg-bin", &[0xff, 0x00, 0x41]),
        );

        assert!(export.content.contains("\"base64:/wBB\""));
    }

    #[test]
    fn build_batch_dlq_export_view_includes_failure_rows() {
        let rows = vec![
            build_failed_dlq_export_row(
                "%DLQ%group-a".to_string(),
                "msg-1".to_string(),
                MessageError::Validation("Failed to query message by Id: msg-1".to_string()),
            ),
            build_failed_dlq_export_row(
                "%DLQ%group-a".to_string(),
                "msg-2".to_string(),
                MessageError::Validation("boom".to_string()),
            ),
        ];

        let export = build_batch_dlq_export_view(rows, 0, 2);

        assert_eq!(export.total, 2);
        assert_eq!(export.success_count, 0);
        assert_eq!(export.failure_count, 2);
        assert!(export.file_name.starts_with("dlqs-"));
        assert!(export.content.contains("\"exception\""));
        assert!(
            export
                .content
                .contains("\"Validation error: Failed to query message by Id: msg-1\"")
        );
        assert!(export.content.contains("\"msg-2\""));
    }

    #[test]
    fn normalize_direct_consume_request_trims_and_drops_blank_client_id() {
        let request = rocketmq_dashboard_common::MessageDirectConsumeRequest {
            topic: " TopicTest ".to_string(),
            consumer_group: " group-a ".to_string(),
            message_id: " msg-1 ".to_string(),
            client_id: Some("   ".to_string()),
        };

        let normalized = normalize_direct_consume_request(request).expect("request should normalize");

        assert_eq!(normalized.topic, "TopicTest");
        assert_eq!(normalized.consumer_group, "group-a");
        assert_eq!(normalized.message_id, "msg-1");
        assert_eq!(normalized.client_id, None);
    }

    #[test]
    fn build_trace_summary_uses_pub_context_fields() {
        let summary = build_trace_summary(
            "msg-1",
            vec![
                trace_seed(
                    "Pub",
                    "producer-group",
                    1_700_000_000_000,
                    12,
                    true,
                    "TopicTest",
                    "TagA",
                    "KeyA",
                    "broker-a:10911",
                    "client-a",
                    0,
                    false,
                ),
                trace_seed(
                    "SubAfter",
                    "consumer-group",
                    1_700_000_000_120,
                    30,
                    true,
                    "TopicTest",
                    "TagA",
                    "KeyA",
                    "broker-a:10911",
                    "client-b",
                    1,
                    false,
                ),
            ],
        )
        .expect("trace summary should build");

        assert_eq!(summary.msg_id, "msg-1");
        assert_eq!(summary.query_msg_id, "msg-1");
        assert_eq!(summary.topic, "TopicTest");
        assert_eq!(summary.tags.as_deref(), Some("TagA"));
        assert_eq!(summary.keys.as_deref(), Some("KeyA"));
        assert_eq!(summary.store_timestamp, 1_700_000_000_000);
    }

    #[test]
    fn build_trace_detail_groups_consumer_nodes_and_transaction_checks() {
        let detail = build_trace_detail(
            "msg-1",
            "RMQ_SYS_TRACE_TOPIC",
            vec![
                trace_seed(
                    "Pub",
                    "producer-group",
                    1_700_000_000_000,
                    12,
                    true,
                    "TopicTest",
                    "TagA",
                    "KeyA",
                    "broker-a:10911",
                    "client-a",
                    0,
                    false,
                ),
                trace_seed(
                    "SubAfter",
                    "consumer-group-a",
                    1_700_000_000_120,
                    30,
                    true,
                    "TopicTest",
                    "TagA",
                    "KeyA",
                    "broker-a:10911",
                    "client-b",
                    1,
                    false,
                ),
                trace_seed(
                    "EndTransaction",
                    "producer-group",
                    1_700_000_000_200,
                    18,
                    true,
                    "TopicTest",
                    "TagA",
                    "KeyA",
                    "broker-a:10911",
                    "client-a",
                    0,
                    true,
                ),
            ],
        )
        .expect("trace detail should build");

        assert_eq!(detail.msg_id, "msg-1");
        assert_eq!(detail.trace_topic, "RMQ_SYS_TRACE_TOPIC");
        assert_eq!(detail.topic.as_deref(), Some("TopicTest"));
        assert_eq!(detail.producer_group.as_deref(), Some("producer-group"));
        assert_eq!(detail.consumer_groups.len(), 1);
        assert_eq!(detail.consumer_groups[0].consumer_group, "consumer-group-a");
        assert_eq!(detail.consumer_groups[0].nodes.len(), 1);
        assert_eq!(detail.transaction_checks.len(), 1);
        assert_eq!(detail.timeline.len(), 3);
        assert_eq!(detail.total_span_ms, Some(200));
    }

    fn trace_seed(
        trace_type: &str,
        group_name: &str,
        timestamp: i64,
        cost_time: i32,
        is_success: bool,
        topic: &str,
        tags: &str,
        keys: &str,
        store_host: &str,
        client_host: &str,
        retry_times: i32,
        from_transaction_check: bool,
    ) -> TraceSeed {
        TraceSeed {
            trace_type: trace_type.to_string(),
            group_name: group_name.to_string(),
            client_host: client_host.to_string(),
            store_host: store_host.to_string(),
            timestamp,
            cost_time,
            status: if is_success {
                "success".to_string()
            } else {
                "failed".to_string()
            },
            topic: Some(topic.to_string()),
            tags: Some(tags.to_string()),
            keys: Some(keys.to_string()),
            retry_times,
            from_transaction_check,
        }
    }
}
