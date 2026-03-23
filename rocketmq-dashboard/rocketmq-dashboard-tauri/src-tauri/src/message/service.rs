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
use cheetah_string::CheetahString;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::TraceDataEncoder;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::mix_all;
#[allow(deprecated)]
use rocketmq_common::common::tools::message_track::MessageTrack;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_dashboard_common::DlqMessagePageQueryRequest;
use rocketmq_dashboard_common::DlqResendMessageRequest;
use rocketmq_dashboard_common::DlqViewMessageRequest;
use rocketmq_dashboard_common::MessageIdQueryRequest;
use rocketmq_dashboard_common::MessageKeyQueryRequest;
use rocketmq_dashboard_common::MessagePageQueryRequest;
use rocketmq_dashboard_common::MessageTraceQueryRequest;
use rocketmq_dashboard_common::ViewMessageRequest;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const PULL_BATCH_SIZE: i32 = 32;
const PULL_TIMEOUT_MILLIS: u64 = 3000;

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

        if let Some(task_id) = query.task_id.as_deref() {
            if let Some(entry) = self.page_cache.get(task_id).await {
                if entry.matches_request(&query, generation) {
                    return self.query_message_page_from_cache(query, entry).await;
                }
            }
        }

        self.query_first_message_page(query.first_page(), generation).await
    }

    pub(crate) async fn query_dlq_message_by_consumer_group(
        &self,
        request: DlqMessagePageQueryRequest,
    ) -> MessageResult<MessagePageResponse> {
        let query = normalize_message_page_query(dlq_page_query_to_message_page_request(request)?)?;
        let generation = self.runtime.generation();

        if let Some(task_id) = query.task_id.as_deref() {
            if let Some(entry) = self.page_cache.get(task_id).await {
                if entry.matches_request(&query, generation) {
                    return self.query_message_page_from_cache(query, entry).await;
                }
            }
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
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("message admin session should be initialized before use");
                self.resend_dlq_message_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "resend_dlq_message failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `resend_dlq_message` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
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
        match result {
            Err(MessageError::RocketMQ(message)) => is_reconnect_worthy_error(message),
            _ => false,
        }
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
        admin: &mut DefaultMQAdminExt,
        query: &NormalizedMessagePageQuery,
        generation: u64,
    ) -> MessageResult<MessagePageResponse> {
        let mut queue_states = self.build_topic_queue_states(admin, query).await?;

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
        admin: &mut DefaultMQAdminExt,
        query: &NormalizedMessagePageQuery,
        generation: u64,
    ) -> MessageResult<MessagePageResponse> {
        if !dlq_topic_exists_with_admin(admin, &query.topic).await? {
            return Ok(build_empty_message_page_response(query));
        }

        self.query_first_message_page_with_admin(admin, query, generation).await
    }

    async fn query_message_page_from_cache_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        query: &NormalizedMessagePageQuery,
        entry: &MessagePageCacheEntry,
    ) -> MessageResult<MessagePageResponse> {
        let selection = build_page_selection(&entry.queue_states, query);
        let items = self.load_page_messages(admin, query, &selection.queue_states).await?;

        Ok(build_message_page_response(items, entry.total, query, &entry.task_id))
    }

    async fn build_topic_queue_states(
        &self,
        admin: &mut DefaultMQAdminExt,
        query: &NormalizedMessagePageQuery,
    ) -> MessageResult<Vec<QueueScanState>> {
        let route = admin
            .examine_topic_route_info(CheetahString::from(query.topic.clone()))
            .await
            .map_err(|error| MessageError::RocketMQ(error.to_string()))?
            .unwrap_or_default();

        let mut broker_addr_map = HashMap::new();
        for broker_data in &route.broker_datas {
            if let Some(addr) = broker_data.select_broker_addr() {
                broker_addr_map.insert(broker_data.broker_name().to_string(), addr.to_string());
            }
        }

        let mut queue_states = Vec::new();
        let mut idx = 0usize;
        for queue_data in &route.queue_datas {
            let broker_name = queue_data.broker_name().to_string();
            let Some(broker_addr) = broker_addr_map.get(&broker_name) else {
                continue;
            };

            for queue_id in 0..queue_data.read_queue_nums() {
                let queue_id = i32::try_from(queue_id).expect("queue id should fit within i32");
                let start = admin
                    .search_offset(
                        CheetahString::from(broker_addr.clone()),
                        CheetahString::from(query.topic.clone()),
                        queue_id,
                        query.begin as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|error| MessageError::RocketMQ(error.to_string()))? as i64;
                let end = admin
                    .search_offset(
                        CheetahString::from(broker_addr.clone()),
                        CheetahString::from(query.topic.clone()),
                        queue_id,
                        query.end as u64,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|error| MessageError::RocketMQ(error.to_string()))? as i64;

                queue_states.push(QueueScanState::new(
                    idx,
                    broker_addr.clone(),
                    rocketmq_common::common::message::message_queue::MessageQueue::from_parts(
                        query.topic.as_str(),
                        broker_name.as_str(),
                        queue_id,
                    ),
                    start,
                    end,
                ));
                idx += 1;
            }
        }

        if queue_states.is_empty() {
            return Err(MessageError::Validation(format!(
                "No readable message queue was found for topic `{}`.",
                query.topic
            )));
        }

        Ok(queue_states)
    }

    async fn align_queue_start(
        &self,
        admin: &mut DefaultMQAdminExt,
        query: &NormalizedMessagePageQuery,
        queue_state: &mut QueueScanState,
    ) -> MessageResult<()> {
        let mut start = queue_state.start;
        let mut has_data = false;

        while start < queue_state.end {
            let batch_size = ((queue_state.end - start).min(PULL_BATCH_SIZE as i64)).max(1) as i32;
            let result = admin
                .pull_message_from_queue(
                    queue_state.broker_addr.as_str(),
                    &queue_state.message_queue,
                    "*",
                    start,
                    batch_size,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

            match result.pull_status() {
                PullStatus::Found => {
                    let Some(messages) = result.msg_found_list() else {
                        break;
                    };
                    if messages.is_empty() {
                        break;
                    }

                    has_data = true;
                    let mut advanced = false;
                    for message in messages {
                        let message_ref: &MessageExt = message;
                        if message_ref.store_timestamp() < query.begin {
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
                PullStatus::NoMatchedMsg | PullStatus::NoNewMsg | PullStatus::OffsetIllegal => break,
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
        admin: &mut DefaultMQAdminExt,
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
                .pull_message_from_queue(
                    queue_state.broker_addr.as_str(),
                    &queue_state.message_queue,
                    "*",
                    pull_offset,
                    pull_size as i32,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

            match result.pull_status() {
                PullStatus::Found => {
                    if let Some(messages) = result.msg_found_list() {
                        for message in messages.iter().rev() {
                            let message_ref: &MessageExt = message;
                            if message_ref.store_timestamp() > query.end {
                                end -= 1;
                            } else {
                                has_illegal_offset = false;
                                break;
                            }
                        }
                    } else {
                        has_illegal_offset = false;
                    }
                }
                PullStatus::NoMatchedMsg | PullStatus::NoNewMsg | PullStatus::OffsetIllegal => {
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
        admin: &mut DefaultMQAdminExt,
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
                    .pull_message_from_queue(
                        queue_state.broker_addr.as_str(),
                        &queue_state.message_queue,
                        "*",
                        pull_offset,
                        batch_size,
                        PULL_TIMEOUT_MILLIS,
                    )
                    .await
                    .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

                match result.pull_status() {
                    PullStatus::Found => {
                        let Some(messages) = result.msg_found_list() else {
                            break;
                        };
                        if messages.is_empty() {
                            break;
                        }

                        for message in messages {
                            if remaining == 0 {
                                break;
                            }
                            let message_ref: &MessageExt = message;
                            if message_ref.store_timestamp() < query.begin || message_ref.store_timestamp() > query.end
                            {
                                continue;
                            }

                            items.push(map_message_summary(message_ref.clone()));
                            remaining -= 1;
                        }

                        let next_begin_offset = result.next_begin_offset() as i64;
                        if next_begin_offset <= pull_offset {
                            break;
                        }
                        pull_offset = next_begin_offset;
                    }
                    PullStatus::NoMatchedMsg | PullStatus::NoNewMsg | PullStatus::OffsetIllegal => break,
                }
            }
        }

        sort_message_summaries_desc(&mut items);
        Ok(items)
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

        let mut items: Vec<MessageSummaryView> =
            result.message_list().iter().cloned().map(map_message_summary).collect();
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
        admin: &mut DefaultMQAdminExt,
        request: ViewMessageRequest,
    ) -> MessageResult<MessageDetailView> {
        let message = self
            .find_message_by_id_with_admin(admin, request.topic, request.message_id)
            .await?;
        let message_tracks = admin
            .message_track_detail(message.clone())
            .await
            .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

        Ok(map_message_detail(message, message_tracks))
    }

    async fn resend_dlq_message_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: DlqResendMessageRequest,
    ) -> MessageResult<MessageResendResult> {
        let consumer_group = normalize_required_field("consumerGroup", request.consumer_group)?;
        let dlq_topic = build_dlq_topic(&consumer_group)?;
        let message_id = normalize_required_field("messageId", request.message_id)?;
        let dlq_message = self.find_message_by_id_with_admin(admin, dlq_topic, message_id).await?;
        let resend_target = build_dlq_resend_target(&dlq_message)?;
        let consume_result = admin
            .consume_message_directly(
                CheetahString::from(consumer_group.clone()),
                CheetahString::default(),
                CheetahString::from(resend_target.topic.clone()),
                CheetahString::from(resend_target.msg_id.clone()),
            )
            .await
            .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

        Ok(build_message_resend_result(
            consumer_group,
            resend_target.topic,
            resend_target.msg_id,
            consume_result,
        ))
    }

    async fn find_message_by_id_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        topic: String,
        message_id: String,
    ) -> MessageResult<MessageExt> {
        let topic = normalize_required_field("topic", topic)?;
        let message_id = normalize_required_field("messageId", message_id)?;

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
        first_message_or_validation_error(messages)
    }

    async fn query_message_trace_by_id_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
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
        admin: &mut DefaultMQAdminExt,
        request: MessageTraceQueryRequest,
    ) -> MessageResult<MessageTraceDetailView> {
        let (trace_topic, message_id, seeds) = self.query_trace_seeds_with_admin(admin, request).await?;
        build_trace_detail(&message_id, &trace_topic, seeds)
    }

    async fn query_trace_seeds_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: MessageTraceQueryRequest,
    ) -> MessageResult<(String, String, Vec<TraceSeed>)> {
        let trace_topic = normalize_trace_topic(request.trace_topic);
        let message_id = normalize_required_field("messageId", request.message_id)?;

        let result = admin
            .query_message_by_key(
                None,
                CheetahString::from(trace_topic.clone()),
                CheetahString::from(message_id.clone()),
                64,
                0,
                i64::MAX,
                CheetahString::from_static_str(""),
                None,
            )
            .await
            .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

        let mut seeds = Vec::new();

        for trace_message in result.message_list() {
            let Some(body) = trace_message.body() else {
                continue;
            };
            let body_text = String::from_utf8_lossy(body.as_ref());
            if body_text.trim().is_empty() {
                continue;
            }

            for context in TraceDataEncoder::decoder_from_trace_data_string(&body_text) {
                let trace_type = context
                    .trace_type
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "Unknown".to_string());
                let group_name = context.group_name.to_string();
                let cost_time = context.cost_time;
                let status = if context.is_success {
                    "success".to_string()
                } else {
                    "failed".to_string()
                };

                if let Some(trace_beans) = context.trace_beans {
                    for bean in trace_beans {
                        if bean.msg_id.as_str() != message_id {
                            continue;
                        }

                        seeds.push(TraceSeed {
                            trace_type: trace_type.clone(),
                            group_name: group_name.clone(),
                            client_host: if bean.client_host.is_empty() {
                                trace_message.born_host().to_string()
                            } else {
                                bean.client_host.to_string()
                            },
                            store_host: if bean.store_host.is_empty() {
                                trace_message.store_host().to_string()
                            } else {
                                bean.store_host.to_string()
                            },
                            timestamp: if bean.store_time > 0 {
                                bean.store_time
                            } else {
                                context.time_stamp as i64
                            },
                            cost_time,
                            status: status.clone(),
                            topic: non_empty(bean.topic.as_str()),
                            tags: non_empty(bean.tags.as_str()),
                            keys: non_empty(bean.keys.as_str()),
                            retry_times: bean.retry_times,
                            from_transaction_check: bean.from_transaction_check,
                        });
                    }
                }
            }
        }

        if seeds.is_empty() {
            return Err(MessageError::Validation(
                "No trace information matched the current message.".to_string(),
            ));
        }

        Ok((trace_topic, message_id, seeds))
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

fn normalize_trace_topic(value: String) -> String {
    let normalized = value.trim();
    if normalized.is_empty() {
        TopicValidator::RMQ_SYS_TRACE_TOPIC.to_string()
    } else {
        normalized.to_string()
    }
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
        .strip_prefix(mix_all::DLQ_GROUP_TOPIC_PREFIX)
        .unwrap_or(consumer_group.trim())
        .trim();
    if group.is_empty() {
        return Err(MessageError::Validation(
            "`consumerGroup` is required for DLQ queries.".to_string(),
        ));
    }

    Ok(format!("{}{}", mix_all::DLQ_GROUP_TOPIC_PREFIX, group))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DlqResendTarget {
    topic: String,
    msg_id: String,
}

fn build_dlq_resend_target(message: &MessageExt) -> MessageResult<DlqResendTarget> {
    let retry_topic = message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC))
        .map(|value| value.to_string())
        .and_then(|value| non_empty(&value))
        .ok_or_else(|| {
            MessageError::Validation("DLQ message is missing `RETRY_TOPIC`, so it cannot be resent safely.".to_string())
        })?;
    let origin_message_id = message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_ORIGIN_MESSAGE_ID,
        ))
        .map(|value| value.to_string())
        .or_else(|| {
            message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_DLQ_ORIGIN_MESSAGE_ID,
                ))
                .map(|value| value.to_string())
        })
        .and_then(|value| non_empty(&value))
        .ok_or_else(|| {
            MessageError::Validation(
                "DLQ message is missing `ORIGIN_MESSAGE_ID`, so it cannot be resent safely.".to_string(),
            )
        })?;

    Ok(DlqResendTarget {
        topic: retry_topic,
        msg_id: origin_message_id,
    })
}

async fn dlq_topic_exists_with_admin(admin: &mut DefaultMQAdminExt, topic: &str) -> MessageResult<bool> {
    let topic_list = admin
        .fetch_all_topic_list()
        .await
        .map_err(|error| MessageError::RocketMQ(error.to_string()))?;

    Ok(topic_list.topic_list.iter().any(|item| item.as_str() == topic))
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
    consume_result: rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult,
) -> MessageResendResult {
    let consume_result_text = consume_result.consume_result().map(|value| value.to_string());
    let remark = consume_result.remark().map(|value| value.to_string());
    let success = matches!(
        consume_result.consume_result(),
        Some(result) if *result == CMResult::CRSuccess
    );
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceSeed {
    trace_type: String,
    group_name: String,
    client_host: String,
    store_host: String,
    timestamp: i64,
    cost_time: i32,
    status: String,
    topic: Option<String>,
    tags: Option<String>,
    keys: Option<String>,
    retry_times: i32,
    from_transaction_check: bool,
}

fn build_trace_summary(message_id: &str, mut seeds: Vec<TraceSeed>) -> MessageResult<MessageSummaryView> {
    if seeds.is_empty() {
        return Err(MessageError::Validation(
            "No trace information matched the current message.".to_string(),
        ));
    }

    seeds.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
    let selected = seeds
        .iter()
        .find(|seed| seed.trace_type == "Pub")
        .or_else(|| seeds.first())
        .expect("trace seeds should not be empty");

    Ok(MessageSummaryView {
        topic: selected.topic.clone().unwrap_or_default(),
        msg_id: message_id.to_string(),
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
            nodes.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
            MessageTraceConsumerGroupView { consumer_group, nodes }
        })
        .collect::<Vec<_>>();
    consumer_groups.sort_by(|left, right| left.consumer_group.cmp(&right.consumer_group));

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

#[allow(deprecated)]
fn map_message_track_list(tracks: Vec<MessageTrack>) -> Vec<crate::message::types::MessageTrackView> {
    tracks
        .into_iter()
        .map(|track| crate::message::types::MessageTrackView {
            consumer_group: track.consumer_group,
            track_type: track
                .track_type
                .map(|value| value.to_string())
                .unwrap_or_else(|| "UNKNOWN".to_string()),
            exception_desc: non_empty(&track.exception_desc),
        })
        .collect()
}

#[allow(deprecated)]
fn map_message_detail(message: MessageExt, message_tracks: Vec<MessageTrack>) -> MessageDetailView {
    let properties = message
        .properties()
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    let body = message.body().map(|body| body.to_vec()).unwrap_or_default();
    let (body_text, body_base64) = decode_body_fields(&body);

    MessageDetailView {
        topic: message.topic().to_string(),
        msg_id: message.msg_id().to_string(),
        born_host: Some(message.born_host().to_string()),
        store_host: Some(message.store_host().to_string()),
        born_timestamp: Some(message.born_timestamp()),
        store_timestamp: Some(message.store_timestamp()),
        queue_id: Some(message.queue_id()),
        queue_offset: Some(message.queue_offset()),
        store_size: Some(message.store_size()),
        reconsume_times: Some(message.reconsume_times()),
        body_crc: Some(message.body_crc()),
        sys_flag: Some(message.sys_flag()),
        flag: Some(message.flag()),
        prepared_transaction_offset: Some(message.prepared_transaction_offset()),
        properties,
        body_text,
        body_base64,
        message_track_list: Some(map_message_track_list(message_tracks)),
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
    use rocketmq_common::common::message::MessageTrait;
    use rocketmq_common::common::message::message_builder::MessageBuilder;
    #[allow(deprecated)]
    use rocketmq_common::common::tools::message_track::MessageTrack;
    #[allow(deprecated)]
    use rocketmq_common::common::tools::track_type::TrackType;

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

    #[test]
    fn build_dlq_topic_uses_rocketmq_prefix() {
        assert_eq!(
            build_dlq_topic("group-a").expect("plain group should map to dlq topic"),
            format!("{}group-a", mix_all::DLQ_GROUP_TOPIC_PREFIX)
        );
        assert_eq!(
            build_dlq_topic("%DLQ%group-a").expect("prefixed group should normalize"),
            format!("{}group-a", mix_all::DLQ_GROUP_TOPIC_PREFIX)
        );
    }

    #[test]
    fn build_dlq_resend_target_uses_retry_topic_and_origin_message_id() {
        let message = MessageBuilder::new()
            .topic("%DLQ%group-a")
            .body_slice(b"payload")
            .build_unchecked();
        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
            CheetahString::from("%RETRY%group-a"),
        );
        message_ext.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID),
            CheetahString::from("origin-msg-1"),
        );

        let target = build_dlq_resend_target(&message_ext).expect("dlq resend target should map");

        assert_eq!(
            target,
            DlqResendTarget {
                topic: "%RETRY%group-a".to_string(),
                msg_id: "origin-msg-1".to_string(),
            }
        );
    }

    #[test]
    fn build_dlq_resend_target_rejects_missing_origin_message_id() {
        let message = MessageBuilder::new()
            .topic("%DLQ%group-a")
            .body_slice(b"payload")
            .build_unchecked();
        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
            CheetahString::from("%RETRY%group-a"),
        );

        let error = build_dlq_resend_target(&message_ext).expect_err("missing origin id should fail");

        assert!(error.to_string().contains("ORIGIN_MESSAGE_ID"));
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
        let message = MessageBuilder::new()
            .topic("TopicTest")
            .body_slice(br#"{"ok":true}"#)
            .tags("TagA")
            .keys(vec!["KeyA".to_string()])
            .trace_switch(true)
            .raw_property("order_source", "mobile")
            .expect("custom property should be accepted")
            .build_unchecked();

        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.set_msg_id(CheetahString::from("msg-2"));
        message_ext.set_store_timestamp(1_700_000_000_123);
        message_ext.set_born_timestamp(1_700_000_000_000);
        message_ext.set_queue_id(3);
        message_ext.set_queue_offset(288);
        message_ext.set_store_size(264);
        message_ext.set_reconsume_times(0);
        message_ext.set_body_crc(613_185_359);
        message_ext.set_sys_flag(7);
        message_ext.set_store_host("172.20.48.1:10911".parse().expect("store host"));
        message_ext.set_born_host("172.20.48.1:61266".parse().expect("born host"));

        let detail = map_message_detail(message_ext, Vec::new());

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
        assert_eq!(detail.message_track_list, Some(Vec::new()));
    }

    #[test]
    fn map_message_detail_falls_back_to_base64_for_binary_body() {
        let message = MessageBuilder::new()
            .topic("TopicTest")
            .body(vec![0xff, 0x00, 0x41])
            .build_unchecked();

        let mut message_ext = MessageExt::default();
        message_ext.set_message_inner(message);
        message_ext.set_msg_id(CheetahString::from("msg-bin"));

        let detail = map_message_detail(message_ext, Vec::new());

        assert_eq!(detail.body_text, None);
        assert_eq!(detail.body_base64.as_deref(), Some("/wBB"));
    }

    #[test]
    #[allow(deprecated)]
    fn map_message_track_list_serializes_track_types_and_exceptions() {
        let tracks = map_message_track_list(vec![MessageTrack {
            consumer_group: "group-a".to_string(),
            track_type: Some(TrackType::ConsumedButFiltered),
            exception_desc: "CODE:213 DESC:broadcast".to_string(),
        }]);

        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].consumer_group, "group-a");
        assert_eq!(tracks[0].track_type, "CONSUMED_BUT_FILTERED");
        assert_eq!(tracks[0].exception_desc.as_deref(), Some("CODE:213 DESC:broadcast"));
    }

    #[test]
    fn build_message_resend_result_marks_non_success_as_failure() {
        let mut consume_result =
            rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult::default();
        consume_result.set_consume_result(CMResult::CRLater);
        consume_result.set_remark(CheetahString::from("retry later"));

        let result = build_message_resend_result(
            "group-a".to_string(),
            "%RETRY%group-a".to_string(),
            "origin-msg-1".to_string(),
            consume_result,
        );

        assert!(!result.success);
        assert_eq!(result.consume_result.as_deref(), Some("CR_LATER"));
        assert_eq!(result.remark.as_deref(), Some("retry later"));
        assert!(result.message.contains("CR_LATER"));
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
