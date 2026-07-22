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

use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_error::RocketMQError;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetrics;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_tieredstore::dispatcher::DefaultTieredDispatcher;
use rocketmq_tieredstore::dispatcher::TieredDispatchRequest;
use rocketmq_tieredstore::dispatcher::TieredDispatcher;
use rocketmq_tieredstore::fetcher::TieredGetMessageResult;
use rocketmq_tieredstore::fetcher::TieredGetMessageStatus;
use rocketmq_tieredstore::fetcher::TieredQueryResult;
use rocketmq_tieredstore::provider::ProviderKind;
use rocketmq_tieredstore::provider::TieredStoreProvider;
use rocketmq_tieredstore::TieredLifecycle;
use rocketmq_tieredstore::TieredMessageFetcher;
use rocketmq_tieredstore::TieredStore;
use rocketmq_tieredstore::TieredStoreConfig;
use tracing::debug;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::query_message_result::QueryMessageResult;
use crate::base::select_result::SelectMappedBufferCacheState;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::select_result::SelectMappedBufferSourceKind;
use crate::filter::ArcMessageFilter;
use crate::log_file::commit_log::CommitLog;
use crate::log_file::commit_log::CommitLogReadHandle;
use crate::store_error::StoreError;

pub type DispatchBodyResolver = dyn Fn(&DispatchRequest) -> Option<Bytes> + Send + Sync;

/// Store-facade decorator that composes Local fallback semantics with the Tiered owner.
pub struct TieredStoreDecorator {
    store: Arc<TieredStore>,
}

impl TieredStoreDecorator {
    pub fn new(config: TieredStoreConfig) -> Result<Self, StoreError> {
        let store = TieredStore::new(config).map_err(|error| {
            StoreError::TieredStore(format!(
                "failed to create tieredstore for local file message store: {error}"
            ))
        })?;
        Ok(Self { store: Arc::new(store) })
    }

    pub fn commit_log_dispatcher(&self, body_resolver: Arc<DispatchBodyResolver>) -> Arc<dyn CommitLogDispatcher> {
        let dispatcher = self.store.dispatcher();
        let retry_body_resolver = body_resolver.clone();
        dispatcher.set_retry_payload_resolver(Arc::new(move |physical_offset, length| {
            let commit_log_offset = i64::try_from(physical_offset).ok()?;
            let msg_size = i32::try_from(length).ok()?;
            retry_body_resolver(&DispatchRequest {
                commit_log_offset,
                msg_size,
                ..DispatchRequest::default()
            })
        }));
        Arc::new(TieredCommitLogDispatcher::new_with_source_epoch(
            dispatcher,
            body_resolver,
            self.store.config().source_epoch,
        ))
    }

    pub async fn load(&self) -> Result<(), StoreError> {
        self.store
            .load()
            .await
            .map_err(|error| StoreError::TieredStore(format!("failed to load tieredstore: {error}")))
    }

    pub async fn start(&self) -> Result<(), StoreError> {
        self.store
            .start()
            .await
            .map_err(|error| StoreError::TieredStore(format!("failed to start tieredstore: {error}")))
    }

    pub async fn shutdown(&self) -> Result<(), StoreError> {
        self.store
            .shutdown()
            .await
            .map_err(|error| StoreError::TieredStore(format!("failed to shutdown tieredstore: {error}")))
    }

    pub fn metrics(&self) -> Arc<TieredStoreMetrics> {
        self.store.metrics()
    }

    pub fn minimum_pinned_wal_segment(&self) -> Option<u64> {
        self.store.dispatcher().health().minimum_pinned_wal_segment()
    }

    pub fn is_dispatch_ready(&self) -> bool {
        self.store.dispatcher().health().is_ready()
    }

    pub const fn should_try_get_message(status: GetMessageStatus) -> bool {
        matches!(
            status,
            GetMessageStatus::NoMatchedLogicQueue
                | GetMessageStatus::NoMessageInQueue
                | GetMessageStatus::OffsetTooSmall
                | GetMessageStatus::OffsetFoundNull
                | GetMessageStatus::MessageWasRemoving
        )
    }

    pub async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        let metrics = self.store.metrics();
        metrics.record_get_message_fallback(topic.as_str(), group.as_str());
        match self
            .store
            .fetcher()
            .get_message(topic.to_string(), queue_id, offset, max_msg_nums)
            .await
        {
            Ok(fetched) => {
                let result = Self::to_store_get_message_result(fetched, offset, max_total_msg_size, message_filter);
                if result.status() == Some(GetMessageStatus::Found) {
                    metrics.record_messages_out(topic.as_str(), group.as_str(), result.message_count().max(0) as u64);
                }
                Some(result)
            }
            Err(error) => {
                warn!(
                    group = %group,
                    topic = %topic,
                    queue_id,
                    offset,
                    error = %error,
                    "tieredstore get_message fallback failed"
                );
                None
            }
        }
    }

    pub async fn query_message(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Option<QueryMessageResult> {
        match self
            .store
            .fetcher()
            .query_message(
                topic.to_string(),
                key.to_string(),
                max_num,
                begin_timestamp,
                end_timestamp,
            )
            .await
        {
            Ok(fetched) if !fetched.values.is_empty() => {
                Some(Self::to_store_query_message_result(fetched, end_timestamp))
            }
            Ok(_) => None,
            Err(error) => {
                warn!(topic = %topic, key = %key, error = %error, "tieredstore query_message fallback failed");
                None
            }
        }
    }

    pub async fn offset_by_time(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> Result<Option<i64>, StoreError> {
        self.store
            .fetcher()
            .get_offset_by_time_with_boundary(topic.to_string(), queue_id, timestamp, boundary_type)
            .await
            .map(|offset| (offset >= 0).then_some(offset))
            .map_err(|error| StoreError::TieredStore(format!("tieredstore offset by time lookup failed: {error}")))
    }

    pub async fn message_timestamp(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        consume_queue_offset: i64,
    ) -> Result<i64, StoreError> {
        self.store
            .fetcher()
            .get_message_timestamp(topic.to_string(), queue_id, consume_queue_offset)
            .await
            .map_err(|error| StoreError::TieredStore(format!("tieredstore timestamp lookup failed: {error}")))
    }

    #[cfg(test)]
    pub(crate) const fn inner(&self) -> &Arc<TieredStore> {
        &self.store
    }

    fn to_store_get_message_result(
        fetched: TieredGetMessageResult,
        requested_offset: i64,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> GetMessageResult {
        let mut result = GetMessageResult::new_result_size(fetched.messages.len());
        result.set_min_offset(fetched.min_offset);
        result.set_max_offset(fetched.max_offset);
        result.set_next_begin_offset(fetched.next_begin_offset);

        let status = map_tiered_get_status(fetched.status);
        if status != GetMessageStatus::Found {
            result.set_status(Some(status));
            return result;
        }

        let max_total_msg_size = max_total_msg_size.max(0);
        let mut next_queue_offset = requested_offset;
        for message in fetched.messages {
            if let Some(filter) = message_filter.as_ref() {
                if !filter.is_matched_by_commit_log(Some(message.as_ref()), None) {
                    next_queue_offset = next_queue_offset.saturating_add(1);
                    continue;
                }
            }
            if result.buffer_total_size() > 0
                && result.buffer_total_size().saturating_add(message.len() as i32) > max_total_msg_size
            {
                break;
            }
            let queue_offset = next_queue_offset.max(0) as u64;
            result.add_message(select_result_from_tiered_message(message), queue_offset, 1);
            next_queue_offset = next_queue_offset.saturating_add(1);
        }

        result.set_status(Some(if result.message_count() > 0 {
            GetMessageStatus::Found
        } else {
            GetMessageStatus::NoMatchedMessage
        }));
        result
    }

    fn to_store_query_message_result(fetched: TieredQueryResult<Bytes>, fallback_timestamp: i64) -> QueryMessageResult {
        let mut result = QueryMessageResult {
            index_last_update_timestamp: fallback_timestamp,
            ..QueryMessageResult::default()
        };
        for message in fetched.values {
            result.add_message(select_result_from_tiered_message(message));
        }
        result
    }
}

pub fn resolve_tiered_dispatch_body(commit_log: &CommitLog, request: &DispatchRequest) -> Option<Bytes> {
    resolve_tiered_dispatch_body_with(request, |offset, size| commit_log.get_message(offset, size))
}

pub(crate) fn resolve_tiered_dispatch_body_with_reader(
    commit_log: &CommitLogReadHandle,
    request: &DispatchRequest,
) -> Option<Bytes> {
    resolve_tiered_dispatch_body_with(request, |offset, size| commit_log.get_message(offset, size))
}

fn resolve_tiered_dispatch_body_with(
    request: &DispatchRequest,
    read_message: impl FnOnce(i64, i32) -> Option<SelectMappedBufferResult>,
) -> Option<Bytes> {
    if request.commit_log_offset < 0 || request.msg_size <= 0 {
        return None;
    }

    let size = usize::try_from(request.msg_size).ok()?;
    let result = read_message(request.commit_log_offset, request.msg_size)?;
    let bytes = result.get_bytes()?;
    if bytes.len() < size {
        return None;
    }
    Some(bytes.slice(..size))
}

fn select_result_from_tiered_message(message: Bytes) -> SelectMappedBufferResult {
    SelectMappedBufferResult {
        start_offset: 0,
        size: message.len() as i32,
        bytes: Some(message),
        mapped_file: None,
        is_in_cache: false,
        source_kind: SelectMappedBufferSourceKind::Bytes,
        file_offset: 0,
        cache_state: SelectMappedBufferCacheState::Unknown,
    }
}

fn map_tiered_get_status(status: TieredGetMessageStatus) -> GetMessageStatus {
    match status {
        TieredGetMessageStatus::Found => GetMessageStatus::Found,
        TieredGetMessageStatus::NoMatchedMessage => GetMessageStatus::NoMatchedMessage,
        TieredGetMessageStatus::OffsetFoundNull => GetMessageStatus::OffsetFoundNull,
        TieredGetMessageStatus::OffsetOverflowBadly => GetMessageStatus::OffsetOverflowBadly,
        TieredGetMessageStatus::OffsetOverflowOne => GetMessageStatus::OffsetOverflowOne,
        TieredGetMessageStatus::OffsetTooSmall => GetMessageStatus::OffsetTooSmall,
        TieredGetMessageStatus::NoMatchedLogicQueue => GetMessageStatus::NoMatchedLogicQueue,
    }
}

pub struct TieredCommitLogDispatcher<P = ProviderKind>
where
    P: TieredStoreProvider,
{
    dispatcher: Arc<DefaultTieredDispatcher<P>>,
    body_resolver: Arc<DispatchBodyResolver>,
    source_epoch: u64,
}

impl<P> TieredCommitLogDispatcher<P>
where
    P: TieredStoreProvider,
{
    pub fn new(dispatcher: Arc<DefaultTieredDispatcher<P>>, body_resolver: Arc<DispatchBodyResolver>) -> Self {
        Self::new_with_source_epoch(dispatcher, body_resolver, 0)
    }

    pub fn new_with_source_epoch(
        dispatcher: Arc<DefaultTieredDispatcher<P>>,
        body_resolver: Arc<DispatchBodyResolver>,
        source_epoch: u64,
    ) -> Self {
        Self {
            dispatcher,
            body_resolver,
            source_epoch,
        }
    }

    fn derived_record(&self, request: &DispatchRequest) -> Result<DerivedRecordId, RocketMQError> {
        let physical_offset = u64::try_from(request.commit_log_offset)
            .map_err(|_| RocketMQError::illegal_argument("tiered CommitLog offset must not be negative"))?;
        let length = u32::try_from(request.msg_size)
            .map_err(|_| RocketMQError::illegal_argument("tiered CommitLog length must be positive"))?;
        DerivedRecordId::try_new(self.source_epoch, physical_offset, length)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
    }
}

impl<P> CommitLogDispatcher for TieredCommitLogDispatcher<P>
where
    P: TieredStoreProvider,
{
    fn dispatch(&self, dispatch_request: &mut DispatchRequest) {
        if !dispatch_request.success {
            return;
        }

        let Some(body) = (self.body_resolver)(dispatch_request) else {
            debug!(
                topic = %dispatch_request.topic,
                queue_id = dispatch_request.queue_id,
                queue_offset = dispatch_request.consume_queue_offset,
                "skip tieredstore dispatch because commitlog body is unavailable"
            );
            return;
        };

        let request = to_tiered_dispatch_request(dispatch_request, body);
        let result = self
            .derived_record(dispatch_request)
            .and_then(|record| self.dispatcher.try_dispatch_derived(record, request));
        if let Err(error) = result {
            warn!(
                topic = %dispatch_request.topic,
                queue_id = dispatch_request.queue_id,
                queue_offset = dispatch_request.consume_queue_offset,
                error = %error,
                "failed to enqueue tieredstore dispatch request"
            );
        }
    }

    fn dispatch_async<'a>(
        &'a self,
        dispatch_request: &'a mut DispatchRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if !dispatch_request.success {
                return;
            }
            let record = match self.derived_record(dispatch_request) {
                Ok(record) => record,
                Err(error) => {
                    warn!(error = %error, "reject invalid tieredstore CommitLog identity");
                    return;
                }
            };
            let body = loop {
                if self.dispatcher.is_shutdown() {
                    return;
                }
                if let Some(body) = (self.body_resolver)(dispatch_request) {
                    break body;
                }
                debug!(
                    topic = %dispatch_request.topic,
                    queue_id = dispatch_request.queue_id,
                    queue_offset = dispatch_request.consume_queue_offset,
                    "pause tieredstore derived reader because CommitLog body is unavailable"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            };
            let request = to_tiered_dispatch_request(dispatch_request, body);
            loop {
                if self.dispatcher.is_shutdown() {
                    return;
                }
                match self.dispatcher.dispatch_derived(record, request.clone()).await {
                    Ok(()) => break,
                    Err(error) => {
                        warn!(
                            topic = %dispatch_request.topic,
                            queue_id = dispatch_request.queue_id,
                            queue_offset = dispatch_request.consume_queue_offset,
                            error = %error,
                            "pause tieredstore derived reader after dispatch failure"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        })
    }

    fn dispatch_batch_async<'a>(
        &'a self,
        dispatch_requests: &'a mut [DispatchRequest],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            for request in dispatch_requests.iter_mut() {
                self.dispatch_async(request).await;
            }
        })
    }

    fn dispatch_progress_offset(&self, _commit_log_min_offset: i64) -> Option<i64> {
        self.dispatcher
            .health()
            .cursor()
            .and_then(|cursor| i64::try_from(cursor.next_offset()).ok())
    }
}

pub fn to_tiered_dispatch_request(dispatch_request: &DispatchRequest, body: Bytes) -> TieredDispatchRequest {
    let keys = dispatch_request.keys.to_string();
    TieredDispatchRequest {
        topic: dispatch_request.topic.to_string(),
        queue_id: dispatch_request.queue_id,
        queue_offset: dispatch_request.consume_queue_offset,
        commit_log_offset: dispatch_request.commit_log_offset,
        message_size: dispatch_request.msg_size,
        tags_code: dispatch_request.tags_code,
        store_timestamp: dispatch_request.store_timestamp,
        keys: (!keys.is_empty()).then_some(keys),
        uniq_key: dispatch_request.uniq_key.as_ref().map(ToString::to_string),
        offset_id: dispatch_request.offset_id.as_ref().map(ToString::to_string),
        sys_flag: dispatch_request.sys_flag,
        body: Some(body),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;
    use rocketmq_tieredstore::fetcher::TieredGetMessageStatus;
    use rocketmq_tieredstore::TieredLifecycle;
    use rocketmq_tieredstore::TieredMessageFetcher;
    use rocketmq_tieredstore::TieredStorageLevel;
    use rocketmq_tieredstore::TieredStore;
    use rocketmq_tieredstore::TieredStoreConfig;

    use super::*;

    #[test]
    fn converts_store_dispatch_request_to_tiered_request() {
        let dispatch_request = DispatchRequest {
            topic: CheetahString::from("TopicA"),
            queue_id: 1,
            commit_log_offset: 1024,
            msg_size: 4,
            tags_code: 7,
            store_timestamp: 100,
            consume_queue_offset: 9,
            keys: CheetahString::from("keyA"),
            success: true,
            uniq_key: Some(CheetahString::from("uniqA")),
            offset_id: Some(CheetahString::from("offsetA")),
            ..DispatchRequest::default()
        };

        let tiered_request = to_tiered_dispatch_request(&dispatch_request, Bytes::from_static(b"test"));

        assert_eq!(tiered_request.topic, "TopicA");
        assert_eq!(tiered_request.queue_id, 1);
        assert_eq!(tiered_request.queue_offset, 9);
        assert_eq!(tiered_request.commit_log_offset, 1024);
        assert_eq!(tiered_request.message_size, 4);
        assert_eq!(tiered_request.tags_code, 7);
        assert_eq!(tiered_request.store_timestamp, 100);
        assert_eq!(tiered_request.keys.as_deref(), Some("keyA"));
        assert_eq!(tiered_request.uniq_key.as_deref(), Some("uniqA"));
        assert_eq!(tiered_request.offset_id.as_deref(), Some("offsetA"));
        assert_eq!(tiered_request.body, Some(Bytes::from_static(b"test")));
    }

    #[tokio::test]
    async fn dispatcher_adapter_writes_commitlog_and_consumequeue_to_tieredstore() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let tiered_store = TieredStore::new(TieredStoreConfig {
            storage_level: TieredStorageLevel::Force,
            backend_provider: "memory".to_owned(),
            store_path_root_dir: temp_dir.path().join("tieredstore"),
            max_pending_tasks: 8,
            ..TieredStoreConfig::default()
        })?;
        tiered_store.load().await?;
        tiered_store.start().await?;

        let body = Bytes::from_static(b"adapter-dispatched-body");
        let resolver_body = body.clone();
        let adapter = TieredCommitLogDispatcher::new(
            tiered_store.dispatcher(),
            Arc::new(move |_| Some(resolver_body.clone())),
        );
        let mut dispatch_request = DispatchRequest {
            topic: CheetahString::from("TopicA"),
            queue_id: 0,
            commit_log_offset: 1024,
            msg_size: body.len() as i32,
            tags_code: 7,
            store_timestamp: 100,
            consume_queue_offset: 0,
            keys: CheetahString::from("keyA"),
            success: true,
            uniq_key: Some(CheetahString::from("uniqA")),
            offset_id: Some(CheetahString::from("offsetA")),
            sys_flag: 0,
            ..DispatchRequest::default()
        };

        adapter.dispatch(&mut dispatch_request);
        tiered_store.shutdown().await?;

        let fetched = tiered_store.fetcher().get_message("TopicA".to_owned(), 0, 0, 1).await?;
        assert_eq!(fetched.status, TieredGetMessageStatus::Found);
        assert_eq!(fetched.messages, vec![body]);
        Ok(())
    }
}
