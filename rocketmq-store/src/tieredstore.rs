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
use rocketmq_tieredstore::dispatcher::DefaultTieredDispatcher;
use rocketmq_tieredstore::dispatcher::TieredDispatchRequest;
use rocketmq_tieredstore::provider::ProviderKind;
use rocketmq_tieredstore::provider::TieredStoreProvider;
use tracing::debug;
use tracing::warn;

use crate::base::commit_log_dispatcher::CommitLogDispatcher;
use crate::base::dispatch_request::DispatchRequest;

pub type DispatchBodyResolver = dyn Fn(&DispatchRequest) -> Option<Bytes> + Send + Sync;

pub struct TieredCommitLogDispatcher<P = ProviderKind>
where
    P: TieredStoreProvider,
{
    dispatcher: Arc<DefaultTieredDispatcher<P>>,
    body_resolver: Arc<DispatchBodyResolver>,
}

impl<P> TieredCommitLogDispatcher<P>
where
    P: TieredStoreProvider,
{
    pub fn new(dispatcher: Arc<DefaultTieredDispatcher<P>>, body_resolver: Arc<DispatchBodyResolver>) -> Self {
        Self {
            dispatcher,
            body_resolver,
        }
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
        if let Err(error) = self.dispatcher.try_dispatch(request) {
            warn!(
                topic = %dispatch_request.topic,
                queue_id = dispatch_request.queue_id,
                queue_offset = dispatch_request.consume_queue_offset,
                error = %error,
                "failed to enqueue tieredstore dispatch request"
            );
        }
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
