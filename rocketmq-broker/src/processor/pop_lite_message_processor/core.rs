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

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_store::BrokerReadWriteStore;

use super::PopLiteMessageProcessor;
use crate::lite::lite_event_dispatcher::LiteEventBatch;
use crate::lite::lite_event_dispatcher::LiteEventBatchExecution;

pub(crate) struct PopLiteCoreResult {
    pub(crate) body: Option<Bytes>,
    pub(crate) fetched_count: i32,
    pub(crate) order_count_info: Option<CheetahString>,
}

impl<MS> PopLiteMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    pub(crate) async fn execute_pop_lite_batch(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        batch: LiteEventBatch,
    ) -> PopLiteCoreResult {
        let events = batch.event_names();
        let (result, requeue_events) = self.execute_pop_lite_events(request_header, events).await;
        let (max_event_count, delay_millis) = self.lite_dispatch_policy(&request_header.consumer_group);
        batch.complete_with_limit(&requeue_events, max_event_count, delay_millis);
        result
    }

    /// Executes a deferred batch while staging settlement for canonical response terminal.
    pub(crate) async fn execute_pop_lite_terminal_batch(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        events: LiteEventBatchExecution,
    ) -> PopLiteCoreResult {
        let batch = events.commit();
        let event_names = batch.event_names();
        let (result, requeue_events) = self.execute_pop_lite_events(request_header, event_names).await;
        let (max_event_count, delay_millis) = self.lite_dispatch_policy(&request_header.consumer_group);
        batch.complete_with_limit(&requeue_events, max_event_count, delay_millis);
        result
    }

    pub(super) async fn execute_pop_lite_without_events(
        &self,
        request_header: &PopLiteMessageRequestHeader,
    ) -> PopLiteCoreResult {
        let (body, _, fetched_count, order_count_info) = self.pop_from_events(request_header, Vec::new()).await;
        PopLiteCoreResult {
            body,
            fetched_count,
            order_count_info,
        }
    }

    async fn execute_pop_lite_events(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        events: Vec<CheetahString>,
    ) -> (PopLiteCoreResult, std::collections::HashSet<CheetahString>) {
        let (body, requeue_events, fetched_count, order_count_info) =
            self.pop_from_events(request_header, events).await;
        (
            PopLiteCoreResult {
                body,
                fetched_count,
                order_count_info,
            },
            requeue_events,
        )
    }
}
