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

use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::RemotingResponse;

use super::core::PopLiteCoreResult;
use super::response::PopLiteResponseKind;
use super::PopLiteMessageProcessor;
use crate::lite::lite_event_dispatcher::LiteEventBatchExecution;
use crate::long_polling::pop_lite_deferred::data::ResumePopLite;

impl<MS> PopLiteMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    /// Executes one claimed PopLite event using its affine dispatcher batch.
    pub(crate) async fn resume_pop_lite(
        &self,
        resume: ResumePopLite,
        reason: DeferredWakeReason,
        events: LiteEventBatchExecution,
    ) -> rocketmq_error::RocketMQResult<RemotingResponse> {
        debug_assert_eq!(reason, DeferredWakeReason::MessageArrived);
        let request_header = resume.into_request().into_header();
        let result = self.execute_pop_lite_terminal_batch(&request_header, events).await;
        let kind = if result.body.is_some() {
            PopLiteResponseKind::Found
        } else {
            PopLiteResponseKind::PollingTimeout
        };
        self.compose_pop_lite_response(&request_header, result, kind)
    }

    pub(crate) fn resume_pop_lite_timeout(
        &self,
        resume: ResumePopLite,
        reason: DeferredWakeReason,
    ) -> rocketmq_error::RocketMQResult<RemotingResponse> {
        debug_assert_eq!(reason, DeferredWakeReason::Timeout);
        let request_header = resume.into_request().into_header();
        self.compose_pop_lite_response(
            &request_header,
            PopLiteCoreResult {
                body: None,
                fetched_count: 0,
                order_count_info: None,
            },
            PopLiteResponseKind::PollingTimeout,
        )
    }
}
