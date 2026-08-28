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

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_store::BrokerReadStore;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::ResponsePlan;

use super::PullMessageProcessor;
use crate::long_polling::pull_deferred::PullHookMetadata;
use crate::long_polling::pull_deferred::ResumePull;
use crate::processor::pull_message_result_handler::PullBroadcastClientResolver;
use crate::processor::pull_message_result_handler::PullMessageResult;

#[derive(Debug, thiserror::Error)]
enum PullResumeError {
    #[error("a resumed Pull unexpectedly requested legacy suspension")]
    UnexpectedSuspension,
}

impl<MS> PullMessageProcessor<MS>
where
    MS: BrokerReadStore,
{
    /// Re-runs the canonical Pull business path for an already-claimed deferred request.
    #[allow(dead_code, reason = "called by the forthcoming V2 Pull leaf")]
    pub(crate) async fn resume_pull(
        &self,
        resume: ResumePull,
        reason: DeferredWakeReason,
    ) -> RocketMQResult<ResponsePlan> {
        let (request, criteria, _wait_deadline) = resume.into_parts();
        drop(criteria);
        let (request_code, header, effective_peer, session_id, hook_metadata) = request.into_parts();
        let broadcast_client_resolver =
            |header: &PullMessageRequestHeader| self.resolve_session_broadcast_client_id(header, session_id);
        self.resume_pull_parts(
            request_code,
            header,
            effective_peer,
            &hook_metadata,
            &broadcast_client_resolver,
            reason,
        )
        .await
    }

    pub(super) async fn resume_pull_parts(
        &self,
        request_code: RequestCode,
        header: PullMessageRequestHeader,
        effective_peer: std::net::SocketAddr,
        hook_metadata: &PullHookMetadata,
        broadcast_client_resolver: &PullBroadcastClientResolver<'_>,
        reason: DeferredWakeReason,
    ) -> RocketMQResult<ResponsePlan> {
        match reason {
            DeferredWakeReason::MessageArrived | DeferredWakeReason::Timeout | DeferredWakeReason::ForcedRefresh => {}
        }
        match self
            .execute_pull(
                request_code,
                header,
                effective_peer,
                hook_metadata,
                broadcast_client_resolver,
                false,
                None,
            )
            .await?
        {
            PullMessageResult::Reply(parts) => parts.into_response_plan(),
            PullMessageResult::Suspend(_) => Err(RocketMQError::internal(
                "resume-pull",
                PullResumeError::UnexpectedSuspension,
            )),
        }
    }
}
