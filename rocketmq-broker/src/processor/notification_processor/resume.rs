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

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::DeferredWakeReason;
use rocketmq_transport::api::ResponsePlan;

use super::core::NotificationCoreOutcome;
use super::NotificationFilterContract;
use super::NotificationProcessor;
use crate::long_polling::notification_deferred::service::ResumeNotification;

#[cfg(test)]
#[path = "../../../tests/unit/processor/notification/resume.rs"]
mod tests;

impl<MS> NotificationProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    /// Performs one real reread after a canonical claim; the request is never re-registered.
    pub(crate) async fn resume_notification(
        &self,
        resume: ResumeNotification,
        reason: DeferredWakeReason,
    ) -> rocketmq_error::RocketMQResult<ResponsePlan> {
        let command = self.resume_notification_command(resume, reason).await?;
        ResponsePlan::command(command)
            .map_err(|_| rocketmq_error::RocketMQError::invariant_violated("invalid Notification response plan"))
    }

    async fn resume_notification_command(
        &self,
        resume: ResumeNotification,
        reason: DeferredWakeReason,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        match reason {
            DeferredWakeReason::MessageArrived | DeferredWakeReason::Timeout | DeferredWakeReason::ForcedRefresh => {}
        }
        let (request, subscription, filter) = resume.into_execution_parts();
        let frozen_filter = match (subscription, filter) {
            (Some(subscription_data), Some(message_filter)) => Some(NotificationFilterContract {
                subscription_data,
                message_filter,
            }),
            _ => None,
        };
        let (header, effective_peer) = request.into_parts();
        let outcome = self
            .execute_notification_core(&header, effective_peer, 0, frozen_filter)
            .await;
        match outcome {
            NotificationCoreOutcome::Reply(command) => Ok(command),
            NotificationCoreOutcome::Ready(ready) => Ok(super::response::compose_notification_response(
                &self.context.command_factory,
                ready.has_msg,
                false,
                0,
            )),
        }
    }
}
