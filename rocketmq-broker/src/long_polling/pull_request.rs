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
use std::sync::Weak;

use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::ArcMessageFilter;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::LegacySessionExecutionEnrollment;

use crate::deferred_generation_handoff::DeferredGenerationTarget;
use crate::deferred_generation_handoff::LegacyWaitHandoff;
use crate::deferred_generation_handoff::LegacyWaitLease;

#[derive(Clone)]
pub struct PullRequest {
    request_command: RemotingCommand,
    client_channel: Channel,
    ctx: ConnectionHandlerContext,
    timeout_millis: u64,
    suspend_timestamp: u64,
    pull_from_this_offset: i64,
    subscription_data: SubscriptionData,
    message_filter: ArcMessageFilter,
    legacy_handoff: Arc<LegacyWaitHandoff>,
}

impl PullRequest {
    pub fn new(
        request_command: RemotingCommand,
        client_channel: Channel,
        ctx: ConnectionHandlerContext,
        timeout_millis: u64,
        suspend_timestamp: u64,
        pull_from_this_offset: i64,
        subscription_data: SubscriptionData,
        message_filter: ArcMessageFilter,
    ) -> Self {
        Self {
            request_command,
            client_channel,
            ctx,
            timeout_millis,
            suspend_timestamp,
            pull_from_this_offset,
            subscription_data,
            message_filter,
            legacy_handoff: Arc::new(LegacyWaitHandoff::default()),
        }
    }

    pub fn request_command(&self) -> &RemotingCommand {
        &self.request_command
    }

    pub fn request_command_mut(&mut self) -> &mut RemotingCommand {
        &mut self.request_command
    }

    pub fn client_channel(&self) -> &Channel {
        &self.client_channel
    }

    pub fn pull_from_this_offset(&self) -> i64 {
        self.pull_from_this_offset
    }

    pub fn subscription_data(&self) -> &SubscriptionData {
        &self.subscription_data
    }

    pub fn message_filter(&self) -> ArcMessageFilter {
        self.message_filter.clone()
    }

    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }

    pub fn deadline_millis(&self) -> u64 {
        self.suspend_timestamp.saturating_add(self.timeout_millis)
    }

    pub fn suspend_timestamp(&self) -> u64 {
        self.suspend_timestamp
    }

    pub fn connection_handler_context(&self) -> &ConnectionHandlerContext {
        &self.ctx
    }

    pub(crate) fn install_legacy_handoff(
        &self,
        expected_target: &DeferredGenerationTarget,
        lease: LegacyWaitLease,
    ) -> Result<(), LegacyWaitLease> {
        self.legacy_handoff.install(expected_target, lease)
    }

    #[must_use]
    pub(crate) fn legacy_handoff_target(&self) -> Option<DeferredGenerationTarget> {
        self.legacy_handoff.target()
    }

    pub(crate) fn take_legacy_wait(&self) -> Option<LegacyWaitLease> {
        self.legacy_handoff.take()
    }

    pub(crate) fn restore_legacy_wait(&self, lease: LegacyWaitLease) -> Result<(), LegacyWaitLease> {
        self.legacy_handoff.restore(lease)
    }

    pub(crate) fn install_legacy_session_cleanup(
        &self,
        cleanup: LegacySessionExecutionEnrollment,
    ) -> Result<(), LegacySessionExecutionEnrollment> {
        self.legacy_handoff.install_session_cleanup(cleanup)
    }

    pub(crate) fn take_legacy_session_execution(&self) -> Option<LegacySessionExecutionEnrollment> {
        self.legacy_handoff.take_session_execution()
    }

    pub(crate) fn release_legacy_session_cleanup(&self) {
        self.legacy_handoff.release_session_cleanup();
    }

    pub(crate) fn mark_legacy_session_closed(&self) {
        self.legacy_handoff.mark_session_closed();
    }

    pub(crate) fn legacy_session_closed(&self) -> bool {
        self.legacy_handoff.session_closed()
    }

    pub(crate) fn legacy_handoff_identity(&self) -> u64 {
        self.legacy_handoff.identity()
    }

    pub(crate) fn legacy_handoff_weak(&self) -> Weak<LegacyWaitHandoff> {
        Arc::downgrade(&self.legacy_handoff)
    }

    pub(crate) fn release_legacy_wait(&self) {
        self.legacy_handoff.release();
    }
}
