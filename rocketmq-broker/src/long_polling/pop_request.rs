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

use std::fmt::Display;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ResourcePermit;
use rocketmq_store::ArcMessageFilter;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::LegacySessionExecutionEnrollment;

use crate::deferred_generation_handoff::DeferredGenerationTarget;
use crate::deferred_generation_handoff::LegacyWaitHandoff;
use crate::deferred_generation_handoff::LegacyWaitLease;

struct PopRequestPermit {
    permit: Mutex<Option<ResourcePermit>>,
}

impl PopRequestPermit {
    fn empty() -> Self {
        Self {
            permit: Mutex::new(None),
        }
    }

    fn new(permit: ResourcePermit) -> Self {
        Self {
            permit: Mutex::new(Some(permit)),
        }
    }

    fn release(&self) -> bool {
        self.permit.lock().take().is_some()
    }
}

pub struct PopRequest {
    remoting_command: RemotingCommand,
    ctx: ConnectionHandlerContext,
    complete: Arc<AtomicBool>,
    op: i64,
    created_at: Instant,
    expired: u64,
    subscription_data: Option<SubscriptionData>,
    message_filter: Option<ArcMessageFilter>,
    resource_permit: PopRequestPermit,
    legacy_handoff: LegacyWaitHandoff,
}

impl PopRequest {
    #[must_use]
    pub fn estimated_retained_bytes(remoting_command: &RemotingCommand) -> usize {
        let extension_bytes = remoting_command.ext_fields().map_or(0, |fields| {
            fields.iter().fold(0usize, |total, (key, value)| {
                total
                    .saturating_add(std::mem::size_of_val(key))
                    .saturating_add(std::mem::size_of_val(value))
                    .saturating_add(key.len())
                    .saturating_add(value.len())
            })
        });
        std::mem::size_of::<Self>()
            .saturating_add(remoting_command.body().map_or(0, |body| body.len()))
            .saturating_add(remoting_command.remark().map_or(0, |remark| remark.len()))
            .saturating_add(extension_bytes)
    }

    pub fn new(
        remoting_command: RemotingCommand,
        ctx: ConnectionHandlerContext,
        expired: u64,
        subscription_data: Option<SubscriptionData>,
        message_filter: Option<ArcMessageFilter>,
    ) -> Self {
        static COUNTER: AtomicI64 = AtomicI64::new(i64::MIN);
        let op = COUNTER.fetch_add(1, Ordering::SeqCst);

        PopRequest {
            remoting_command,
            ctx,
            complete: Arc::new(AtomicBool::new(false)),
            op,
            created_at: Instant::now(),
            expired,
            subscription_data,
            message_filter,
            resource_permit: PopRequestPermit::empty(),
            legacy_handoff: LegacyWaitHandoff::default(),
        }
    }

    pub fn new_with_resource_permit(
        remoting_command: RemotingCommand,
        ctx: ConnectionHandlerContext,
        expired: u64,
        subscription_data: Option<SubscriptionData>,
        message_filter: Option<ArcMessageFilter>,
        resource_permit: ResourcePermit,
    ) -> Self {
        let mut request = Self::new(remoting_command, ctx, expired, subscription_data, message_filter);
        request.resource_permit = PopRequestPermit::new(resource_permit);
        request
    }

    /// Releases retained-request budget at the logical terminal, independently of node reclamation.
    pub(crate) fn release_resource_permit(&self) -> bool {
        self.resource_permit.release()
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

    pub(crate) fn release_legacy_wait(&self) {
        self.legacy_handoff.release();
    }

    pub fn get_channel(&self) -> &Channel {
        self.ctx.channel()
    }

    #[deprecated(note = "channel writes are serialized through Channel send methods")]
    pub fn get_channel_mut(&self) -> &Channel {
        self.ctx.channel()
    }

    pub fn get_ctx(&self) -> &ConnectionHandlerContext {
        &self.ctx
    }

    pub fn get_remoting_command(&self) -> &RemotingCommand {
        &self.remoting_command
    }

    pub fn remoting_command_mut(&mut self) -> &mut RemotingCommand {
        &mut self.remoting_command
    }

    pub fn is_timeout(&self) -> bool {
        let now = current_millis();
        now > self.expired.saturating_sub(50)
    }

    pub fn complete(&self) -> bool {
        self.complete
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    pub fn get_expired(&self) -> u64 {
        self.expired
    }

    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    pub fn get_subscription_data(&self) -> Option<&SubscriptionData> {
        self.subscription_data.as_ref()
    }

    pub fn get_message_filter(&self) -> Option<&ArcMessageFilter> {
        self.message_filter.as_ref()
    }
}

impl PartialEq for PopRequest {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
    }
}

impl Eq for PopRequest {}

impl PartialOrd for PopRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PopRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expired.cmp(&other.expired).then_with(|| self.op.cmp(&other.op))
    }
}

impl Display for PopRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopRequest [op={}, expired={}, subscription_data={:?}]",
            self.op, self.expired, self.subscription_data
        )
    }
}
