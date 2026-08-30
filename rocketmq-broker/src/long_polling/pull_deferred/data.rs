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

use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerStatsManager;
use rocketmq_transport::api::SessionId;

/// Named request metadata consumed by the existing Pull consume hooks.
#[derive(Default)]
pub(crate) struct PullHookMetadata {
    commercial_owner: Option<CheetahString>,
    account_auth_type: Option<CheetahString>,
    account_owner_parent: Option<CheetahString>,
    account_owner_self: Option<CheetahString>,
}

impl PullHookMetadata {
    pub(crate) fn from_fields(fields: Option<&std::collections::HashMap<CheetahString, CheetahString>>) -> Self {
        let value = |key: &str| fields.and_then(|fields| fields.get(key)).cloned();
        Self {
            commercial_owner: value(BrokerStatsManager::COMMERCIAL_OWNER),
            account_auth_type: value(BrokerStatsManager::ACCOUNT_AUTH_TYPE),
            account_owner_parent: value(BrokerStatsManager::ACCOUNT_OWNER_PARENT),
            account_owner_self: value(BrokerStatsManager::ACCOUNT_OWNER_SELF),
        }
    }

    pub(crate) fn from_command(command: &RemotingCommand) -> Self {
        Self::from_fields(command.get_ext_fields())
    }

    #[must_use]
    pub(crate) const fn commercial_owner(&self) -> Option<&CheetahString> {
        self.commercial_owner.as_ref()
    }

    #[must_use]
    pub(crate) const fn account_auth_type(&self) -> Option<&CheetahString> {
        self.account_auth_type.as_ref()
    }

    #[must_use]
    pub(crate) const fn account_owner_parent(&self) -> Option<&CheetahString> {
        self.account_owner_parent.as_ref()
    }

    #[must_use]
    pub(crate) const fn account_owner_self(&self) -> Option<&CheetahString> {
        self.account_owner_self.as_ref()
    }

    pub(super) fn dynamic_bytes(&self) -> Option<usize> {
        [
            self.commercial_owner.as_ref(),
            self.account_auth_type.as_ref(),
            self.account_owner_parent.as_ref(),
            self.account_owner_self.as_ref(),
        ]
        .into_iter()
        .flatten()
        .try_fold(0usize, |total, value| total.checked_add(value.len()))
    }
}

/// Typed Pull request facts retained without a command, body, or write capability.
pub(crate) struct PullRequestData {
    request_code: RequestCode,
    original_header: PullMessageRequestHeader,
    effective_peer: SocketAddr,
    session_id: SessionId,
    hook_metadata: PullHookMetadata,
}

impl PullRequestData {
    pub(super) const fn new(
        request_code: RequestCode,
        original_header: PullMessageRequestHeader,
        effective_peer: SocketAddr,
        session_id: SessionId,
        hook_metadata: PullHookMetadata,
    ) -> Self {
        Self {
            request_code,
            original_header,
            effective_peer,
            session_id,
            hook_metadata,
        }
    }

    #[cfg(test)]
    pub(crate) const fn from_test_parts(
        request_code: RequestCode,
        original_header: PullMessageRequestHeader,
        effective_peer: SocketAddr,
        session_id: SessionId,
        hook_metadata: PullHookMetadata,
    ) -> Self {
        Self::new(request_code, original_header, effective_peer, session_id, hook_metadata)
    }

    #[must_use]
    pub(crate) const fn request_code(&self) -> RequestCode {
        self.request_code
    }

    #[must_use]
    pub(crate) const fn original_header(&self) -> &PullMessageRequestHeader {
        &self.original_header
    }

    #[must_use]
    pub(crate) const fn effective_peer(&self) -> SocketAddr {
        self.effective_peer
    }

    #[must_use]
    pub(crate) const fn session_id(&self) -> SessionId {
        self.session_id
    }

    #[must_use]
    pub(crate) const fn hook_metadata(&self) -> &PullHookMetadata {
        &self.hook_metadata
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        RequestCode,
        PullMessageRequestHeader,
        SocketAddr,
        SessionId,
        PullHookMetadata,
    ) {
        (
            self.request_code,
            self.original_header,
            self.effective_peer,
            self.session_id,
            self.hook_metadata,
        )
    }

    pub(super) fn dynamic_bytes(&self) -> Option<usize> {
        let header = &self.original_header;
        let mut total = [
            header.consumer_group.len(),
            header.topic.len(),
            header.lite_topic.as_ref().map_or(0, CheetahString::len),
            header.subscription.as_ref().map_or(0, CheetahString::len),
            header.expression_type.as_ref().map_or(0, CheetahString::len),
            header.proxy_forward_client_id.as_ref().map_or(0, CheetahString::len),
            self.hook_metadata.dynamic_bytes()?,
        ]
        .into_iter()
        .try_fold(0usize, |sum, value| sum.checked_add(value))?;
        if let Some(rpc) = header.topic_request.as_ref().and_then(|topic| topic.rpc.as_ref()) {
            for value in [rpc.namespace.as_ref(), rpc.broker_name.as_ref()].into_iter().flatten() {
                total = total.checked_add(value.len())?;
            }
        }
        Some(total)
    }
}

/// Immutable physical matching facts shared by the index and resume owner.
pub(crate) struct PullMatchCriteria {
    physical_topic: CheetahString,
    physical_queue_id: i32,
    pull_from_offset: i64,
    subscription: SubscriptionData,
    filter: ArcMessageFilter,
}

impl PullMatchCriteria {
    pub(crate) fn new(
        physical_topic: CheetahString,
        physical_queue_id: i32,
        pull_from_offset: i64,
        subscription: SubscriptionData,
        filter: ArcMessageFilter,
    ) -> Self {
        Self {
            physical_topic,
            physical_queue_id,
            pull_from_offset,
            subscription,
            filter,
        }
    }

    #[must_use]
    pub(crate) const fn physical_topic(&self) -> &CheetahString {
        &self.physical_topic
    }

    #[must_use]
    pub(crate) const fn physical_queue_id(&self) -> i32 {
        self.physical_queue_id
    }

    #[must_use]
    pub(crate) const fn pull_from_offset(&self) -> i64 {
        self.pull_from_offset
    }

    #[must_use]
    pub(crate) const fn subscription(&self) -> &SubscriptionData {
        &self.subscription
    }

    #[must_use]
    pub(crate) const fn filter(&self) -> &ArcMessageFilter {
        &self.filter
    }

    pub(super) fn dynamic_bytes(&self) -> Option<usize> {
        let fixed_strings = [
            self.physical_topic.len(),
            self.subscription.topic.len(),
            self.subscription.sub_string.len(),
            self.subscription.expression_type.len(),
            self.subscription.filter_class_source.len(),
        ];
        let mut total = fixed_strings
            .into_iter()
            .try_fold(0usize, |sum, value| sum.checked_add(value))?;
        for value in &self.subscription.tags_set {
            total = total.checked_add(value.len())?;
        }
        total.checked_add(
            self.subscription
                .code_set
                .len()
                .checked_mul(std::mem::size_of::<i32>())?,
        )
    }
}

/// Dynamic session-to-client lookup used by resumed broadcast pulls.
pub(crate) trait PullSessionClientLookup: Send + Sync {
    fn client_id(&self, session_id: SessionId, consumer_group: &CheetahString) -> Option<CheetahString>;
}
