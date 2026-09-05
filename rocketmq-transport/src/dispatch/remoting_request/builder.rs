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

use std::time::Instant;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::TaskGroup;

use super::IngressRequestView;
use super::RemotingRequest;
use crate::contract::TransportContractViolation;
use crate::dispatch::DeferredResponseSeed;
use crate::dispatch::InlineResponseSlot;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestContext;
use crate::dispatch::RequestControlView;
use crate::dispatch::RequestMeta;
use crate::dispatch::RequestOrigin;
use crate::server::SessionHandle;
use crate::session_view::EmbeddedSessionRecord;
use crate::session_view::SessionView;

/// Sealed lifecycle facts that bind a request to its real session and owner.
///
/// Network provenance is derived only from the canonical [`SessionHandle`],
/// which supplies both the session view and its owning task group. Embedded
/// provenance similarly binds one [`EmbeddedSessionRecord`] to the actual
/// dispatch parent task group. The fields remain private so callers cannot
/// combine a view from one session with state or cancellation from another.
pub(crate) struct RequestLifecycleProvenance {
    session: SessionView,
    parent_task_group: TaskGroup,
}

impl RequestLifecycleProvenance {
    /// Derives network request lifecycle facts from one canonical session.
    pub(crate) fn from_network_session(session: &SessionHandle) -> Self {
        Self::from_session_view(session.session_view(), session.task_group())
    }

    /// Derives embedded request lifecycle facts from the record that owns its
    /// session publishers and the dispatch owner that will run the request.
    pub(crate) fn from_embedded_session(session: &EmbeddedSessionRecord, parent_task_group: &TaskGroup) -> Self {
        Self::from_session_view(session.view(), parent_task_group)
    }

    fn from_session_view(session: SessionView, parent_task_group: &TaskGroup) -> Self {
        Self {
            session,
            parent_task_group: parent_task_group.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn network_for_test(
        session_id: u64,
        local_addr: std::net::SocketAddr,
        remote_addr: std::net::SocketAddr,
        transport_peer_addr: std::net::SocketAddr,
        proxy_protocol: Option<&crate::proxy_protocol::ProxyProtocolMetadata>,
        state_rx: tokio::sync::watch::Receiver<crate::connection::ConnectionState>,
        closed_rx: tokio::sync::watch::Receiver<bool>,
        parent_task_group: &TaskGroup,
    ) -> Self {
        Self::from_session_view(
            SessionView::network(
                session_id,
                local_addr,
                remote_addr,
                transport_peer_addr,
                proxy_protocol,
                state_rx,
                closed_rx,
            ),
            parent_task_group,
        )
    }
}

/// Crate-private assembly point for trusted ingress request facts.
pub(crate) struct RemotingRequestBuilder {
    original: OriginalRequestIdentity,
    meta: RequestMeta,
    origin: crate::dispatch::RequestOrigin,
    authentication: crate::dispatch::AuthenticationState,
    session: SessionView,
    control: RequestControlView,
    inline_response: InlineResponseSlot,
    command: RemotingCommand,
}

impl RemotingRequestBuilder {
    pub(crate) fn new(
        original: OriginalRequestIdentity,
        received_at: Instant,
        context: RequestContext,
        lifecycle: RequestLifecycleProvenance,
        command: RemotingCommand,
    ) -> Self {
        let context = context.into_parts();
        let meta = RequestMeta::new(received_at, context.deadline);
        let control =
            RequestControlView::from_meta(&meta, lifecycle.session.state().clone(), &lifecycle.parent_task_group);
        Self {
            original,
            meta,
            origin: context.origin,
            authentication: context.authentication,
            session: lifecycle.session,
            control,
            inline_response: InlineResponseSlot::disabled(),
            command,
        }
    }

    pub(crate) fn with_deferred_response_seed(mut self, seed: DeferredResponseSeed) -> Self {
        self.inline_response = InlineResponseSlot::with_deferred_seed(seed);
        self
    }

    #[cfg(test)]
    pub(crate) fn reserve_deferred_response(mut self) -> Self {
        self.inline_response = InlineResponseSlot::deferred_capable();
        self
    }

    /// Borrows immutable ingress fields while the builder still owns the
    /// unmodified command. Ordering must finish before [`Self::build`] moves
    /// that command into a processor-facing request.
    #[must_use]
    pub(crate) fn ingress_view(&self) -> IngressRequestView<'_> {
        IngressRequestView {
            original: self.original,
            ext_fields: self.command.ext_fields(),
        }
    }

    pub(crate) const fn control(&self) -> &RequestControlView {
        &self.control
    }

    pub(crate) const fn deadline(&self) -> Option<crate::deadline::RequestDeadline> {
        self.meta.deadline()
    }

    pub(crate) const fn received_at(&self) -> Instant {
        self.meta.received_at()
    }

    pub(crate) const fn command(&self) -> &RemotingCommand {
        &self.command
    }

    pub(crate) const fn peer(&self) -> Option<&rocketmq_security_api::PeerInfo> {
        match &self.origin {
            crate::dispatch::RequestOrigin::Network { peer } => Some(peer),
            crate::dispatch::RequestOrigin::Embedded { .. } => None,
        }
    }

    pub(crate) const fn principal(&self) -> Option<&rocketmq_security_api::Principal> {
        self.authentication.principal()
    }

    pub(crate) const fn origin(&self) -> &crate::dispatch::RequestOrigin {
        &self.origin
    }

    pub(crate) const fn authentication(&self) -> &crate::dispatch::AuthenticationState {
        &self.authentication
    }

    pub(crate) fn build(self) -> Result<RemotingRequest, TransportContractViolation> {
        if self.command.is_response_type() {
            return Err(TransportContractViolation::RequestFromResponseCommand);
        }
        if !self.original.matches_command(&self.command) {
            return Err(TransportContractViolation::OriginalCommandMismatch);
        }
        if self.original.request_id().owner_id() != self.session.id().owner_id() {
            return Err(TransportContractViolation::SessionOwnerMismatch);
        }

        match (&self.origin, &self.session) {
            (RequestOrigin::Network { peer }, SessionView::Network { remote_addr, .. }) => {
                if peer.address() != *remote_addr {
                    return Err(TransportContractViolation::NetworkPeerMismatch);
                }
            }
            (RequestOrigin::Network { .. }, SessionView::Embedded { .. }) => {
                return Err(TransportContractViolation::NetworkSessionMismatch);
            }
            (RequestOrigin::Embedded { .. }, SessionView::Network { .. }) => {
                return Err(TransportContractViolation::EmbeddedSessionMismatch);
            }
            (RequestOrigin::Embedded { .. }, SessionView::Embedded { .. }) => {
                if self.authentication.principal().is_none() {
                    return Err(TransportContractViolation::MissingEmbeddedAuthentication);
                }
            }
        }

        if self.original.is_one_way() && self.inline_response.has_deferred_capability() {
            return Err(TransportContractViolation::OneWayDeferredResponse);
        }

        Ok(RemotingRequest {
            original: self.original,
            meta: self.meta,
            origin: self.origin,
            authentication: self.authentication,
            session: self.session,
            control: self.control,
            extensions: Default::default(),
            inline_response: self.inline_response,
            command: self.command,
        })
    }
}
