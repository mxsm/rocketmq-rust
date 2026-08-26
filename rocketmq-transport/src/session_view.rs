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

use tokio::sync::watch;

use crate::connection::ConnectionState;
use crate::proxy_protocol::ProxyProtocolMetadata;

/// Stable process-local identity for one network or embedded session.
///
/// Session identifiers are allocated by the trusted transport entry boundary.
/// They are suitable for equality and hashing within one process, but are not
/// protocol identifiers or peer identities.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionId(u64);

impl SessionId {
    /// Creates an identifier from an owner already allocated by the trusted
    /// transport boundary.
    pub(crate) const fn from_session_owner(owner_id: u64) -> Self {
        Self(owner_id)
    }
}

/// Read-only source and destination facts supplied by a trusted PROXY header.
///
/// The direct TCP peer remains available separately on
/// [`SessionView::Network`]. Raw PROXY TLVs are deliberately not exposed.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ProxyInfoSnapshot;
///
/// fn cannot_read_raw_tlv_field(proxy: &ProxyInfoSnapshot) {
///     let _ = proxy.tlvs;
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ProxyInfoSnapshot;
///
/// fn cannot_read_raw_tlvs(proxy: &ProxyInfoSnapshot) {
///     let _ = proxy.tlvs();
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ProxyInfoSnapshot {
    source: SocketAddr,
    destination: SocketAddr,
}

impl ProxyInfoSnapshot {
    fn from_metadata(metadata: &ProxyProtocolMetadata) -> Self {
        Self {
            source: metadata.source,
            destination: metadata.destination,
        }
    }

    /// Returns the original client address asserted by the trusted proxy.
    #[must_use]
    pub const fn source(&self) -> SocketAddr {
        self.source
    }

    /// Returns the destination address asserted by the trusted proxy.
    #[must_use]
    pub const fn destination(&self) -> SocketAddr {
        self.destination
    }
}

/// Read-only lifecycle state for one session.
///
/// This view observes the canonical session state without retaining a state
/// sender, connection, writer, task group, or cancellation capability.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionStateView;
///
/// fn cannot_close(state: &SessionStateView) {
///     state.close();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionStateView;
///
/// fn cannot_write(state: &SessionStateView) {
///     state.write();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionStateView;
///
/// fn cannot_cancel(state: &SessionStateView) {
///     state.cancel();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionStateView;
///
/// fn cannot_get_a_task_group(state: &SessionStateView) {
///     let _ = state.task_group();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionStateView;
///
/// fn cannot_get_a_cancellation_token(state: &SessionStateView) {
///     let _ = state.cancellation_token();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionStateView;
///
/// fn cannot_get_a_connection_state_handle(state: &SessionStateView) {
///     let _ = state.connection_state_handle();
/// }
/// ```
#[derive(Clone)]
pub struct SessionStateView {
    state_rx: watch::Receiver<ConnectionState>,
    closed_rx: watch::Receiver<bool>,
}

impl SessionStateView {
    pub(crate) fn from_receivers(state_rx: watch::Receiver<ConnectionState>, closed_rx: watch::Receiver<bool>) -> Self {
        Self { state_rx, closed_rx }
    }

    /// Returns whether the session is currently accepting inbound work.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self.is_closed() && *self.state_rx.borrow() == ConnectionState::Healthy
    }

    /// Returns whether the session has stopped accepting inbound work.
    ///
    /// A response writer may finish already accepted work after this becomes
    /// `true`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.publishers_dropped() || *self.closed_rx.borrow() || *self.state_rx.borrow() == ConnectionState::Closed
    }

    /// Waits until the session closes or its lifecycle publisher is dropped.
    ///
    /// This method is observational only. Dropping the lifecycle publisher
    /// also ends the wait because no subsequent state transition is possible.
    pub async fn closed(&self) {
        let mut state_rx = self.state_rx.clone();
        let mut closed_rx = self.closed_rx.clone();
        while !self.is_closed() {
            tokio::select! {
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = closed_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }

    fn publishers_dropped(&self) -> bool {
        self.state_rx.has_changed().is_err() || self.closed_rx.has_changed().is_err()
    }
}

/// Immutable session metadata available to V2 request processing.
///
/// Network values are captured when the canonical transport session is
/// established. Embedded sessions intentionally have no socket addresses.
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionView;
///
/// fn cannot_close(view: &SessionView) {
///     view.close();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionView;
///
/// fn cannot_write(view: &SessionView) {
///     view.write();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionView;
///
/// fn cannot_cancel(view: &SessionView) {
///     view.cancel();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionView;
///
/// fn cannot_get_a_task_group(view: &SessionView) {
///     let _ = view.task_group();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionView;
///
/// fn cannot_get_a_cancellation_token(view: &SessionView) {
///     let _ = view.cancellation_token();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionView;
///
/// fn cannot_get_a_connection(view: &SessionView) {
///     let _ = view.connection();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::SessionHandle;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::Channel;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::Connection;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::ConnectionStateHandle;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::OperationContext;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::TaskGroup;
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::v2::CancellationToken;
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub enum SessionView {
    /// Metadata for a TCP or TLS session.
    Network {
        /// Stable process-local session identity.
        id: SessionId,
        /// Address accepted by the transport listener or trusted proxy.
        local_addr: SocketAddr,
        /// Effective remote address after trusted PROXY source rewriting.
        remote_addr: SocketAddr,
        /// Direct TCP peer before trusted PROXY source rewriting.
        transport_peer_addr: SocketAddr,
        /// Trusted PROXY source/destination snapshot, when a header was present.
        proxy: Option<ProxyInfoSnapshot>,
        /// Read-only lifecycle state shared with this session's clones.
        state: SessionStateView,
    },
    /// Metadata for an in-process embedded session.
    Embedded {
        /// Stable process-local session identity.
        id: SessionId,
        /// Read-only lifecycle state shared with this session's clones.
        state: SessionStateView,
    },
}

impl SessionView {
    pub(crate) fn network(
        session_id: u64,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        transport_peer_addr: SocketAddr,
        proxy_protocol: Option<&ProxyProtocolMetadata>,
        state_rx: watch::Receiver<ConnectionState>,
        closed_rx: watch::Receiver<bool>,
    ) -> Self {
        Self::Network {
            id: SessionId::from_session_owner(session_id),
            local_addr,
            remote_addr,
            transport_peer_addr,
            proxy: proxy_protocol.map(ProxyInfoSnapshot::from_metadata),
            state: SessionStateView::from_receivers(state_rx, closed_rx),
        }
    }

    /// Returns the stable process-local identity for this session.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        match self {
            Self::Network { id, .. } | Self::Embedded { id, .. } => *id,
        }
    }

    /// Returns the read-only lifecycle view for this session.
    #[must_use]
    pub const fn state(&self) -> &SessionStateView {
        match self {
            Self::Network { state, .. } | Self::Embedded { state, .. } => state,
        }
    }
}

/// Crate-private lifecycle owner for one embedded dispatch session.
///
/// The record owns the only sender for embedded session state. Views only hold
/// receivers, so dropping this record closes every cloned embedded view without
/// exposing a close capability to processors.
pub(crate) struct EmbeddedSessionRecord {
    view: SessionView,
    state_tx: watch::Sender<ConnectionState>,
    closed_tx: watch::Sender<bool>,
}

impl EmbeddedSessionRecord {
    pub(crate) fn new(session_id: u64) -> Self {
        let (state_tx, state_rx) = watch::channel(ConnectionState::Healthy);
        let (closed_tx, closed_rx) = watch::channel(false);
        Self {
            view: SessionView::Embedded {
                id: SessionId::from_session_owner(session_id),
                state: SessionStateView::from_receivers(state_rx, closed_rx),
            },
            state_tx,
            closed_tx,
        }
    }

    #[allow(
        dead_code,
        reason = "REQ-04 retains the embedded view for the REQ-06 request builder"
    )]
    pub(crate) fn view(&self) -> SessionView {
        self.view.clone()
    }
}

impl Drop for EmbeddedSessionRecord {
    fn drop(&mut self) {
        let _ = self.state_tx.send(ConnectionState::Closed);
        let _ = self.closed_tx.send(true);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use super::*;

    fn address(value: &str) -> SocketAddr {
        value.parse().expect("test socket address must parse")
    }

    #[tokio::test]
    async fn cloned_state_views_observe_the_same_close_transition() {
        let (state_tx, state_rx) = watch::channel(ConnectionState::Healthy);
        let (closed_tx, closed_rx) = watch::channel(false);
        let first = SessionStateView::from_receivers(state_rx, closed_rx);
        let second = first.clone();

        assert!(first.is_healthy());
        assert!(!first.is_closed());
        state_tx
            .send(ConnectionState::Degraded)
            .expect("test state receiver must remain subscribed");
        assert!(!first.is_healthy());
        assert!(!first.is_closed());

        closed_tx
            .send(true)
            .expect("test close receiver must remain subscribed");
        first.closed().await;
        second.closed().await;

        assert!(first.is_closed());
        assert!(second.is_closed());
    }

    #[tokio::test]
    async fn cloned_state_views_treat_publisher_loss_as_closed() {
        let (state_tx, state_rx) = watch::channel(ConnectionState::Healthy);
        let (closed_tx, closed_rx) = watch::channel(false);
        let first = SessionStateView::from_receivers(state_rx, closed_rx);
        let second = first.clone();

        drop(state_tx);
        drop(closed_tx);
        first.closed().await;
        second.closed().await;

        assert!(first.is_closed());
        assert!(second.is_closed());
        assert!(!first.is_healthy());
        assert!(!second.is_healthy());
    }

    #[tokio::test]
    async fn embedded_views_have_no_socket_addresses_and_close_with_their_record() {
        let record = EmbeddedSessionRecord::new(41);
        let view = record.view();

        let SessionView::Embedded { id, state } = &view else {
            panic!("embedded record must create an embedded session view");
        };
        assert_eq!(*id, view.id());
        assert!(state.is_healthy());
        drop(record);

        let SessionView::Embedded { state, .. } = &view else {
            panic!("embedded session view must retain its variant");
        };
        state.closed().await;
        assert!(state.is_closed());
    }

    #[test]
    fn network_view_keeps_proxy_source_distinct_from_the_transport_peer() {
        let transport_peer = address("203.0.113.9:31000");
        let metadata = ProxyProtocolMetadata {
            transport_peer,
            source: address("198.51.100.44:43123"),
            destination: address("192.0.2.10:10911"),
            tlvs: Default::default(),
        };
        let (_state_tx, state_rx) = watch::channel(ConnectionState::Healthy);
        let (_closed_tx, closed_rx) = watch::channel(false);
        let view = SessionView::network(
            77,
            metadata.destination,
            metadata.source,
            transport_peer,
            Some(&metadata),
            state_rx,
            closed_rx,
        );

        let SessionView::Network {
            id,
            local_addr,
            remote_addr,
            transport_peer_addr,
            proxy,
            state,
        } = view
        else {
            panic!("network constructor must create a network session view");
        };
        let proxy = proxy.expect("trusted proxy metadata must have a snapshot");

        assert_eq!(local_addr, metadata.destination);
        assert_eq!(remote_addr, metadata.source);
        assert_eq!(transport_peer_addr, transport_peer);
        assert_ne!(remote_addr, transport_peer_addr);
        assert_eq!(proxy.source(), metadata.source);
        assert_eq!(proxy.destination(), metadata.destination);
        assert!(state.is_healthy());
        assert_eq!(id, SessionId::from_session_owner(77));
    }

    #[test]
    fn session_ids_are_stable_hash_keys() {
        let ids = [
            SessionId::from_session_owner(51),
            SessionId::from_session_owner(52),
            SessionId::from_session_owner(51),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        assert_eq!(ids.len(), 2);
    }
}
