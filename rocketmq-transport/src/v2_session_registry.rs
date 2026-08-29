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

//! Composition-owned V2 server session lifecycle control.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_error::NetworkError;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_protocol::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
use rocketmq_protocol::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use tokio::sync::broadcast;

use crate::base::pending_request_table::materialize_and_estimate_remoting_command_retained_bytes;
use crate::base::pending_request_table::PendingRequestOwner;
use crate::base::pending_request_table::PendingRequestTable;
use crate::deadline::RequestDeadline;
use crate::server::SessionHandle;
use crate::session_view::SessionId;
use crate::session_view::SessionView;

/// Server-initiated commands explicitly permitted on a V2 client session.
///
/// Callers cannot supply an arbitrary command or encoded frame. Each variant
/// owns a protocol header whose request code is selected by an exhaustive
/// transport match before the command enters the canonical session writer.
#[derive(Debug)]
#[non_exhaustive]
pub enum ServerPushCommand {
    /// Notifies one client that its consumer-group membership changed.
    NotifyConsumerIdsChanged {
        /// Typed protocol header forwarded to the client.
        header: NotifyConsumerIdsChangedRequestHeader,
        /// Optional correlation identity retained from a triggering command.
        /// `None` preserves the command factory's generated identity.
        opaque: Option<i32>,
    },
    /// Notifies one Lite consumer that a Lite topic was unsubscribed.
    NotifyUnsubscribeLite {
        /// Typed protocol header forwarded to the client.
        header: NotifyUnsubscribeLiteRequestHeader,
        /// Optional correlation identity retained from a triggering command.
        /// `None` preserves the command factory's generated identity.
        opaque: Option<i32>,
    },
    /// Asks a producer to check the state of one transactional message.
    CheckTransactionState {
        /// Typed transaction-state request header.
        header: CheckTransactionStateRequestHeader,
        /// Encoded message supplied to the producer.
        body: Bytes,
    },
    /// Replaces one consumer's offset table.
    ResetConsumerClientOffset {
        /// Typed reset-offset request header.
        header: ResetOffsetRequestHeader,
        /// Encoded Java or C++ compatible reset-offset body.
        body: Bytes,
    },
}

impl ServerPushCommand {
    fn kind(&self) -> ServerPushKind {
        match self {
            Self::NotifyConsumerIdsChanged { .. } => ServerPushKind::NotifyConsumerIdsChanged,
            Self::NotifyUnsubscribeLite { .. } => ServerPushKind::NotifyUnsubscribeLite,
            Self::CheckTransactionState { .. } => ServerPushKind::CheckTransactionState,
            Self::ResetConsumerClientOffset { .. } => ServerPushKind::ResetConsumerClientOffset,
        }
    }

    fn into_command(self) -> RemotingCommand {
        let mut command = match self {
            Self::NotifyConsumerIdsChanged { header, opaque } => {
                let command = RemotingCommand::create_request_command(RequestCode::NotifyConsumerIdsChanged, header);
                match opaque {
                    Some(opaque) => command.set_opaque(opaque),
                    None => command,
                }
            }
            Self::NotifyUnsubscribeLite { header, opaque } => {
                let command = RemotingCommand::create_request_command(RequestCode::NotifyUnsubscribeLite, header);
                match opaque {
                    Some(opaque) => command.set_opaque(opaque),
                    None => command,
                }
            }
            Self::CheckTransactionState { header, body } => {
                RemotingCommand::create_request_command(RequestCode::CheckTransactionState, header).set_body(body)
            }
            Self::ResetConsumerClientOffset { header, body } => {
                RemotingCommand::create_request_command(RequestCode::ResetConsumerClientOffset, header).set_body(body)
            }
        };
        command.make_custom_header_to_net();
        command.mark_oneway_rpc()
    }
}

/// Low-cardinality kind of server push accepted by the V2 transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServerPushKind {
    /// Consumer group membership changed.
    NotifyConsumerIdsChanged,
    /// Lite subscription was removed.
    NotifyUnsubscribeLite,
    /// Producer transaction-state check.
    CheckTransactionState,
    /// Consumer offset reset.
    ResetConsumerClientOffset,
}

/// Receipt proving one typed push completed the canonical socket write.
///
/// A receipt is not an acknowledgement from the peer and does not prove that
/// the remote client processed the command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerPushReceipt {
    session_id: SessionId,
    kind: ServerPushKind,
}

impl ServerPushReceipt {
    /// Returns the session whose canonical writer completed the push.
    #[must_use]
    pub const fn session_id(self) -> SessionId {
        self.session_id
    }

    /// Returns the low-cardinality command kind that was written.
    #[must_use]
    pub const fn kind(self) -> ServerPushKind {
        self.kind
    }
}

/// Low-cardinality cause supplied when a capability owner closes a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionCloseReason {
    /// The owning client binding was explicitly retired.
    ClientBindingRetired,
    /// The session stopped refreshing its application heartbeat before expiry.
    HeartbeatTimeout,
    /// An administrative operation requested the close.
    Administrative,
    /// The service is shutting down.
    ServiceShutdown,
}

/// Failure to enqueue a typed server push on its canonical V2 session.
#[derive(Debug, thiserror::Error)]
#[error("V2 server push failed")]
pub struct ServerPushError {
    session_id: SessionId,
    #[source]
    source: RocketMQError,
}

impl ServerPushError {
    /// Returns the session that rejected the push.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns the transport error reported by the canonical writer.
    #[must_use]
    pub const fn source_error(&self) -> &RocketMQError {
        &self.source
    }
}

/// Failure to gracefully retire a canonical V2 session.
#[derive(Debug, thiserror::Error)]
#[error("V2 session retirement failed")]
pub struct SessionCloseError {
    session_id: SessionId,
    reason: SessionCloseReason,
    #[source]
    source: RocketMQError,
}

impl SessionCloseError {
    /// Returns the session whose graceful retirement failed.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns the transport error reported by the canonical close owner.
    #[must_use]
    pub const fn source_error(&self) -> &RocketMQError {
        &self.source
    }

    /// Returns the low-cardinality reason supplied by the close owner.
    #[must_use]
    pub const fn reason(&self) -> SessionCloseReason {
        self.reason
    }
}

/// Narrow sender for explicitly typed server pushes on one canonical V2 session.
///
/// This capability cannot expose the session writer, send arbitrary commands,
/// close the session, or recover a raw [`SessionHandle`].
#[derive(Clone)]
pub struct ServerPushSender {
    session: SessionHandle,
}

impl ServerPushSender {
    /// Returns the canonical session identity owned by this sender.
    #[must_use]
    pub fn session_id(&self) -> SessionId {
        self.session.session_view().id()
    }

    /// Enqueues one permitted one-way command before `timeout` expires.
    ///
    /// # Errors
    ///
    /// Returns [`ServerPushError`] when encoding, admission, deadline, or the
    /// canonical session writer rejects the command.
    pub async fn send(
        &self,
        command: ServerPushCommand,
        timeout: Duration,
    ) -> Result<ServerPushReceipt, ServerPushError> {
        let session_id = self.session_id();
        let kind = command.kind();
        let mut connection = self.session.connection();
        connection
            .send_command_with_deadline(
                command.into_command(),
                RequestDeadline::after(timeout),
                "v2-server-push",
            )
            .await
            .map_err(|source| ServerPushError { session_id, source })?;
        Ok(ServerPushReceipt { session_id, kind })
    }
}

/// Broker-to-client requests explicitly permitted on a V2 session.
///
/// Each variant fixes its request code and typed header. Callers cannot submit
/// an arbitrary command, opaque, or encoded header map.
#[derive(Debug)]
#[non_exhaustive]
pub enum ServerRequestCommand {
    /// Delivers a reply message and waits for the client acknowledgement.
    PushReplyMessageToClient {
        /// Typed reply-message request header.
        header: ReplyMessageRequestHeader,
        /// Optional reply-message bytes.
        body: Option<Bytes>,
    },
    /// Reads the client's current consumer offsets.
    GetConsumerStatusFromClient {
        /// Typed consumer-status request header.
        header: GetConsumerStatusRequestHeader,
    },
    /// Reads the client's running diagnostic snapshot.
    GetConsumerRunningInfo {
        /// Typed running-info request header.
        header: GetConsumerRunningInfoRequestHeader,
    },
    /// Asks the client to consume one encoded message directly.
    ConsumeMessageDirectly {
        /// Typed direct-consume request header.
        header: ConsumeMessageDirectlyResultRequestHeader,
        /// Encoded message bytes.
        body: Bytes,
    },
}

impl ServerRequestCommand {
    fn kind(&self) -> ServerRequestKind {
        match self {
            Self::PushReplyMessageToClient { .. } => ServerRequestKind::PushReplyMessageToClient,
            Self::GetConsumerStatusFromClient { .. } => ServerRequestKind::GetConsumerStatusFromClient,
            Self::GetConsumerRunningInfo { .. } => ServerRequestKind::GetConsumerRunningInfo,
            Self::ConsumeMessageDirectly { .. } => ServerRequestKind::ConsumeMessageDirectly,
        }
    }

    fn into_command(self) -> RemotingCommand {
        let mut command = match self {
            Self::PushReplyMessageToClient { header, body } => {
                let command = RemotingCommand::create_request_command(RequestCode::PushReplyMessageToClient, header);
                match body {
                    Some(body) => command.set_body(body),
                    None => command,
                }
            }
            Self::GetConsumerStatusFromClient { header } => {
                RemotingCommand::create_request_command(RequestCode::GetConsumerStatusFromClient, header)
            }
            Self::GetConsumerRunningInfo { header } => {
                RemotingCommand::create_request_command(RequestCode::GetConsumerRunningInfo, header)
            }
            Self::ConsumeMessageDirectly { header, body } => {
                RemotingCommand::create_request_command(RequestCode::ConsumeMessageDirectly, header).set_body(body)
            }
        };
        command.make_custom_header_to_net();
        command
    }
}

/// Low-cardinality broker-to-client request kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServerRequestKind {
    /// Reply-message delivery.
    PushReplyMessageToClient,
    /// Consumer-status query.
    GetConsumerStatusFromClient,
    /// Consumer-running-info query.
    GetConsumerRunningInfo,
    /// Direct-consume request.
    ConsumeMessageDirectly,
}

/// Response correlated to one typed broker-to-client request.
///
/// The wrapper is response data only; it carries no session writer or close
/// authority. Consumers that must preserve protocol compatibility may recover
/// the correlated command with [`Self::into_command`].
pub struct ServerRequestResponse {
    session_id: SessionId,
    kind: ServerRequestKind,
    command: RemotingCommand,
}

impl std::fmt::Debug for ServerRequestResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerRequestResponse")
            .field("session_id", &self.session_id)
            .field("kind", &self.kind)
            .field("code", &self.command.code())
            .finish_non_exhaustive()
    }
}

impl ServerRequestResponse {
    /// Returns the canonical session that supplied the response.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns the typed request kind completed by this response.
    #[must_use]
    pub const fn kind(&self) -> ServerRequestKind {
        self.kind
    }

    /// Returns the protocol response code.
    #[must_use]
    pub fn code(&self) -> i32 {
        self.command.code()
    }

    /// Returns the protocol response remark when one was supplied.
    #[must_use]
    pub fn remark(&self) -> Option<&CheetahString> {
        self.command.remark()
    }

    /// Returns the encoded response body without transferring ownership.
    #[must_use]
    pub fn body(&self) -> Option<&Bytes> {
        self.command.body()
    }

    /// Recovers the correlated response command for compatibility forwarding.
    #[must_use]
    pub fn into_command(self) -> RemotingCommand {
        self.command
    }
}

/// Low-cardinality stage at which a server request failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServerRequestErrorStage {
    /// Pending-response admission or owner validation.
    Register,
    /// Canonical socket write.
    Write,
    /// Response correlation, disconnect, or absolute deadline.
    AwaitResponse,
}

/// Failure of a typed broker-to-client request.
#[derive(Debug, thiserror::Error)]
#[error("V2 server request failed during {stage:?}")]
pub struct ServerRequestError {
    session_id: SessionId,
    kind: ServerRequestKind,
    stage: ServerRequestErrorStage,
    #[source]
    source: RocketMQError,
}

impl ServerRequestError {
    /// Returns the canonical session selected for the request.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns the typed request kind that failed.
    #[must_use]
    pub const fn kind(&self) -> ServerRequestKind {
        self.kind
    }

    /// Returns the low-cardinality failure stage.
    #[must_use]
    pub const fn stage(&self) -> ServerRequestErrorStage {
        self.stage
    }

    /// Returns the underlying transport failure.
    #[must_use]
    pub const fn source_error(&self) -> &RocketMQError {
        &self.source
    }
}

/// Narrow request/response sender for one canonical V2 client session.
///
/// The sender can issue only [`ServerRequestCommand`] variants. It owns the
/// exact pending-response generation allocated for the physical session and
/// cannot expose its writer, pending table, or raw session handle.
#[derive(Clone)]
pub struct ServerRequestSender {
    session: SessionHandle,
    response_table: PendingRequestTable,
    response_owner: PendingRequestOwner,
}

struct ServerRequestFailClosedGuard {
    session: SessionHandle,
    response_table: PendingRequestTable,
    response_owner: PendingRequestOwner,
    armed: bool,
}

impl ServerRequestFailClosedGuard {
    fn new(sender: &ServerRequestSender) -> Self {
        Self {
            session: sender.session.clone(),
            response_table: sender.response_table.clone(),
            response_owner: sender.response_owner.clone(),
            armed: false,
        }
    }

    fn arm(&mut self) {
        self.armed = true;
    }

    fn complete(&mut self) {
        self.armed = false;
    }
}

impl Drop for ServerRequestFailClosedGuard {
    fn drop(&mut self) {
        if self.armed {
            self.response_table.retire_owner(&self.response_owner);
            self.session.abort();
        }
    }
}

fn write_failure_may_have_reached_socket(error: &RocketMQError) -> bool {
    match error {
        RocketMQError::Network(NetworkError::QueueFull { .. } | NetworkError::DeadlineExceededBeforeSend { .. }) => {
            false
        }
        RocketMQError::Network(_) | RocketMQError::Shared(_) => true,
        _ => false,
    }
}

impl ServerRequestSender {
    /// Returns the canonical session identity owned by this sender.
    #[must_use]
    pub fn session_id(&self) -> SessionId {
        self.session.session_view().id()
    }

    #[cfg(test)]
    pub(crate) fn pending_usage(&self) -> crate::base::pending_request_table::PendingRequestUsage {
        self.response_table.usage()
    }

    /// Sends one typed request and waits for its exactly correlated response.
    ///
    /// `timeout` is frozen into one absolute deadline before registration. The
    /// same deadline governs pending admission, canonical write, and response
    /// wait, so queueing cannot reset the caller's time budget. A response
    /// timeout terminates the canonical session because its correlation owner
    /// cannot be safely reused after a late response.
    ///
    /// # Errors
    ///
    /// Returns [`ServerRequestError`] when the session generation is closed,
    /// pending admission fails, the canonical write fails, the response is
    /// dropped, or the absolute deadline expires.
    pub async fn request(
        &self,
        command: ServerRequestCommand,
        timeout: Duration,
    ) -> Result<ServerRequestResponse, ServerRequestError> {
        let session_id = self.session_id();
        let kind = command.kind();
        let deadline = RequestDeadline::after(timeout);
        let mut command = command.into_command();
        let opaque = command.opaque();
        let retained_bytes = materialize_and_estimate_remoting_command_retained_bytes(&mut command);
        let (response_tx, mut response_rx) = tokio::sync::oneshot::channel();
        let guard = self
            .response_table
            .register_for_owner_with_bytes(&self.response_owner, opaque, deadline, retained_bytes, response_tx)
            .map_err(|source| ServerRequestError {
                session_id,
                kind,
                stage: ServerRequestErrorStage::Register,
                source,
            })?;
        let mut connection = self.session.connection();
        let mut fail_closed = ServerRequestFailClosedGuard::new(self);
        fail_closed.arm();
        let write_result = connection
            .send_command_with_deadline(command, deadline, "v2-server-request")
            .await;
        if let Err(source) = write_result {
            if !write_failure_may_have_reached_socket(&source) {
                fail_closed.complete();
            }
            return Err(ServerRequestError {
                session_id,
                kind,
                stage: ServerRequestErrorStage::Write,
                source,
            });
        }

        let response = match deadline.timeout(&mut response_rx).await {
            Ok(Ok(result)) => result.map_err(|source| ServerRequestError {
                session_id,
                kind,
                stage: ServerRequestErrorStage::AwaitResponse,
                source,
            })?,
            Ok(Err(_)) => {
                return Err(ServerRequestError {
                    session_id,
                    kind,
                    stage: ServerRequestErrorStage::AwaitResponse,
                    source: RocketMQError::network_connection_failed(
                        "v2-server-request",
                        "response correlation owner was dropped",
                    ),
                })
            }
            Err(_) => {
                let source = guard.expire("v2-server-request");
                self.session.terminate().await;
                return Err(ServerRequestError {
                    session_id,
                    kind,
                    stage: ServerRequestErrorStage::AwaitResponse,
                    source,
                });
            }
        };
        fail_closed.complete();
        Ok(ServerRequestResponse {
            session_id,
            kind,
            command: response,
        })
    }
}

/// Narrow close authority for one canonical V2 session.
///
/// This capability cannot send data, recover a writer, or expose a raw
/// [`SessionHandle`]. Graceful close reuses the transport session's existing
/// close owner.
#[derive(Clone)]
pub struct SessionCloseHandle {
    session: SessionHandle,
}

impl SessionCloseHandle {
    /// Returns the canonical session identity owned by this handle.
    #[must_use]
    pub fn session_id(&self) -> SessionId {
        self.session.session_view().id()
    }

    /// Gracefully retires the session writer.
    ///
    /// # Errors
    ///
    /// Returns [`SessionCloseError`] when the canonical writer cannot finish
    /// retirement before its transport-owned deadline.
    pub async fn close(&self, reason: SessionCloseReason) -> Result<(), SessionCloseError> {
        let session_id = self.session_id();
        self.session.retire().await.map_err(|source| SessionCloseError {
            session_id,
            reason,
            source,
        })
    }
}

/// Typed lifecycle observer installed by the server composition root.
///
/// Callbacks run synchronously after the registry has committed the matching
/// session-table mutation. Implementations must remain nonblocking and must
/// not re-enter this registry or retain transport authority; only read-only
/// session facts are exposed.
pub trait V2SessionLifecycleListener: Send + Sync + 'static {
    /// Observes a newly registered canonical network session.
    fn on_session_connected(&self, session: &SessionView);

    /// Observes removal of a canonical network session.
    fn on_session_disconnected(&self, session_id: SessionId);
}

/// Read-only lifecycle event emitted by a V2 server session registry.
#[derive(Clone)]
pub enum V2SessionEvent {
    /// A canonical network session became available to request dispatch.
    Connected(SessionView),
    /// The canonical network session stopped accepting inbound frames.
    Disconnected(SessionId),
}

struct RegisteredV2Session {
    handle: SessionHandle,
    requests: ServerRequestSender,
}

/// Narrow composition-root capability for observing and closing V2 sessions.
///
/// The registry retains transport authority privately. Request processors only
/// receive [`SessionView`], and lifecycle events never expose a writer,
/// connection, task group, cancellation token, or raw session handle.
pub struct V2SessionRegistry {
    sessions: DashMap<SessionId, RegisteredV2Session>,
    events: broadcast::Sender<V2SessionEvent>,
    lifecycle_listener: Option<Arc<dyn V2SessionLifecycleListener>>,
}

impl V2SessionRegistry {
    /// Creates an empty registry with a bounded best-effort event stream.
    #[must_use]
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            sessions: DashMap::new(),
            events,
            lifecycle_listener: None,
        }
    }

    /// Creates an empty registry with one synchronous typed lifecycle observer.
    #[must_use]
    pub fn with_lifecycle_listener(listener: Arc<dyn V2SessionLifecycleListener>) -> Self {
        let mut registry = Self::new();
        registry.lifecycle_listener = Some(listener);
        registry
    }

    /// Subscribes to typed connected and disconnected events.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<V2SessionEvent> {
        self.events.subscribe()
    }

    /// Returns whether a currently registered session owns `id`.
    #[must_use]
    pub fn contains(&self, id: SessionId) -> bool {
        self.sessions.contains_key(&id)
    }

    /// Returns the number of currently registered V2 network sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Returns whether no V2 network session is currently registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Resolves the independent push and close capabilities for `id`.
    ///
    /// Both values are derived from one registry lookup and retain the same
    /// canonical session owner. The raw session handle remains private.
    #[must_use]
    pub fn capabilities(&self, id: SessionId) -> Option<(ServerPushSender, SessionCloseHandle)> {
        let entry = self.sessions.get(&id)?;
        let session = &entry.value().handle;
        if !session.session_view().state().is_healthy() {
            return None;
        }
        let session = session.clone();
        Some((
            ServerPushSender {
                session: session.clone(),
            },
            SessionCloseHandle { session },
        ))
    }

    /// Resolves the typed broker-to-client request sender for `id`.
    ///
    /// The returned sender retains the exact response-correlation generation
    /// allocated for this physical session. It cannot expose the session
    /// writer or issue arbitrary request codes.
    #[must_use]
    pub fn server_request_sender(&self, id: SessionId) -> Option<ServerRequestSender> {
        self.sessions.get(&id).and_then(|entry| {
            let registered = entry.value();
            (registered.handle.session_view().state().is_healthy() && registered.requests.response_owner.is_accepting())
                .then(|| registered.requests.clone())
        })
    }

    /// Closes the session identified by `id` when it is still registered.
    ///
    /// Returns `false` when the session is already absent. A retirement error
    /// falls back to aborting the same canonical session before returning
    /// `true`, so the caller never receives raw transport authority.
    pub async fn close(&self, id: SessionId) -> bool {
        let Some(session) = self.sessions.get(&id).map(|entry| entry.value().handle.clone()) else {
            return false;
        };
        if session.retire().await.is_err() {
            session.abort();
        }
        true
    }

    /// Immediately cancels the session identified by `id`.
    ///
    /// This preserves synchronous compatibility boundaries such as periodic
    /// liveness scans. The canonical session owner still performs task drain
    /// and unregisters the session before server shutdown completes.
    pub fn close_now(&self, id: SessionId) -> bool {
        let Some(session) = self.sessions.get(&id).map(|entry| entry.value().handle.clone()) else {
            return false;
        };
        session.abort();
        true
    }

    pub(crate) fn register(
        &self,
        session: &SessionHandle,
        response_table: PendingRequestTable,
        response_owner: PendingRequestOwner,
    ) {
        let view = session.session_view();
        let id = view.id();
        self.sessions.insert(
            id,
            RegisteredV2Session {
                handle: session.clone(),
                requests: ServerRequestSender {
                    session: session.clone(),
                    response_table,
                    response_owner,
                },
            },
        );
        self.publish_connected(&view);
    }

    fn publish_connected(&self, view: &SessionView) {
        if let Some(listener) = &self.lifecycle_listener {
            listener.on_session_connected(view);
        }
        let _ = self.events.send(V2SessionEvent::Connected(view.clone()));
    }

    pub(crate) fn unregister(&self, session: &SessionHandle) {
        let id = session.session_view().id();
        if self.sessions.remove(&id).is_some() {
            self.publish_disconnected(id);
        }
    }

    fn publish_disconnected(&self, id: SessionId) {
        if let Some(listener) = &self.lifecycle_listener {
            listener.on_session_disconnected(id);
        }
        let _ = self.events.send(V2SessionEvent::Disconnected(id));
    }
}

impl Default for V2SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::session_view::EmbeddedSessionRecord;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ObservedEvent {
        Connected(SessionId),
        Disconnected(SessionId),
    }

    #[derive(Default)]
    struct RecordingListener {
        events: Mutex<Vec<ObservedEvent>>,
    }

    impl V2SessionLifecycleListener for RecordingListener {
        fn on_session_connected(&self, session: &SessionView) {
            self.events
                .lock()
                .expect("lifecycle event lock")
                .push(ObservedEvent::Connected(session.id()));
        }

        fn on_session_disconnected(&self, session_id: SessionId) {
            self.events
                .lock()
                .expect("lifecycle event lock")
                .push(ObservedEvent::Disconnected(session_id));
        }
    }

    #[test]
    fn lifecycle_listener_observes_each_committed_event_once() {
        let listener = Arc::new(RecordingListener::default());
        let registry = V2SessionRegistry::with_lifecycle_listener(listener.clone());
        let record = EmbeddedSessionRecord::new(9_851);
        let view = record.view();

        registry.publish_connected(&view);
        registry.publish_disconnected(view.id());

        assert_eq!(
            *listener.events.lock().expect("lifecycle event lock"),
            vec![
                ObservedEvent::Connected(view.id()),
                ObservedEvent::Disconnected(view.id()),
            ]
        );
    }
}
