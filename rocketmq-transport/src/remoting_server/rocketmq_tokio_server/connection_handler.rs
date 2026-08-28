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

use super::lifecycle_events::LifecycleEventPublisher;
use super::*;
use crate::admission::PartialFramePermit;
use crate::dispatch::AuthorizedCommandDispatcherV2;
use crate::dispatch::AuthorizedDispatchSession;
use crate::dispatch::LegacyNetworkSession;
use crate::dispatch::RequestContext;
use crate::runtime::processor_v2::RequestProcessorV2;
use crate::server::AuthorizedFrameRoute;

#[cfg(all(test, not(doctest)))]
pub(super) enum TestRequestHookResult {
    Continue,
    Intercept,
}

#[cfg(all(test, not(doctest)))]
pub(super) type TestDeferredResponse = Box<
    dyn FnOnce(
            rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send,
>;

#[cfg(all(test, not(doctest)))]
pub(super) type TestRequestHook =
    Arc<dyn Fn(i32, i32, Channel, TaskGroup, TestDeferredResponse) -> TestRequestHookResult + Send + Sync>;

#[cfg(all(test, not(doctest)))]
pub(super) trait SessionCommandInterceptor: Send + Sync + 'static {
    fn intercept(&self, code: i32, opaque: i32, channel: Channel, request_executor_group: TaskGroup) -> bool;
}

pub(super) struct ConnectionHandler<RP> {
    pub(super) shutdown_complete_tx: mpsc::Sender<()>,
    pub(super) conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    pub(super) dispatcher: Arc<AuthorizedCommandDispatcher<RP>>,
    pub(super) event_publisher: Option<LifecycleEventPublisher>,
    pub(super) sessions: dashmap::DashMap<u64, RemotingSession<ConnectionHandlerContext>>,
}

#[cfg(all(test, not(doctest)))]
pub(super) struct InterceptingConnectionHandler<RP> {
    pub(super) inner: ConnectionHandler<RP>,
    pub(super) command_interceptor: Arc<dyn SessionCommandInterceptor>,
}

pub(super) struct RemotingSession<C> {
    context: C,
    endpoint: LegacyNetworkSession,
    _shutdown_complete: mpsc::Sender<()>,
}

pub(crate) struct V1NetworkRouteState {
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "test interceptor observes the canonical V1 channel")
    )]
    context: ConnectionHandlerContext,
    endpoint: LegacyNetworkSession,
    deferred_cleanup: crate::dispatch::DeferredSessionCleanupOwner,
}

pub(super) struct V2ConnectionHandler<P> {
    pub(super) shutdown_complete_tx: mpsc::Sender<()>,
    pub(super) conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    pub(super) dispatcher: Arc<AuthorizedCommandDispatcherV2<P>>,
    pub(super) session_registry: Option<Arc<crate::v2_session_registry::V2SessionRegistry>>,
}

pub(crate) struct V2NetworkRouteState {
    _shutdown_complete: mpsc::Sender<()>,
    deferred_cleanup: crate::dispatch::DeferredSessionCleanupOwner,
}

impl<RP: RequestProcessor + Sync + Clone + 'static> ConnectionHandler<RP> {
    async fn connected_state(&self, session: crate::server::SessionHandle) -> Option<V1NetworkRouteState> {
        let endpoint = self.dispatcher.open_network_session();
        let channel_inner = ChannelInner::new_transport_session_with_owner(
            session.connection(),
            endpoint.response_table().clone(),
            endpoint.owner().clone(),
            session.task_group().clone(),
        )
        .ok()?;
        let mut channel = Channel::new_with_proxy_protocol(
            Arc::new(channel_inner),
            session.local_addr(),
            session.remote_addr(),
            session.transport_peer_addr(),
            session.proxy_protocol().cloned(),
        );
        channel.set_channel_id(format!("transport-session-{}", session.session_id()));
        let context = Arc::new(ConnectionHandlerContextWrapper::new(channel));
        let remoting_session = RemotingSession {
            context: context.clone(),
            endpoint: endpoint.clone(),
            _shutdown_complete: self.shutdown_complete_tx.clone(),
        };
        let event_channel = context.channel().clone();
        self.sessions.insert(session.session_id(), remoting_session);
        if let Some(publisher) = &self.event_publisher {
            let outcome = publisher
                .publish(TokioEvent::new(
                    ConnectionNetEvent::CONNECTED(session.remote_addr()),
                    session.remote_addr(),
                    event_channel,
                ))
                .await;
            if !outcome.is_queued() {
                warn!(?outcome, event = "connected", "Remoting lifecycle event was not queued");
            }
        }
        Some(V1NetworkRouteState {
            context,
            endpoint,
            deferred_cleanup: crate::dispatch::DeferredSessionCleanupOwner::new(session.session_view().id()),
        })
    }

    async fn request(
        &self,
        state: &V1NetworkRouteState,
        authorized_session: &AuthorizedDispatchSession,
        session: crate::server::SessionHandle,
        context: RequestContext,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
        #[cfg(all(test, not(doctest)))] command_interceptor: Option<&dyn SessionCommandInterceptor>,
    ) -> bool {
        #[cfg(all(test, not(doctest)))]
        if let Some(command_interceptor) = command_interceptor {
            if command_interceptor.intercept(
                command.code(),
                command.opaque(),
                state.context.channel().clone(),
                session.task_group().clone(),
            ) {
                return true;
            }
        }
        self.dispatcher
            .dispatch_network(
                authorized_session,
                state.endpoint.clone(),
                session,
                context,
                command,
                received_at,
                retained_bytes,
                partial_frame_permit,
                state.deferred_cleanup.registration(),
            )
            .await
            .is_ok()
    }

    async fn disconnected_state(&self, state: V1NetworkRouteState, session: crate::server::SessionHandle) {
        let event_publisher = self.event_publisher.clone();
        let conn_disconnect_notify = self.conn_disconnect_notify.clone();
        let Some((_, remoting_session)) = self.sessions.remove(&session.session_id()) else {
            return;
        };
        debug_assert!(remoting_session.endpoint.owner().same_owner(state.endpoint.owner()));
        let channel_report = remoting_session
            .context
            .channel()
            .close_with_report(Duration::from_secs(3))
            .await;
        channel_report.log_if_unhealthy();
        if let Some(notify) = conn_disconnect_notify {
            let _ = notify.send(session.remote_addr());
        }
        if let Some(publisher) = event_publisher {
            let outcome = publisher
                .publish(TokioEvent::new(
                    ConnectionNetEvent::DISCONNECTED,
                    session.remote_addr(),
                    remoting_session.context.channel().clone(),
                ))
                .await;
            if !outcome.is_queued() {
                warn!(
                    ?outcome,
                    event = "disconnected",
                    "Remoting lifecycle event was not queued"
                );
            }
        }
    }
}

impl<RP: RequestProcessor + Sync + Clone + 'static> AuthorizedFrameRoute for ConnectionHandler<RP> {
    type SessionState = V1NetworkRouteState;

    async fn connected(&self, session: crate::server::SessionHandle) -> Option<Self::SessionState> {
        self.connected_state(session).await
    }

    async fn response(
        &self,
        state: &Self::SessionState,
        _session: crate::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) {
        self.dispatcher.complete_network_response(&state.endpoint, command);
    }

    async fn request(
        &self,
        state: &Self::SessionState,
        authorized_session: &AuthorizedDispatchSession,
        session: crate::server::SessionHandle,
        context: RequestContext,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
    ) -> bool {
        self.request(
            state,
            authorized_session,
            session,
            context,
            command,
            received_at,
            retained_bytes,
            partial_frame_permit,
            #[cfg(all(test, not(doctest)))]
            None,
        )
        .await
    }

    fn close_pending(&self, state: &Self::SessionState, _session: crate::server::SessionHandle) {
        let _ = state.deferred_cleanup.close();
        self.dispatcher.close_network_session(&state.endpoint);
    }

    async fn disconnected(&self, state: Self::SessionState, session: crate::server::SessionHandle) {
        self.disconnected_state(state, session).await;
    }
}

impl<P> AuthorizedFrameRoute for V2ConnectionHandler<P>
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    type SessionState = V2NetworkRouteState;

    async fn connected(&self, session: crate::server::SessionHandle) -> Option<Self::SessionState> {
        if let Some(registry) = &self.session_registry {
            registry.register(&session);
        }
        Some(V2NetworkRouteState {
            _shutdown_complete: self.shutdown_complete_tx.clone(),
            deferred_cleanup: crate::dispatch::DeferredSessionCleanupOwner::new(session.session_view().id()),
        })
    }

    async fn response(
        &self,
        _state: &Self::SessionState,
        _session: crate::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) {
        self.dispatcher.complete_network_response(command);
    }

    async fn request(
        &self,
        state: &Self::SessionState,
        authorized_session: &AuthorizedDispatchSession,
        session: crate::server::SessionHandle,
        context: RequestContext,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
    ) -> bool {
        self.dispatcher
            .dispatch_network(
                authorized_session,
                session,
                context,
                command,
                received_at,
                retained_bytes,
                partial_frame_permit,
                state.deferred_cleanup.registration(),
            )
            .await
            .is_ok()
    }

    fn close_pending(&self, state: &Self::SessionState, _session: crate::server::SessionHandle) {
        self.dispatcher.close_network_session();
        let _ = state.deferred_cleanup.close();
    }

    async fn disconnected(&self, _state: Self::SessionState, session: crate::server::SessionHandle) {
        if let Some(registry) = &self.session_registry {
            registry.unregister(&session);
        }
        if let Some(notify) = &self.conn_disconnect_notify {
            let _ = notify.send(session.remote_addr());
        }
    }
}

#[cfg(all(test, not(doctest)))]
impl<RP: RequestProcessor + Sync + Clone + 'static> AuthorizedFrameRoute for InterceptingConnectionHandler<RP> {
    type SessionState = V1NetworkRouteState;

    async fn connected(&self, session: crate::server::SessionHandle) -> Option<Self::SessionState> {
        self.inner.connected_state(session).await
    }

    async fn response(
        &self,
        state: &Self::SessionState,
        _session: crate::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) {
        self.inner
            .dispatcher
            .complete_network_response(&state.endpoint, command);
    }

    async fn request(
        &self,
        state: &Self::SessionState,
        authorized_session: &AuthorizedDispatchSession,
        session: crate::server::SessionHandle,
        context: RequestContext,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        received_at: Instant,
        retained_bytes: usize,
        partial_frame_permit: Option<PartialFramePermit>,
    ) -> bool {
        self.inner
            .request(
                state,
                authorized_session,
                session,
                context,
                command,
                received_at,
                retained_bytes,
                partial_frame_permit,
                Some(self.command_interceptor.as_ref()),
            )
            .await
    }

    fn close_pending(&self, state: &Self::SessionState, _session: crate::server::SessionHandle) {
        let _ = state.deferred_cleanup.close();
        self.inner.dispatcher.close_network_session(&state.endpoint);
    }

    async fn disconnected(&self, state: Self::SessionState, session: crate::server::SessionHandle) {
        self.inner.disconnected_state(state, session).await;
    }
}
