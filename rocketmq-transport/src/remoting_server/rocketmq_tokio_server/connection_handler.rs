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
    _shutdown_complete: mpsc::Sender<()>,
}

enum RemotingSessionAction {
    Connect,
    Command(rocketmq_protocol::protocol::remoting_command::RemotingCommand),
}

impl<RP: RequestProcessor + Sync + Clone + 'static> ConnectionHandler<RP> {
    async fn run(
        &self,
        session: crate::server::SessionHandle,
        action: RemotingSessionAction,
        #[cfg(all(test, not(doctest)))] command_interceptor: Option<&dyn SessionCommandInterceptor>,
    ) {
        let channel_id = match &action {
            RemotingSessionAction::Connect => format!("transport-session-{}", session.session_id()),
            RemotingSessionAction::Command(_) => {
                let Some(channel_id) = self
                    .sessions
                    .get(&session.session_id())
                    .map(|remoting_session| remoting_session.context.channel().channel_id().to_owned())
                else {
                    return;
                };
                channel_id
            }
        };
        let channel_inner = match &action {
            RemotingSessionAction::Connect => ChannelInner::new_transport_session(
                session.connection(),
                self.dispatcher.response_table(),
                session.task_group().clone(),
            ),
            RemotingSessionAction::Command(_) => ChannelInner::new_transport_session_with_task_group(
                session.connection(),
                self.dispatcher.response_table(),
                session.task_group().clone(),
            ),
        };
        let Ok(channel_inner) = channel_inner else {
            return;
        };
        let mut channel = Channel::new_with_proxy_protocol(
            Arc::new(channel_inner),
            session.local_addr(),
            session.remote_addr(),
            session.transport_peer_addr(),
            session.proxy_protocol().cloned(),
        );
        channel.set_channel_id(channel_id);
        let remoting_session = RemotingSession {
            context: Arc::new(ConnectionHandlerContextWrapper::new(channel)),
            _shutdown_complete: self.shutdown_complete_tx.clone(),
        };
        match action {
            RemotingSessionAction::Connect => {
                let event_channel = remoting_session.context.channel().clone();
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
            }
            RemotingSessionAction::Command(command) => {
                #[cfg(all(test, not(doctest)))]
                if let Some(command_interceptor) = command_interceptor {
                    if command_interceptor.intercept(
                        command.code(),
                        command.opaque(),
                        remoting_session.context.channel().clone(),
                        session.task_group().clone(),
                    ) {
                        return;
                    }
                }
                let dispatcher = self.dispatcher.clone();
                dispatcher.process_network(&remoting_session.context, command).await;
            }
        }
    }
}

impl<RP: RequestProcessor + Sync + Clone + 'static> TransportConnectionHandler for ConnectionHandler<RP> {
    fn connected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.run(
                session,
                RemotingSessionAction::Connect,
                #[cfg(all(test, not(doctest)))]
                None,
            )
            .await;
        })
    }

    fn request_ordering(
        &self,
        command: &rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> crate::request_ordering::RequestOrdering {
        self.dispatcher.request_ordering(command)
    }

    fn command(
        &self,
        session: crate::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.run(
                session,
                RemotingSessionAction::Command(command),
                #[cfg(all(test, not(doctest)))]
                None,
            )
            .await;
        })
    }

    fn disconnected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let event_publisher = self.event_publisher.clone();
        let conn_disconnect_notify = self.conn_disconnect_notify.clone();
        Box::pin(async move {
            let Some((_, remoting_session)) = self.sessions.remove(&session.session_id()) else {
                return;
            };
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
        })
    }
}

#[cfg(all(test, not(doctest)))]
impl<RP: RequestProcessor + Sync + Clone + 'static> TransportConnectionHandler for InterceptingConnectionHandler<RP> {
    fn connected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.connected(session)
    }

    fn request_ordering(
        &self,
        command: &rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> crate::request_ordering::RequestOrdering {
        self.inner.request_ordering(command)
    }

    fn command(
        &self,
        session: crate::server::SessionHandle,
        command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.inner
                .run(
                    session,
                    RemotingSessionAction::Command(command),
                    Some(self.command_interceptor.as_ref()),
                )
                .await;
        })
    }

    fn disconnected(&self, session: crate::server::SessionHandle) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.disconnected(session)
    }
}
