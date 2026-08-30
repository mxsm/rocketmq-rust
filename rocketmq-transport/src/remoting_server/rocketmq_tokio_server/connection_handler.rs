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

use super::*;
use crate::admission::PartialFramePermit;
use crate::dispatch::AuthorizedCommandDispatcher;
use crate::dispatch::AuthorizedDispatchSession;
use crate::dispatch::NetworkSession;
use crate::dispatch::RequestContext;
use crate::runtime::processor::RequestProcessor;
use crate::server::AuthorizedFrameRoute;

pub(super) struct ConnectionHandler<P> {
    pub(super) shutdown_complete_tx: mpsc::Sender<()>,
    pub(super) conn_disconnect_notify: Option<broadcast::Sender<SocketAddr>>,
    pub(super) dispatcher: Arc<AuthorizedCommandDispatcher<P>>,
    pub(super) session_registry: Option<Arc<crate::session_registry::SessionRegistry>>,
}

pub(crate) struct NetworkRouteState {
    _shutdown_complete: mpsc::Sender<()>,
    endpoint: NetworkSession,
    deferred_cleanup: crate::dispatch::DeferredSessionCleanupOwner,
    #[cfg(test)]
    _panicking_cleanup: Option<crate::dispatch::SessionCleanupEnrollment>,
}

impl<P> AuthorizedFrameRoute for ConnectionHandler<P>
where
    P: RequestProcessor + Clone + Sync + 'static,
{
    type SessionState = NetworkRouteState;

    async fn connected(&self, session: crate::server::SessionHandle) -> Option<Self::SessionState> {
        let endpoint = self.dispatcher.open_network_session();
        let deferred_cleanup = crate::dispatch::DeferredSessionCleanupOwner::new(session.session_view().id());
        #[cfg(test)]
        let panicking_cleanup = if self
            .session_registry
            .as_ref()
            .is_some_and(|registry| registry.take_cleanup_panic_for_test())
        {
            let capability = crate::dispatch::SessionCleanupCapability::new(deferred_cleanup.registration());
            let mut enrollment = None;
            capability
                .install(
                    || panic!("test session cleanup panic"),
                    |cleanup| {
                        enrollment = Some(cleanup);
                        Ok::<_, ((), crate::dispatch::SessionCleanupEnrollment)>(())
                    },
                )
                .expect("test cleanup panic enrollment");
            enrollment
        } else {
            None
        };
        if let Some(registry) = &self.session_registry {
            if !registry.register(&session, endpoint.response_table().clone(), endpoint.owner().clone()) {
                return None;
            }
        }
        Some(NetworkRouteState {
            _shutdown_complete: self.shutdown_complete_tx.clone(),
            endpoint,
            deferred_cleanup,
            #[cfg(test)]
            _panicking_cleanup: panicking_cleanup,
        })
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

    fn close_pending(
        &self,
        state: &Self::SessionState,
        _session: crate::server::SessionHandle,
    ) -> crate::dispatch::DeferredSessionCleanupReport {
        let report = state.deferred_cleanup.close();
        self.dispatcher.close_network_session(&state.endpoint);
        report
    }

    async fn disconnected(&self, state: Self::SessionState, session: crate::server::SessionHandle) -> usize {
        let cleanup = state.deferred_cleanup.clone();
        drop(state);
        if let Some(registry) = &self.session_registry {
            registry.unregister(&session);
        }
        if let Some(notify) = &self.conn_disconnect_notify {
            let _ = notify.send(session.remote_addr());
        }
        cleanup.remaining_wait_permits()
    }
}
