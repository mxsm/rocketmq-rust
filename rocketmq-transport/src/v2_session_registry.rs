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

use dashmap::DashMap;
use tokio::sync::broadcast;

use crate::server::SessionHandle;
use crate::session_view::SessionId;
use crate::session_view::SessionView;

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

/// Narrow composition-root capability for observing and closing V2 sessions.
///
/// The registry retains transport authority privately. Request processors only
/// receive [`SessionView`], and lifecycle events never expose a writer,
/// connection, task group, cancellation token, or raw session handle.
pub struct V2SessionRegistry {
    sessions: DashMap<SessionId, SessionHandle>,
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

    /// Closes the session identified by `id` when it is still registered.
    ///
    /// Returns `false` when the session is already absent. A retirement error
    /// falls back to aborting the same canonical session before returning
    /// `true`, so the caller never receives raw transport authority.
    pub async fn close(&self, id: SessionId) -> bool {
        let Some(session) = self.sessions.get(&id).map(|entry| entry.value().clone()) else {
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
        let Some(session) = self.sessions.get(&id).map(|entry| entry.value().clone()) else {
            return false;
        };
        session.abort();
        true
    }

    pub(crate) fn register(&self, session: &SessionHandle) {
        let view = session.session_view();
        let id = view.id();
        self.sessions.insert(id, session.clone());
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
