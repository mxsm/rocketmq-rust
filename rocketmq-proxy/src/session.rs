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

//! Proxy session contracts and remoting capability binding.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;

use rocketmq_transport::api::ServerPushCommand;
use rocketmq_transport::api::ServerPushError;
use rocketmq_transport::api::ServerPushReceipt;
use rocketmq_transport::api::ServerPushSender;
use rocketmq_transport::api::SessionCloseError;
use rocketmq_transport::api::SessionCloseHandle;
use rocketmq_transport::api::SessionCloseReason;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionLifecycleListener;
use rocketmq_transport::api::SessionRegistry;
use rocketmq_transport::api::SessionView;

use crate::context::ProxyContext;

pub use rocketmq_proxy_core::session::build_lite_subscription_sync_request;
pub use rocketmq_proxy_core::session::ClientSession;
pub use rocketmq_proxy_core::session::ClientSettingsSnapshot;
pub use rocketmq_proxy_core::session::LiteSubscriptionSnapshot;
pub use rocketmq_proxy_core::session::LiteSubscriptionSyncRequest;
pub use rocketmq_proxy_core::session::PendingLiteUnsubscribeNotice;
pub use rocketmq_proxy_core::session::PendingTelemetryCommand;
pub use rocketmq_proxy_core::session::PreparedTransactionHandle;
pub use rocketmq_proxy_core::session::PreparedTransactionRegistration;
pub use rocketmq_proxy_core::session::ReapSummary;
pub use rocketmq_proxy_core::session::ReceiptHandleRegistration;
pub use rocketmq_proxy_core::session::SubscriptionSettingsSnapshot;
pub use rocketmq_proxy_core::session::TelemetryCommandKind;
pub use rocketmq_proxy_core::session::TelemetryLink;
pub use rocketmq_proxy_core::session::ThreadStackTraceReport;
pub use rocketmq_proxy_core::session::TrackedReceiptHandle;
pub use rocketmq_proxy_core::session::VerifyMessageReport;

/// Lifecycle-owned capabilities associated with one Proxy remoting client.
///
/// The value exposes only typed server push and close operations. It cannot
/// reveal a transport writer, connection, task group, raw channel, or session
/// handle.
#[derive(Clone)]
pub struct RemotingSessionCapability {
    session_id: SessionId,
    push: ServerPushSender,
    close: SessionCloseHandle,
}

impl RemotingSessionCapability {
    /// Returns the transport session identity behind this binding.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Sends one explicitly permitted server notification.
    ///
    /// # Errors
    ///
    /// Returns [`ServerPushError`] when the canonical session writer rejects
    /// encoding, admission, its deadline, or the socket write.
    pub async fn send(
        &self,
        command: ServerPushCommand,
        timeout: Duration,
    ) -> Result<ServerPushReceipt, ServerPushError> {
        self.push.send(command, timeout).await
    }

    /// Gracefully retires the bound transport session.
    ///
    /// # Errors
    ///
    /// Returns [`SessionCloseError`] when transport-owned retirement fails.
    pub async fn close(&self, reason: SessionCloseReason) -> Result<(), SessionCloseError> {
        self.close.close(reason).await
    }
}

/// Proxy session state with a narrow remoting capability specialization.
pub type ClientSessionRegistry = rocketmq_proxy_core::session::ClientSessionRegistry<RemotingSessionCapability>;

/// Typed instruction submitted by request processing after a valid heartbeat.
pub(crate) struct ClientSessionBindInstruction {
    client_id: String,
    session_id: SessionId,
    producer_groups: BTreeSet<String>,
    consumer_groups: BTreeSet<String>,
}

/// Result of atomically accepting one remoting heartbeat.
///
/// A replacement is returned with the generation that was removed from the
/// client binding table.  The caller must retire that exact capability only
/// after this result is returned, so network I/O never runs while the binding
/// mutex is held.
pub(crate) struct HeartbeatCommit {
    pub(crate) membership_changed: bool,
    pub(crate) retired: Option<RetiredRemotingSession>,
}

/// A live remoting session superseded by a newer heartbeat for the same
/// client identifier.
pub(crate) struct RetiredRemotingSession {
    binding: BindingGeneration,
    capability: RemotingSessionCapability,
    registry: Weak<SessionRegistry>,
}

impl RetiredRemotingSession {
    fn new(
        binding: BindingGeneration,
        capability: RemotingSessionCapability,
        registry: Weak<SessionRegistry>,
    ) -> Option<Self> {
        (capability.session_id() == binding.session_id).then_some(Self {
            binding,
            capability,
            registry,
        })
    }

    /// Retires the exact session generation that lost the client binding.
    ///
    /// A graceful retirement failure is contained here: the composition-root
    /// registry is asked to abort that same canonical session before the
    /// already-committed replacement heartbeat can complete.
    pub(crate) async fn retire(self) -> RetiredSessionRetirement {
        debug_assert_eq!(self.capability.session_id(), self.binding.session_id);
        if self
            .capability
            .close(SessionCloseReason::ClientBindingRetired)
            .await
            .is_ok()
        {
            return RetiredSessionRetirement::Graceful;
        }
        if self
            .registry
            .upgrade()
            .is_some_and(|registry| registry.close_now(self.binding.session_id))
        {
            RetiredSessionRetirement::Forced
        } else {
            RetiredSessionRetirement::AlreadyDisconnected
        }
    }
}

/// Low-cardinality completion state for a superseded session retirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetiredSessionRetirement {
    /// The canonical session completed graceful transport retirement.
    Graceful,
    /// Graceful retirement failed and the composition root aborted the same session.
    Forced,
    /// The session disappeared before the fallback could abort it.
    AlreadyDisconnected,
}

impl ClientSessionBindInstruction {
    pub(crate) fn new(
        client_id: impl Into<String>,
        session_id: SessionId,
        producer_groups: BTreeSet<String>,
        consumer_groups: BTreeSet<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            session_id,
            producer_groups,
            consumer_groups,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BindingGeneration {
    session_id: SessionId,
    generation: u64,
}

#[derive(Clone)]
struct PublishedRemotingBinding<C> {
    generation: BindingGeneration,
    producer_groups: BTreeSet<String>,
    consumer_groups: BTreeSet<String>,
    capability: C,
}

struct BindingState<C> {
    next_generation: u64,
    client_sessions: HashMap<String, BindingGeneration>,
    session_clients: HashMap<SessionId, HashMap<String, u64>>,
    retired_sessions: HashSet<SessionId>,
    published: HashMap<String, PublishedRemotingBinding<C>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnregisterBindingResult {
    Applied,
    Unbound,
    ForeignSession,
}

impl<C> Default for BindingState<C> {
    fn default() -> Self {
        Self {
            next_generation: 0,
            client_sessions: HashMap::new(),
            session_clients: HashMap::new(),
            retired_sessions: HashSet::new(),
            published: HashMap::new(),
        }
    }
}

impl<C> BindingState<C> {
    fn install(&mut self, client_id: &str, session_id: SessionId) -> Option<BindingGeneration> {
        self.next_generation = self.next_generation.checked_add(1)?;
        let binding = BindingGeneration {
            session_id,
            generation: self.next_generation,
        };
        if let Some(previous) = self.client_sessions.insert(client_id.to_owned(), binding) {
            let remove_reverse_entry = self
                .session_clients
                .get_mut(&previous.session_id)
                .is_some_and(|clients| {
                    clients.remove(client_id);
                    clients.is_empty()
                });
            if remove_reverse_entry {
                self.session_clients.remove(&previous.session_id);
            }
        }
        self.session_clients
            .entry(session_id)
            .or_default()
            .insert(client_id.to_owned(), binding.generation);
        Some(binding)
    }

    fn remove_session(&mut self, session_id: SessionId) -> Vec<String> {
        self.retired_sessions.remove(&session_id);
        let Some(clients) = self.session_clients.remove(&session_id) else {
            return Vec::new();
        };
        clients
            .into_iter()
            .filter_map(|(client_id, generation)| {
                let expected = BindingGeneration { session_id, generation };
                if self.client_sessions.get(client_id.as_str()) != Some(&expected) {
                    return None;
                }
                self.client_sessions.remove(client_id.as_str());
                self.published.remove(client_id.as_str());
                Some(client_id)
            })
            .collect()
    }

    fn remove_client(&mut self, client_id: &str) {
        let Some(binding) = self.client_sessions.remove(client_id) else {
            self.published.remove(client_id);
            return;
        };
        let remove_reverse_entry = self
            .session_clients
            .get_mut(&binding.session_id)
            .is_some_and(|clients| {
                clients.remove(client_id);
                clients.is_empty()
            });
        if remove_reverse_entry {
            self.session_clients.remove(&binding.session_id);
        }
        self.published.remove(client_id);
    }

    fn publish(
        &mut self,
        client_id: String,
        generation: BindingGeneration,
        producer_groups: BTreeSet<String>,
        consumer_groups: BTreeSet<String>,
        capability: C,
    ) -> Option<PublishedRemotingBinding<C>> {
        debug_assert_eq!(self.client_sessions.get(client_id.as_str()), Some(&generation));
        self.published.insert(
            client_id,
            PublishedRemotingBinding {
                generation,
                producer_groups,
                consumer_groups,
                capability,
            },
        )
    }

    fn unregister_groups(&mut self, client_id: &str, producer_group: Option<&str>, consumer_group: Option<&str>) {
        if producer_group.is_none() && consumer_group.is_none() {
            self.remove_client(client_id);
            return;
        }
        let Some(binding) = self.published.get_mut(client_id) else {
            return;
        };
        if let Some(group) = producer_group {
            binding.producer_groups.remove(group);
        }
        if let Some(group) = consumer_group {
            binding.consumer_groups.remove(group);
        }
    }

    fn unregister_groups_if_owned(
        &mut self,
        client_id: &str,
        caller_session_id: SessionId,
        producer_group: Option<&str>,
        consumer_group: Option<&str>,
    ) -> UnregisterBindingResult {
        let Some(binding) = self.client_sessions.get(client_id) else {
            return UnregisterBindingResult::Unbound;
        };
        if binding.session_id != caller_session_id {
            return UnregisterBindingResult::ForeignSession;
        }
        self.unregister_groups(client_id, producer_group, consumer_group);
        UnregisterBindingResult::Applied
    }
}

impl<C: Clone> BindingState<C> {
    fn consumer_bindings(&self, consumer_group: &str) -> Vec<(String, C)> {
        let mut bindings = self
            .published
            .iter()
            .filter(|(_, binding)| binding.consumer_groups.contains(consumer_group))
            .map(|(client_id, binding)| (client_id.clone(), binding.capability.clone()))
            .collect::<Vec<_>>();
        bindings.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        bindings
    }

    fn consumer_binding(&self, client_id: &str, consumer_group: &str) -> Option<C> {
        self.published.get(client_id).and_then(|binding| {
            binding
                .consumer_groups
                .contains(consumer_group)
                .then(|| binding.capability.clone())
        })
    }
}

/// Independent lifecycle binder used by the processor and session registry.
///
/// Request processing can only submit [`ClientSessionBindInstruction`]. The
/// registry lookup and capability acquisition remain private to this binder.
#[derive(Clone)]
pub(crate) struct ProxySessionBinder {
    sessions: ClientSessionRegistry,
    transport_registry: Arc<OnceLock<Weak<SessionRegistry>>>,
    state: Arc<Mutex<BindingState<RemotingSessionCapability>>>,
}

impl ProxySessionBinder {
    pub(crate) fn new(sessions: ClientSessionRegistry) -> Self {
        Self {
            sessions,
            transport_registry: Arc::new(OnceLock::new()),
            state: Arc::new(Mutex::new(BindingState::default())),
        }
    }

    pub(crate) fn attach(&self, registry: &Arc<SessionRegistry>) -> bool {
        self.transport_registry.set(Arc::downgrade(registry)).is_ok()
    }

    /// Atomically commits heartbeat membership and its live remoting capability.
    ///
    /// Disconnect processing takes the same state lock and removes only the
    /// exact binding generation it observed. A late disconnect from a replaced
    /// session therefore cannot delete the replacement's membership.
    pub(crate) fn commit_heartbeat(
        &self,
        context: &ProxyContext,
        instruction: ClientSessionBindInstruction,
    ) -> Option<HeartbeatCommit> {
        let registry = self.transport_registry.get().and_then(Weak::upgrade)?;
        let (push, close) = registry.capabilities(instruction.session_id)?;

        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !registry.contains(instruction.session_id) || state.retired_sessions.contains(&instruction.session_id) {
            return None;
        }
        let replaced = state
            .client_sessions
            .get(instruction.client_id.as_str())
            .copied()
            .filter(|binding| binding.session_id != instruction.session_id);
        let generation = state.install(instruction.client_id.as_str(), instruction.session_id)?;
        if let Some(binding) = replaced {
            state.retired_sessions.insert(binding.session_id);
        }
        let client_id = instruction.client_id;
        let producer_groups = instruction.producer_groups;
        let consumer_groups = instruction.consumer_groups;
        let changed = self.sessions.update_membership_from_remoting_heartbeat(
            context,
            client_id.as_str(),
            producer_groups.clone(),
            consumer_groups.clone(),
        );
        let capability = RemotingSessionCapability {
            session_id: instruction.session_id,
            push,
            close,
        };
        self.sessions
            .bind_remoting_channel(client_id.clone(), capability.clone());
        let previous = state.publish(client_id, generation, producer_groups, consumer_groups, capability);
        let retired = replaced.and_then(|binding| {
            previous
                .filter(|previous| previous.generation == binding)
                .and_then(|previous| {
                    RetiredRemotingSession::new(binding, previous.capability, Arc::downgrade(&registry))
                })
        });
        Some(HeartbeatCommit {
            membership_changed: changed,
            retired,
        })
    }

    pub(crate) fn unregister_client_groups_for_session(
        &self,
        client_id: &str,
        caller_session_id: SessionId,
        producer_group: Option<&str>,
        consumer_group: Option<&str>,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.unregister_groups_if_owned(client_id, caller_session_id, producer_group, consumer_group) {
            UnregisterBindingResult::Applied => {
                self.sessions
                    .unregister_client_groups(client_id, producer_group, consumer_group);
                true
            }
            UnregisterBindingResult::Unbound => true,
            UnregisterBindingResult::ForeignSession => false,
        }
    }

    pub(crate) fn consumer_bindings(&self, consumer_group: &str) -> Vec<(String, RemotingSessionCapability)> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .consumer_bindings(consumer_group)
    }

    pub(crate) fn consumer_binding(&self, client_id: &str, consumer_group: &str) -> Option<RemotingSessionCapability> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .consumer_binding(client_id, consumer_group)
    }

    fn unbind_session(&self, session_id: SessionId) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for client_id in state.remove_session(session_id) {
            if self
                .sessions
                .remoting_channel(client_id.as_str())
                .is_some_and(|binding| binding.session_id() == session_id)
            {
                self.sessions.remove_client(client_id.as_str());
            }
        }
    }
}

impl SessionLifecycleListener for ProxySessionBinder {
    fn on_session_connected(&self, _session: &SessionView) {}

    fn on_session_disconnected(&self, session_id: SessionId) {
        self.unbind_session(session_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use rocketmq_transport::test_support::session_id_for_test;

    use super::*;

    #[test]
    fn disconnect_waiting_at_commit_checkpoint_removes_the_committed_generation() {
        let state = Arc::new(Mutex::new(BindingState::<()>::default()));
        let commit_reached = Arc::new(Barrier::new(2));
        let release_commit = Arc::new(Barrier::new(2));
        let disconnect_started = Arc::new(Barrier::new(2));
        let session_id = session_id_for_test(101);

        let commit = {
            let state = Arc::clone(&state);
            let commit_reached = Arc::clone(&commit_reached);
            let release_commit = Arc::clone(&release_commit);
            thread::spawn(move || {
                let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state
                    .install("client-a", session_id)
                    .expect("generation should be available");
                commit_reached.wait();
                release_commit.wait();
            })
        };
        commit_reached.wait();
        let disconnect = {
            let state = Arc::clone(&state);
            let disconnect_started = Arc::clone(&disconnect_started);
            thread::spawn(move || {
                disconnect_started.wait();
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove_session(session_id)
            })
        };
        disconnect_started.wait();
        release_commit.wait();

        commit.join().expect("commit checkpoint thread should join");
        assert_eq!(
            disconnect.join().expect("disconnect checkpoint thread should join"),
            ["client-a"]
        );
        let state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.client_sessions.is_empty());
        assert!(state.session_clients.is_empty());
    }

    #[test]
    fn old_disconnect_waiting_at_replacement_checkpoint_preserves_new_generation() {
        let old_session = session_id_for_test(201);
        let new_session = session_id_for_test(202);
        let mut initial = BindingState::<()>::default();
        initial
            .install("client-a", old_session)
            .expect("old generation should be available");
        let state = Arc::new(Mutex::new(initial));
        let replacement_reached = Arc::new(Barrier::new(2));
        let release_replacement = Arc::new(Barrier::new(2));
        let disconnect_started = Arc::new(Barrier::new(2));

        let replacement = {
            let state = Arc::clone(&state);
            let replacement_reached = Arc::clone(&replacement_reached);
            let release_replacement = Arc::clone(&release_replacement);
            thread::spawn(move || {
                let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state
                    .install("client-a", new_session)
                    .expect("replacement generation should be available");
                replacement_reached.wait();
                release_replacement.wait();
            })
        };
        replacement_reached.wait();
        let disconnect = {
            let state = Arc::clone(&state);
            let disconnect_started = Arc::clone(&disconnect_started);
            thread::spawn(move || {
                disconnect_started.wait();
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove_session(old_session)
            })
        };
        disconnect_started.wait();
        release_replacement.wait();

        replacement.join().expect("replacement checkpoint thread should join");
        assert!(
            disconnect
                .join()
                .expect("old disconnect checkpoint thread should join")
                .is_empty(),
            "old session must not own the replacement generation"
        );
        let state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            state.client_sessions.get("client-a").map(|binding| binding.session_id),
            Some(new_session)
        );
        assert!(state.session_clients.contains_key(&new_session));
        assert!(!state.session_clients.contains_key(&old_session));
    }

    #[test]
    fn published_consumer_snapshot_waits_for_complete_replacement() {
        let old_session = session_id_for_test(301);
        let new_session = session_id_for_test(302);
        let mut initial = BindingState::<u64>::default();
        let old_generation = initial
            .install("client-a", old_session)
            .expect("old generation should be available");
        initial.publish(
            "client-a".to_owned(),
            old_generation,
            BTreeSet::new(),
            BTreeSet::from(["GroupA".to_owned()]),
            1,
        );
        assert_eq!(initial.consumer_binding("client-a", "GroupA"), Some(1));

        let state = Arc::new(Mutex::new(initial));
        let replacement_reached = Arc::new(Barrier::new(2));
        let reader_started = Arc::new(Barrier::new(2));
        let release_replacement = Arc::new(Barrier::new(2));

        let replacement = {
            let state = Arc::clone(&state);
            let replacement_reached = Arc::clone(&replacement_reached);
            let release_replacement = Arc::clone(&release_replacement);
            thread::spawn(move || {
                let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let new_generation = state
                    .install("client-a", new_session)
                    .expect("replacement generation should be available");
                replacement_reached.wait();
                release_replacement.wait();
                let previous = state.publish(
                    "client-a".to_owned(),
                    new_generation,
                    BTreeSet::new(),
                    BTreeSet::from(["GroupA".to_owned()]),
                    2,
                );
                assert_eq!(previous.map(|binding| binding.generation), Some(old_generation));
            })
        };
        replacement_reached.wait();
        let reader = {
            let state = Arc::clone(&state);
            let reader_started = Arc::clone(&reader_started);
            thread::spawn(move || {
                reader_started.wait();
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .consumer_binding("client-a", "GroupA")
            })
        };
        reader_started.wait();
        release_replacement.wait();

        replacement.join().expect("replacement checkpoint thread should join");
        assert_eq!(
            reader.join().expect("snapshot reader should join"),
            Some(2),
            "a callback snapshot must not pair replacement membership with the retired capability"
        );
    }

    #[test]
    fn late_unregister_from_replaced_session_cannot_remove_new_generation() {
        let old_session = session_id_for_test(401);
        let new_session = session_id_for_test(402);
        let mut state = BindingState::<u64>::default();
        let old_generation = state
            .install("client-a", old_session)
            .expect("old generation should be available");
        state.publish(
            "client-a".to_owned(),
            old_generation,
            BTreeSet::new(),
            BTreeSet::from(["GroupA".to_owned()]),
            1,
        );
        let new_generation = state
            .install("client-a", new_session)
            .expect("replacement generation should be available");
        state.publish(
            "client-a".to_owned(),
            new_generation,
            BTreeSet::new(),
            BTreeSet::from(["GroupA".to_owned()]),
            2,
        );

        assert_eq!(
            state.unregister_groups_if_owned("client-a", old_session, None, Some("GroupA")),
            UnregisterBindingResult::ForeignSession
        );
        assert_eq!(state.consumer_binding("client-a", "GroupA"), Some(2));
        assert_eq!(state.client_sessions.get("client-a"), Some(&new_generation));

        assert_eq!(
            state.unregister_groups_if_owned("client-a", new_session, None, Some("GroupA")),
            UnregisterBindingResult::Applied
        );
        assert_eq!(state.consumer_binding("client-a", "GroupA"), None);
    }
}
