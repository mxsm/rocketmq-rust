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

//! Immutable request metadata and observer-only request control.

use std::any::Any;
use std::any::TypeId;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use rocketmq_runtime::CancellationObserver;
use rocketmq_runtime::OperationContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupId;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use tokio_util::sync::CancellationToken;

use crate::connection::Connection;
use crate::connection::ConnectionStateHandle;
use crate::deadline::RequestDeadline;
use crate::dispatch::remoting_request::DeferredSlot;
use crate::dispatch::remoting_request::RequestLifecycleProvenance;
use crate::dispatch::request_context::RequestContextParts;
use crate::dispatch::AuthenticationState;
use crate::dispatch::EmbeddedCaller;
use crate::dispatch::IngressRequestView;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RemotingRequest;
use crate::dispatch::RequestContext;
use crate::dispatch::RequestId;
use crate::dispatch::RequestOrigin;
use crate::dispatch::RequestTransport;
use crate::dispatch::ResponseSink;
use crate::net::channel::ArcChannel;
use crate::net::channel::Channel;
use crate::net::channel::ChannelInner;
use crate::server::SessionHandle;
use crate::session_executor::SessionExecutor;
use crate::session_view::ProxyInfoSnapshot;
use crate::session_view::SessionId;
use crate::session_view::SessionStateView;
use crate::session_view::SessionView;

/// Immutable metadata captured when a request enters the trusted transport boundary.
///
/// `received_at` records when that boundary accepted the request. `deadline`
/// is the canonical request deadline, when the ingress supplied one. Neither
/// value is recomputed or mutable after construction.
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestMeta;
///
/// fn cannot_read_the_ingress_timestamp_field(meta: &RequestMeta) {
///     let _ = meta.received_at;
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestMeta;
///
/// fn cannot_read_the_deadline_field(meta: &RequestMeta) {
///     let _ = meta.deadline;
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestMeta;
///
/// fn cannot_extend_a_request(meta: &RequestMeta) {
///     meta.set_deadline(None);
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestMeta {
    received_at: Instant,
    deadline: Option<RequestDeadline>,
}

impl RequestMeta {
    /// Captures request metadata at the trusted ingress boundary.
    pub(crate) const fn new(received_at: Instant, deadline: Option<RequestDeadline>) -> Self {
        Self { received_at, deadline }
    }

    /// Returns when the trusted ingress boundary accepted the request.
    #[must_use]
    pub const fn received_at(&self) -> Instant {
        self.received_at
    }

    /// Returns the canonical ingress deadline, when one was supplied.
    #[must_use]
    pub const fn deadline(&self) -> Option<RequestDeadline> {
        self.deadline
    }
}

/// Read-only cancellation and deadline state for one request.
///
/// Clones observe the same canonical deadline, session close transition, and
/// parent task-group cancellation. This view intentionally exposes no raw
/// cancellation token, operation context, or cancellation capability.
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestControlView;
///
/// fn cannot_cancel_a_request(control: &RequestControlView) {
///     control.cancel();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestControlView;
///
/// fn cannot_get_a_cancellation_token(control: &RequestControlView) {
///     let _ = control.cancellation_token();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestControlView;
///
/// fn cannot_get_an_operation_context(control: &RequestControlView) {
///     let _ = control.operation_context();
/// }
/// ```
///
/// ```compile_fail
/// use rocketmq_transport::api::RequestControlView;
///
/// fn cannot_get_the_parent_cancellation_owner(control: &RequestControlView) {
///     control.task_group().cancel();
/// }
/// ```
#[derive(Clone)]
pub struct RequestControlView {
    deadline: Option<RequestDeadline>,
    // A request clones its control for each layer that may stop it. Sharing one lifecycle per
    // request keeps those clones off the session's watch channels and the task group's
    // cancellation token, whose reference counts and lock every request of the session shares.
    lifecycle: Arc<RequestLifecycle>,
}

/// Session and parent state, independent of the request's deadline.
pub(crate) enum RequestLifecycleState {
    Active,
    ParentCancelled,
    SessionClosed,
}

struct RequestLifecycle {
    session: SessionStateView,
    parent_task_group_id: TaskGroupId,
    stop: LifecycleStop,
}

/// How a request observes the cancellation of its parent task group.
enum LifecycleStop {
    /// A network request observes its session's request stop: a child of the parent task group's
    /// token that the session cancels whenever it publishes its close or loses its close
    /// publisher. Waiting on it alone replaces waiting on both session watch channels and the
    /// parent token, and the observer answers checks and waits without the token's lock, which
    /// every request of the session would otherwise share.
    ///
    /// The parent's cancellation cancels the stop before it returns. Once the session closes
    /// or the observer wakes, the parent token distinguishes cancellation from a session close.
    Session {
        stop: CancellationObserver,
        parent: TaskGroup,
    },
    /// Other requests check and wait on the session channels and the parent token directly.
    Parent(CancellationToken),
}

impl RequestControlView {
    /// Creates a view that waits on the session channels and the parent token directly.
    #[cfg(test)]
    pub(crate) fn from_meta(meta: &RequestMeta, session: SessionStateView, parent_task_group: &TaskGroup) -> Self {
        Self::from_meta_with_session_stop(meta, session, parent_task_group.clone(), None)
    }

    /// Creates an observer view from the canonical metadata and its lifecycle owners.
    ///
    /// The deadline is copied directly from [`RequestMeta`] so every request
    /// layer observes the same absolute expiry instant and original budget.
    ///
    /// With `session_stop`, the waits observe that token instead of the session channels and
    /// the parent token. It must observe a child of `parent_task_group`'s token that is cancelled
    /// whenever `session` publishes its close or loses its close publisher.
    pub(crate) fn from_meta_with_session_stop(
        meta: &RequestMeta,
        session: SessionStateView,
        parent_task_group: TaskGroup,
        session_stop: Option<CancellationObserver>,
    ) -> Self {
        let parent_task_group_id = parent_task_group.id();
        let stop = match session_stop {
            Some(stop) => LifecycleStop::Session {
                stop,
                parent: parent_task_group,
            },
            None => LifecycleStop::Parent(parent_task_group.cancellation_token()),
        };
        Self {
            deadline: meta.deadline(),
            lifecycle: Arc::new(RequestLifecycle {
                session,
                parent_task_group_id,
                stop,
            }),
        }
    }

    /// Proves that this control observes the supplied session and task-group
    /// owners rather than merely matching their diagnostic identifiers.
    pub(crate) fn same_lifecycle_owner(&self, session: &SessionStateView, parent_task_group: &TaskGroup) -> bool {
        self.lifecycle.session.same_canonical_owner(session)
            && self.lifecycle.parent_task_group_id == parent_task_group.id()
    }

    #[cfg(test)]
    pub(crate) fn same_lifecycle_view(&self, other: &Self) -> bool {
        self.deadline == other.deadline
            && self.lifecycle.session.same_canonical_owner(&other.lifecycle.session)
            && self.lifecycle.parent_task_group_id == other.lifecycle.parent_task_group_id
    }

    /// Returns the canonical ingress deadline, when one was supplied.
    #[must_use]
    pub const fn deadline(&self) -> Option<RequestDeadline> {
        self.deadline
    }

    /// Returns whether the request can no longer continue.
    ///
    /// A request is cancelled when its deadline expires, its session closes,
    /// or its parent task group is cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        let expired = || self.deadline.is_some_and(RequestDeadline::is_expired);
        match &self.lifecycle.stop {
            // The stop covers the parent; the session channels still answer for a close that the
            // session published before it cancelled the stop.
            LifecycleStop::Session { stop, .. } => {
                stop.is_cancelled() || expired() || self.lifecycle.session.is_closed()
            }
            LifecycleStop::Parent(parent) => expired() || self.lifecycle.session.is_closed() || parent.is_cancelled(),
        }
    }

    pub(crate) fn lifecycle_state(&self) -> RequestLifecycleState {
        // The session reader can publish close after its parent token is cancelled but
        // before the child observer wakes. Either signal warrants checking the parent.
        let session_stop_pending =
            matches!(&self.lifecycle.stop, LifecycleStop::Session { stop, .. } if !stop.is_cancelled());
        let session_closed = self.lifecycle.session.is_closed();
        if (session_closed || !session_stop_pending) && self.parent_is_cancelled() {
            RequestLifecycleState::ParentCancelled
        } else if session_closed {
            RequestLifecycleState::SessionClosed
        } else {
            RequestLifecycleState::Active
        }
    }

    pub(crate) fn parent_is_cancelled(&self) -> bool {
        match &self.lifecycle.stop {
            LifecycleStop::Session { parent, .. } => parent.cancellation_token().is_cancelled(),
            LifecycleStop::Parent(parent) => parent.is_cancelled(),
        }
    }

    /// Derives the lifecycle-only control used to deliver a boundary rejection.
    ///
    /// The rejected request deadline has already reached its terminal decision,
    /// so it must not suppress the corresponding protocol response. Session and
    /// parent cancellation remain authoritative for the write.
    pub(crate) fn boundary_response_control(&self) -> Self {
        Self {
            deadline: None,
            lifecycle: Arc::clone(&self.lifecycle),
        }
    }

    /// Waits until the deadline expires, the session closes, or the parent
    /// task group is cancelled.
    ///
    /// This method only observes lifecycle state; it cannot cause any of the
    /// transitions it waits for.
    pub fn cancelled(&self) -> impl Future<Output = ()> + '_ {
        self.stopped(self.deadline)
    }

    /// Waits for an external lifecycle stop without observing the request deadline.
    ///
    /// Original one-way requests use this narrower waiter so an admitted
    /// processor timeout can produce and consume its owned deadline plan
    /// through the normal one-way policy before terminal completion.
    pub(crate) fn parent_or_session_cancelled(&self) -> impl Future<Output = ()> + '_ {
        self.stopped(None)
    }

    async fn stopped(&self, deadline: Option<RequestDeadline>) {
        let RequestLifecycle { session, stop, .. } = &*self.lifecycle;

        let parent_cancellation = match stop {
            LifecycleStop::Session { stop, .. } => {
                match deadline {
                    Some(deadline) => {
                        tokio::select! {
                            _ = deadline.timeout(std::future::pending::<()>()) => {}
                            () = stop.cancelled() => {}
                        }
                    }
                    None => stop.cancelled().await,
                }
                return;
            }
            LifecycleStop::Parent(parent) => parent,
        };
        match deadline {
            Some(deadline) => {
                tokio::select! {
                    _ = deadline.timeout(std::future::pending::<()>()) => {}
                    _ = session.closed() => {}
                    _ = parent_cancellation.cancelled() => {}
                }
            }
            None => {
                tokio::select! {
                    _ = session.closed() => {}
                    _ = parent_cancellation.cancelled() => {}
                }
            }
        }
    }
}

type ExtensionValue = Box<dyn Any + Send + Sync>;

/// Request-local type map that remains unallocated until its first successful insertion.
///
/// Trusted ingress facts and lifecycle capabilities never belong in this map:
/// they have dedicated read-only request fields and views instead.
#[derive(Default)]
pub(crate) struct LazyExtensions {
    values: Option<HashMap<TypeId, ExtensionValue>>,
}

impl LazyExtensions {
    pub(crate) fn get<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.values.as_ref()?.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Inserts an extension, replacing and returning a value of the same type.
    ///
    /// Rejected types return their original value and leave the map absent,
    /// preserving the allocation-free request path when no eligible extension
    /// has been inserted.
    pub(crate) fn try_insert<T>(&mut self, value: T) -> Result<Option<T>, T>
    where
        T: Send + Sync + 'static,
    {
        if is_reserved_extension_type::<T>() {
            return Err(value);
        }

        let values = self.values.get_or_insert_with(HashMap::new);
        Ok(values
            .insert(TypeId::of::<T>(), Box::new(value))
            .and_then(|previous| previous.downcast::<T>().ok())
            .map(|previous| *previous))
    }
}

fn is_reserved_extension_type<T>() -> bool
where
    T: Send + Sync + 'static,
{
    is_reserved_extension_type_id(TypeId::of::<T>())
}

fn is_reserved_extension_type_id(type_id: TypeId) -> bool {
    [
        TypeId::of::<AuthenticationState>(),
        TypeId::of::<ArcChannel>(),
        TypeId::of::<CancellationToken>(),
        TypeId::of::<Channel>(),
        TypeId::of::<ChannelInner>(),
        TypeId::of::<Connection>(),
        TypeId::of::<ConnectionStateHandle>(),
        TypeId::of::<EmbeddedCaller>(),
        TypeId::of::<IngressRequestView<'static>>(),
        TypeId::of::<OperationContext>(),
        TypeId::of::<OriginalRequestIdentity>(),
        TypeId::of::<PeerInfo>(),
        TypeId::of::<Principal>(),
        TypeId::of::<ProxyInfoSnapshot>(),
        TypeId::of::<RequestContext>(),
        TypeId::of::<RequestContextParts>(),
        TypeId::of::<RequestControlView>(),
        TypeId::of::<RequestDeadline>(),
        TypeId::of::<RequestId>(),
        TypeId::of::<RequestMeta>(),
        TypeId::of::<RequestLifecycleProvenance>(),
        TypeId::of::<RemotingRequest>(),
        TypeId::of::<RequestOrigin>(),
        TypeId::of::<RequestTransport>(),
        TypeId::of::<ResponseSink>(),
        TypeId::of::<DeferredSlot>(),
        TypeId::of::<SessionExecutor>(),
        TypeId::of::<SessionHandle>(),
        TypeId::of::<SessionId>(),
        TypeId::of::<SessionStateView>(),
        TypeId::of::<SessionView>(),
        TypeId::of::<TaskGroup>(),
    ]
    .contains(&type_id)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::TaskKind;

    use super::*;
    use crate::connection::ConnectionState;

    fn session_view() -> (
        SessionStateView,
        tokio::sync::watch::Sender<ConnectionState>,
        tokio::sync::watch::Sender<bool>,
    ) {
        let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        (
            SessionStateView::from_receivers(state_rx, closed_rx),
            state_tx,
            closed_tx,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn cloned_request_controls_observe_the_exact_canonical_deadline() {
        let runtime = RuntimeContext::from_current("request-control-deadline");
        let deadline = RequestDeadline::after(Duration::from_secs(5));
        let meta = RequestMeta::new(Instant::now(), Some(deadline));
        let (session, _state_tx, _closed_tx) = session_view();
        let parent = runtime.service_context("request-control-deadline").task_group().clone();
        let first = RequestControlView::from_meta(&meta, session, &parent);
        let second = first.clone();

        assert_eq!(meta.deadline(), Some(deadline));
        assert_eq!(first.deadline(), meta.deadline());
        assert_eq!(second.deadline(), meta.deadline());
        assert!(!first.is_cancelled());
        assert!(!second.is_cancelled());

        tokio::time::advance(Duration::from_secs(5)).await;
        first.cancelled().await;
        second.cancelled().await;

        assert!(first.is_cancelled());
        assert!(second.is_cancelled());

        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn request_control_observes_session_closure_without_a_close_capability() {
        let runtime = RuntimeContext::from_current("request-control-session-closure");
        let meta = RequestMeta::new(Instant::now(), None);
        let (session, _state_tx, closed_tx) = session_view();
        let parent = runtime
            .service_context("request-control-session-closure")
            .task_group()
            .clone();
        let control = RequestControlView::from_meta(&meta, session, &parent);

        closed_tx
            .send(true)
            .expect("request control must retain the session closure receiver");
        control.cancelled().await;

        assert!(control.is_cancelled());

        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn cloned_request_controls_observe_parent_task_group_cancellation_with_session_publishers_open() {
        let runtime = RuntimeContext::from_current("request-control-parent-cancellation");
        let meta = RequestMeta::new(Instant::now(), None);
        let (session, state_tx, closed_tx) = session_view();
        let session_observer = session.clone();
        let parent = runtime
            .service_context("request-control-parent-cancellation")
            .task_group()
            .clone();
        let first = RequestControlView::from_meta(&meta, session, &parent);
        let second = first.clone();

        parent.cancel();
        first.cancelled().await;
        second.cancelled().await;

        assert!(first.is_cancelled());
        assert!(second.is_cancelled());
        assert!(!session_observer.is_closed());
        assert_eq!(*state_tx.borrow(), ConnectionState::Healthy);
        assert!(!*closed_tx.borrow());

        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test(start_paused = true)]
    async fn request_control_with_a_session_stop_ends_on_that_stop_its_parent_or_its_deadline() {
        let runtime = RuntimeContext::from_current("request-control-session-stop");
        let parent = runtime
            .service_context("request-control-session-stop")
            .task_group()
            .clone();

        let (session, _state_tx, _closed_tx) = session_view();
        let session_stop = parent.cancellation_token().child_token();
        let closing = RequestControlView::from_meta_with_session_stop(
            &RequestMeta::new(Instant::now(), None),
            session,
            parent.clone(),
            Some(CancellationObserver::new(session_stop.clone())),
        );
        assert!(tokio::time::timeout(Duration::from_secs(1), closing.cancelled())
            .await
            .is_err());
        session_stop.cancel();
        closing.cancelled().await;
        closing.parent_or_session_cancelled().await;

        let (session, _state_tx, _closed_tx) = session_view();
        let deadline = RequestDeadline::after(Duration::from_secs(5));
        let expiring = RequestControlView::from_meta_with_session_stop(
            &RequestMeta::new(Instant::now(), Some(deadline)),
            session,
            parent.clone(),
            Some(CancellationObserver::new(parent.cancellation_token().child_token())),
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        expiring.cancelled().await;
        assert!(
            tokio::time::timeout(Duration::from_secs(1), expiring.parent_or_session_cancelled())
                .await
                .is_err(),
            "the narrower wait ignores the deadline"
        );

        parent.cancel();
        expiring.parent_or_session_cancelled().await;
        assert!(expiring.is_cancelled());

        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test(start_paused = true)]
    async fn request_control_without_a_cancellation_source_remains_pending() {
        let runtime = RuntimeContext::from_current("request-control-pending");
        let meta = RequestMeta::new(Instant::now(), None);
        let (session, _state_tx, _closed_tx) = session_view();
        let parent = runtime.service_context("request-control-pending").task_group().clone();
        let control = RequestControlView::from_meta(&meta, session, &parent);

        assert!(!control.is_cancelled());
        assert!(tokio::time::timeout(Duration::from_secs(1), control.cancelled())
            .await
            .is_err());
        assert!(!control.is_cancelled());

        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn lazy_extensions_allocate_only_for_successful_insertions_and_replace_by_type() {
        let mut extensions = LazyExtensions::default();

        assert!(extensions.values.is_none());
        assert_eq!(extensions.get::<String>(), None);
        assert_eq!(extensions.try_insert("first".to_owned()), Ok(None));
        assert!(extensions.values.is_some());
        assert_eq!(extensions.get::<String>(), Some(&"first".to_owned()));
        assert_eq!(extensions.try_insert("second".to_owned()), Ok(Some("first".to_owned())));
        assert_eq!(extensions.get::<String>(), Some(&"second".to_owned()));
    }

    #[tokio::test]
    async fn lazy_extensions_reject_trusted_facts_and_capabilities_without_allocating() {
        fn rejected_extension<T>(extensions: &mut LazyExtensions, value: T) -> T
        where
            T: Send + Sync + 'static,
        {
            match extensions.try_insert(value) {
                Err(value) => value,
                Ok(_) => panic!("trusted request facts and capabilities must be rejected"),
            }
        }

        let mut extensions = LazyExtensions::default();
        let command = RemotingCommand::create_remoting_command(17);
        let identity = OriginalRequestIdentity::capture(7, &AtomicU64::new(1), &command)
            .expect("test request identity must allocate");
        let request_id = identity.request_id();
        let meta = RequestMeta::new(Instant::now(), None);
        let (session_state, _state_tx, _closed_tx) = session_view();
        let session_id = SessionId::from_session_owner(11);
        let session = SessionView::Embedded {
            id: session_id,
            state: session_state.clone(),
        };
        let runtime = RuntimeContext::from_current("request-control-lazy-extensions");
        let parent = runtime
            .service_context("request-control-lazy-extensions")
            .task_group()
            .clone();
        let control = RequestControlView::from_meta(&meta, session_state.clone(), &parent);

        let deadline = RequestDeadline::after(Duration::from_secs(1));
        assert_eq!(rejected_extension(&mut extensions, deadline), deadline);
        assert_eq!(rejected_extension(&mut extensions, meta), meta);
        assert_eq!(rejected_extension(&mut extensions, request_id), request_id);
        assert_eq!(rejected_extension(&mut extensions, identity), identity);
        assert_eq!(
            rejected_extension(&mut extensions, EmbeddedCaller::BrokerProxy),
            EmbeddedCaller::BrokerProxy
        );
        assert_eq!(
            rejected_extension(
                &mut extensions,
                RequestOrigin::Embedded {
                    caller: EmbeddedCaller::BrokerProxy,
                    client_addr: None,
                }
            ),
            RequestOrigin::Embedded {
                caller: EmbeddedCaller::BrokerProxy,
                client_addr: None,
            }
        );
        assert_eq!(
            rejected_extension(&mut extensions, RequestTransport::Network),
            RequestTransport::Network
        );
        assert_eq!(
            rejected_extension(&mut extensions, AuthenticationState::Anonymous),
            AuthenticationState::Anonymous
        );
        let principal = Principal::new("trusted-principal");
        assert_eq!(rejected_extension(&mut extensions, principal.clone()), principal);
        let peer = PeerInfo::new("192.0.2.55:10911".parse().expect("test address must parse"), true);
        assert_eq!(rejected_extension(&mut extensions, peer.clone()), peer);
        assert_eq!(rejected_extension(&mut extensions, session_id), session_id);
        let _session_state = rejected_extension(&mut extensions, session_state);
        let _session = rejected_extension(&mut extensions, session);
        let _control = rejected_extension(&mut extensions, control);
        let cancellation = rejected_extension(&mut extensions, CancellationToken::new());
        cancellation.cancel();
        assert!(cancellation.is_cancelled());
        let operation = rejected_extension(&mut extensions, OperationContext::without_deadline(TaskKind::Worker));
        operation.cancel();
        assert!(operation.is_cancelled());
        assert!(extensions.values.is_none());

        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn lazy_extensions_reserve_legacy_transport_capability_types() {
        assert!(is_reserved_extension_type_id(TypeId::of::<Channel>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<ArcChannel>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<ChannelInner>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<Connection>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<ConnectionStateHandle>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<ResponseSink>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<SessionExecutor>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<SessionHandle>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<TaskGroup>()));
        assert!(is_reserved_extension_type_id(
            TypeId::of::<IngressRequestView<'static>>()
        ));
        assert!(is_reserved_extension_type_id(TypeId::of::<RemotingRequest>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<DeferredSlot>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<RequestContextParts>()));
        assert!(is_reserved_extension_type_id(TypeId::of::<RequestLifecycleProvenance>()));
    }
}
