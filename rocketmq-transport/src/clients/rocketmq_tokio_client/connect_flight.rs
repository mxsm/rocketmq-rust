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

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SharedRocketMQError;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::connection_registry::ConnectionRegistry;
use super::endpoint_state::EndpointLease;
use super::lifecycle::ConnectionCommitFence;
use super::TransportClient;
use crate::clients::client::SessionConnectTarget;
use crate::clients::nameserver_endpoint::NameServerEndpoint;
use crate::clients::TransportSession;
use crate::deadline::RequestDeadline;

enum ConnectFlightState<PR> {
    Connecting,
    Complete(Box<Result<Option<TransportSession<PR>>, SharedRocketMQError>>),
}

/// A shared connection attempt associated with one endpoint generation.
pub(super) struct ConnectFlight<PR> {
    lease: Option<EndpointLease>,
    state: Mutex<ConnectFlightState<PR>>,
    changed: tokio::sync::Notify,
}

impl<PR> ConnectFlight<PR>
where
    PR: Sync + Clone + Send + 'static,
{
    pub(super) fn new(lease: Option<EndpointLease>) -> Self {
        Self {
            lease,
            state: Mutex::new(ConnectFlightState::Connecting),
            changed: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn belongs_to(&self, lease: Option<&EndpointLease>) -> bool {
        match (self.lease.as_ref(), lease) {
            (None, None) => true,
            (Some(current), Some(expected)) => current.same_generation(expected),
            _ => false,
        }
    }

    pub(super) fn lease(&self) -> Option<&EndpointLease> {
        self.lease.as_ref()
    }

    pub(super) fn complete(&self, result: RocketMQResult<Option<TransportSession<PR>>>) {
        let mut state = self.state.lock();
        if matches!(*state, ConnectFlightState::Complete(_)) {
            return;
        }
        *state = ConnectFlightState::Complete(Box::new(result.map_err(SharedRocketMQError::new)));
        drop(state);
        self.changed.notify_waiters();
    }

    pub(super) fn complete_not_started(&self) {
        self.complete(Err(RocketMQError::ClientNotStarted));
    }

    pub(super) fn complete_without_session(&self) {
        self.complete(Ok(None));
    }

    pub(super) async fn wait(
        &self,
        deadline: RequestDeadline,
        target: &CheetahString,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let ConnectFlightState::Complete(result) = &*self.state.lock() {
                return (**result).clone().map_err(SharedRocketMQError::into_error);
            }
            deadline
                .timeout(changed)
                .await
                .map_err(|_| RocketMQError::network_connection_timeout(target.to_string(), deadline.budget_millis()))?;
        }
    }
}

/// Completes and unregisters a leader flight if its owned task is cancelled.
///
/// This guard is created before the task future is submitted, so dropping an
/// unpolled future cannot strand its waiters behind a removed worker task.
struct FlightCompletionGuard<PR>
where
    PR: Sync + Clone + Send + 'static,
{
    registry: Arc<ConnectionRegistry<PR>>,
    target: CheetahString,
    flight: Arc<ConnectFlight<PR>>,
}

impl<PR> FlightCompletionGuard<PR>
where
    PR: Sync + Clone + Send + 'static,
{
    fn new(registry: Arc<ConnectionRegistry<PR>>, target: CheetahString, flight: Arc<ConnectFlight<PR>>) -> Self {
        Self {
            registry,
            target,
            flight,
        }
    }
}

impl<PR> Drop for FlightCompletionGuard<PR>
where
    PR: Sync + Clone + Send + 'static,
{
    fn drop(&mut self) {
        self.flight.complete_not_started();
        self.registry.remove_flight_if_matches(&self.target, &self.flight);
    }
}

impl<PR: Send + Sync + Clone + 'static> TransportClient<PR> {
    /// Get an existing healthy client or create a new connection.
    pub(super) async fn get_and_create_client_until(
        &self,
        addr: Option<&CheetahString>,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        let target_addr = match addr {
            None => {
                return self
                    .get_and_create_nameserver_client_until(deadline)
                    .await
                    .map(|selection| selection.map(|selection| selection.session));
            }
            Some(addr) if addr.is_empty() => {
                return self
                    .get_and_create_nameserver_client_until(deadline)
                    .await
                    .map(|selection| selection.map(|selection| selection.session));
            }
            Some(addr) => addr,
        };
        deadline.ensure_before_send(target_addr.to_string())?;

        if let Some(client) = self.connection_registry.healthy_session(target_addr, None) {
            return Ok(Some(client));
        }
        self.create_client_until(target_addr, deadline).await
    }

    #[cfg(test)]
    pub(super) async fn create_client(&self, addr: &CheetahString, duration: Duration) -> Option<TransportSession<PR>> {
        match self.create_client_until(addr, RequestDeadline::after(duration)).await {
            Ok(client) => client,
            Err(error) => {
                error!(remote_addr = %addr, error = ?error, "Failed to create remoting client");
                None
            }
        }
    }

    async fn create_client_until(
        &self,
        addr: &CheetahString,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        self.create_client_with_lease_until(addr, None, None, deadline).await
    }

    pub(super) async fn create_client_for_nameserver_until(
        &self,
        addr: &CheetahString,
        endpoint: NameServerEndpoint,
        lease: EndpointLease,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        self.create_client_with_lease_until(addr, Some(endpoint), Some(lease), deadline)
            .await
    }

    async fn create_client_with_lease_until(
        &self,
        addr: &CheetahString,
        configured_nameserver: Option<NameServerEndpoint>,
        lease: Option<EndpointLease>,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        deadline.ensure_before_send(addr.to_string())?;
        if !self.can_commit_endpoint_lease(lease.as_ref()) {
            return Ok(None);
        }
        if let Some(client) = self.connection_registry.healthy_session(addr, lease.as_ref()) {
            return Ok(Some(client));
        }

        let worker_owner = self
            .capture_worker_task_owner()
            .ok_or(RocketMQError::ClientNotStarted)?;
        let (flight, leader) = self.connection_registry.acquire_flight(addr.clone(), lease.clone());
        if leader {
            let target = addr.clone();
            let completion_guard = FlightCompletionGuard::new(
                Arc::clone(&self.connection_registry),
                target.clone(),
                Arc::clone(&flight),
            );
            #[cfg(test)]
            self.wait_for_leader_before_worker_spawn_test_hook().await;

            let client = self.clone();
            let flight_for_task = Arc::clone(&flight);
            let target_for_task = target.clone();
            let lease_for_task = lease.clone();
            let connect_timeout = self.tokio_client_config.connect.timeout;
            let commit_fence = worker_owner.commit_fence();
            let spawned = self.spawn_worker_task_with_owner(
                &worker_owner,
                format!("rocketmq.transport.connect.{target}"),
                async move {
                    let _completion_guard = completion_guard;
                    client.connect_attempts.fetch_add(1, Ordering::Relaxed);
                    let result = client
                        .connect_endpoint_until(
                            &target_for_task,
                            configured_nameserver,
                            lease_for_task,
                            RequestDeadline::after(connect_timeout),
                            &commit_fence,
                        )
                        .await;
                    let result = if client.matches_connection_commit_fence(&commit_fence) {
                        result
                    } else {
                        Err(RocketMQError::ClientNotStarted)
                    };
                    flight_for_task.complete(result);
                },
            );
            if spawned.is_none() {
                flight.complete_not_started();
                self.connection_registry.remove_flight_if_matches(&target, &flight);
            }
        }
        flight.wait(deadline, addr).await
    }

    pub(super) async fn connect_endpoint_until(
        &self,
        addr: &CheetahString,
        configured_nameserver: Option<NameServerEndpoint>,
        lease: Option<EndpointLease>,
        deadline: RequestDeadline,
        commit_fence: &ConnectionCommitFence,
    ) -> RocketMQResult<Option<TransportSession<PR>>> {
        deadline.ensure_before_send(addr.to_string())?;
        if !self.matches_connection_commit_fence(commit_fence) {
            return Err(RocketMQError::ClientNotStarted);
        }
        if !self.can_commit_endpoint_lease(lease.as_ref()) {
            return Ok(None);
        }
        if let Some(client) = self.connection_registry.healthy_session(addr, lease.as_ref()) {
            return Ok(Some(client));
        }

        let allowed = match lease.as_ref() {
            Some(lease) => self
                .nameserver_health
                .connection_admission_if_current(addr, lease, || self.endpoint_state.is_current(lease))
                .is_some_and(|admission| admission != crate::clients::reconnect::CircuitAdmission::Rejected),
            None => self
                .direct_circuit_breakers
                .entry(addr.clone())
                .or_insert_with(crate::clients::reconnect::CircuitBreaker::default_breaker)
                .allow_request(),
        };
        if !allowed {
            warn!("Circuit breaker OPEN for {}, rejecting connection attempt", addr);
            return Ok(None);
        }

        let session_target = match configured_nameserver.as_ref() {
            Some(endpoint) => match endpoint.connect_target() {
                Some(target) => SessionConnectTarget::Resolved(target.clone()),
                None => SessionConnectTarget::Legacy(endpoint.compatibility_address().to_string()),
            },
            None => SessionConnectTarget::Legacy(addr.to_string()),
        };
        let transport_config = (*self.tokio_client_config).clone();
        let frame_limits = self.frame_limits;
        let transport_security = self.transport_security.clone();
        let connect_result = TransportSession::connect_target_with_service_context_until_and_telemetry(
            &self.service_context,
            session_target,
            self.cmd_handler.clone(),
            self.tx.as_ref(),
            transport_config,
            frame_limits,
            deadline,
            self.telemetry.clone(),
        )
        .await;
        let connect_result = match transport_security {
            Some(transport_security) => connect_result.map(|client| client.with_transport_security(transport_security)),
            None => connect_result,
        };

        match connect_result {
            Ok(new_client) => {
                #[cfg(test)]
                self.wait_for_connect_completion_test_hook().await;
                if !self.matches_connection_commit_fence(commit_fence) {
                    new_client.begin_drain();
                    let _ = new_client.close_with_report(Duration::from_secs(1)).await;
                    return Err(RocketMQError::ClientNotStarted);
                }
                if !self.can_commit_endpoint_lease(lease.as_ref()) {
                    new_client.begin_drain();
                    let _ = new_client.close_with_report(Duration::from_secs(1)).await;
                    return Ok(None);
                }
                match lease.as_ref() {
                    Some(lease) => self
                        .nameserver_health
                        .record_connection_success_if_current(addr, lease, || self.endpoint_state.is_current(lease)),
                    None => {
                        if let Some(mut breaker) = self.direct_circuit_breakers.get_mut(addr) {
                            breaker.record_success();
                        }
                    }
                }
                let session_lease = lease.clone();
                match self
                    .connection_registry
                    .insert_session(addr.clone(), new_client.clone(), lease, || {
                        self.matches_connection_commit_fence(commit_fence)
                            && self.can_commit_endpoint_lease(session_lease.as_ref())
                    }) {
                    Some(client) => {
                        info!("Successfully created client for {}", addr);
                        Ok(Some(client))
                    }
                    None => {
                        new_client.begin_drain();
                        let _ = new_client.close_with_report(Duration::from_secs(1)).await;
                        if self.matches_connection_commit_fence(commit_fence) {
                            Ok(None)
                        } else {
                            Err(RocketMQError::ClientNotStarted)
                        }
                    }
                }
            }
            Err(error) => {
                error!(remote_addr = %addr, error = ?error, "Failed to connect");
                if !self.matches_connection_commit_fence(commit_fence) {
                    return Err(RocketMQError::ClientNotStarted);
                }
                match lease.as_ref() {
                    Some(lease) => self
                        .nameserver_health
                        .record_connection_failure_if_current(addr, lease, || self.endpoint_state.is_current(lease)),
                    None => {
                        if let Some(mut breaker) = self.direct_circuit_breakers.get_mut(addr) {
                            breaker.record_failure();
                        }
                    }
                }
                Err(error)
            }
        }
    }

    pub(super) fn can_commit_endpoint_lease(&self, lease: Option<&EndpointLease>) -> bool {
        lease.is_none_or(|lease| self.endpoint_state.is_current(lease))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use rocketmq_error::DomainError;
    use rocketmq_runtime::RuntimeContext;
    use tokio::sync::Barrier;
    use tokio::sync::Notify;
    use tokio::task::JoinSet;

    use super::*;
    use crate::request_processor::default_request_processor::DefaultRequestProcessor;

    async fn assert_connect_flight_preserves_failure(error: RocketMQError) {
        const WAITERS: usize = 3;

        let expected_kind = error.kind();
        let expected_context = error.context();
        let expected_boundary = error.boundary_view();
        let expected_retry = error.retry();
        let expected_severity = error.severity();
        let expected_redaction = error.redaction();
        let expected_display = error.to_string();
        let expected_source = error.source().map(ToString::to_string);

        let flight = Arc::new(ConnectFlight::<DefaultRequestProcessor>::new(None));
        let target = CheetahString::from_static_str("127.0.0.1:10911");
        let barrier = Arc::new(Barrier::new(WAITERS + 1));
        let ready_count = Arc::new(AtomicUsize::new(0));
        let waiters_ready = Arc::new(Notify::new());
        let mut tasks = JoinSet::new();

        {
            let barrier = Arc::clone(&barrier);
            let flight = Arc::clone(&flight);
            let target = target.clone();
            let waiters_ready = Arc::clone(&waiters_ready);
            tasks.spawn(async move {
                barrier.wait().await;
                waiters_ready.notified().await;
                flight.complete(Err(error));
                flight
                    .wait(RequestDeadline::after(Duration::from_secs(1)), &target)
                    .await
            });
        }

        for _ in 0..WAITERS {
            let barrier = Arc::clone(&barrier);
            let flight = Arc::clone(&flight);
            let target = target.clone();
            let ready_count = Arc::clone(&ready_count);
            let waiters_ready = Arc::clone(&waiters_ready);
            tasks.spawn(async move {
                barrier.wait().await;
                if ready_count.fetch_add(1, Ordering::AcqRel) + 1 == WAITERS {
                    waiters_ready.notify_one();
                }
                flight
                    .wait(RequestDeadline::after(Duration::from_secs(1)), &target)
                    .await
            });
        }

        let mut snapshots = Vec::with_capacity(WAITERS + 1);
        while let Some(result) = tasks.join_next().await {
            let error = match result.expect("connect-flight task must complete") {
                Err(error) => error,
                Ok(_) => panic!("connect flight must return the shared failure"),
            };
            let RocketMQError::Shared(snapshot) = error else {
                panic!("connect flight must return a shared typed error");
            };
            snapshots.push(snapshot);
        }

        let first = snapshots.first().expect("leader and waiters return snapshots");
        for snapshot in &snapshots {
            assert_eq!(snapshot.kind(), expected_kind);
            assert_eq!(snapshot.context(), expected_context);
            assert_eq!(snapshot.boundary_view(), expected_boundary);
            assert_eq!(snapshot.retry(), expected_retry);
            assert_eq!(snapshot.severity(), expected_severity);
            assert_eq!(snapshot.redaction(), expected_redaction);
            assert_eq!(snapshot.to_string(), expected_display);
            assert!(std::ptr::eq(first.as_error(), snapshot.as_error()));

            let source = snapshot.source().expect("shared error source");
            let original = source
                .downcast_ref::<RocketMQError>()
                .expect("shared source must be the original error");
            assert!(std::ptr::eq(snapshot.as_error(), original));
            assert_eq!(source.to_string(), expected_display);
            assert_eq!(source.source().map(ToString::to_string), expected_source);
        }

        flight.complete_not_started();
        let error = match flight
            .wait(RequestDeadline::after(Duration::from_secs(1)), &target)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("a completed flight cannot be overwritten"),
        };
        let RocketMQError::Shared(snapshot) = error else {
            panic!("completed flight must retain its shared error");
        };
        assert!(std::ptr::eq(first.as_error(), snapshot.as_error()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn connect_flight_shares_exact_typed_failures_with_leader_and_waiters() {
        assert_connect_flight_preserves_failure(RocketMQError::network_connection_failed(
            "127.0.0.1:10911",
            "connection refused",
        ))
        .await;
        assert_connect_flight_preserves_failure(RocketMQError::ConfigInvalidValue {
            key: "connect.timeout",
            value: "invalid".to_owned(),
            reason: "must be positive".to_owned(),
        })
        .await;
        assert_connect_flight_preserves_failure(RocketMQError::ClientNotStarted).await;
        assert_connect_flight_preserves_failure(RocketMQError::from(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            io::Error::new(io::ErrorKind::TimedOut, "inner connect timeout"),
        )))
        .await;
    }

    #[tokio::test]
    async fn waiter_deadline_does_not_cancel_the_shared_connect_flight() {
        let flight = ConnectFlight::<DefaultRequestProcessor>::new(None);
        let target = CheetahString::from_static_str("127.0.0.1:10911");

        let timeout = match flight.wait(RequestDeadline::after(Duration::ZERO), &target).await {
            Err(error) => error,
            Ok(_) => panic!("an expired waiter deadline must time out"),
        };
        assert!(matches!(timeout, RocketMQError::Network(_)));

        flight.complete(Ok(None));
        assert!(flight
            .wait(RequestDeadline::after(Duration::from_secs(1)), &target)
            .await
            .expect("later waiters retain the shared flight completion")
            .is_none());
    }

    #[tokio::test]
    async fn dropped_completion_guard_unblocks_waiters_with_client_not_started() {
        let registry = Arc::new(ConnectionRegistry::<DefaultRequestProcessor>::new());
        let target = CheetahString::from_static_str("127.0.0.1:10911");
        let (flight, leader) = registry.acquire_flight(target.clone(), None);
        assert!(leader);

        drop(FlightCompletionGuard::new(
            Arc::clone(&registry),
            target.clone(),
            Arc::clone(&flight),
        ));

        let error = match flight
            .wait(RequestDeadline::after(Duration::from_secs(1)), &target)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("a cancelled leader must publish a terminal error"),
        };
        let RocketMQError::Shared(snapshot) = error else {
            panic!("cancelled leader completion must be shared");
        };
        assert!(matches!(snapshot.as_error(), RocketMQError::ClientNotStarted));
        assert_eq!(registry.flight_count(), 0);
    }

    #[tokio::test]
    async fn rejected_service_spawn_drops_an_unpolled_connect_flight_guard() {
        let runtime = RuntimeContext::from_current("connect-flight-closed-spawn-test");
        let task_group = runtime
            .service_context("connect-flight-closed-spawn")
            .task_group()
            .clone();
        let _shutdown_report = task_group.shutdown_now();
        let registry = Arc::new(ConnectionRegistry::<DefaultRequestProcessor>::new());
        let target = CheetahString::from_static_str("127.0.0.1:10911");
        let (flight, leader) = registry.acquire_flight(target.clone(), None);
        assert!(leader);

        let completion_guard = FlightCompletionGuard::new(Arc::clone(&registry), target.clone(), Arc::clone(&flight));
        let spawn_result = task_group.spawn_service("connect-flight.closed", async move {
            let _completion_guard = completion_guard;
            std::future::pending::<()>().await;
        });
        assert!(
            spawn_result.is_err(),
            "a closed task group must reject the service future"
        );

        let error = match flight
            .wait(RequestDeadline::after(Duration::from_secs(1)), &target)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("dropping an unpolled service future must complete the flight"),
        };
        let RocketMQError::Shared(snapshot) = error else {
            panic!("unpolled service cancellation must preserve the shared typed error");
        };
        assert!(matches!(snapshot.as_error(), RocketMQError::ClientNotStarted));
        assert_eq!(registry.flight_count(), 0);
    }

    #[tokio::test]
    async fn aborting_a_polled_connect_flight_service_completes_once_and_unregisters() {
        let runtime = RuntimeContext::from_current("connect-flight-abort-test");
        let task_group = runtime.service_context("connect-flight-abort").task_group().clone();
        let registry = Arc::new(ConnectionRegistry::<DefaultRequestProcessor>::new());
        let target = CheetahString::from_static_str("127.0.0.1:10911");
        let (flight, leader) = registry.acquire_flight(target.clone(), None);
        assert!(leader);
        let first_poll = Arc::new(Notify::new());
        let parked = Arc::new(Notify::new());
        let observed_first_poll = first_poll.notified();
        let completion_guard = FlightCompletionGuard::new(Arc::clone(&registry), target.clone(), Arc::clone(&flight));
        let task_id = task_group
            .spawn_service("connect-flight.abort", {
                let first_poll = Arc::clone(&first_poll);
                async move {
                    let _completion_guard = completion_guard;
                    first_poll.notify_one();
                    parked.notified().await;
                }
            })
            .expect("open task group must accept the service future");
        observed_first_poll.await;
        assert!(
            task_group.abort_task_and_wait(task_id, Duration::from_secs(1)).await,
            "aborting a polled service must finish its tracked task"
        );

        let error = match flight
            .wait(RequestDeadline::after(Duration::from_secs(1)), &target)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("aborting a leader must publish a terminal typed error"),
        };
        let RocketMQError::Shared(snapshot) = error else {
            panic!("aborted service completion must be shared");
        };
        assert!(matches!(snapshot.as_error(), RocketMQError::ClientNotStarted));
        assert_eq!(registry.flight_count(), 0);

        let preserved_target = CheetahString::from_static_str("127.0.0.1:10912");
        let (preserved_flight, preserved_leader) = registry.acquire_flight(preserved_target.clone(), None);
        assert!(preserved_leader);
        let preserved_first_poll = Arc::new(Notify::new());
        let preserved_parked = Arc::new(Notify::new());
        let observed_preserved_poll = preserved_first_poll.notified();
        let preserved_guard = FlightCompletionGuard::new(
            Arc::clone(&registry),
            preserved_target.clone(),
            Arc::clone(&preserved_flight),
        );
        let preserved_task_id = task_group
            .spawn_service("connect-flight.abort-preserved", {
                let preserved_first_poll = Arc::clone(&preserved_first_poll);
                async move {
                    let _completion_guard = preserved_guard;
                    preserved_first_poll.notify_one();
                    preserved_parked.notified().await;
                }
            })
            .expect("open task group must accept the preserving service future");
        observed_preserved_poll.await;
        preserved_flight.complete(Ok(None));
        assert!(
            task_group
                .abort_task_and_wait(preserved_task_id, Duration::from_secs(1))
                .await,
            "aborting a precompleted service must finish its tracked task"
        );
        assert!(preserved_flight
            .wait(RequestDeadline::after(Duration::from_secs(1)), &preserved_target)
            .await
            .expect("the first completion must survive task abortion")
            .is_none());
        assert_eq!(registry.flight_count(), 0);
    }
}
