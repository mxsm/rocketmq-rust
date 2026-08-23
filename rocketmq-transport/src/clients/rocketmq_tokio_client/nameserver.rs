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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_error::RocketMQResult;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use super::endpoint_state::EndpointLease;
use super::endpoint_state::EndpointState;
use super::TransportClient;
use crate::clients::nameserver_endpoint::ConnectTarget;
use crate::clients::nameserver_endpoint::NameServerEndpoint;
use crate::clients::nameserver_failover::build_nameserver_failover_candidates;
use crate::clients::nameserver_selector::LatencyTracker;
use crate::clients::reconnect::CircuitAdmission;
use crate::clients::reconnect::CircuitBreaker;
use crate::clients::reconnect::CircuitState;
use crate::clients::TransportSession;
use crate::deadline::RequestDeadline;
use crate::runtime::processor::RequestProcessor;
use crate::telemetry::TransportNameServerFailoverReason;

struct CircuitRecord {
    breaker: CircuitBreaker,
    lease: EndpointLease,
}

/// Generation-aware nameserver health state.
///
/// The mutex serializes only the small ownership transition with latency and
/// circuit bookkeeping; network work remains outside it.
pub(super) struct NameServerHealth {
    latency: LatencyTracker,
    latency_leases: DashMap<CheetahString, EndpointLease>,
    circuits: DashMap<CheetahString, CircuitRecord>,
    mutation_lock: Mutex<()>,
}

pub(super) struct NameServerSession<PR> {
    pub(super) session: TransportSession<PR>,
    pub(super) identity: CheetahString,
    pub(super) lease: EndpointLease,
    pub(super) state: Arc<EndpointState>,
}

impl NameServerHealth {
    pub(super) fn new() -> Self {
        Self {
            latency: LatencyTracker::new(),
            latency_leases: DashMap::with_capacity(64),
            circuits: DashMap::with_capacity(64),
            mutation_lock: Mutex::new(()),
        }
    }

    pub(super) fn latency_tracker(&self) -> &LatencyTracker {
        &self.latency
    }

    /// Serializes topology publication with health ownership transitions, so a
    /// caller that observed a lease cannot install it after that lease retired.
    pub(super) fn with_mutation_lock<T>(&self, action: impl FnOnce() -> T) -> T {
        let _guard = self.mutation_lock.lock();
        action()
    }

    pub(super) fn is_healthy(&self, identity: &CheetahString, lease: &EndpointLease) -> bool {
        self.latency_leases
            .get(identity)
            .is_none_or(|owner| owner.same_generation(lease) && self.latency.is_healthy(identity))
    }

    pub(super) fn p99(&self, identity: &CheetahString, lease: &EndpointLease) -> Option<Duration> {
        self.latency_leases
            .get(identity)
            .filter(|owner| owner.same_generation(lease))
            .and_then(|_| self.latency.get_p99(identity))
    }

    pub(super) fn error_count(&self, identity: &CheetahString, lease: &EndpointLease) -> u32 {
        self.latency_leases
            .get(identity)
            .filter(|owner| owner.same_generation(lease))
            .map_or(0, |_| self.latency.get_error_count(identity))
    }

    fn prepare_locked(&self, identity: &CheetahString, lease: &EndpointLease) {
        let stale_latency_lease = self
            .latency_leases
            .get(identity)
            .filter(|owner| !owner.same_generation(lease))
            .map(|owner| owner.clone());
        if stale_latency_lease.is_some_and(|stale| {
            self.latency_leases
                .remove_if(identity, |_, owner| owner.same_generation(&stale))
                .is_some()
        }) {
            self.latency.remove(identity);
        }
        self.latency_leases
            .entry(identity.clone())
            .or_insert_with(|| lease.clone());
        let stale_circuit_lease = self
            .circuits
            .get(identity)
            .filter(|record| !record.lease.same_generation(lease))
            .map(|record| record.lease.clone());
        if let Some(stale) = stale_circuit_lease {
            self.circuits
                .remove_if(identity, |_, record| record.lease.same_generation(&stale));
        }
    }

    pub(super) fn connection_admission_if_current(
        &self,
        identity: &CheetahString,
        lease: &EndpointLease,
        is_current: impl FnOnce() -> bool,
    ) -> Option<CircuitAdmission> {
        let _guard = self.mutation_lock.lock();
        if !is_current() {
            return None;
        }
        self.prepare_locked(identity, lease);
        Some(match self.circuits.entry(identity.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) if entry.get().lease.same_generation(lease) => {
                entry.get_mut().breaker.connection_admission()
            }
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                entry.insert(CircuitRecord {
                    breaker: CircuitBreaker::default_breaker(),
                    lease: lease.clone(),
                });
                entry.get_mut().breaker.connection_admission()
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => entry
                .insert(CircuitRecord {
                    breaker: CircuitBreaker::default_breaker(),
                    lease: lease.clone(),
                })
                .breaker
                .connection_admission(),
        })
    }

    pub(super) fn record_connection_success_if_current(
        &self,
        identity: &CheetahString,
        lease: &EndpointLease,
        is_current: impl FnOnce() -> bool,
    ) {
        let _guard = self.mutation_lock.lock();
        if !is_current() {
            return;
        }
        let Some(mut record) = self.circuits.get_mut(identity) else {
            return;
        };
        if record.lease.same_generation(lease) {
            record.breaker.record_success();
        }
    }

    pub(super) fn record_connection_failure_if_current(
        &self,
        identity: &CheetahString,
        lease: &EndpointLease,
        is_current: impl FnOnce() -> bool,
    ) {
        let _guard = self.mutation_lock.lock();
        if !is_current() {
            return;
        }
        let Some(mut record) = self.circuits.get_mut(identity) else {
            return;
        };
        if record.lease.same_generation(lease) {
            record.breaker.record_failure();
        }
    }

    pub(super) fn record_outcome_if_current(
        &self,
        identity: &CheetahString,
        lease: &EndpointLease,
        latency: Duration,
        success: bool,
        is_current: impl FnOnce() -> bool,
    ) {
        let _guard = self.mutation_lock.lock();
        if !is_current() {
            return;
        }
        if self
            .latency_leases
            .get(identity)
            .is_some_and(|owner| owner.same_generation(lease))
        {
            if success {
                self.latency.record_success(identity, latency);
            } else {
                self.latency.record_error(identity);
            }
        }
    }

    pub(super) fn circuit_state(&self, identity: &CheetahString, lease: &EndpointLease) -> Option<CircuitState> {
        self.circuits
            .get(identity)
            .filter(|record| record.lease.same_generation(lease))
            .map(|record| record.breaker.state())
    }

    pub(super) fn remove_if_owned(&self, identity: &CheetahString, lease: &EndpointLease) {
        let _guard = self.mutation_lock.lock();
        if self
            .latency_leases
            .remove_if(identity, |_, owner| owner.same_generation(lease))
            .is_some()
        {
            self.latency.remove(identity);
        }
        self.circuits
            .remove_if(identity, |_, record| record.lease.same_generation(lease));
    }

    #[cfg(test)]
    pub(super) fn latency_p99_for_test(&self, identity: &CheetahString) -> Option<Duration> {
        self.latency.get_p99(identity)
    }

    #[cfg(test)]
    pub(super) fn latency_error_count_for_test(&self, identity: &CheetahString) -> u32 {
        self.latency.get_error_count(identity)
    }

    #[cfg(test)]
    pub(super) fn owns_latency_for_test(&self, identity: &CheetahString, lease: &EndpointLease) -> bool {
        self.latency_leases
            .get(identity)
            .is_some_and(|owner| owner.same_generation(lease))
    }
}

struct DrainingEndpointGuard(Arc<AtomicUsize>);

impl Drop for DrainingEndpointGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<PR: RequestProcessor + Sync + Clone + 'static> TransportClient<PR> {
    /// Records request completion only while the selected endpoint lease is current.
    pub(super) fn record_nameserver_outcome(
        &self,
        addr: Option<&CheetahString>,
        lease: Option<&EndpointLease>,
        latency: Duration,
        success: bool,
    ) {
        let (Some(addr), Some(lease)) = (addr, lease) else {
            return;
        };
        self.nameserver_health
            .record_outcome_if_current(addr, lease, latency, success, || self.endpoint_state.is_current(lease));
    }

    pub(super) fn apply_name_server_endpoint_snapshot(
        &self,
        endpoints: Vec<NameServerEndpoint>,
        drain_timeout: Duration,
    ) {
        let update = self
            .nameserver_health
            .with_mutation_lock(|| self.endpoint_state.replace_topology(endpoints));
        let Some(update) = update else {
            return;
        };
        info!(
            added = update.diff.added.len(),
            unchanged = update.diff.unchanged.len(),
            removed = update.diff.removed.len(),
            old_count = update.previous.endpoints().len(),
            new_count = update.current.endpoints().len(),
            generation = update.current.generation(),
            "NameServer endpoint snapshot updated"
        );

        for endpoint in update.diff.removed {
            let identity = endpoint.identity().clone();
            let Some(retired_lease) = update.previous.lease_for(&identity) else {
                continue;
            };
            self.connection_registry
                .remove_flight_for_lease(&identity, &retired_lease);
            let session = self
                .connection_registry
                .remove_session_for_lease(&identity, &retired_lease);
            self.nameserver_health.remove_if_owned(&identity, &retired_lease);
            if let Some(session) = session {
                self.start_removed_endpoint_drain(identity, session.into_session(), retired_lease, drain_timeout);
            }
        }
    }

    pub(super) fn name_server_endpoint(state: &EndpointState, identity: &CheetahString) -> Option<NameServerEndpoint> {
        state
            .endpoints()
            .iter()
            .find(|endpoint| endpoint.identity() == identity)
            .cloned()
    }

    fn start_removed_endpoint_drain(
        &self,
        identity: CheetahString,
        session: TransportSession<PR>,
        lease: EndpointLease,
        drain_timeout: Duration,
    ) {
        session.begin_drain();
        self.namesrv_draining_count.fetch_add(1, Ordering::AcqRel);
        let drain_guard = DrainingEndpointGuard(Arc::clone(&self.namesrv_draining_count));
        self.telemetry
            .record_nameserver_failover(TransportNameServerFailoverReason::Draining);
        let client = self.clone();
        let task_name = format!("rocketmq.transport.nameserver-drain.{identity}");
        let spawned = self.spawn_worker_task(task_name, async move {
            let _drain_guard = drain_guard;
            let report = session.drain_and_close(drain_timeout).await;
            if !client.endpoint_state.is_current(&lease) {
                client.nameserver_health.remove_if_owned(&identity, &lease);
            }
            if !report.is_healthy() {
                warn!(report = %report.to_json(), "removed NameServer endpoint drain was unhealthy");
            }
        });
        if spawned.is_none() {
            warn!("removed NameServer endpoint drain could not be scheduled because the client is shutting down");
        }
    }

    pub(super) async fn get_and_create_nameserver_client_until(
        &self,
        deadline: RequestDeadline,
    ) -> RocketMQResult<Option<NameServerSession<PR>>> {
        deadline.ensure_before_send("<nameserver>")?;
        let state = self.endpoint_state.load();
        let cached_addr = state.chosen().cloned();

        if let Some(addr) = cached_addr {
            if let Some(lease) = state.lease_for(&addr) {
                if let Some(session) = self.connection_registry.healthy_session(&addr, Some(&lease)) {
                    if self.nameserver_health.is_healthy(&addr, &lease) && session.connection().is_healthy() {
                        return Ok(Some(NameServerSession {
                            session,
                            identity: addr,
                            lease,
                            state,
                        }));
                    }
                }
            }
            debug!(%addr, "Cached nameserver is unhealthy, selecting new one");
            self.telemetry
                .record_nameserver_failover(TransportNameServerFailoverReason::Unhealthy);
        }

        let identities = state
            .endpoints()
            .iter()
            .map(|endpoint| endpoint.identity().clone())
            .collect::<Vec<_>>();
        if identities.is_empty() {
            warn!("No nameservers configured in namesrv_addr_list");
            return Ok(None);
        }

        let mut half_open_probe_selected = false;
        let mut candidates = build_nameserver_failover_candidates(
            &identities,
            state.available(),
            self.nameserver_health.latency_tracker(),
            |identity| {
                let Some(lease) = state.lease_for(identity) else {
                    return false;
                };
                match self
                    .nameserver_health
                    .connection_admission_if_current(identity, &lease, || self.endpoint_state.is_current(&lease))
                {
                    Some(CircuitAdmission::Regular) => true,
                    Some(CircuitAdmission::Probe) if !half_open_probe_selected => {
                        half_open_probe_selected = true;
                        true
                    }
                    Some(CircuitAdmission::Probe | CircuitAdmission::Rejected) | None => {
                        self.telemetry
                            .record_nameserver_failover(TransportNameServerFailoverReason::CircuitOpen);
                        false
                    }
                }
            },
        );
        candidates.sort_by_key(|identity| {
            state.lease_for(identity).is_none_or(|lease| {
                self.connection_registry
                    .healthy_session(identity, Some(&lease))
                    .is_none()
            })
        });
        if candidates.is_empty() {
            error!(
                configured = ?identities,
                available = ?state.available(),
                "Failed to select healthy nameserver"
            );
            return Ok(None);
        }

        let mut last_error = None;
        for identity in candidates {
            deadline.ensure_before_send(identity.to_string())?;
            let Some(endpoint) = Self::name_server_endpoint(&state, &identity) else {
                continue;
            };
            let Some(lease) = state.lease_for(&identity) else {
                continue;
            };
            info!(
                selected = %identity,
                p99 = ?self.nameserver_health.p99(&identity, &lease),
                errors = self.nameserver_health.error_count(&identity, &lease),
                "Selected nameserver"
            );
            match self
                .create_client_for_nameserver_until(&identity, endpoint, lease.clone(), deadline)
                .await
            {
                Ok(Some(session)) => {
                    self.endpoint_state.set_chosen(&lease);
                    return Ok(Some(NameServerSession {
                        session,
                        identity,
                        lease,
                        state,
                    }));
                }
                Ok(None) => {}
                Err(error) => {
                    self.telemetry
                        .record_nameserver_failover(TransportNameServerFailoverReason::ConnectFailure);
                    self.nameserver_health
                        .record_outcome_if_current(&identity, &lease, Duration::ZERO, false, || {
                            self.endpoint_state.is_current(&lease)
                        });
                    last_error = Some(error);
                }
            }
        }

        if let Some(chosen) = state.chosen() {
            if let Some(lease) = state.lease_for(chosen) {
                self.endpoint_state.clear_chosen_if_matches(&lease);
            }
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    pub(super) async fn scan_available_name_srv(&self) {
        let state = self.endpoint_state.load();
        if state.endpoints().is_empty() {
            debug!("No nameservers configured, skipping availability scan");
            return;
        }

        let results = futures::future::join_all(state.endpoints().iter().cloned().map(|endpoint| {
            let identity = endpoint.identity().clone();
            let lease = state.lease_for(&identity);
            async move {
                let is_available = match lease.as_ref() {
                    Some(lease) => self
                        .create_client_for_nameserver_until(
                            &identity,
                            endpoint,
                            lease.clone(),
                            RequestDeadline::after(self.tokio_client_config.connect.timeout),
                        )
                        .await
                        .is_ok_and(|session| session.is_some()),
                    None => false,
                };
                (identity, lease, is_available)
            }
        }))
        .await;

        let available_count = results.iter().filter(|(_, _, available)| *available).count();
        for (identity, lease, is_available) in results {
            if lease.is_some_and(|lease| self.endpoint_state.update_availability(&lease, &identity, is_available)) {
                if is_available {
                    info!(%identity, "Nameserver is now available");
                } else {
                    warn!(%identity, "Nameserver is now unavailable");
                }
            }
        }

        debug!(
            available = available_count,
            configured = state.endpoints().len(),
            generation = state.generation(),
            "Availability scan complete"
        );
    }

    pub(super) fn name_server_connect_targets_to_endpoints(targets: Vec<ConnectTarget>) -> Vec<NameServerEndpoint> {
        targets.into_iter().map(NameServerEndpoint::resolved).collect()
    }
}
