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

//! Broker heartbeat management module
//!
//! This module defines the trait for managing broker heartbeats, tracking broker
//! liveness, and notifying listeners when brokers become inactive.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionStateView;

use crate::heartbeat::broker_live_info::BrokerLiveInfo;
use crate::helper::broker_lifecycle_listener::BrokerLifecycleListener;

/// Default broker channel expiration time in milliseconds
pub const DEFAULT_BROKER_CHANNEL_EXPIRED_TIME: u64 = 10_000;

/// Result of applying transport-session fencing before recording a broker heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerHeartbeatAdmission {
    /// The heartbeat belongs to the current session and may be replicated.
    Accepted,
    /// A newer session already owns the broker identity.
    Superseded,
    /// The bounded session-fencing table cannot admit another broker identity.
    CapacityExceeded,
    /// The broker identity or heartbeat timeout is outside the admitted domain.
    Invalid,
}

#[derive(Clone)]
enum BrokerSessionLifecycle {
    Transport(SessionStateView),
    #[cfg(test)]
    Test(Arc<std::sync::atomic::AtomicBool>),
}

impl BrokerSessionLifecycle {
    fn is_closed(&self) -> bool {
        match self {
            Self::Transport(state) => state.is_closed(),
            #[cfg(test)]
            Self::Test(closed) => closed.load(std::sync::atomic::Ordering::Acquire),
        }
    }
}

/// Stable identity and read-only lifecycle observation for one broker session.
///
/// The capability deliberately cannot write, close, or cancel the transport
/// session. It is sufficient for heartbeat registration and disconnect cleanup.
#[derive(Clone)]
pub struct BrokerSession {
    id: BrokerSessionId,
    generation: u64,
    lifecycle: BrokerSessionLifecycle,
}

/// Stable, process-local identity for one registered broker session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerSessionId(BrokerSessionIdInner);

#[derive(Clone, Debug, Eq, PartialEq)]
enum BrokerSessionIdInner {
    Transport(SessionId),
    #[cfg(test)]
    Test(u64),
}

impl From<SessionId> for BrokerSessionId {
    fn from(id: SessionId) -> Self {
        Self(BrokerSessionIdInner::Transport(id))
    }
}

impl BrokerSessionId {
    #[cfg(test)]
    pub(crate) const fn for_test(id: u64) -> Self {
        Self(BrokerSessionIdInner::Test(id))
    }
}

impl BrokerSession {
    pub(crate) fn new(id: SessionId, generation: u64, lifecycle: SessionStateView) -> Self {
        Self {
            id: id.into(),
            generation,
            lifecycle: BrokerSessionLifecycle::Transport(lifecycle),
        }
    }

    /// Returns the stable process-local transport owner identity.
    #[must_use]
    pub fn id(&self) -> BrokerSessionId {
        self.id.clone()
    }

    /// Returns the monotonic owner generation allocated by the transport.
    #[must_use]
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns whether the canonical transport session has closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.lifecycle.is_closed()
    }

    #[cfg(test)]
    pub(crate) fn for_test(id: u64, closed: Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self {
            id: BrokerSessionId::for_test(id),
            generation: id,
            lifecycle: BrokerSessionLifecycle::Test(closed),
        }
    }
}

impl fmt::Debug for BrokerSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerSession")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Broker heartbeat manager trait
///
/// This trait defines the interface for managing broker heartbeats, tracking broker
/// liveness, and notifying listeners of broker state changes.
///
/// # Thread Safety
///
/// All methods in this trait must be thread-safe and can be called concurrently
/// from multiple tasks/threads.
pub trait BrokerHeartbeatManager: Send + Sync {
    /// Initialize the heartbeat manager resources
    ///
    /// This should be called once before calling `start()`.
    fn initialize(&mut self);

    /// Process a broker heartbeat for the canonical transport session.
    ///
    /// Updates the local heartbeat state after fencing superseded sessions. Callers
    /// must replicate the heartbeat only when this returns
    /// [`BrokerHeartbeatAdmission::Accepted`].
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    /// * `broker_addr` - Broker address
    /// * `broker_id` - Broker ID
    /// * `timeout_millis` - Heartbeat timeout in milliseconds
    /// * `session` - Stable identity and read-only lifecycle for the broker connection
    /// * `epoch` - Broker epoch (for leader election)
    /// * `max_offset` - Maximum message offset
    /// * `confirm_offset` - Confirmed message offset
    /// * `election_priority` - Election priority (lower is higher priority)
    #[allow(
        clippy::too_many_arguments,
        reason = "preserves the typed broker heartbeat protocol fields on the public manager boundary"
    )]
    fn on_broker_session_heartbeat(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_addr: &str,
        broker_id: i64,
        timeout_millis: Option<u64>,
        session: BrokerSession,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        election_priority: Option<i32>,
    ) -> BrokerHeartbeatAdmission;

    /// Start the heartbeat manager
    ///
    /// This starts the background task that periodically scans for inactive brokers.
    fn start(&mut self);

    /// Shutdown the heartbeat manager
    ///
    /// This stops the background scanning task and cleans up resources.
    fn shutdown(&mut self);

    /// Register a broker lifecycle listener
    ///
    /// # Arguments
    ///
    /// * `listener` - The listener to register
    fn register_broker_lifecycle_listener(&mut self, listener: Arc<dyn BrokerLifecycleListener>);

    /// Handle canonical broker session closure.
    ///
    /// This removes the broker registered to the closed session and notifies listeners.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Stable identity of the closed session
    fn on_broker_session_close(&self, session_id: BrokerSessionId);

    /// Get broker live information
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    /// * `broker_id` - Broker ID
    ///
    /// # Returns
    ///
    /// The broker live information if the broker is registered, None otherwise
    fn get_broker_live_info(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> Option<BrokerLiveInfo>;

    /// Check if a broker is active
    ///
    /// A broker is considered active if:
    /// 1. It is registered in the live table
    /// 2. Its last heartbeat timestamp plus timeout is >= current time
    ///
    /// # Arguments
    ///
    /// * `cluster_name` - Cluster name
    /// * `broker_name` - Broker name
    /// * `broker_id` - Broker ID
    ///
    /// # Returns
    ///
    /// true if the broker is active, false otherwise
    fn is_broker_active(&self, cluster_name: &str, broker_name: &str, broker_id: i64) -> bool;

    /// Get the number of active brokers in each cluster and broker set
    ///
    /// # Returns
    ///
    /// A nested map: cluster_name -> broker_name -> active_broker_count
    fn get_active_brokers_num(&self) -> HashMap<String, HashMap<String, u32>>;
}
