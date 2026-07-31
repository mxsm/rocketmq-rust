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

//! Race-free, reversible admission drain used before a supervised Proxy restart.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use rocketmq_runtime::ServiceLifecycle;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ClientSessionRegistry;

pub const PROXY_DRAIN_SCHEMA_VERSION: &str = "rocketmq.proxy-drain.v1";
const MAX_OPERATION_ID_BYTES: usize = 128;

/// Stable Proxy admission phases exposed to supervised SRE callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyDrainPhase {
    Accepting,
    Draining,
    Drained,
}

/// Exact pending-work counters required before a single Proxy may restart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyDrainPending {
    pub active_connections: usize,
    pub sessions: usize,
    pub receipt_handles: usize,
    pub prepared_transactions: usize,
    pub telemetry_links: usize,
    pub remoting_channels: usize,
    pub telemetry_commands: usize,
    pub rpc_in_flight: usize,
}

impl ProxyDrainPending {
    pub const fn is_zero(self) -> bool {
        self.active_connections == 0
            && self.sessions == 0
            && self.receipt_handles == 0
            && self.prepared_transactions == 0
            && self.telemetry_links == 0
            && self.remoting_channels == 0
            && self.telemetry_commands == 0
            && self.rpc_in_flight == 0
    }
}

/// Authenticated management view of one Proxy drain operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyDrainSnapshot {
    pub schema_version: String,
    pub phase: ProxyDrainPhase,
    pub operation_id: Option<String>,
    pub admission_open: bool,
    pub routing_open: bool,
    pub readiness_published: bool,
    pub zero_pending: bool,
    pub pending: ProxyDrainPending,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProxyDrainError {
    #[error("Proxy drain operation id must contain between 1 and {MAX_OPERATION_ID_BYTES} bytes")]
    InvalidOperationId,

    #[error("Proxy drain requires an attached ready service lifecycle")]
    LifecycleUnavailable,

    #[error("Proxy drain operation '{active_operation_id}' is already active")]
    OperationConflict { active_operation_id: String },

    #[error("Proxy is draining and does not accept new requests")]
    AdmissionClosed,

    #[error("Proxy RPC in-flight counter overflowed")]
    CounterOverflow,

    #[error("Proxy drain operation does not match the active operation")]
    OperationMismatch,

    #[error("Proxy readiness transition failed: {message}")]
    ReadinessTransition { message: String },
}

#[derive(Debug)]
struct ProxyDrainState {
    phase: ProxyDrainPhase,
    operation_id: Option<String>,
    rpc_in_flight: usize,
    lifecycle: Option<ServiceLifecycle>,
}

impl Default for ProxyDrainState {
    fn default() -> Self {
        Self {
            phase: ProxyDrainPhase::Accepting,
            operation_id: None,
            rpc_in_flight: 0,
            lifecycle: None,
        }
    }
}

/// Shared admission controller for both gRPC and Remoting ingress.
#[derive(Debug, Clone, Default)]
pub struct ProxyDrainController {
    state: Arc<Mutex<ProxyDrainState>>,
}

impl ProxyDrainController {
    pub fn attach_lifecycle(&self, lifecycle: ServiceLifecycle) -> Result<(), ProxyDrainError> {
        let mut state = self.lock_state();
        if state.phase != ProxyDrainPhase::Accepting {
            return Err(ProxyDrainError::LifecycleUnavailable);
        }
        state.lifecycle = Some(lifecycle);
        Ok(())
    }

    /// Acquires a request-lifetime admission token.
    ///
    /// The phase check and counter increment occur under one lock, so beginning
    /// a drain cannot race a newly admitted request.
    pub fn try_admit(&self) -> Result<ProxyDrainAdmission, ProxyDrainError> {
        let mut state = self.lock_state();
        if state.phase != ProxyDrainPhase::Accepting {
            return Err(ProxyDrainError::AdmissionClosed);
        }
        state.rpc_in_flight = state
            .rpc_in_flight
            .checked_add(1)
            .ok_or(ProxyDrainError::CounterOverflow)?;
        Ok(ProxyDrainAdmission {
            state: Arc::clone(&self.state),
            active: true,
        })
    }

    /// Stops admission and removes Kubernetes readiness for one operation.
    pub fn begin(&self, operation_id: &str) -> Result<(), ProxyDrainError> {
        let operation_id = validate_operation_id(operation_id)?;
        let mut state = self.lock_state();
        if state.phase != ProxyDrainPhase::Accepting {
            return match state.operation_id.as_deref() {
                Some(active) if active == operation_id => Ok(()),
                Some(active) => Err(ProxyDrainError::OperationConflict {
                    active_operation_id: active.to_owned(),
                }),
                None => Err(ProxyDrainError::LifecycleUnavailable),
            };
        }
        let lifecycle = state.lifecycle.as_ref().ok_or(ProxyDrainError::LifecycleUnavailable)?;
        lifecycle
            .suspend_readiness_for_maintenance()
            .map_err(|error| ProxyDrainError::ReadinessTransition {
                message: error.to_string(),
            })?;
        state.phase = ProxyDrainPhase::Draining;
        state.operation_id = Some(operation_id.to_owned());
        Ok(())
    }

    /// Cancels a timed-out drain and atomically restores admission/readiness.
    pub fn cancel(&self, operation_id: &str) -> Result<(), ProxyDrainError> {
        let operation_id = validate_operation_id(operation_id)?;
        let mut state = self.lock_state();
        if state.phase == ProxyDrainPhase::Accepting && state.operation_id.is_none() {
            return Ok(());
        }
        if state.operation_id.as_deref() != Some(operation_id) {
            return Err(ProxyDrainError::OperationMismatch);
        }
        let lifecycle = state.lifecycle.as_ref().ok_or(ProxyDrainError::LifecycleUnavailable)?;
        lifecycle
            .restore_readiness_after_maintenance()
            .map_err(|error| ProxyDrainError::ReadinessTransition {
                message: error.to_string(),
            })?;
        state.phase = ProxyDrainPhase::Accepting;
        state.operation_id = None;
        Ok(())
    }

    /// Returns a bounded snapshot and promotes `Draining` to `Drained` only
    /// after every required counter is exactly zero.
    pub fn snapshot<C>(&self, sessions: &ClientSessionRegistry<C>) -> ProxyDrainSnapshot {
        let mut state = self.lock_state();
        let pending = ProxyDrainPending {
            active_connections: sessions
                .telemetry_link_count()
                .saturating_add(sessions.remoting_channel_count()),
            sessions: sessions.len(),
            receipt_handles: sessions.tracked_handle_count(),
            prepared_transactions: sessions.prepared_transaction_count(),
            telemetry_links: sessions.telemetry_link_count(),
            remoting_channels: sessions.remoting_channel_count(),
            telemetry_commands: sessions.pending_telemetry_command_count(),
            rpc_in_flight: state.rpc_in_flight,
        };
        let zero_pending = pending.is_zero();
        if state.phase == ProxyDrainPhase::Draining && zero_pending {
            state.phase = ProxyDrainPhase::Drained;
        }
        let readiness_published = state.lifecycle.as_ref().is_some_and(ServiceLifecycle::is_ready);
        ProxyDrainSnapshot {
            schema_version: PROXY_DRAIN_SCHEMA_VERSION.to_owned(),
            phase: state.phase,
            operation_id: state.operation_id.clone(),
            admission_open: state.phase == ProxyDrainPhase::Accepting,
            routing_open: state.phase == ProxyDrainPhase::Accepting,
            readiness_published,
            zero_pending,
            pending,
        }
    }

    pub fn phase(&self) -> ProxyDrainPhase {
        self.lock_state().phase
    }

    fn lock_state(&self) -> MutexGuard<'_, ProxyDrainState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// RAII token proving that one ingress request was admitted before drain.
#[derive(Debug)]
pub struct ProxyDrainAdmission {
    state: Arc<Mutex<ProxyDrainState>>,
    active: bool,
}

impl Drop for ProxyDrainAdmission {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.rpc_in_flight = state.rpc_in_flight.saturating_sub(1);
        self.active = false;
    }
}

fn validate_operation_id(operation_id: &str) -> Result<&str, ProxyDrainError> {
    let operation_id = operation_id.trim();
    if operation_id.is_empty() || operation_id.len() > MAX_OPERATION_ID_BYTES {
        return Err(ProxyDrainError::InvalidOperationId);
    }
    Ok(operation_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_runtime::ServiceLifecycleConfig;

    use super::*;

    fn ready_lifecycle() -> ServiceLifecycle {
        let lifecycle = ServiceLifecycle::new(ServiceLifecycleConfig {
            service_name: Arc::from("proxy-drain-test"),
            probe_bind_addr: None,
            shutdown_timeout: Duration::from_secs(45),
            liveness_stale_after: Duration::from_secs(30),
        });
        lifecycle.mark_ready().unwrap();
        lifecycle
    }

    #[test]
    fn begin_is_race_safe_idempotent_and_cancel_restores_readiness() {
        let sessions = ClientSessionRegistry::<()>::default();
        let controller = ProxyDrainController::default();
        let lifecycle = ready_lifecycle();
        controller.attach_lifecycle(lifecycle.clone()).unwrap();

        let admission = controller.try_admit().unwrap();
        controller.begin("restart-1").unwrap();
        controller.begin("restart-1").unwrap();
        assert!(!lifecycle.is_ready());
        assert!(matches!(controller.try_admit(), Err(ProxyDrainError::AdmissionClosed)));
        assert_eq!(controller.snapshot(&sessions).pending.rpc_in_flight, 1);
        assert!(matches!(
            controller.begin("restart-2"),
            Err(ProxyDrainError::OperationConflict { .. })
        ));

        drop(admission);
        let drained = controller.snapshot(&sessions);
        assert_eq!(drained.phase, ProxyDrainPhase::Drained);
        assert!(drained.zero_pending);

        controller.cancel("restart-1").unwrap();
        assert!(lifecycle.is_ready());
        assert!(controller.try_admit().is_ok());
    }

    #[test]
    fn begin_fails_closed_without_ready_lifecycle() {
        let controller = ProxyDrainController::default();
        assert_eq!(
            controller.begin("restart-1"),
            Err(ProxyDrainError::LifecycleUnavailable)
        );

        let lifecycle = ServiceLifecycle::new(ServiceLifecycleConfig {
            service_name: Arc::from("proxy-drain-test"),
            probe_bind_addr: None,
            shutdown_timeout: Duration::from_secs(45),
            liveness_stale_after: Duration::from_secs(30),
        });
        controller.attach_lifecycle(lifecycle).unwrap();
        assert!(matches!(
            controller.begin("restart-1"),
            Err(ProxyDrainError::ReadinessTransition { .. })
        ));
        assert_eq!(controller.phase(), ProxyDrainPhase::Accepting);
    }
}
