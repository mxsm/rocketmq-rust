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

use serde::Deserialize;
use serde::Serialize;

use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryProxyDrainStateRequest {
    pub proxy_addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyDrainOperationRequest {
    pub proxy_addr: String,
    pub operation_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyDrainPhase {
    Accepting,
    Draining,
    Drained,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyDrainState {
    pub schema_version: String,
    pub phase: ProxyDrainPhase,
    pub operation_id: Option<String>,
    pub admission_open: bool,
    pub routing_open: bool,
    pub readiness_published: bool,
    pub zero_pending: bool,
    pub pending: ProxyDrainPending,
}

impl ProxyDrainState {
    #[allow(clippy::too_many_arguments, reason = "mirrors the closed drain wire contract")]
    pub fn try_from_wire_parts(
        schema_version: String,
        phase: &str,
        operation_id: Option<String>,
        admission_open: bool,
        routing_open: bool,
        readiness_published: bool,
        zero_pending: bool,
        pending: ProxyDrainPending,
    ) -> AdminResult<Self> {
        if schema_version != "rocketmq.proxy-drain.v1" {
            return Err(AdminError::backend(
                "decode_proxy_drain_state",
                "unsupported Proxy drain schema",
            ));
        }
        let phase = match phase {
            "accepting" => ProxyDrainPhase::Accepting,
            "draining" => ProxyDrainPhase::Draining,
            "drained" => ProxyDrainPhase::Drained,
            _ => {
                return Err(AdminError::backend(
                    "decode_proxy_drain_state",
                    "unknown Proxy drain phase",
                ));
            }
        };
        if zero_pending != pending.is_zero()
            || (phase == ProxyDrainPhase::Accepting && (!admission_open || !routing_open))
            || (phase != ProxyDrainPhase::Accepting && (admission_open || routing_open))
            || (phase == ProxyDrainPhase::Drained && !zero_pending)
        {
            return Err(AdminError::backend(
                "decode_proxy_drain_state",
                "inconsistent Proxy drain state",
            ));
        }
        Ok(Self {
            schema_version,
            phase,
            operation_id,
            admission_open,
            routing_open,
            readiness_published,
            zero_pending,
            pending,
        })
    }
}

pub trait ProxyQueryAdmin: Send {
    fn query_drain_state<'a>(
        &'a mut self,
        request: &'a QueryProxyDrainStateRequest,
    ) -> AdminFuture<'a, ProxyDrainState>;
}

pub trait ProxyMutationAdmin: Send {
    fn begin_drain<'a>(&'a mut self, request: &'a ProxyDrainOperationRequest) -> AdminFuture<'a, ProxyDrainState>;

    fn cancel_drain<'a>(&'a mut self, request: &'a ProxyDrainOperationRequest) -> AdminFuture<'a, ProxyDrainState>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_pending_requires_every_dimension_to_be_zero() {
        assert!(ProxyDrainPending::default().is_zero());
        assert!(!ProxyDrainPending {
            rpc_in_flight: 1,
            ..ProxyDrainPending::default()
        }
        .is_zero());
    }

    #[test]
    fn inconsistent_wire_state_fails_closed() {
        assert!(ProxyDrainState::try_from_wire_parts(
            "rocketmq.proxy-drain.v1".to_owned(),
            "drained",
            Some("restart-1".to_owned()),
            false,
            false,
            false,
            true,
            ProxyDrainPending {
                sessions: 1,
                ..ProxyDrainPending::default()
            },
        )
        .is_err());
    }
}
