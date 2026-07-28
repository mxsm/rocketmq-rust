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

use std::collections::BTreeSet;

use rocketmq_sre_contracts::ExecutionId;
use rocketmq_sre_contracts::PlanStepId;
use serde::Deserialize;
use serde::Serialize;

use super::AgentActionHandler;
use super::DriverFuture;

/// Closed Broker fields supported by the supervised patch action.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerConfigPatch {
    pub send_message_thread_pool_nums: Option<u32>,
    pub pull_message_thread_pool_nums: Option<u32>,
    pub flush_delay_offset_interval_ms: Option<u64>,
}

impl BrokerConfigPatch {
    #[must_use]
    pub fn field_names(&self) -> BTreeSet<String> {
        [
            self.send_message_thread_pool_nums
                .map(|_| "send_message_thread_pool_nums".to_owned()),
            self.pull_message_thread_pool_nums
                .map(|_| "pull_message_thread_pool_nums".to_owned()),
            self.flush_delay_offset_interval_ms
                .map(|_| "flush_delay_offset_interval_ms".to_owned()),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.send_message_thread_pool_nums.is_none()
            && self.pull_message_thread_pool_nums.is_none()
            && self.flush_delay_offset_interval_ms.is_none()
    }
}

/// Sanitized allowlisted Broker configuration and CAS generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BrokerConfigPatchState {
    pub generation: u64,
    pub values: BrokerConfigPatch,
    pub supported_fields: BTreeSet<String>,
    pub restart_required_fields: BTreeSet<String>,
    pub last_operation_id: Option<String>,
}

/// Closed forward Broker CAS mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerConfigPatchWrite {
    pub broker_addr: String,
    pub expected_generation: u64,
    pub patch: BrokerConfigPatch,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Closed inverse Broker CAS mutation bound to a prior step snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerConfigPatchRestore {
    pub broker_addr: String,
    pub operation_id: String,
    pub execution_id: ExecutionId,
    pub plan_step_id: PlanStepId,
}

/// Known outcome of a Broker generation-CAS operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerConfigPatchApplyOutcome {
    Applied {
        previous_generation: u64,
        generation: u64,
    },
    GenerationConflict {
        expected_generation: u64,
        actual_generation: u64,
    },
}

/// Exact Admin operations available to the allowlisted Broker config action.
///
/// Forward apply must persist the three-field before snapshot with the effect
/// identity before issuing the Admin Core CAS call. Restore must read the
/// latest generation and apply that snapshot as an inverse CAS; it must never
/// write the old generation value or use the legacy non-CAS update path.
pub trait BrokerConfigPatchClient: Send + Sync {
    fn broker_config_patch_state<'a>(&'a self, broker_addr: &'a str) -> DriverFuture<'a, BrokerConfigPatchState>;

    fn patch_broker_config<'a>(
        &'a self,
        request: &'a BrokerConfigPatchWrite,
    ) -> DriverFuture<'a, BrokerConfigPatchApplyOutcome>;

    fn restore_broker_config<'a>(
        &'a self,
        request: &'a BrokerConfigPatchRestore,
    ) -> DriverFuture<'a, BrokerConfigPatchApplyOutcome>;
}

/// Typed RocketMQ Admin mutation adapter.
///
/// Implementations must map closed action DTOs to `rocketmq-admin-core`
/// mutation methods. Raw RequestCode, delete, clean, and arbitrary property
/// maps are not part of this boundary.
pub trait AdminCoreDriver: AgentActionHandler {}
