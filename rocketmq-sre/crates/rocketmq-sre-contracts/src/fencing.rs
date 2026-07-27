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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::ClusterId;
use crate::LeaseId;

/// Monotonically increasing cluster fencing epoch.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseEpoch(pub u64);

/// Two-phase executor lease state.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    PendingFence,
    Active,
    Expired,
}

/// Short-lived grant authorizing one active epoch to dispatch.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseFenceGrant {
    pub lease_id: LeaseId,
    pub owner: String,
    pub cluster_id: ClusterId,
    pub epoch: LeaseEpoch,
    pub audience: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}

/// Read-only grant used while a pending owner reconciles old effects.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileGrant {
    pub lease_id: LeaseId,
    pub owner: String,
    pub cluster_id: ClusterId,
    pub pending_epoch: LeaseEpoch,
    pub audience: String,
    pub expires_at: DateTime<Utc>,
    pub nonce: String,
    pub signature: String,
}

/// Agent acknowledgement that the durable highest epoch has advanced.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FenceAck {
    pub cluster_id: ClusterId,
    pub epoch: LeaseEpoch,
    pub pending_nonce: String,
    pub agent_subject: String,
    pub acknowledged_at: DateTime<Utc>,
    pub signature: String,
}
