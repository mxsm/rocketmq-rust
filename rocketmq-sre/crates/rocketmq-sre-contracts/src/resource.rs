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
use crate::EvidenceId;
use crate::ExecutionId;
use crate::ResourceQuarantineId;
use crate::TenantId;

/// Persistent fail-closed resource block independent of a temporary lock.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQuarantine {
    pub id: ResourceQuarantineId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub resource_key: String,
    pub action_id: Option<String>,
    pub reason_code: String,
    pub source_execution_id: Option<ExecutionId>,
    #[serde(default)]
    pub evidence_ids: Vec<EvidenceId>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub cleared_by: Option<String>,
    pub clear_reason: Option<String>,
    #[serde(default)]
    pub clear_evidence_ids: Vec<EvidenceId>,
    pub cleared_at: Option<DateTime<Utc>>,
}

impl ResourceQuarantine {
    /// Returns whether the resource is still blocked.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.cleared_at.is_none()
    }
}
