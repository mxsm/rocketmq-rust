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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::InspectionRunId;
use rocketmq_sre_contracts::TenantId;
use serde_json::Value;

#[derive(Clone, Debug)]
pub(crate) struct InspectionPackRun {
    pub(crate) pack_id: String,
    pub(crate) pack_version: String,
    pub(crate) input_evidence_ids: Vec<EvidenceId>,
    pub(crate) output: Value,
    pub(crate) partial: bool,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub(crate) struct NewRecommendation {
    pub(crate) severity: String,
    pub(crate) title: String,
    pub(crate) rationale: String,
    pub(crate) evidence_ids: Vec<EvidenceId>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DueInspection {
    pub(crate) id: InspectionRunId,
    pub(crate) tenant_id: TenantId,
    pub(crate) cluster_id: ClusterId,
}
