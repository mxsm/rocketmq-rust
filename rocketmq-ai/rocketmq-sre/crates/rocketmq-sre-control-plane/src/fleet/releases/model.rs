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
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::FleetRelease;
use rocketmq_sre_contracts::FleetReleaseStatus;
use rocketmq_sre_contracts::FleetReleaseTarget;
use rocketmq_sre_contracts::FleetReleaseTargetState;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::ReleaseId;
use serde::Deserialize;
use serde::Serialize;

pub(super) const FLEET_RELEASE_API_SCHEMA_VERSION: &str = "rocketmq-sre.fleet-release-api.v1";
pub(super) const FLEET_RELEASE_SCHEMA_VERSION: &str = "rocketmq-sre.fleet-release.v1";
pub(super) const FLEET_RELEASE_REPORT_SCHEMA_VERSION: &str = "rocketmq-sre.fleet-release-report.v1";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FleetReleaseTargetSpec {
    pub(super) cluster_id: ClusterId,
    pub(super) region_id: RegionId,
    #[serde(default)]
    pub(super) canary: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateFleetReleaseRequest {
    pub(super) fleet_id: FleetId,
    pub(super) release_ref: String,
    pub(super) artifact_digest: String,
    pub(super) target_version: String,
    pub(super) owner: String,
    pub(super) maintenance_window_start: DateTime<Utc>,
    pub(super) maintenance_window_end: DateTime<Utc>,
    pub(super) rollback_artifact_digest: String,
    pub(super) slo_policy_id: String,
    pub(super) regional_max_concurrency: u32,
    pub(super) targets: Vec<FleetReleaseTargetSpec>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FleetReleaseQuery {
    pub(super) status: Option<FleetReleaseStatus>,
    #[serde(default = "default_limit")]
    pub(super) limit: u16,
    #[serde(default)]
    pub(super) offset: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct FleetReleasePage {
    pub(super) schema_version: &'static str,
    pub(super) items: Vec<FleetRelease>,
    pub(super) total: u64,
    pub(super) limit: u16,
    pub(super) offset: u32,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct FleetReleaseView {
    pub(super) schema_version: &'static str,
    pub(super) release: FleetRelease,
    pub(super) targets: Vec<FleetReleaseTarget>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecordFleetTargetReadinessRequest {
    pub(super) eligible: bool,
    pub(super) release_id: Option<ReleaseId>,
    #[serde(default)]
    pub(super) reason_codes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StartFleetReleaseBatchRequest {
    pub(super) expected_sequence: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecordFleetTargetOutcomeRequest {
    pub(super) state: FleetReleaseTargetState,
    #[serde(default)]
    pub(super) regression_detected: bool,
    pub(super) sanitized_outcome: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FleetReleaseReasonRequest {
    pub(super) reason: String,
}

#[derive(Clone, Debug)]
pub(super) struct FleetReleaseTransition {
    pub(super) release: FleetRelease,
    pub(super) targets: Vec<FleetReleaseTarget>,
    pub(super) reason_code: &'static str,
    pub(super) actor_subject: String,
    pub(super) details: serde_json::Value,
}

pub(super) const fn bounded_limit(limit: u16) -> u16 {
    if limit == 0 {
        1
    } else if limit > 100 {
        100
    } else {
        limit
    }
}

const fn default_limit() -> u16 {
    50
}
