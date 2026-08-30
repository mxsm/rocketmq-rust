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

use std::collections::BTreeMap;

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::FleetRelease;
use rocketmq_sre_contracts::FleetReleaseBatch;
use rocketmq_sre_contracts::FleetReleaseTarget;
use rocketmq_sre_contracts::FleetReleaseTargetState;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::ReleaseStatus;
use semver::Version;

use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::fleet::repository::FleetRepository;

use super::super::model::CreateFleetReleaseRequest;

const MAX_FLEET_RELEASE_TARGETS: usize = 100;
const MAX_REASON_CODES: usize = 32;
const MAX_WINDOW_DAYS: i64 = 30;

pub(super) fn build_batches(request: &CreateFleetReleaseRequest) -> Result<Vec<FleetReleaseBatch>, ControlPlaneError> {
    let canary = request
        .targets
        .iter()
        .find(|target| target.canary)
        .ok_or_else(|| invalid_request("Fleet release requires one canary target"))?;
    let mut batches = vec![FleetReleaseBatch {
        sequence: 0,
        region_id: canary.region_id,
        cluster_ids: vec![canary.cluster_id],
        max_concurrency: 1,
        canary: true,
    }];
    let mut regional = BTreeMap::<RegionId, Vec<ClusterId>>::new();
    for target in request.targets.iter().filter(|target| !target.canary) {
        regional.entry(target.region_id).or_default().push(target.cluster_id);
    }
    for clusters in regional.values_mut() {
        clusters.sort();
    }
    let mut ordered_regions = Vec::new();
    if regional.contains_key(&canary.region_id) {
        ordered_regions.push(canary.region_id);
    }
    ordered_regions.extend(
        regional
            .keys()
            .copied()
            .filter(|region_id| *region_id != canary.region_id),
    );
    for region_id in ordered_regions {
        let clusters = regional
            .remove(&region_id)
            .ok_or_else(|| ControlPlaneError::configuration("Fleet release regional batch is missing"))?;
        let chunk_size = usize::try_from(request.regional_max_concurrency)
            .map_err(|_| ControlPlaneError::configuration("Fleet release concurrency is invalid"))?;
        for chunk in clusters.chunks(chunk_size) {
            let sequence = u32::try_from(batches.len())
                .map_err(|_| ControlPlaneError::configuration("Fleet release batch count is invalid"))?;
            let max_concurrency = u32::try_from(chunk.len())
                .map_err(|_| ControlPlaneError::configuration("Fleet release batch size is invalid"))?;
            batches.push(FleetReleaseBatch {
                sequence,
                region_id,
                cluster_ids: chunk.to_vec(),
                max_concurrency,
                canary: false,
            });
        }
    }
    Ok(batches)
}

pub(super) fn project_aggregate_state(release: &mut FleetRelease, targets: &[FleetReleaseTarget]) {
    if targets.iter().any(|target| {
        target.regression_detected
            || matches!(
                target.state,
                FleetReleaseTargetState::Paused | FleetReleaseTargetState::Failed
            )
    }) {
        release.status = rocketmq_sre_contracts::FleetReleaseStatus::Paused;
        release.active_batch = None;
        return;
    }
    if targets
        .iter()
        .any(|target| target.state == FleetReleaseTargetState::RollingBack)
    {
        release.status = rocketmq_sre_contracts::FleetReleaseStatus::RollingBack;
        return;
    }
    if targets
        .iter()
        .any(|target| target.state == FleetReleaseTargetState::RolledBack)
    {
        release.status = rocketmq_sre_contracts::FleetReleaseStatus::RolledBack;
        release.active_batch = None;
        return;
    }
    let active = release.active_batch.is_some_and(|sequence| {
        targets.iter().any(|target| {
            target.batch_sequence == sequence
                && matches!(
                    target.state,
                    FleetReleaseTargetState::CanaryRunning | FleetReleaseTargetState::BatchRunning
                )
        })
    });
    if active {
        return;
    }
    release.active_batch = None;
    if targets
        .iter()
        .any(|target| target.state == FleetReleaseTargetState::Ready)
    {
        release.status = rocketmq_sre_contracts::FleetReleaseStatus::Ready;
    } else if targets.iter().all(is_complete_for_aggregate) {
        release.status = rocketmq_sre_contracts::FleetReleaseStatus::Completed;
    }
}

pub(super) async fn require_linked_outcome(
    repository: &FleetRepository,
    auth: &AuthContext,
    target: &FleetReleaseTarget,
    state: FleetReleaseTargetState,
) -> Result<(), ControlPlaneError> {
    if state == FleetReleaseTargetState::Skipped {
        return Ok(());
    }
    let release_id = target
        .release_id
        .ok_or_else(|| ControlPlaneError::configuration("Fleet release target has no linked release workflow"))?;
    let actual = repository
        .linked_release_status(auth.tenant_id, target.cluster_id, release_id)
        .await?;
    let matches = match state {
        FleetReleaseTargetState::Completed => actual == ReleaseStatus::Completed,
        FleetReleaseTargetState::Paused => actual == ReleaseStatus::Paused,
        FleetReleaseTargetState::RollingBack => actual == ReleaseStatus::RollingBack,
        FleetReleaseTargetState::RolledBack => actual == ReleaseStatus::RolledBack,
        FleetReleaseTargetState::Failed => {
            matches!(actual, ReleaseStatus::Failed | ReleaseStatus::ManualTakeover)
        }
        FleetReleaseTargetState::Ready => actual == ReleaseStatus::Ready,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(ControlPlaneError::conflict_code(
            "fleet_release_outcome_mismatch",
            "Fleet target outcome does not match its independently supervised release workflow",
        ))
    }
}

pub(super) fn validate_create_request(request: &CreateFleetReleaseRequest) -> Result<(), ControlPlaneError> {
    if request.targets.len() < 2 || request.targets.len() > MAX_FLEET_RELEASE_TARGETS {
        return Err(invalid_request("Fleet release must target between 2 and 100 clusters"));
    }
    if request.targets.iter().filter(|target| target.canary).count() != 1 {
        return Err(invalid_request("Fleet release requires exactly one canary target"));
    }
    if request.regional_max_concurrency == 0 || request.regional_max_concurrency > 32 {
        return Err(invalid_request(
            "Fleet release regional concurrency must be between 1 and 32",
        ));
    }
    validate_safe_text(&request.release_ref, "Fleet release reference", 256)?;
    validate_digest(&request.artifact_digest, "Fleet release artifact digest")?;
    validate_digest(
        &request.rollback_artifact_digest,
        "Fleet release rollback artifact digest",
    )?;
    validate_safe_text(&request.owner, "Fleet release owner", 256)?;
    validate_safe_text(&request.slo_policy_id, "Fleet release SLO policy", 256)?;
    Version::parse(request.target_version.trim())
        .map_err(|_| invalid_request("Fleet release target version must be semantic"))?;
    if request.maintenance_window_end <= request.maintenance_window_start
        || request.maintenance_window_end <= Utc::now()
        || request.maintenance_window_end - request.maintenance_window_start > Duration::days(MAX_WINDOW_DAYS)
    {
        return Err(invalid_request(
            "Fleet release maintenance window must end in the future and last no more than 30 days",
        ));
    }
    Ok(())
}

pub(super) fn validate_target_transition(
    current: FleetReleaseTargetState,
    next: FleetReleaseTargetState,
    regression_detected: bool,
    canary: bool,
) -> Result<(), ControlPlaneError> {
    let allowed = matches!(
        (current, next),
        (
            FleetReleaseTargetState::CanaryRunning | FleetReleaseTargetState::BatchRunning,
            FleetReleaseTargetState::Completed
                | FleetReleaseTargetState::Paused
                | FleetReleaseTargetState::RollingBack
                | FleetReleaseTargetState::Failed
        ) | (
            FleetReleaseTargetState::Paused,
            FleetReleaseTargetState::Ready
                | FleetReleaseTargetState::RollingBack
                | FleetReleaseTargetState::RolledBack
                | FleetReleaseTargetState::Skipped
                | FleetReleaseTargetState::Failed
        ) | (
            FleetReleaseTargetState::RollingBack,
            FleetReleaseTargetState::RolledBack | FleetReleaseTargetState::Failed
        )
    );
    if !allowed || (regression_detected && next != FleetReleaseTargetState::Paused) {
        return Err(state_conflict("Fleet release target transition is not allowed"));
    }
    if canary && next == FleetReleaseTargetState::Skipped {
        return Err(state_conflict(
            "Fleet release canary cannot be skipped after execution starts",
        ));
    }
    Ok(())
}

pub(super) fn validate_reason_codes(values: &[String]) -> Result<(), ControlPlaneError> {
    if values.len() > MAX_REASON_CODES {
        return Err(invalid_request("Fleet release readiness reason list is too large"));
    }
    for value in values {
        let valid = !value.is_empty()
            && value.len() <= 64
            && value.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || "_-.".contains(character)
            });
        if !valid {
            return Err(invalid_request("Fleet release readiness reason code is invalid"));
        }
    }
    Ok(())
}

pub(super) fn validate_optional_safe_text(
    value: Option<&str>,
    field: &str,
    max: usize,
) -> Result<(), ControlPlaneError> {
    if let Some(value) = value {
        validate_safe_text(value, field, max)?;
    }
    Ok(())
}

pub(super) fn validate_safe_text(value: &str, field: &str, max: usize) -> Result<(), ControlPlaneError> {
    let trimmed = value.trim();
    let lowercase = trimmed.to_ascii_lowercase();
    if trimmed.is_empty()
        || trimmed.len() > max
        || trimmed.chars().any(char::is_control)
        || [
            "authorization:",
            "bearer ",
            "password=",
            "private key",
            "secret=",
            "token=",
        ]
        .iter()
        .any(|marker| lowercase.contains(marker))
    {
        Err(invalid_request(&format!("{field} is invalid or sensitive")))
    } else {
        Ok(())
    }
}

pub(super) fn transition_time(previous: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    Utc::now().max(previous + Duration::microseconds(1))
}

pub(super) fn require_read_role(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.iter().any(|role| {
        matches!(
            role.as_str(),
            "diagnose" | "rocketmq:diagnose" | "operator" | "approver"
        )
    }) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "Fleet release read access requires a diagnose or operator role",
        ))
    }
}

pub(super) fn require_operator(auth: &AuthContext) -> Result<(), ControlPlaneError> {
    if auth.roles.contains("operator") {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "unauthorized_scope",
            "Fleet release coordination requires the operator role",
        ))
    }
}

pub(super) fn authorize_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if auth.clusters.contains(&cluster_id) {
        Ok(())
    } else {
        Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "cluster is outside the authenticated Fleet release scope",
        ))
    }
}

pub(super) fn allowed_clusters(auth: &AuthContext) -> Vec<ClusterId> {
    auth.clusters.iter().copied().collect()
}

pub(super) fn state_conflict(message: &str) -> ControlPlaneError {
    ControlPlaneError::conflict_code("fleet_release_state_invalid", message)
}

pub(super) fn invalid_request(message: &str) -> ControlPlaneError {
    ControlPlaneError::validation("invalid_request", message)
}

fn validate_digest(value: &str, field: &str) -> Result<(), ControlPlaneError> {
    let value = value.strip_prefix("sha256:").unwrap_or_default();
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(invalid_request(&format!("{field} must be a SHA-256 digest")))
    }
}

fn is_complete_for_aggregate(target: &FleetReleaseTarget) -> bool {
    matches!(
        target.state,
        FleetReleaseTargetState::Completed | FleetReleaseTargetState::Ineligible | FleetReleaseTargetState::Skipped
    )
}
