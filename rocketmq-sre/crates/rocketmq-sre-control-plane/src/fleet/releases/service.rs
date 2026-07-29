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
use std::collections::BTreeSet;

use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ClusterRegistrationState;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::FleetRelease;
use rocketmq_sre_contracts::FleetReleaseId;
use rocketmq_sre_contracts::FleetReleaseReport;
use rocketmq_sre_contracts::FleetReleaseStatus;
use rocketmq_sre_contracts::FleetReleaseTarget;
use rocketmq_sre_contracts::FleetReleaseTargetState;
use rocketmq_sre_contracts::ReleaseStatus;

use self::support::allowed_clusters;
use self::support::authorize_cluster;
use self::support::build_batches;
use self::support::invalid_request;
use self::support::project_aggregate_state;
use self::support::require_linked_outcome;
use self::support::require_operator;
use self::support::require_read_role;
use self::support::state_conflict;
use self::support::transition_time;
use self::support::validate_create_request;
use self::support::validate_optional_safe_text;
use self::support::validate_reason_codes;
use self::support::validate_safe_text;
use self::support::validate_target_transition;
use super::model::CreateFleetReleaseRequest;
use super::model::FLEET_RELEASE_API_SCHEMA_VERSION;
use super::model::FLEET_RELEASE_REPORT_SCHEMA_VERSION;
use super::model::FLEET_RELEASE_SCHEMA_VERSION;
use super::model::FleetReleasePage;
use super::model::FleetReleaseQuery;
use super::model::FleetReleaseReasonRequest;
use super::model::FleetReleaseTransition;
use super::model::FleetReleaseView;
use super::model::RecordFleetTargetOutcomeRequest;
use super::model::RecordFleetTargetReadinessRequest;
use super::model::StartFleetReleaseBatchRequest;
use super::model::bounded_limit;
use crate::ControlPlaneError;
use crate::auth::AuthContext;
use crate::fleet::FleetService;

mod support;

impl FleetService {
    pub(crate) async fn create_fleet_release(
        &self,
        auth: &AuthContext,
        request: &CreateFleetReleaseRequest,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_operator(auth)?;
        validate_create_request(request)?;
        let mut seen = BTreeSet::new();
        for target in &request.targets {
            authorize_cluster(auth, target.cluster_id)?;
            if !seen.insert(target.cluster_id) {
                return Err(invalid_request("Fleet release contains a duplicate cluster"));
            }
            let registration = self
                .repository
                .cluster_registration(auth.tenant_id, target.cluster_id)
                .await?;
            if registration.fleet_id != request.fleet_id
                || registration.region_id != target.region_id
                || !matches!(
                    registration.state,
                    ClusterRegistrationState::Active | ClusterRegistrationState::ReadOnlyDegraded
                )
            {
                return Err(ControlPlaneError::forbidden(
                    "fleet_release_scope_mismatch",
                    "Fleet release target does not match an active tenant, Fleet, and region registration",
                ));
            }
        }

        let batches = build_batches(request)?;
        let batch_by_cluster = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .cluster_ids
                    .iter()
                    .map(move |cluster_id| (*cluster_id, (batch.sequence, batch.canary)))
            })
            .collect::<BTreeMap<_, _>>();
        let now = Utc::now();
        let id = FleetReleaseId::new();
        let release = FleetRelease {
            schema_version: FLEET_RELEASE_SCHEMA_VERSION.to_owned(),
            id,
            fleet_id: request.fleet_id,
            tenant_id: auth.tenant_id,
            correlation_id: CorrelationId::new(),
            release_ref: request.release_ref.trim().to_owned(),
            artifact_digest: request.artifact_digest.to_ascii_lowercase(),
            target_version: request.target_version.trim().to_owned(),
            owner: request.owner.trim().to_owned(),
            maintenance_window_start: request.maintenance_window_start,
            maintenance_window_end: request.maintenance_window_end,
            rollback_artifact_digest: request.rollback_artifact_digest.to_ascii_lowercase(),
            slo_policy_id: request.slo_policy_id.trim().to_owned(),
            status: FleetReleaseStatus::Planned,
            active_batch: None,
            batches,
            created_at: now,
            updated_at: now,
        };
        let mut targets = request
            .targets
            .iter()
            .map(|spec| {
                let (batch_sequence, canary) = batch_by_cluster
                    .get(&spec.cluster_id)
                    .copied()
                    .ok_or_else(|| ControlPlaneError::configuration("Fleet release batch mapping is incomplete"))?;
                Ok(FleetReleaseTarget {
                    fleet_release_id: id,
                    tenant_id: auth.tenant_id,
                    cluster_id: spec.cluster_id,
                    region_id: spec.region_id,
                    batch_sequence,
                    canary,
                    state: FleetReleaseTargetState::Pending,
                    release_id: None,
                    readiness_reason_codes: Vec::new(),
                    regression_detected: false,
                    sanitized_outcome: None,
                    updated_at: now,
                })
            })
            .collect::<Result<Vec<_>, ControlPlaneError>>()?;
        targets.sort_by_key(|target| (target.batch_sequence, target.cluster_id));
        self.repository
            .create_fleet_release(&release, &targets, &auth.subject)
            .await?;
        Ok(FleetReleaseView {
            schema_version: FLEET_RELEASE_API_SCHEMA_VERSION,
            release,
            targets,
        })
    }

    pub(crate) async fn fleet_releases(
        &self,
        auth: &AuthContext,
        query: &FleetReleaseQuery,
    ) -> Result<FleetReleasePage, ControlPlaneError> {
        require_read_role(auth)?;
        let (items, total) = self
            .repository
            .fleet_releases(auth.tenant_id, &allowed_clusters(auth), query)
            .await?;
        Ok(FleetReleasePage {
            schema_version: FLEET_RELEASE_API_SCHEMA_VERSION,
            items,
            total,
            limit: bounded_limit(query.limit),
            offset: query.offset,
        })
    }

    pub(crate) async fn fleet_release(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_read_role(auth)?;
        self.repository
            .fleet_release(auth.tenant_id, id, &allowed_clusters(auth))
            .await
    }

    pub(crate) async fn begin_fleet_release_readiness(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.fleet_release(auth, id).await?;
        if current.release.status != FleetReleaseStatus::Planned {
            return Err(state_conflict("Fleet release readiness may start only from planned"));
        }
        let now = transition_time(current.release.updated_at);
        let mut release = current.release.clone();
        release.status = FleetReleaseStatus::ReadinessChecking;
        release.updated_at = now;
        let mut targets = current.targets.clone();
        for target in &mut targets {
            target.state = FleetReleaseTargetState::ReadinessChecking;
            target.updated_at = now;
        }
        self.persist_transition(
            &current,
            release,
            targets,
            "fleet_release_readiness_started",
            auth,
            serde_json::json!({}),
        )
        .await
    }

    pub(crate) async fn record_fleet_target_readiness(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
        cluster_id: ClusterId,
        request: &RecordFleetTargetReadinessRequest,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_operator(auth)?;
        authorize_cluster(auth, cluster_id)?;
        validate_reason_codes(&request.reason_codes)?;
        let current = self.fleet_release(auth, id).await?;
        if current.release.status != FleetReleaseStatus::ReadinessChecking {
            return Err(state_conflict(
                "Fleet target readiness may be recorded only during readiness checking",
            ));
        }
        let Some(index) = current
            .targets
            .iter()
            .position(|target| target.cluster_id == cluster_id)
        else {
            return Err(ControlPlaneError::NotFound);
        };
        if current.targets[index].state != FleetReleaseTargetState::ReadinessChecking {
            return Err(state_conflict("Fleet target readiness was already recorded"));
        }
        if request.eligible {
            let release_id = request.release_id.ok_or_else(|| {
                invalid_request("eligible Fleet target requires an independently approved release workflow")
            })?;
            let linked_status = self
                .repository
                .linked_release_status(auth.tenant_id, cluster_id, release_id)
                .await?;
            if linked_status != ReleaseStatus::Ready {
                return Err(ControlPlaneError::conflict_code(
                    "fleet_release_target_not_ready",
                    "linked per-cluster release must pass readiness before Fleet scheduling",
                ));
            }
        } else if request.release_id.is_some() || request.reason_codes.is_empty() {
            return Err(invalid_request(
                "ineligible Fleet target requires reasons and cannot bind a release workflow",
            ));
        }

        let now = transition_time(current.release.updated_at);
        let mut release = current.release.clone();
        let mut targets = current.targets.clone();
        targets[index].state = if request.eligible {
            FleetReleaseTargetState::Ready
        } else {
            FleetReleaseTargetState::Ineligible
        };
        targets[index].release_id = request.release_id;
        targets[index].readiness_reason_codes = request.reason_codes.clone();
        targets[index].updated_at = now;
        if targets
            .iter()
            .all(|target| target.state != FleetReleaseTargetState::ReadinessChecking)
        {
            release.status = if targets
                .iter()
                .find(|target| target.canary)
                .is_some_and(|target| target.state == FleetReleaseTargetState::Ready)
            {
                FleetReleaseStatus::Ready
            } else {
                FleetReleaseStatus::Failed
            };
        }
        release.updated_at = now;
        self.persist_transition(
            &current,
            release,
            targets,
            if request.eligible {
                "fleet_release_target_ready"
            } else {
                "fleet_release_target_ineligible"
            },
            auth,
            serde_json::json!({
                "cluster_id": cluster_id,
                "eligible": request.eligible,
                "reason_codes": request.reason_codes,
            }),
        )
        .await
    }

    pub(crate) async fn start_fleet_release_batch(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
        request: &StartFleetReleaseBatchRequest,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_operator(auth)?;
        let current = self.fleet_release(auth, id).await?;
        if current.release.status != FleetReleaseStatus::Ready || current.release.active_batch.is_some() {
            return Err(state_conflict(
                "Fleet release batch may start only when the aggregate is ready and idle",
            ));
        }
        let now = Utc::now();
        if now < current.release.maintenance_window_start || now > current.release.maintenance_window_end {
            return Err(ControlPlaneError::conflict_code(
                "fleet_release_outside_window",
                "Fleet release batch is outside the approved maintenance window",
            ));
        }
        let next_batch = current
            .release
            .batches
            .iter()
            .find(|batch| {
                batch.cluster_ids.iter().any(|cluster_id| {
                    current.targets.iter().any(|target| {
                        target.cluster_id == *cluster_id && target.state == FleetReleaseTargetState::Ready
                    })
                })
            })
            .ok_or_else(|| state_conflict("Fleet release has no ready batch to start"))?;
        if next_batch.sequence != request.expected_sequence {
            return Err(ControlPlaneError::conflict_code(
                "fleet_release_batch_out_of_order",
                "Fleet release batches must start in deterministic regional order",
            ));
        }
        let ready_targets = current
            .targets
            .iter()
            .filter(|target| {
                target.batch_sequence == next_batch.sequence && target.state == FleetReleaseTargetState::Ready
            })
            .collect::<Vec<_>>();
        if ready_targets.len() > next_batch.max_concurrency as usize {
            return Err(ControlPlaneError::conflict_code(
                "fleet_release_batch_capacity_exceeded",
                "Fleet release batch exceeds its regional concurrency bound",
            ));
        }
        for target in &ready_targets {
            let release_id = target
                .release_id
                .ok_or_else(|| ControlPlaneError::configuration("ready Fleet target lost its release workflow"))?;
            if self
                .repository
                .linked_release_status(auth.tenant_id, target.cluster_id, release_id)
                .await?
                != ReleaseStatus::Ready
            {
                return Err(ControlPlaneError::conflict_code(
                    "fleet_release_linked_state_changed",
                    "linked per-cluster release changed after readiness and Fleet scheduling stopped",
                ));
            }
        }

        let changed_at = transition_time(current.release.updated_at);
        let mut release = current.release.clone();
        release.status = if next_batch.canary {
            FleetReleaseStatus::CanaryRunning
        } else {
            FleetReleaseStatus::BatchRunning
        };
        release.active_batch = Some(next_batch.sequence);
        release.updated_at = changed_at;
        let mut targets = current.targets.clone();
        for target in &mut targets {
            if target.batch_sequence == next_batch.sequence && target.state == FleetReleaseTargetState::Ready {
                target.state = if next_batch.canary {
                    FleetReleaseTargetState::CanaryRunning
                } else {
                    FleetReleaseTargetState::BatchRunning
                };
                target.updated_at = changed_at;
            }
        }
        self.persist_transition(
            &current,
            release,
            targets,
            if next_batch.canary {
                "fleet_release_canary_started"
            } else {
                "fleet_release_batch_started"
            },
            auth,
            serde_json::json!({
                "batch_sequence": next_batch.sequence,
                "max_concurrency": next_batch.max_concurrency,
                "region_id": next_batch.region_id,
            }),
        )
        .await
    }

    pub(crate) async fn record_fleet_target_outcome(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
        cluster_id: ClusterId,
        request: &RecordFleetTargetOutcomeRequest,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_operator(auth)?;
        authorize_cluster(auth, cluster_id)?;
        validate_optional_safe_text(request.sanitized_outcome.as_deref(), "Fleet release outcome", 1_024)?;
        let current = self.fleet_release(auth, id).await?;
        let Some(index) = current
            .targets
            .iter()
            .position(|target| target.cluster_id == cluster_id)
        else {
            return Err(ControlPlaneError::NotFound);
        };
        validate_target_transition(
            current.targets[index].state,
            request.state,
            request.regression_detected,
            current.targets[index].canary,
        )?;
        require_linked_outcome(&self.repository, auth, &current.targets[index], request.state).await?;

        let now = transition_time(current.release.updated_at);
        let mut release = current.release.clone();
        let mut targets = current.targets.clone();
        targets[index].state = request.state;
        targets[index].regression_detected = request.regression_detected;
        targets[index].sanitized_outcome = request.sanitized_outcome.clone();
        targets[index].updated_at = now;
        project_aggregate_state(&mut release, &targets);
        release.updated_at = now;
        self.persist_transition(
            &current,
            release,
            targets,
            if request.regression_detected {
                "fleet_release_canary_regression"
            } else {
                "fleet_release_target_outcome"
            },
            auth,
            serde_json::json!({
                "cluster_id": cluster_id,
                "regression_detected": request.regression_detected,
                "target_state": super::repository::target_state_name(request.state),
            }),
        )
        .await
    }

    pub(crate) async fn pause_fleet_release(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
        request: &FleetReleaseReasonRequest,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_operator(auth)?;
        validate_safe_text(&request.reason, "Fleet release pause reason", 512)?;
        let current = self.fleet_release(auth, id).await?;
        if !matches!(
            current.release.status,
            FleetReleaseStatus::Ready | FleetReleaseStatus::CanaryRunning | FleetReleaseStatus::BatchRunning
        ) {
            return Err(state_conflict("Fleet release cannot be paused from its current state"));
        }
        let now = transition_time(current.release.updated_at);
        let mut release = current.release.clone();
        release.status = FleetReleaseStatus::Paused;
        release.active_batch = None;
        release.updated_at = now;
        let mut targets = current.targets.clone();
        for target in &mut targets {
            if matches!(
                target.state,
                FleetReleaseTargetState::CanaryRunning | FleetReleaseTargetState::BatchRunning
            ) {
                target.state = FleetReleaseTargetState::Paused;
                target.updated_at = now;
            }
        }
        self.persist_transition(
            &current,
            release,
            targets,
            "fleet_release_paused",
            auth,
            serde_json::json!({"reason": request.reason}),
        )
        .await
    }

    pub(crate) async fn resume_fleet_release(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
        request: &FleetReleaseReasonRequest,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        require_operator(auth)?;
        validate_safe_text(&request.reason, "Fleet release resume reason", 512)?;
        let current = self.fleet_release(auth, id).await?;
        if current.release.status != FleetReleaseStatus::Paused {
            return Err(state_conflict("Fleet release resume requires a paused aggregate"));
        }
        if current.targets.iter().any(|target| target.regression_detected) {
            return Err(ControlPlaneError::conflict_code(
                "fleet_release_regression_unresolved",
                "Fleet release cannot resume until every regression is rolled back or skipped",
            ));
        }
        let now = transition_time(current.release.updated_at);
        let mut release = current.release.clone();
        release.status = FleetReleaseStatus::Ready;
        release.active_batch = None;
        release.updated_at = now;
        let mut targets = current.targets.clone();
        for target in &mut targets {
            if target.state == FleetReleaseTargetState::Paused {
                target.state = FleetReleaseTargetState::Ready;
                target.updated_at = now;
            }
        }
        self.persist_transition(
            &current,
            release,
            targets,
            "fleet_release_resumed",
            auth,
            serde_json::json!({"reason": request.reason}),
        )
        .await
    }

    pub(crate) async fn fleet_release_report(
        &self,
        auth: &AuthContext,
        id: FleetReleaseId,
    ) -> Result<FleetReleaseReport, ControlPlaneError> {
        let view = self.fleet_release(auth, id).await?;
        let mut state_counts = BTreeMap::new();
        let mut skipped_clusters = Vec::new();
        for target in &view.targets {
            let state = super::repository::target_state_name(target.state).to_owned();
            *state_counts.entry(state).or_insert(0) += 1;
            if matches!(
                target.state,
                FleetReleaseTargetState::Ineligible | FleetReleaseTargetState::Skipped
            ) {
                skipped_clusters.push(target.cluster_id);
            }
        }
        Ok(FleetReleaseReport {
            schema_version: FLEET_RELEASE_REPORT_SCHEMA_VERSION.to_owned(),
            release: view.release,
            targets: view.targets,
            state_counts,
            skipped_clusters,
            generated_at: Utc::now(),
        })
    }

    async fn persist_transition(
        &self,
        current: &FleetReleaseView,
        release: FleetRelease,
        targets: Vec<FleetReleaseTarget>,
        reason_code: &'static str,
        auth: &AuthContext,
        details: serde_json::Value,
    ) -> Result<FleetReleaseView, ControlPlaneError> {
        let transition = FleetReleaseTransition {
            release,
            targets,
            reason_code,
            actor_subject: auth.subject.clone(),
            details,
        };
        self.repository
            .apply_fleet_release_transition(current, &transition)
            .await?;
        Ok(FleetReleaseView {
            schema_version: FLEET_RELEASE_API_SCHEMA_VERSION,
            release: transition.release,
            targets: transition.targets,
        })
    }
}
