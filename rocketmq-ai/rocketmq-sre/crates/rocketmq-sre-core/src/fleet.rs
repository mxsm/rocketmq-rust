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

use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ClusterRegistration;
use rocketmq_sre_contracts::DataResidencyClass;
use rocketmq_sre_contracts::FleetId;
use rocketmq_sre_contracts::QuotaPolicy;
use rocketmq_sre_contracts::QuotaUsage;
use rocketmq_sre_contracts::RegionId;
use rocketmq_sre_contracts::TenantId;

/// ABAC scope resolved from one authenticated Fleet operator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FleetActorScope {
    pub fleet_id: FleetId,
    pub tenant_id: TenantId,
    pub region_ids: BTreeSet<RegionId>,
    pub cluster_ids: BTreeSet<ClusterId>,
}

/// Stable reason for a Fleet authorization decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FleetAuthorizationReason {
    Allowed,
    FleetMismatch,
    TenantMismatch,
    RegionNotAllowed,
    ClusterNotAllowed,
}

/// Fail-closed result of applying tenant, region, and cluster ABAC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FleetAuthorizationDecision {
    pub allowed: bool,
    pub reason: FleetAuthorizationReason,
}

impl FleetActorScope {
    /// Authorizes access to one cluster registration.
    #[must_use]
    pub fn authorize(&self, registration: &ClusterRegistration) -> FleetAuthorizationDecision {
        let reason = if self.fleet_id != registration.fleet_id {
            FleetAuthorizationReason::FleetMismatch
        } else if self.tenant_id != registration.tenant_id {
            FleetAuthorizationReason::TenantMismatch
        } else if !self.region_ids.contains(&registration.region_id) {
            FleetAuthorizationReason::RegionNotAllowed
        } else if !self.cluster_ids.is_empty() && !self.cluster_ids.contains(&registration.cluster_id) {
            FleetAuthorizationReason::ClusterNotAllowed
        } else {
            FleetAuthorizationReason::Allowed
        };
        FleetAuthorizationDecision {
            allowed: reason == FleetAuthorizationReason::Allowed,
            reason,
        }
    }
}

/// Resource measured by one bounded Fleet quota.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuotaResource {
    Query,
    ModelToken,
    ConcurrentWorkflow,
    ConcurrentInspection,
    EvidenceByte,
    Notification,
    AutomaticAction,
}

/// Work classes whose safety-critical members retain reserved capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FleetWorkPriority {
    SafetyCritical,
    Interactive,
    Background,
}

/// Increment requested from one quota policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuotaRequest {
    pub resource: QuotaResource,
    pub amount: u64,
    pub priority: FleetWorkPriority,
}

/// Stable quota decision reason suitable for API responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuotaDecisionReason {
    Allowed,
    SafetyCriticalReservedCapacity,
    PolicyInactive,
    ScopeMismatch,
    LimitExceeded,
}

/// Fail-closed quota result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuotaDecision {
    pub allowed: bool,
    pub reason: QuotaDecisionReason,
    pub observed: u64,
    pub limit: u64,
}

/// Deterministic Fleet quota evaluator.
pub struct FleetQuotaEvaluator;

impl FleetQuotaEvaluator {
    /// Evaluates one request. Verification, rollback, audit, and active
    /// Incident work use `SafetyCritical` and retain reserved capacity even
    /// while lower-priority work is rejected.
    #[must_use]
    pub fn evaluate(policy: &QuotaPolicy, usage: &QuotaUsage, request: QuotaRequest) -> QuotaDecision {
        if !policy.active {
            return QuotaDecision {
                allowed: false,
                reason: QuotaDecisionReason::PolicyInactive,
                observed: 0,
                limit: 0,
            };
        }
        if policy.id != usage.policy_id {
            return QuotaDecision {
                allowed: false,
                reason: QuotaDecisionReason::ScopeMismatch,
                observed: 0,
                limit: 0,
            };
        }
        let (observed, limit) = match request.resource {
            QuotaResource::Query => (usage.queries, u64::from(policy.limits.queries_per_minute)),
            QuotaResource::ModelToken => (usage.model_tokens, policy.limits.model_tokens_per_hour),
            QuotaResource::ConcurrentWorkflow => (
                u64::from(usage.active_workflows),
                u64::from(policy.limits.concurrent_workflows),
            ),
            QuotaResource::ConcurrentInspection => (
                u64::from(usage.active_inspections),
                u64::from(policy.limits.concurrent_inspections),
            ),
            QuotaResource::EvidenceByte => (usage.evidence_bytes, policy.limits.evidence_bytes_per_hour),
            QuotaResource::Notification => (usage.notifications, u64::from(policy.limits.notifications_per_hour)),
            QuotaResource::AutomaticAction => (
                usage.automatic_actions,
                u64::from(policy.limits.automatic_actions_per_hour),
            ),
        };
        if request.priority == FleetWorkPriority::SafetyCritical {
            return QuotaDecision {
                allowed: true,
                reason: QuotaDecisionReason::SafetyCriticalReservedCapacity,
                observed,
                limit,
            };
        }
        let allowed = observed.saturating_add(request.amount) <= limit;
        QuotaDecision {
            allowed,
            reason: if allowed {
                QuotaDecisionReason::Allowed
            } else {
                QuotaDecisionReason::LimitExceeded
            },
            observed,
            limit,
        }
    }
}

/// Returns whether an artifact may be routed from its source region to a
/// selected runtime region.
#[must_use]
pub fn residency_allows_route(
    residency: DataResidencyClass,
    source_region: RegionId,
    selected_region: RegionId,
) -> bool {
    match residency {
        DataResidencyClass::RegionLocal => source_region == selected_region,
        DataResidencyClass::AggregatedMetadata | DataResidencyClass::ExportAllowed => true,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterRegistrationState;
    use rocketmq_sre_contracts::FleetEnvironment;
    use rocketmq_sre_contracts::QuotaLimits;
    use rocketmq_sre_contracts::QuotaPolicyId;

    use super::*;

    #[test]
    fn abac_denies_cross_tenant_region_and_cluster_access() {
        let fleet_id = FleetId::new();
        let tenant_id = TenantId::new();
        let region_id = RegionId::new();
        let cluster_id = ClusterId::new();
        let registration = registration(fleet_id, tenant_id, region_id, cluster_id);
        let allowed = FleetActorScope {
            fleet_id,
            tenant_id,
            region_ids: BTreeSet::from([region_id]),
            cluster_ids: BTreeSet::from([cluster_id]),
        };
        assert!(allowed.authorize(&registration).allowed);

        let cross_tenant = FleetActorScope {
            tenant_id: TenantId::new(),
            ..allowed.clone()
        };
        assert_eq!(
            cross_tenant.authorize(&registration).reason,
            FleetAuthorizationReason::TenantMismatch
        );
        let cross_region = FleetActorScope {
            region_ids: BTreeSet::from([RegionId::new()]),
            ..allowed.clone()
        };
        assert_eq!(
            cross_region.authorize(&registration).reason,
            FleetAuthorizationReason::RegionNotAllowed
        );
        let cross_cluster = FleetActorScope {
            cluster_ids: BTreeSet::from([ClusterId::new()]),
            ..allowed
        };
        assert_eq!(
            cross_cluster.authorize(&registration).reason,
            FleetAuthorizationReason::ClusterNotAllowed
        );
    }

    #[test]
    fn quota_rejects_background_work_but_preserves_safety_critical_capacity() {
        let policy_id = QuotaPolicyId::new();
        let policy = quota_policy(policy_id);
        let usage = QuotaUsage {
            policy_id,
            queries: 10,
            model_tokens: 100,
            active_workflows: 2,
            active_inspections: 1,
            evidence_bytes: 1_000,
            notifications: 10,
            automatic_actions: 1,
            observed_at: Utc::now(),
        };
        let background = FleetQuotaEvaluator::evaluate(
            &policy,
            &usage,
            QuotaRequest {
                resource: QuotaResource::ConcurrentWorkflow,
                amount: 1,
                priority: FleetWorkPriority::Background,
            },
        );
        assert!(!background.allowed);
        assert_eq!(background.reason, QuotaDecisionReason::LimitExceeded);

        let rollback = FleetQuotaEvaluator::evaluate(
            &policy,
            &usage,
            QuotaRequest {
                resource: QuotaResource::ConcurrentWorkflow,
                amount: 1,
                priority: FleetWorkPriority::SafetyCritical,
            },
        );
        assert!(rollback.allowed);
        assert_eq!(rollback.reason, QuotaDecisionReason::SafetyCriticalReservedCapacity);
    }

    #[test]
    fn region_local_evidence_never_routes_cross_region() {
        let source = RegionId::new();
        assert!(residency_allows_route(DataResidencyClass::RegionLocal, source, source));
        assert!(!residency_allows_route(
            DataResidencyClass::RegionLocal,
            source,
            RegionId::new()
        ));
    }

    fn registration(
        fleet_id: FleetId,
        tenant_id: TenantId,
        region_id: RegionId,
        cluster_id: ClusterId,
    ) -> ClusterRegistration {
        ClusterRegistration {
            cluster_id,
            fleet_id,
            tenant_id,
            region_id,
            external_cluster_key: "cluster-a".to_owned(),
            environment: FleetEnvironment::Test,
            owner: "messaging-platform".to_owned(),
            state: ClusterRegistrationState::Active,
            residency_tags: BTreeSet::from(["region-local".to_owned()]),
            lifecycle_revision: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn quota_policy(id: QuotaPolicyId) -> QuotaPolicy {
        QuotaPolicy {
            id,
            fleet_id: FleetId::new(),
            tenant_id: TenantId::new(),
            region_id: None,
            cluster_id: None,
            version: 1,
            limits: QuotaLimits {
                queries_per_minute: 10,
                model_tokens_per_hour: 100,
                concurrent_workflows: 2,
                concurrent_inspections: 1,
                evidence_bytes_per_hour: 1_000,
                notifications_per_hour: 10,
                automatic_actions_per_hour: 1,
            },
            owner: "messaging-platform".to_owned(),
            active: true,
            created_at: Utc::now(),
        }
    }
}
