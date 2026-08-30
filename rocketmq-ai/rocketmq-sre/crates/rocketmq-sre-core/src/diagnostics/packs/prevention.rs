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

use super::super::EvidenceRequirement;
use super::super::FollowUpQuery;
use super::super::Severity;
use super::catalog::Condition;
use super::catalog::PackSpec;
use super::catalog::RuleSpec;

pub(super) fn specs() -> &'static [&'static PackSpec] {
    const SPECS: &[&PackSpec] = &[
        &UPGRADE_READINESS,
        &CAPACITY_RUNWAY,
        &COLD_DATA_FLOW,
        &DR_READINESS,
        &SECURITY_POSTURE,
        &CHANGE_REGRESSION,
    ];
    SPECS
}

const PREVENTION_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "prevention-trend",
    source: "prometheus",
    resource_prefix: "prevention-trend/",
    purpose: "Seven and thirty day bounded trend evidence for preventive diagnostics",
}];
const PREVENTION_FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "prometheus",
        resource_template: "prevention-trend/{resource}",
        reason: "Refresh exact seven and thirty day trend evidence",
    },
    FollowUpQuery {
        source: "kubernetes",
        resource_template: "change-timeline/{component}",
        reason: "Compare bounded rollout, configuration, and certificate changes",
    },
];

const UPGRADE_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "upgrade-readiness",
    source: "kubernetes",
    resource_prefix: "upgrade-readiness/",
    purpose: "Version/feature/protocol, PDB, capacity, quorum, recovery, canary, and rollback readiness",
}];
const UPGRADE_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "UPGRADE_PROTOCOL_INCOMPATIBLE",
        root_cause: "Target version, feature, or protocol compatibility is not satisfied",
        rationale: "The compiled compatibility matrix rejects the observed upgrade path",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "protocol_compatible",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "UPGRADE_QUORUM_OR_PDB_UNSAFE",
        root_cause: "Quorum or PodDisruptionBudget cannot tolerate the planned rollout",
        rationale: "The read-only disruption safety predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "quorum_and_pdb_safe",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "UPGRADE_ROLLBACK_UNREADY",
        root_cause: "Canary evidence or rollback prerequisites are incomplete",
        rationale: "The canary and rollback readiness predicate is false",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "canary_and_rollback_ready",
            expected: false,
        },
    },
];
const UPGRADE_CODES: &[&str] = &[
    "UPGRADE_PROTOCOL_INCOMPATIBLE",
    "UPGRADE_QUORUM_OR_PDB_UNSAFE",
    "UPGRADE_ROLLBACK_UNREADY",
    "UPGRADE_READY",
    "UPGRADE_READINESS_EVIDENCE_INCOMPLETE",
];

pub(super) const UPGRADE_READINESS: PackSpec = PackSpec {
    id: "upgrade-readiness",
    components: &["broker", "nameserver", "controller", "proxy", "kubernetes"],
    required: UPGRADE_REQUIRED,
    optional: PREVENTION_OPTIONAL,
    rules: UPGRADE_RULES,
    rule_codes: UPGRADE_CODES,
    healthy_code: "UPGRADE_READY",
    healthy_summary: "Compatibility, capacity, quorum, recovery, canary, and rollback prerequisites are satisfied",
    incomplete_code: "UPGRADE_READINESS_EVIDENCE_INCOMPLETE",
    follow_up: PREVENTION_FOLLOW_UP,
    max_freshness_seconds: 600,
};

const CAPACITY_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "capacity-runway",
    source: "prometheus",
    resource_prefix: "capacity-runway/",
    purpose: "Broker, Proxy, TPS, connection, PVC, Store, tiered, and backlog runway",
}];
const CAPACITY_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "CAPACITY_DISK_RUNWAY_LOW",
        root_cause: "Projected durable storage runway is below thirty days",
        rationale: "The bounded disk runway estimate is below the configured threshold",
        severity: Severity::Critical,
        condition: Condition::NumberBelow {
            path: "disk_runway_days",
            threshold: 30.0,
        },
    },
    RuleSpec {
        reason_code: "CAPACITY_BACKLOG_RUNWAY_LOW",
        root_cause: "Projected backlog headroom is below seven days",
        rationale: "The bounded backlog runway estimate is below the configured threshold",
        severity: Severity::Warning,
        condition: Condition::NumberBelow {
            path: "backlog_runway_days",
            threshold: 7.0,
        },
    },
    RuleSpec {
        reason_code: "CAPACITY_CONNECTION_HEADROOM_LOW",
        root_cause: "Broker or Proxy connection headroom is below twenty percent",
        rationale: "The connection headroom ratio is below the configured threshold",
        severity: Severity::Warning,
        condition: Condition::NumberBelow {
            path: "connection_headroom_ratio",
            threshold: 0.2,
        },
    },
];
const CAPACITY_CODES: &[&str] = &[
    "CAPACITY_DISK_RUNWAY_LOW",
    "CAPACITY_BACKLOG_RUNWAY_LOW",
    "CAPACITY_CONNECTION_HEADROOM_LOW",
    "CAPACITY_RUNWAY_HEALTHY",
    "CAPACITY_RUNWAY_EVIDENCE_INCOMPLETE",
];

pub(super) const CAPACITY_RUNWAY: PackSpec = PackSpec {
    id: "capacity-runway",
    components: &["broker", "proxy", "store", "tiered-store"],
    required: CAPACITY_REQUIRED,
    optional: PREVENTION_OPTIONAL,
    rules: CAPACITY_RULES,
    rule_codes: CAPACITY_CODES,
    healthy_code: "CAPACITY_RUNWAY_HEALTHY",
    healthy_summary: "Traffic, connection, storage, tiered, and backlog runway exceed configured thresholds",
    incomplete_code: "CAPACITY_RUNWAY_EVIDENCE_INCOMPLETE",
    follow_up: PREVENTION_FOLLOW_UP,
    max_freshness_seconds: 900,
};

const COLD_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "cold-data-flow",
    source: "admin-query",
    resource_prefix: "cold-data-flow/",
    purpose: "Cold-data throttling, hot/cold hit rate, provider cost, fallback, and local retention pressure",
}];
const COLD_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "COLD_DATA_HIT_RATE_LOW",
        root_cause: "Tiered cold-data hit rate is below the configured target",
        rationale: "The cold-data hit ratio is below the configured threshold",
        severity: Severity::Warning,
        condition: Condition::NumberBelow {
            path: "cold_hit_ratio",
            threshold: 0.7,
        },
    },
    RuleSpec {
        reason_code: "COLD_DATA_FALLBACK_HIGH",
        root_cause: "Tiered reads are falling back to local storage excessively",
        rationale: "The fallback ratio crosses the configured threshold",
        severity: Severity::Critical,
        condition: Condition::NumberAtLeast {
            path: "fallback_ratio",
            threshold: 0.2,
        },
    },
    RuleSpec {
        reason_code: "COLD_DATA_LOCAL_PRESSURE",
        root_cause: "Cold-data retention is creating local Store pressure",
        rationale: "The local retention pressure predicate is active",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "local_retention_pressure",
            expected: true,
        },
    },
];
const COLD_CODES: &[&str] = &[
    "COLD_DATA_HIT_RATE_LOW",
    "COLD_DATA_FALLBACK_HIGH",
    "COLD_DATA_LOCAL_PRESSURE",
    "COLD_DATA_FLOW_HEALTHY",
    "COLD_DATA_FLOW_EVIDENCE_INCOMPLETE",
];

pub(super) const COLD_DATA_FLOW: PackSpec = PackSpec {
    id: "cold-data-flow",
    components: &["store", "tiered-store"],
    required: COLD_REQUIRED,
    optional: PREVENTION_OPTIONAL,
    rules: COLD_RULES,
    rule_codes: COLD_CODES,
    healthy_code: "COLD_DATA_FLOW_HEALTHY",
    healthy_summary: "Cold-data hit rate, fallback, provider, cost, and local retention pressure are healthy",
    incomplete_code: "COLD_DATA_FLOW_EVIDENCE_INCOMPLETE",
    follow_up: PREVENTION_FOLLOW_UP,
    max_freshness_seconds: 900,
};

const DR_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "dr-readiness",
    source: "admin-query",
    resource_prefix: "dr-readiness/",
    purpose: "Backup/restore, Controller snapshot, Store/tiered metadata, RTO/RPO, and cross-zone dependencies",
}];
const DR_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "DR_BACKUP_OR_SNAPSHOT_STALE",
        root_cause: "Backup or Controller snapshot freshness is outside the RPO",
        rationale: "The latest durable recovery point is too old",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "backup_or_snapshot_stale",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "DR_RESTORE_UNVERIFIED",
        root_cause: "Restore and metadata reconciliation have not been verified",
        rationale: "The bounded restore verification predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "restore_verified",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "DR_RTO_RPO_BREACH",
        root_cause: "Projected recovery time or recovery point exceeds policy",
        rationale: "The deterministic RTO/RPO policy predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "rto_rpo_met",
            expected: false,
        },
    },
];
const DR_CODES: &[&str] = &[
    "DR_BACKUP_OR_SNAPSHOT_STALE",
    "DR_RESTORE_UNVERIFIED",
    "DR_RTO_RPO_BREACH",
    "DR_READY",
    "DR_READINESS_EVIDENCE_INCOMPLETE",
];

pub(super) const DR_READINESS: PackSpec = PackSpec {
    id: "dr-readiness",
    components: &["controller", "broker", "store", "tiered-store"],
    required: DR_REQUIRED,
    optional: PREVENTION_OPTIONAL,
    rules: DR_RULES,
    rule_codes: DR_CODES,
    healthy_code: "DR_READY",
    healthy_summary: "Backup, restore, snapshots, metadata, RTO/RPO, and cross-zone dependencies are ready",
    incomplete_code: "DR_READINESS_EVIDENCE_INCOMPLETE",
    follow_up: PREVENTION_FOLLOW_UP,
    max_freshness_seconds: 900,
};

const SECURITY_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "security-posture",
    source: "admin-query",
    resource_prefix: "security-posture/",
    purpose: "User/ACL/config diff, least privilege, certificate/JWKS/Secret expiry, and unapproved changes",
}];
const SECURITY_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "SECURITY_PRIVILEGE_DRIFT",
        root_cause: "User, ACL, or authorization policy exceeds the approved least-privilege posture",
        rationale: "The sanitized policy digest comparison reports privilege drift",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "privilege_drift",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "SECURITY_CREDENTIAL_EXPIRING",
        root_cause: "Certificate, JWKS, or Secret metadata is inside the renewal window",
        rationale: "Credential metadata reports an upcoming expiry without exposing material",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "credential_expiring",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "SECURITY_UNAPPROVED_CHANGE",
        root_cause: "A security-sensitive configuration change has no approved change record",
        rationale: "The bounded change correlation reports an unapproved security change",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "unapproved_change",
            expected: true,
        },
    },
];
const SECURITY_CODES: &[&str] = &[
    "SECURITY_PRIVILEGE_DRIFT",
    "SECURITY_CREDENTIAL_EXPIRING",
    "SECURITY_UNAPPROVED_CHANGE",
    "SECURITY_POSTURE_HEALTHY",
    "SECURITY_POSTURE_EVIDENCE_INCOMPLETE",
];

pub(super) const SECURITY_POSTURE: PackSpec = PackSpec {
    id: "security-posture",
    components: &["auth", "broker", "proxy", "mcp", "kubernetes"],
    required: SECURITY_REQUIRED,
    optional: PREVENTION_OPTIONAL,
    rules: SECURITY_RULES,
    rule_codes: SECURITY_CODES,
    healthy_code: "SECURITY_POSTURE_HEALTHY",
    healthy_summary: "Least privilege, credential lifecycle, configuration, and change approval posture are healthy",
    incomplete_code: "SECURITY_POSTURE_EVIDENCE_INCOMPLETE",
    follow_up: PREVENTION_FOLLOW_UP,
    max_freshness_seconds: 900,
};

const CHANGE_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "change-regression",
    source: "kubernetes",
    resource_prefix: "change-regression/",
    purpose: "Before/after image, configuration, Secret metadata, action, SLO, and impact comparison",
}];
const CHANGE_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "CHANGE_SLO_REGRESSION",
        root_cause: "SLO performance regressed after a bounded deployment or configuration change",
        rationale: "The before/after SLO comparison crosses its regression threshold",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "slo_regressed",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "CHANGE_ERROR_LATENCY_REGRESSION",
        root_cause: "Error rate or latency increased after the change",
        rationale: "The bounded before/after error or latency comparison regressed",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "error_or_latency_regressed",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "CHANGE_IMPACT_EXPANDED",
        root_cause: "The observed change impact expanded beyond the planned resource scope",
        rationale: "The topology impact comparison exceeds the approved scope",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "impact_scope_expanded",
            expected: true,
        },
    },
];
const CHANGE_CODES: &[&str] = &[
    "CHANGE_SLO_REGRESSION",
    "CHANGE_ERROR_LATENCY_REGRESSION",
    "CHANGE_IMPACT_EXPANDED",
    "CHANGE_NO_REGRESSION",
    "CHANGE_REGRESSION_EVIDENCE_INCOMPLETE",
];

pub(super) const CHANGE_REGRESSION: PackSpec = PackSpec {
    id: "change-regression",
    components: &["kubernetes", "broker", "proxy", "store"],
    required: CHANGE_REQUIRED,
    optional: PREVENTION_OPTIONAL,
    rules: CHANGE_RULES,
    rule_codes: CHANGE_CODES,
    healthy_code: "CHANGE_NO_REGRESSION",
    healthy_summary: "The bounded before/after comparison found no SLO, latency, error, or impact regression",
    incomplete_code: "CHANGE_REGRESSION_EVIDENCE_INCOMPLETE",
    follow_up: PREVENTION_FOLLOW_UP,
    max_freshness_seconds: 600,
};
