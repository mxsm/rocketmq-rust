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

const HA_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "ha-network",
    source: "prometheus",
    resource_prefix: "ha-network/",
    purpose: "Bounded replication and controller network trend evidence",
}];
const HA_FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "admin-query",
        resource_template: "broker-ha/{broker}",
        reason: "Refresh replica offsets, SyncStateSet, and bounded Broker readiness",
    },
    FollowUpQuery {
        source: "prometheus",
        resource_template: "ha-network/{broker}",
        reason: "Compare replica lag with bounded network telemetry",
    },
];

const BROKER_HA_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "broker-ha",
    source: "admin-query",
    resource_prefix: "broker-ha/",
    purpose: "Primary and replica offsets, replication latency, SyncStateSet, network, and replica health",
}];
const BROKER_HA_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "BROKER_HA_REPLICA_LAG",
        root_cause: "Broker replicas are outside the configured offset or latency envelope",
        rationale: "The HA snapshot reports excessive replica lag",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "replica_lag_high",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "BROKER_HA_SYNC_STATE_INSUFFICIENT",
        root_cause: "The Broker SyncStateSet does not contain enough healthy replicas",
        rationale: "The effective SyncStateSet is below the configured quorum requirement",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "sync_state_set_sufficient",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "BROKER_HA_REPLICA_UNHEALTHY",
        root_cause: "One or more Broker replicas are unavailable or unhealthy",
        rationale: "Replica readiness is explicitly unhealthy",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "replicas_healthy",
            expected: false,
        },
    },
];
const BROKER_HA_CODES: &[&str] = &[
    "BROKER_HA_REPLICA_LAG",
    "BROKER_HA_SYNC_STATE_INSUFFICIENT",
    "BROKER_HA_REPLICA_UNHEALTHY",
    "BROKER_HA_HEALTHY",
    "BROKER_HA_EVIDENCE_INCOMPLETE",
];

pub(super) const BROKER_HA: PackSpec = PackSpec {
    id: "broker-ha",
    components: &["broker", "store"],
    required: BROKER_HA_REQUIRED,
    optional: HA_OPTIONAL,
    rules: BROKER_HA_RULES,
    rule_codes: BROKER_HA_CODES,
    healthy_code: "BROKER_HA_HEALTHY",
    healthy_summary: "Broker replicas, offsets, SyncStateSet, and replication latency are healthy",
    incomplete_code: "BROKER_HA_EVIDENCE_INCOMPLETE",
    follow_up: HA_FOLLOW_UP,
    max_freshness_seconds: 180,
};

const CONTROLLER_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "controller-ha",
    source: "prometheus",
    resource_prefix: "controller-ha/",
    purpose: "Controller leader, quorum, commit/apply progress, and Broker heartbeat health",
}];
const CONTROLLER_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "CONTROLLER_LEADER_UNKNOWN",
        root_cause: "The active Controller leader is unknown",
        rationale: "Leader discovery returned an explicit unknown state",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "leader_known",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "CONTROLLER_QUORUM_INSUFFICIENT",
        root_cause: "The Controller quorum is insufficient for safe progress",
        rationale: "The quorum health predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "quorum_healthy",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "CONTROLLER_HEARTBEAT_STALE",
        root_cause: "Controller Broker heartbeat state is stale",
        rationale: "At least one required Broker heartbeat crossed the configured age window",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "broker_heartbeat_stale",
            expected: true,
        },
    },
];
const CONTROLLER_CODES: &[&str] = &[
    "CONTROLLER_LEADER_UNKNOWN",
    "CONTROLLER_QUORUM_INSUFFICIENT",
    "CONTROLLER_HEARTBEAT_STALE",
    "CONTROLLER_HA_HEALTHY",
    "CONTROLLER_HA_EVIDENCE_INCOMPLETE",
];

pub(super) const CONTROLLER_HA: PackSpec = PackSpec {
    id: "controller-ha",
    components: &["controller", "broker"],
    required: CONTROLLER_REQUIRED,
    optional: HA_OPTIONAL,
    rules: CONTROLLER_RULES,
    rule_codes: CONTROLLER_CODES,
    healthy_code: "CONTROLLER_HA_HEALTHY",
    healthy_summary: "Controller leader, quorum, commit/apply progress, and Broker heartbeats are healthy",
    incomplete_code: "CONTROLLER_HA_EVIDENCE_INCOMPLETE",
    follow_up: HA_FOLLOW_UP,
    max_freshness_seconds: 120,
};

const NAMESRV_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "namesrv-route",
    source: "rocketmq-mcp",
    resource_prefix: "namesrv-route/",
    purpose: "Cross-NameServer route consistency, registration freshness, reachability, and permissions",
}];
const NAMESRV_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "namesrv-network",
    source: "prometheus",
    resource_prefix: "namesrv-network/",
    purpose: "NameServer route request and registration error telemetry",
}];
const NAMESRV_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "NAMESRV_ROUTE_DIVERGENT",
        root_cause: "NameServers disagree on the effective Topic route",
        rationale: "The cross-NameServer route digest comparison diverged",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "route_divergent",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "NAMESRV_REGISTRATION_STALE",
        root_cause: "Broker registration data is stale in one or more NameServers",
        rationale: "Registration age crossed the configured freshness window",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "registration_stale",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "NAMESRV_BROKER_UNREACHABLE",
        root_cause: "A Broker referenced by the route is unreachable",
        rationale: "Route reachability validation found an unavailable Broker",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "broker_unreachable",
            expected: true,
        },
    },
];
const NAMESRV_CODES: &[&str] = &[
    "NAMESRV_ROUTE_DIVERGENT",
    "NAMESRV_REGISTRATION_STALE",
    "NAMESRV_BROKER_UNREACHABLE",
    "NAMESRV_ROUTE_HEALTHY",
    "NAMESRV_ROUTE_EVIDENCE_INCOMPLETE",
];
const NAMESRV_FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "rocketmq-mcp",
    resource_template: "namesrv-route/{topic}",
    reason: "Refresh the bounded route view from every configured NameServer",
}];

pub(super) const NAMESRV_ROUTE: PackSpec = PackSpec {
    id: "namesrv-route",
    components: &["nameserver", "broker"],
    required: NAMESRV_REQUIRED,
    optional: NAMESRV_OPTIONAL,
    rules: NAMESRV_RULES,
    rule_codes: NAMESRV_CODES,
    healthy_code: "NAMESRV_ROUTE_HEALTHY",
    healthy_summary: "NameServer routes, registrations, reachability, and permissions are consistent",
    incomplete_code: "NAMESRV_ROUTE_EVIDENCE_INCOMPLETE",
    follow_up: NAMESRV_FOLLOW_UP,
    max_freshness_seconds: 180,
};
