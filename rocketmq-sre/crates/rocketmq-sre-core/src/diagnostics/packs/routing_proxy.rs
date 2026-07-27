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

const ROUTING_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "routing-trace",
    source: "tempo",
    resource_prefix: "routing-trace/",
    purpose: "Bounded cross-component span timing without message bodies or client addresses",
}];
const ROUTING_FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "prometheus",
        resource_template: "send-latency/{resource}",
        reason: "Refresh bounded segment latency and error metrics",
    },
    FollowUpQuery {
        source: "tempo",
        resource_template: "routing-trace/{correlation}",
        reason: "Collect a pseudonymized bounded trace when available",
    },
];

const SEND_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "send-latency",
    source: "prometheus",
    resource_prefix: "send-latency/",
    purpose: "Client, Proxy, Remoting, Broker, and Store segment latency",
}];
const SEND_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "SEND_CLIENT_PROXY_LATENCY",
        root_cause: "Client-to-Proxy latency is the dominant send-path segment",
        rationale: "The client/Proxy segment crosses its configured P99 threshold",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "client_or_proxy_slow",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "SEND_REMOTING_BROKER_LATENCY",
        root_cause: "Remoting or Broker request handling dominates send latency",
        rationale: "The remoting/Broker segment crosses its configured P99 threshold",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "remoting_or_broker_slow",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "SEND_STORE_LATENCY",
        root_cause: "Store append or flush latency dominates the send path",
        rationale: "The Store segment crosses its configured P99 threshold",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "store_slow",
            expected: true,
        },
    },
];
const SEND_CODES: &[&str] = &[
    "SEND_CLIENT_PROXY_LATENCY",
    "SEND_REMOTING_BROKER_LATENCY",
    "SEND_STORE_LATENCY",
    "SEND_LATENCY_HEALTHY",
    "SEND_LATENCY_EVIDENCE_INCOMPLETE",
];

pub(super) const SEND_LATENCY: PackSpec = PackSpec {
    id: "send-latency",
    components: &["client", "proxy", "remoting", "broker", "store"],
    required: SEND_REQUIRED,
    optional: ROUTING_OPTIONAL,
    rules: SEND_RULES,
    rule_codes: SEND_CODES,
    healthy_code: "SEND_LATENCY_HEALTHY",
    healthy_summary: "Client, Proxy, Remoting, Broker, and Store send segments are within latency bounds",
    incomplete_code: "SEND_LATENCY_EVIDENCE_INCOMPLETE",
    follow_up: ROUTING_FOLLOW_UP,
    max_freshness_seconds: 180,
};

const PROXY_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "proxy-connectivity",
    source: "prometheus",
    resource_prefix: "proxy-connectivity/",
    purpose: "gRPC/remoting, admission, session, TLS/Auth, forwarding, and backend route health",
}];
const PROXY_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "proxy-workload",
    source: "kubernetes",
    resource_prefix: "proxy-workload/",
    purpose: "Proxy rollout, readiness, and immutable image digest",
}];
const PROXY_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "PROXY_GRPC_REMOTING_UNHEALTHY",
        root_cause: "Proxy gRPC or remoting transport is unhealthy",
        rationale: "The transport health predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "transport_healthy",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "PROXY_BACKEND_ROUTE_MISSING",
        root_cause: "Proxy cannot resolve or reach its Broker backend route",
        rationale: "The backend route predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "backend_route_available",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "PROXY_TLS_AUTH_FAILURE",
        root_cause: "TLS or authorization failure is preventing Proxy connectivity",
        rationale: "The sanitized TLS/Auth failure signal is active",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "tls_or_auth_failed",
            expected: true,
        },
    },
];
const PROXY_CODES: &[&str] = &[
    "PROXY_GRPC_REMOTING_UNHEALTHY",
    "PROXY_BACKEND_ROUTE_MISSING",
    "PROXY_TLS_AUTH_FAILURE",
    "PROXY_CONNECTIVITY_HEALTHY",
    "PROXY_CONNECTIVITY_EVIDENCE_INCOMPLETE",
];
const PROXY_FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "prometheus",
    resource_template: "proxy-connectivity/{proxy}",
    reason: "Refresh bounded Proxy transport, request, error, and latency signals",
}];

pub(super) const PROXY_CONNECTIVITY: PackSpec = PackSpec {
    id: "proxy-connectivity",
    components: &["proxy", "remoting", "broker"],
    required: PROXY_REQUIRED,
    optional: PROXY_OPTIONAL,
    rules: PROXY_RULES,
    rule_codes: PROXY_CODES,
    healthy_code: "PROXY_CONNECTIVITY_HEALTHY",
    healthy_summary: "Proxy transport, TLS/Auth, sessions, forwarding, and backend route are healthy",
    incomplete_code: "PROXY_CONNECTIVITY_EVIDENCE_INCOMPLETE",
    follow_up: PROXY_FOLLOW_UP,
    max_freshness_seconds: 180,
};

const STATIC_ROUTE_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "static-topic-route",
    source: "rocketmq-mcp",
    resource_prefix: "static-topic-route/",
    purpose: "Logical queues, mapping epoch, Broker mapping, route consistency, and expansion prerequisites",
}];
const STATIC_ROUTE_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "STATIC_ROUTE_EPOCH_DIVERGENT",
        root_cause: "Static Topic queue mapping epochs diverge across Brokers",
        rationale: "The mapping epoch comparison is inconsistent",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "mapping_epoch_consistent",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "STATIC_ROUTE_QUEUE_UNMAPPED",
        root_cause: "A logical queue has no valid Broker mapping",
        rationale: "The logical queue mapping completeness predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "all_logical_queues_mapped",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "STATIC_ROUTE_EXPANSION_UNSAFE",
        root_cause: "Static Topic expansion prerequisites are not satisfied",
        rationale: "Read-only preflight evidence rejects safe expansion",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "expansion_preconditions_met",
            expected: false,
        },
    },
];
const STATIC_ROUTE_CODES: &[&str] = &[
    "STATIC_ROUTE_EPOCH_DIVERGENT",
    "STATIC_ROUTE_QUEUE_UNMAPPED",
    "STATIC_ROUTE_EXPANSION_UNSAFE",
    "STATIC_TOPIC_ROUTE_HEALTHY",
    "STATIC_TOPIC_ROUTE_EVIDENCE_INCOMPLETE",
];
const ROUTE_FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "rocketmq-mcp",
    resource_template: "static-topic-route/{topic}",
    reason: "Refresh the read-only logical and physical queue mapping",
}];

pub(super) const STATIC_TOPIC_ROUTE: PackSpec = PackSpec {
    id: "static-topic-route",
    components: &["nameserver", "broker", "topic"],
    required: STATIC_ROUTE_REQUIRED,
    optional: ROUTING_OPTIONAL,
    rules: STATIC_ROUTE_RULES,
    rule_codes: STATIC_ROUTE_CODES,
    healthy_code: "STATIC_TOPIC_ROUTE_HEALTHY",
    healthy_summary: "Static Topic mappings, epochs, routes, and expansion prerequisites are consistent",
    incomplete_code: "STATIC_TOPIC_ROUTE_EVIDENCE_INCOMPLETE",
    follow_up: ROUTE_FOLLOW_UP,
    max_freshness_seconds: 300,
};

const CONFIG_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "topic-subscription-config",
    source: "admin-query",
    resource_prefix: "topic-subscription-config/",
    purpose: "Topic and Group permission, filter, order, retry, consumption mode, and version consistency",
}];
const CONFIG_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "TOPIC_GROUP_PERMISSION_MISMATCH",
        root_cause: "Topic and Consumer Group permissions are incompatible",
        rationale: "The read-only permission compatibility predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "permissions_compatible",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "SUBSCRIPTION_FILTER_DRIFT",
        root_cause: "Consumer subscription filters differ across active clients",
        rationale: "The bounded subscription filter digest comparison diverged",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "filter_consistent",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "TOPIC_GROUP_MODE_DRIFT",
        root_cause: "Order, retry, or consumption mode configuration has drifted",
        rationale: "The Topic/Group semantic configuration comparison diverged",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "mode_consistent",
            expected: false,
        },
    },
];
const CONFIG_CODES: &[&str] = &[
    "TOPIC_GROUP_PERMISSION_MISMATCH",
    "SUBSCRIPTION_FILTER_DRIFT",
    "TOPIC_GROUP_MODE_DRIFT",
    "TOPIC_SUBSCRIPTION_CONFIG_HEALTHY",
    "TOPIC_SUBSCRIPTION_CONFIG_EVIDENCE_INCOMPLETE",
];
const CONFIG_FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "admin-query",
    resource_template: "topic-subscription-config/{group}/{topic}",
    reason: "Refresh sanitized Topic, Group, and subscription metadata",
}];

pub(super) const TOPIC_SUBSCRIPTION_CONFIG: PackSpec = PackSpec {
    id: "topic-subscription-config",
    components: &["topic", "consumer", "broker"],
    required: CONFIG_REQUIRED,
    optional: ROUTING_OPTIONAL,
    rules: CONFIG_RULES,
    rule_codes: CONFIG_CODES,
    healthy_code: "TOPIC_SUBSCRIPTION_CONFIG_HEALTHY",
    healthy_summary: "Topic, Group, subscription, retry, order, and consumption settings are consistent",
    incomplete_code: "TOPIC_SUBSCRIPTION_CONFIG_EVIDENCE_INCOMPLETE",
    follow_up: CONFIG_FOLLOW_UP,
    max_freshness_seconds: 300,
};
