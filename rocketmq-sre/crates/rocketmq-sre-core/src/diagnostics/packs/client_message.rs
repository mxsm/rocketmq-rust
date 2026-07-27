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

const MESSAGE_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "message-metadata",
    source: "admin-query",
    resource_prefix: "message-metadata/",
    purpose: "Pseudonymized message metadata without body, token, address, or secret material",
}];
const MESSAGE_FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "prometheus",
        resource_template: "message-semantics/{resource}",
        reason: "Refresh bounded message-semantic counters and latency",
    },
    FollowUpQuery {
        source: "admin-query",
        resource_template: "message-metadata/{message_hash}",
        reason: "Collect body-free metadata only when an approved pseudonymous key exists",
    },
];

const RETRY_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "retry-dlq",
    source: "prometheus",
    resource_prefix: "retry-dlq/",
    purpose: "Retry and DLQ growth, failure classification, consumer errors, and downstream availability",
}];
const RETRY_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "RETRY_DLQ_GROWTH",
        root_cause: "Retry or dead-letter backlog is growing persistently",
        rationale: "The bounded retry/DLQ trend crosses its configured growth threshold",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "retry_or_dlq_growing",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "RETRY_POISON_MESSAGE_PATTERN",
        root_cause: "Pseudonymized metadata indicates a repeated poison-message pattern",
        rationale: "Body-free message metadata repeats across failed deliveries",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "poison_metadata_pattern",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "RETRY_DOWNSTREAM_UNAVAILABLE",
        root_cause: "A downstream dependency is unavailable during consumption",
        rationale: "The sanitized downstream availability predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "downstream_available",
            expected: false,
        },
    },
];
const RETRY_CODES: &[&str] = &[
    "RETRY_DLQ_GROWTH",
    "RETRY_POISON_MESSAGE_PATTERN",
    "RETRY_DOWNSTREAM_UNAVAILABLE",
    "RETRY_DLQ_HEALTHY",
    "RETRY_DLQ_EVIDENCE_INCOMPLETE",
];

pub(super) const RETRY_DLQ: PackSpec = PackSpec {
    id: "retry-dlq",
    components: &["consumer", "broker", "store"],
    required: RETRY_REQUIRED,
    optional: MESSAGE_OPTIONAL,
    rules: RETRY_RULES,
    rule_codes: RETRY_CODES,
    healthy_code: "RETRY_DLQ_HEALTHY",
    healthy_summary: "Retry, DLQ, consumer failure, and downstream signals are within bounds",
    incomplete_code: "RETRY_DLQ_EVIDENCE_INCOMPLETE",
    follow_up: MESSAGE_FOLLOW_UP,
    max_freshness_seconds: 300,
};

const TRANSACTION_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "transaction-message",
    source: "prometheus",
    resource_prefix: "transaction-message/",
    purpose: "Half, commit, rollback, checkback, prepared transaction, and producer reachability signals",
}];
const TRANSACTION_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "TRANSACTION_HALF_BACKLOG",
        root_cause: "Transactional half-message backlog is growing",
        rationale: "The half-message backlog trend crosses its configured threshold",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "half_backlog_growing",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "TRANSACTION_CHECKBACK_STALLED",
        root_cause: "Transaction checkback progress is stalled",
        rationale: "The checkback age exceeds its configured window",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "checkback_stalled",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "TRANSACTION_PRODUCER_UNREACHABLE",
        root_cause: "The transactional producer cannot be reached for state checks",
        rationale: "Producer reachability is explicitly false",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "producer_reachable",
            expected: false,
        },
    },
];
const TRANSACTION_CODES: &[&str] = &[
    "TRANSACTION_HALF_BACKLOG",
    "TRANSACTION_CHECKBACK_STALLED",
    "TRANSACTION_PRODUCER_UNREACHABLE",
    "TRANSACTION_MESSAGE_HEALTHY",
    "TRANSACTION_MESSAGE_EVIDENCE_INCOMPLETE",
];

pub(super) const TRANSACTION_MESSAGE: PackSpec = PackSpec {
    id: "transaction-message",
    components: &["producer", "broker", "store"],
    required: TRANSACTION_REQUIRED,
    optional: MESSAGE_OPTIONAL,
    rules: TRANSACTION_RULES,
    rule_codes: TRANSACTION_CODES,
    healthy_code: "TRANSACTION_MESSAGE_HEALTHY",
    healthy_summary: "Transactional half, checkback, finalization, and producer reachability signals are healthy",
    incomplete_code: "TRANSACTION_MESSAGE_EVIDENCE_INCOMPLETE",
    follow_up: MESSAGE_FOLLOW_UP,
    max_freshness_seconds: 300,
};

const POP_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "pop-revive",
    source: "prometheus",
    resource_prefix: "pop-revive/",
    purpose: "POP inflight, checkpoints, revive lag/latency/retry, receipt handle, and invisibility signals",
}];
const POP_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "POP_INFLIGHT_PRESSURE",
        root_cause: "POP inflight messages exceed the configured safe envelope",
        rationale: "The POP inflight pressure predicate is active",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "inflight_pressure",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "POP_REVIVE_LAG",
        root_cause: "POP revive checkpoint lag or latency is excessive",
        rationale: "The revive progress predicate crosses its configured threshold",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "revive_lag_high",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "POP_RECEIPT_FAILURE",
        root_cause: "POP receipt handle processing is failing",
        rationale: "The body-free receipt handle failure signal is active",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "receipt_handle_failed",
            expected: true,
        },
    },
];
const POP_CODES: &[&str] = &[
    "POP_INFLIGHT_PRESSURE",
    "POP_REVIVE_LAG",
    "POP_RECEIPT_FAILURE",
    "POP_REVIVE_HEALTHY",
    "POP_REVIVE_EVIDENCE_INCOMPLETE",
];

pub(super) const POP_REVIVE: PackSpec = PackSpec {
    id: "pop-revive",
    components: &["consumer", "broker", "store"],
    required: POP_REQUIRED,
    optional: MESSAGE_OPTIONAL,
    rules: POP_RULES,
    rule_codes: POP_CODES,
    healthy_code: "POP_REVIVE_HEALTHY",
    healthy_summary: "POP inflight, revive, receipt, and invisibility signals are healthy",
    incomplete_code: "POP_REVIVE_EVIDENCE_INCOMPLETE",
    follow_up: MESSAGE_FOLLOW_UP,
    max_freshness_seconds: 180,
};

const TIMER_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "timer-backlog",
    source: "prometheus",
    resource_prefix: "timer-backlog/",
    purpose: "Timer enqueue/dequeue lag, timing count, snapshot, clock, and Store pressure",
}];
const TIMER_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "TIMER_DEQUEUE_LAG",
        root_cause: "Timer dequeue progress is behind the scheduled delivery time",
        rationale: "Timer dequeue lag crosses its configured threshold",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "dequeue_lag_high",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "TIMER_SNAPSHOT_STALE",
        root_cause: "Timer checkpoint or snapshot state is stale",
        rationale: "The timer snapshot age crosses its configured freshness window",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "snapshot_stale",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "TIMER_CLOCK_OR_STORE_PRESSURE",
        root_cause: "Clock skew or Store pressure is blocking timer progress",
        rationale: "The timer dependency pressure predicate is active",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "clock_or_store_pressure",
            expected: true,
        },
    },
];
const TIMER_CODES: &[&str] = &[
    "TIMER_DEQUEUE_LAG",
    "TIMER_SNAPSHOT_STALE",
    "TIMER_CLOCK_OR_STORE_PRESSURE",
    "TIMER_BACKLOG_HEALTHY",
    "TIMER_BACKLOG_EVIDENCE_INCOMPLETE",
];

pub(super) const TIMER_BACKLOG: PackSpec = PackSpec {
    id: "timer-backlog",
    components: &["broker", "store", "timer"],
    required: TIMER_REQUIRED,
    optional: MESSAGE_OPTIONAL,
    rules: TIMER_RULES,
    rule_codes: TIMER_CODES,
    healthy_code: "TIMER_BACKLOG_HEALTHY",
    healthy_summary: "Timer enqueue/dequeue, snapshot, clock, and Store signals are healthy",
    incomplete_code: "TIMER_BACKLOG_EVIDENCE_INCOMPLETE",
    follow_up: MESSAGE_FOLLOW_UP,
    max_freshness_seconds: 180,
};

const HOTSPOT_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "queue-hotspot",
    source: "prometheus",
    resource_prefix: "queue-hotspot/",
    purpose: "Queue TPS, size, latency, key skew, Broker and disk distribution, and expansion demand",
}];
const HOTSPOT_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "QUEUE_TRAFFIC_HOTSPOT",
        root_cause: "Traffic is concentrated on a small subset of queues",
        rationale: "The queue TPS skew ratio crosses its configured threshold",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "tps_skew_high",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "QUEUE_STORAGE_HOTSPOT",
        root_cause: "Queue size or disk placement is uneven",
        rationale: "The queue storage distribution predicate reports a hotspot",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "storage_skew_high",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "QUEUE_EXPANSION_REQUIRED",
        root_cause: "Queue and Broker headroom is insufficient for projected traffic",
        rationale: "The read-only expansion demand predicate is active",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "expansion_required",
            expected: true,
        },
    },
];
const HOTSPOT_CODES: &[&str] = &[
    "QUEUE_TRAFFIC_HOTSPOT",
    "QUEUE_STORAGE_HOTSPOT",
    "QUEUE_EXPANSION_REQUIRED",
    "QUEUE_HOTSPOT_HEALTHY",
    "QUEUE_HOTSPOT_EVIDENCE_INCOMPLETE",
];

pub(super) const QUEUE_HOTSPOT: PackSpec = PackSpec {
    id: "queue-hotspot",
    components: &["topic", "queue", "broker", "store"],
    required: HOTSPOT_REQUIRED,
    optional: MESSAGE_OPTIONAL,
    rules: HOTSPOT_RULES,
    rule_codes: HOTSPOT_CODES,
    healthy_code: "QUEUE_HOTSPOT_HEALTHY",
    healthy_summary: "Queue traffic, storage, latency, key distribution, and Broker placement are balanced",
    incomplete_code: "QUEUE_HOTSPOT_EVIDENCE_INCOMPLETE",
    follow_up: MESSAGE_FOLLOW_UP,
    max_freshness_seconds: 300,
};
