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

const AUTH_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "auth-failure",
    source: "admin-query",
    resource_prefix: "auth-failure/",
    purpose: "Scope, certificate, credential rotation, clock skew, replay window, and bounded deny reasons",
}];
const AUTH_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "auth-telemetry",
    source: "prometheus",
    resource_prefix: "auth-telemetry/",
    purpose: "Bounded authentication and authorization result metrics",
}];
const AUTH_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "AUTH_SCOPE_DENIED",
        root_cause: "The caller scope does not authorize the requested resource",
        rationale: "The bounded deny category is scope or resource authorization",
        severity: Severity::Critical,
        condition: Condition::TextEquals {
            path: "deny_category",
            expected: "scope",
        },
    },
    RuleSpec {
        reason_code: "AUTH_CERTIFICATE_INVALID",
        root_cause: "A certificate is expired, not yet valid, or outside its renewal window",
        rationale: "Certificate validity is explicitly false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "certificate_valid",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "AUTH_REPLAY_OR_CLOCK_SKEW",
        root_cause: "Clock skew or replay-window validation is rejecting requests",
        rationale: "The replay/clock predicate is active without exposing credential material",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "replay_or_clock_skew",
            expected: true,
        },
    },
];
const AUTH_CODES: &[&str] = &[
    "AUTH_SCOPE_DENIED",
    "AUTH_CERTIFICATE_INVALID",
    "AUTH_REPLAY_OR_CLOCK_SKEW",
    "AUTH_FAILURE_NOT_OBSERVED",
    "AUTH_FAILURE_EVIDENCE_INCOMPLETE",
];
const AUTH_FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "admin-query",
    resource_template: "auth-failure/{resource}",
    reason: "Refresh bounded Auth generation, reload, certificate, and deny-category evidence",
}];

pub(super) const AUTH_FAILURE: PackSpec = PackSpec {
    id: "auth-failure",
    components: &["auth", "broker", "proxy", "mcp"],
    required: AUTH_REQUIRED,
    optional: AUTH_OPTIONAL,
    rules: AUTH_RULES,
    rule_codes: AUTH_CODES,
    healthy_code: "AUTH_FAILURE_NOT_OBSERVED",
    healthy_summary: "No scope, certificate, replay-window, or clock-skew failure is observed",
    incomplete_code: "AUTH_FAILURE_EVIDENCE_INCOMPLETE",
    follow_up: AUTH_FOLLOW_UP,
    max_freshness_seconds: 180,
};

const RUNTIME_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "runtime-saturation",
    source: "runtime",
    resource_prefix: "runtime-saturation/",
    purpose: "TaskGroup, BlockingExecutor, scheduled drift/overlap, admission, and shutdown health",
}];
const RUNTIME_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "runtime-telemetry",
    source: "prometheus",
    resource_prefix: "runtime-telemetry/",
    purpose: "Runtime queue, timeout, and lifecycle telemetry",
}];
const RUNTIME_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "RUNTIME_TASKGROUP_SATURATED",
        root_cause: "A runtime TaskGroup is saturated or contains long-running work",
        rationale: "The bounded TaskKind aggregate crosses its configured saturation envelope",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "taskgroup_saturated",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "RUNTIME_BLOCKING_EXECUTOR_PRESSURE",
        root_cause: "BlockingExecutor queue or concurrency is saturated",
        rationale: "The bounded blocking lane pressure predicate is active",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "blocking_executor_pressure",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "RUNTIME_SCHEDULE_OR_SHUTDOWN_STALL",
        root_cause: "Scheduled work is drifting/overlapping or shutdown is stalled",
        rationale: "The runtime lifecycle progress predicate is false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "schedule_or_shutdown_stalled",
            expected: true,
        },
    },
];
const RUNTIME_CODES: &[&str] = &[
    "RUNTIME_TASKGROUP_SATURATED",
    "RUNTIME_BLOCKING_EXECUTOR_PRESSURE",
    "RUNTIME_SCHEDULE_OR_SHUTDOWN_STALL",
    "RUNTIME_SATURATION_HEALTHY",
    "RUNTIME_SATURATION_EVIDENCE_INCOMPLETE",
];
const RUNTIME_FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "runtime",
    resource_template: "runtime-saturation/{component}",
    reason: "Refresh the existing bounded runtime diagnostics endpoint",
}];

pub(super) const RUNTIME_SATURATION: PackSpec = PackSpec {
    id: "runtime-saturation",
    components: &["runtime", "broker", "nameserver", "controller", "proxy", "mcp", "sre"],
    required: RUNTIME_REQUIRED,
    optional: RUNTIME_OPTIONAL,
    rules: RUNTIME_RULES,
    rule_codes: RUNTIME_CODES,
    healthy_code: "RUNTIME_SATURATION_HEALTHY",
    healthy_summary: "TaskGroup, BlockingExecutor, scheduler, admission, and shutdown signals are healthy",
    incomplete_code: "RUNTIME_SATURATION_EVIDENCE_INCOMPLETE",
    follow_up: RUNTIME_FOLLOW_UP,
    max_freshness_seconds: 120,
};
