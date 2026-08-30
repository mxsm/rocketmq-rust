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

const STORE_OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "store-trend",
    source: "prometheus",
    resource_prefix: "store-trend/",
    purpose: "Seven and thirty day Store pressure and latency trends",
}];

const STORE_FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "admin-query",
        resource_template: "store-diagnostics/{broker}",
        reason: "Refresh bounded Store health, recovery, and backend state",
    },
    FollowUpQuery {
        source: "prometheus",
        resource_template: "store-trend/{broker}",
        reason: "Compare current pressure with bounded historical trends",
    },
];

const STORE_PRESSURE_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "store-pressure",
    source: "admin-query",
    resource_prefix: "store-pressure/",
    purpose: "Disk, flush, dispatch, CommitLog/CQ, RocksDB, and tiered WAL pressure",
}];
const STORE_PRESSURE_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "STORE_DISK_PRESSURE",
        root_cause: "Store disk usage has crossed the configured pressure threshold",
        rationale: "The bounded Store snapshot reports disk pressure",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "disk_pressure",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "STORE_FLUSH_DISPATCH_STALL",
        root_cause: "CommitLog flush or ConsumeQueue dispatch is stalled",
        rationale: "The Store persistence pipeline reports a stalled stage",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "flush_or_dispatch_stalled",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "STORE_TIERED_WAL_PRESSURE",
        root_cause: "Tiered dispatch is pinning WAL segments under local disk pressure",
        rationale: "The tiered WAL pressure signal is active",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "tiered_wal_pressure",
            expected: true,
        },
    },
];
const STORE_PRESSURE_CODES: &[&str] = &[
    "STORE_DISK_PRESSURE",
    "STORE_FLUSH_DISPATCH_STALL",
    "STORE_TIERED_WAL_PRESSURE",
    "STORE_PRESSURE_HEALTHY",
    "STORE_PRESSURE_EVIDENCE_INCOMPLETE",
];

pub(super) const STORE_PRESSURE: PackSpec = PackSpec {
    id: "store-pressure",
    components: &["broker", "store", "tiered-store"],
    required: STORE_PRESSURE_REQUIRED,
    optional: STORE_OPTIONAL,
    rules: STORE_PRESSURE_RULES,
    rule_codes: STORE_PRESSURE_CODES,
    healthy_code: "STORE_PRESSURE_HEALTHY",
    healthy_summary: "Store disk, flush, dispatch, and tiered WAL pressure are within bounds",
    incomplete_code: "STORE_PRESSURE_EVIDENCE_INCOMPLETE",
    follow_up: STORE_FOLLOW_UP,
    max_freshness_seconds: 300,
};

const STORE_INTEGRITY_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "store-integrity",
    source: "admin-query",
    resource_prefix: "store-integrity/",
    purpose: "CommitLog, ConsumeQueue, Index, Checkpoint, recovery, and acknowledged-message integrity",
}];
const STORE_INTEGRITY_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "STORE_OFFSET_INCONSISTENT",
        root_cause: "CommitLog, ConsumeQueue, Index, or Checkpoint offsets are inconsistent",
        rationale: "The read-only integrity comparison detected an offset invariant violation",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "offset_inconsistent",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "STORE_RECOVERY_INCOMPLETE",
        root_cause: "Store recovery or acknowledged-message verification is incomplete",
        rationale: "The recovery report did not reach a verified terminal state",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "recovery_incomplete",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "STORE_INDEX_REBUILD_FAILED",
        root_cause: "Background index reconstruction has failed or stopped progressing",
        rationale: "The bounded index rebuild status reports failure",
        severity: Severity::Warning,
        condition: Condition::Boolean {
            path: "index_rebuild_failed",
            expected: true,
        },
    },
];
const STORE_INTEGRITY_CODES: &[&str] = &[
    "STORE_OFFSET_INCONSISTENT",
    "STORE_RECOVERY_INCOMPLETE",
    "STORE_INDEX_REBUILD_FAILED",
    "STORE_INTEGRITY_HEALTHY",
    "STORE_INTEGRITY_EVIDENCE_INCOMPLETE",
];

pub(super) const STORE_INTEGRITY: PackSpec = PackSpec {
    id: "store-integrity",
    components: &["broker", "store"],
    required: STORE_INTEGRITY_REQUIRED,
    optional: STORE_OPTIONAL,
    rules: STORE_INTEGRITY_RULES,
    rule_codes: STORE_INTEGRITY_CODES,
    healthy_code: "STORE_INTEGRITY_HEALTHY",
    healthy_summary: "Store offsets, recovery, checkpoints, and indexes are consistent",
    incomplete_code: "STORE_INTEGRITY_EVIDENCE_INCOMPLETE",
    follow_up: STORE_FOLLOW_UP,
    max_freshness_seconds: 300,
};

const ROCKSDB_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "rocksdb-health",
    source: "admin-query",
    resource_prefix: "rocksdb-health/",
    purpose: "RocksDB cache, I/O, compression, amplification, maintenance, checkpoint, and disk health",
}];
const ROCKSDB_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "ROCKSDB_MAINTENANCE_STUCK",
        root_cause: "RocksDB maintenance or checkpoint work is not progressing",
        rationale: "The bounded maintenance state exceeds its configured window",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "maintenance_stuck",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "ROCKSDB_READ_AMPLIFICATION_HIGH",
        root_cause: "RocksDB read amplification is above the supported operating envelope",
        rationale: "The read amplification ratio crosses the configured threshold",
        severity: Severity::Warning,
        condition: Condition::NumberAtLeast {
            path: "read_amplification",
            threshold: 8.0,
        },
    },
    RuleSpec {
        reason_code: "ROCKSDB_CACHE_PRESSURE",
        root_cause: "RocksDB block cache pressure is degrading read performance",
        rationale: "The cache hit ratio is below the configured threshold",
        severity: Severity::Warning,
        condition: Condition::NumberBelow {
            path: "cache_hit_ratio",
            threshold: 0.8,
        },
    },
];
const ROCKSDB_CODES: &[&str] = &[
    "ROCKSDB_MAINTENANCE_STUCK",
    "ROCKSDB_READ_AMPLIFICATION_HIGH",
    "ROCKSDB_CACHE_PRESSURE",
    "ROCKSDB_HEALTHY",
    "ROCKSDB_EVIDENCE_INCOMPLETE",
];

pub(super) const ROCKSDB_HEALTH: PackSpec = PackSpec {
    id: "rocksdb-health",
    components: &["store", "rocksdb"],
    required: ROCKSDB_REQUIRED,
    optional: STORE_OPTIONAL,
    rules: ROCKSDB_RULES,
    rule_codes: ROCKSDB_CODES,
    healthy_code: "ROCKSDB_HEALTHY",
    healthy_summary: "RocksDB cache, amplification, maintenance, checkpoint, and disk signals are healthy",
    incomplete_code: "ROCKSDB_EVIDENCE_INCOMPLETE",
    follow_up: STORE_FOLLOW_UP,
    max_freshness_seconds: 300,
};

const TIERED_REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "tiered-store",
    source: "admin-query",
    resource_prefix: "tiered-store/",
    purpose: "Tiered dispatch, provider, transfer, fallback, read-ahead, pinned offset, and metadata health",
}];
const TIERED_RULES: &[RuleSpec] = &[
    RuleSpec {
        reason_code: "TIERED_PROVIDER_UNAVAILABLE",
        root_cause: "The configured tiered storage provider is unavailable",
        rationale: "Provider readiness is explicitly false",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "provider_available",
            expected: false,
        },
    },
    RuleSpec {
        reason_code: "TIERED_DISPATCH_STALLED",
        root_cause: "Tiered dispatch or upload progress is stalled",
        rationale: "The tiered dispatch progress signal is stalled",
        severity: Severity::Critical,
        condition: Condition::Boolean {
            path: "dispatch_stalled",
            expected: true,
        },
    },
    RuleSpec {
        reason_code: "TIERED_METADATA_RECONCILE_FAILED",
        root_cause: "Tiered metadata reconciliation failed",
        rationale: "The metadata reconciliation state is failed",
        severity: Severity::Warning,
        condition: Condition::TextEquals {
            path: "metadata_reconcile_state",
            expected: "failed",
        },
    },
];
const TIERED_CODES: &[&str] = &[
    "TIERED_PROVIDER_UNAVAILABLE",
    "TIERED_DISPATCH_STALLED",
    "TIERED_METADATA_RECONCILE_FAILED",
    "TIERED_STORE_HEALTHY",
    "TIERED_STORE_EVIDENCE_INCOMPLETE",
];

pub(super) const TIERED_STORE: PackSpec = PackSpec {
    id: "tiered-store",
    components: &["store", "tiered-store"],
    required: TIERED_REQUIRED,
    optional: STORE_OPTIONAL,
    rules: TIERED_RULES,
    rule_codes: TIERED_CODES,
    healthy_code: "TIERED_STORE_HEALTHY",
    healthy_summary: "Tiered provider, dispatch, fallback, read-ahead, pinned offsets, and metadata are healthy",
    incomplete_code: "TIERED_STORE_EVIDENCE_INCOMPLETE",
    follow_up: STORE_FOLLOW_UP,
    max_freshness_seconds: 300,
};
