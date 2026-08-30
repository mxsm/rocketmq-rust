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

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const root = resolve(scriptDir, "..");

const bool = (path, normal, fault) => ({ path, normal, fault });
const number = (path, normal, fault) => ({ path, normal, fault });
const text = (path, normal, fault) => ({ path, normal, fault });
const pack = (
  wave,
  id,
  source,
  prefix,
  faultCode,
  healthyCode,
  incompleteCode,
  signals,
  thresholds,
  window,
) => ({
  wave,
  id,
  qualifiedId: `${id}.v1`,
  source,
  prefix,
  faultCode,
  healthyCode,
  incompleteCode,
  signals,
  thresholds,
  window,
});

const packs = [
  pack("B", "store-pressure", "admin-query", "store-pressure/", "STORE_DISK_PRESSURE", "STORE_PRESSURE_HEALTHY", "STORE_PRESSURE_EVIDENCE_INCOMPLETE", [
    bool("disk_pressure", false, true),
    bool("flush_or_dispatch_stalled", false, true),
    bool("tiered_wal_pressure", false, true),
  ], ["disk_used_percent: 85", "flush_latency_ms: 500", "dispatch_backlog: 1000"], "5m"),
  pack("B", "store-integrity", "admin-query", "store-integrity/", "STORE_OFFSET_INCONSISTENT", "STORE_INTEGRITY_HEALTHY", "STORE_INTEGRITY_EVIDENCE_INCOMPLETE", [
    bool("offset_inconsistent", false, true),
    bool("recovery_incomplete", false, true),
    bool("index_rebuild_failed", false, true),
  ], ["checkpoint_offset_tolerance_bytes: 0"], "5m"),
  pack("B", "rocksdb-health", "admin-query", "rocksdb-health/", "ROCKSDB_MAINTENANCE_STUCK", "ROCKSDB_HEALTHY", "ROCKSDB_EVIDENCE_INCOMPLETE", [
    bool("maintenance_stuck", false, true),
    number("read_amplification", 2.0, 8.0),
    number("cache_hit_ratio", 0.95, 0.7),
  ], ["read_amplification: 8", "cache_hit_ratio: 0.8"], "10m"),
  pack("B", "tiered-store", "admin-query", "tiered-store/", "TIERED_PROVIDER_UNAVAILABLE", "TIERED_STORE_HEALTHY", "TIERED_STORE_EVIDENCE_INCOMPLETE", [
    bool("provider_available", true, false),
    bool("dispatch_stalled", false, true),
    text("metadata_reconcile_state", "healthy", "failed"),
  ], ["fallback_ratio: 0.2", "dispatch_stall_seconds: 300"], "10m"),
  pack("B", "broker-ha", "admin-query", "broker-ha/", "BROKER_HA_REPLICA_LAG", "BROKER_HA_HEALTHY", "BROKER_HA_EVIDENCE_INCOMPLETE", [
    bool("replica_lag_high", false, true),
    bool("sync_state_set_sufficient", true, false),
    bool("replicas_healthy", true, false),
  ], ["replica_lag_bytes: 10485760", "replication_latency_ms: 1000"], "5m"),
  pack("B", "controller-ha", "prometheus", "controller-ha/", "CONTROLLER_LEADER_UNKNOWN", "CONTROLLER_HA_HEALTHY", "CONTROLLER_HA_EVIDENCE_INCOMPLETE", [
    bool("leader_known", true, false),
    bool("quorum_healthy", true, false),
    bool("broker_heartbeat_stale", false, true),
  ], ["heartbeat_age_seconds: 30", "commit_apply_gap: 100"], "2m"),
  pack("B", "namesrv-route", "rocketmq-mcp", "namesrv-route/", "NAMESRV_ROUTE_DIVERGENT", "NAMESRV_ROUTE_HEALTHY", "NAMESRV_ROUTE_EVIDENCE_INCOMPLETE", [
    bool("route_divergent", false, true),
    bool("registration_stale", false, true),
    bool("broker_unreachable", false, true),
  ], ["registration_age_seconds: 60"], "3m"),
  pack("B", "send-latency", "prometheus", "send-latency/", "SEND_CLIENT_PROXY_LATENCY", "SEND_LATENCY_HEALTHY", "SEND_LATENCY_EVIDENCE_INCOMPLETE", [
    bool("client_or_proxy_slow", false, true),
    bool("remoting_or_broker_slow", false, true),
    bool("store_slow", false, true),
  ], ["segment_p99_ms: 500"], "5m"),
  pack("B", "proxy-connectivity", "prometheus", "proxy-connectivity/", "PROXY_GRPC_REMOTING_UNHEALTHY", "PROXY_CONNECTIVITY_HEALTHY", "PROXY_CONNECTIVITY_EVIDENCE_INCOMPLETE", [
    bool("transport_healthy", true, false),
    bool("backend_route_available", true, false),
    bool("tls_or_auth_failed", false, true),
  ], ["grpc_error_rate_percent: 5"], "3m"),
  pack("B", "static-topic-route", "rocketmq-mcp", "static-topic-route/", "STATIC_ROUTE_EPOCH_DIVERGENT", "STATIC_TOPIC_ROUTE_HEALTHY", "STATIC_TOPIC_ROUTE_EVIDENCE_INCOMPLETE", [
    bool("mapping_epoch_consistent", true, false),
    bool("all_logical_queues_mapped", true, false),
    bool("expansion_preconditions_met", true, false),
  ], ["mapping_epoch_tolerance: 0"], "5m"),
  pack("B", "topic-subscription-config", "admin-query", "topic-subscription-config/", "TOPIC_GROUP_PERMISSION_MISMATCH", "TOPIC_SUBSCRIPTION_CONFIG_HEALTHY", "TOPIC_SUBSCRIPTION_CONFIG_EVIDENCE_INCOMPLETE", [
    bool("permissions_compatible", true, false),
    bool("filter_consistent", true, false),
    bool("mode_consistent", true, false),
  ], ["version_drift_tolerance: 0"], "5m"),
  pack("B", "retry-dlq", "prometheus", "retry-dlq/", "RETRY_DLQ_GROWTH", "RETRY_DLQ_HEALTHY", "RETRY_DLQ_EVIDENCE_INCOMPLETE", [
    bool("retry_or_dlq_growing", false, true),
    bool("poison_metadata_pattern", false, true),
    bool("downstream_available", true, false),
  ], ["growth_window_minutes: 15"], "15m"),
  pack("B", "transaction-message", "prometheus", "transaction-message/", "TRANSACTION_HALF_BACKLOG", "TRANSACTION_MESSAGE_HEALTHY", "TRANSACTION_MESSAGE_EVIDENCE_INCOMPLETE", [
    bool("half_backlog_growing", false, true),
    bool("checkback_stalled", false, true),
    bool("producer_reachable", true, false),
  ], ["checkback_age_seconds: 120"], "10m"),
  pack("B", "pop-revive", "prometheus", "pop-revive/", "POP_INFLIGHT_PRESSURE", "POP_REVIVE_HEALTHY", "POP_REVIVE_EVIDENCE_INCOMPLETE", [
    bool("inflight_pressure", false, true),
    bool("revive_lag_high", false, true),
    bool("receipt_handle_failed", false, true),
  ], ["revive_lag_seconds: 60"], "5m"),
  pack("B", "timer-backlog", "prometheus", "timer-backlog/", "TIMER_DEQUEUE_LAG", "TIMER_BACKLOG_HEALTHY", "TIMER_BACKLOG_EVIDENCE_INCOMPLETE", [
    bool("dequeue_lag_high", false, true),
    bool("snapshot_stale", false, true),
    bool("clock_or_store_pressure", false, true),
  ], ["dequeue_lag_seconds: 60", "snapshot_age_seconds: 300"], "5m"),
  pack("B", "queue-hotspot", "prometheus", "queue-hotspot/", "QUEUE_TRAFFIC_HOTSPOT", "QUEUE_HOTSPOT_HEALTHY", "QUEUE_HOTSPOT_EVIDENCE_INCOMPLETE", [
    bool("tps_skew_high", false, true),
    bool("storage_skew_high", false, true),
    bool("expansion_required", false, true),
  ], ["queue_skew_ratio: 3"], "15m"),
  pack("B", "auth-failure", "admin-query", "auth-failure/", "AUTH_SCOPE_DENIED", "AUTH_FAILURE_NOT_OBSERVED", "AUTH_FAILURE_EVIDENCE_INCOMPLETE", [
    text("deny_category", "none", "scope"),
    bool("certificate_valid", true, false),
    bool("replay_or_clock_skew", false, true),
  ], ["certificate_renewal_days: 30", "clock_skew_seconds: 30"], "3m"),
  pack("B", "runtime-saturation", "runtime", "runtime-saturation/", "RUNTIME_TASKGROUP_SATURATED", "RUNTIME_SATURATION_HEALTHY", "RUNTIME_SATURATION_EVIDENCE_INCOMPLETE", [
    bool("taskgroup_saturated", false, true),
    bool("blocking_executor_pressure", false, true),
    bool("schedule_or_shutdown_stalled", false, true),
  ], ["queue_utilization_ratio: 0.8", "long_running_seconds: 60"], "2m"),
  pack("C", "upgrade-readiness", "kubernetes", "upgrade-readiness/", "UPGRADE_PROTOCOL_INCOMPATIBLE", "UPGRADE_READY", "UPGRADE_READINESS_EVIDENCE_INCOMPLETE", [
    bool("protocol_compatible", true, false),
    bool("quorum_and_pdb_safe", true, false),
    bool("canary_and_rollback_ready", true, false),
  ], ["minimum_capacity_headroom_ratio: 0.2"], "10m"),
  pack("C", "capacity-runway", "prometheus", "capacity-runway/", "CAPACITY_DISK_RUNWAY_LOW", "CAPACITY_RUNWAY_HEALTHY", "CAPACITY_RUNWAY_EVIDENCE_INCOMPLETE", [
    number("disk_runway_days", 60, 10),
    number("backlog_runway_days", 14, 3),
    number("connection_headroom_ratio", 0.5, 0.1),
  ], ["disk_runway_days: 30", "backlog_runway_days: 7", "connection_headroom_ratio: 0.2"], "30d"),
  pack("C", "cold-data-flow", "admin-query", "cold-data-flow/", "COLD_DATA_HIT_RATE_LOW", "COLD_DATA_FLOW_HEALTHY", "COLD_DATA_FLOW_EVIDENCE_INCOMPLETE", [
    number("cold_hit_ratio", 0.9, 0.5),
    number("fallback_ratio", 0.05, 0.3),
    bool("local_retention_pressure", false, true),
  ], ["cold_hit_ratio: 0.7", "fallback_ratio: 0.2"], "30d"),
  pack("C", "dr-readiness", "admin-query", "dr-readiness/", "DR_BACKUP_OR_SNAPSHOT_STALE", "DR_READY", "DR_READINESS_EVIDENCE_INCOMPLETE", [
    bool("backup_or_snapshot_stale", false, true),
    bool("restore_verified", true, false),
    bool("rto_rpo_met", true, false),
  ], ["rpo_minutes: 15", "rto_minutes: 60"], "30d"),
  pack("C", "security-posture", "admin-query", "security-posture/", "SECURITY_PRIVILEGE_DRIFT", "SECURITY_POSTURE_HEALTHY", "SECURITY_POSTURE_EVIDENCE_INCOMPLETE", [
    bool("privilege_drift", false, true),
    bool("credential_expiring", false, true),
    bool("unapproved_change", false, true),
  ], ["credential_renewal_days: 30"], "7d"),
  pack("C", "change-regression", "kubernetes", "change-regression/", "CHANGE_SLO_REGRESSION", "CHANGE_NO_REGRESSION", "CHANGE_REGRESSION_EVIDENCE_INCOMPLETE", [
    bool("slo_regressed", false, true),
    bool("error_or_latency_regressed", false, true),
    bool("impact_scope_expanded", false, true),
  ], ["comparison_before_minutes: 30", "comparison_after_minutes: 30"], "1h"),
];

function setPath(target, path, value) {
  const parts = path.split(".");
  let current = target;
  for (const part of parts.slice(0, -1)) {
    current[part] ??= {};
    current = current[part];
  }
  current[parts.at(-1)] = value;
}

function contentFor(entry, fault) {
  const content = {};
  entry.signals.forEach((signal, index) => {
    setPath(content, signal.path, fault && index === 0 ? signal.fault : signal.normal);
  });
  return content;
}

function fixturesFor(entry) {
  const evidence = (content) => [{
    source: entry.source,
    resource: `${entry.prefix}fixture`,
    coverage: "available",
    content,
  }];
  return [
    {
      pack: entry.qualifiedId,
      scenario: "normal",
      expected_status: "healthy",
      expected_reason_codes: [entry.healthyCode],
      evidence: evidence(contentFor(entry, false)),
    },
    {
      pack: entry.qualifiedId,
      scenario: "fault",
      expected_status: "fault",
      expected_reason_codes: [entry.faultCode],
      evidence: evidence(contentFor(entry, true)),
    },
    {
      pack: entry.qualifiedId,
      scenario: "missing",
      expected_status: "inconclusive",
      expected_reason_codes: [],
      evidence: [],
    },
  ];
}

function yamlFor(wavePacks, wave) {
  const lines = [
    "# Generated by scripts/generate_phase2_diagnostic_assets.mjs. Do not edit by hand.",
    "schema: rocketmq-sre.diagnostic-pack-catalog.v1",
    `wave: ${wave}`,
    "runtime: compiled-rust-rules",
    "rules_dsl_enabled: false",
    "packs:",
  ];
  for (const entry of wavePacks) {
    lines.push(`  - id: ${entry.id}`);
    lines.push("    version: 1.0.0");
    lines.push(`    qualified_id: ${entry.qualifiedId}`);
    lines.push(`    required_evidence: ${entry.source}:${entry.prefix}`);
    lines.push(`    max_window: ${entry.window}`);
    lines.push("    thresholds:");
    for (const threshold of entry.thresholds) {
      lines.push(`      - ${threshold}`);
    }
    lines.push("    fixture_scenarios: [normal, fault, missing]");
    lines.push(`    fault_reason_code: ${entry.faultCode}`);
    lines.push(`    healthy_reason_code: ${entry.healthyCode}`);
    lines.push(`    incomplete_reason_code: ${entry.incompleteCode}`);
  }
  return `${lines.join("\n")}\n`;
}

for (const wave of ["B", "C"]) {
  const wavePacks = packs.filter((entry) => entry.wave === wave);
  const slug = wave.toLowerCase();
  const configPath = resolve(root, "config", "diagnostics", `wave-${slug}`, "packs.v1.yaml");
  const fixturePath = resolve(root, "tests", "fixtures", "diagnostics", `wave-${slug}`, "catalog.v1.json");
  mkdirSync(dirname(configPath), { recursive: true });
  mkdirSync(dirname(fixturePath), { recursive: true });
  writeFileSync(configPath, yamlFor(wavePacks, wave));
  writeFileSync(fixturePath, `${JSON.stringify({
    schema: "rocketmq-sre.diagnostic-fixture-catalog.v1",
    wave,
    fixtures: wavePacks.flatMap(fixturesFor),
  }, null, 2)}\n`);
}

console.log(`generated ${packs.length} diagnostic packs and ${packs.length * 3} replay fixtures`);
