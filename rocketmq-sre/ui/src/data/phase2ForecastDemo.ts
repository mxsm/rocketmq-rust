import type {
  ClusterForecastReport,
  DrReadinessReport,
  UpgradeReadinessReport,
  WhatIfSimulation,
  WhatIfSimulationRequest,
} from "@/api/types";

const OBSERVED_AT = "2026-07-27T09:00:00Z";
const EVIDENCE_ID = "40000000-0000-4000-8000-000000000001";

function points(initial: number, slope: number) {
  return Array.from({ length: 8 }, (_, index) => ({
    at: new Date(
      Date.parse(OBSERVED_AT) - (7 - index) * 3_600_000,
    ).toISOString(),
    value: initial + slope * index,
    projected: index >= 6,
  }));
}

export function demoForecastReport(
  tenantId: string,
  clusterId: string,
): ClusterForecastReport {
  const base = {
    tenant_id: tenantId,
    cluster_id: clusterId,
    status: "ready" as const,
    quality: "high" as const,
    algorithm_version: "rocketmq-sre.explainable-forecast.v1",
    sample_start: "2026-07-20T09:00:00Z",
    sample_end: OBSERVED_AT,
    coverage_ratio: 0.96,
    backtest: {
      evaluated_points: 32,
      mean_absolute_error: 0.021,
      bias: -0.006,
      interval_coverage_ratio: 0.91,
    },
    evidence_ids: [EVIDENCE_ID],
    execution_eligible: false,
    observed_at: OBSERVED_AT,
  };
  return {
    schema_version: "rocketmq-sre.cluster-forecast.v1",
    tenant_id: tenantId,
    cluster_id: clusterId,
    forecasts: [
      {
        ...base,
        id: "71000000-0000-4000-8000-000000000001",
        resource: {
          kind: "broker",
          key: "forecast/broker_disk",
          display_name: "broker_disk",
        },
        metric: "rocketmq_broker_disk_used_ratio",
        window: "seven_days",
        trend: "increasing",
        slope_per_hour: 0.0018,
        volatility: 0.032,
        threshold: 0.85,
        exhaustion_at: "2026-08-04T18:00:00Z",
        points: points(0.68, 0.018),
        advisories: ["review_capacity_before_projected_threshold"],
      },
      {
        ...base,
        id: "71000000-0000-4000-8000-000000000002",
        resource: {
          kind: "proxy",
          key: "forecast/proxy_capacity",
          display_name: "proxy_capacity",
        },
        metric: "rocketmq_proxy_capacity_utilization",
        window: "thirty_days",
        trend: "stable",
        slope_per_hour: 0.00002,
        volatility: 0.018,
        threshold: 0.8,
        exhaustion_at: null,
        points: points(0.51, 0.002),
        advisories: ["continue_observation"],
      },
      {
        ...base,
        id: "71000000-0000-4000-8000-000000000003",
        resource: {
          kind: "certificate",
          key: "forecast/certificate_expiry",
          display_name: "certificate_expiry",
        },
        metric: "rocketmq_certificate_seconds_until_expiry",
        window: "thirty_days",
        trend: "decreasing",
        slope_per_hour: -3600,
        volatility: 0.001,
        threshold: 0,
        exhaustion_at: "2026-08-18T00:00:00Z",
        points: points(2_000_000, -86_400),
        advisories: ["rotate_or_review_before_expiry"],
      },
    ],
    backlog_etas: [
      {
        ...base,
        id: "72000000-0000-4000-8000-000000000001",
        resource: {
          kind: "consumer_group",
          key: "forecast/consumer_lag",
          display_name: "consumer_lag",
        },
        backlog_kind: "consumer_lag",
        window: "seven_days",
        trend: "decreasing",
        current_value: 18_240,
        slope_per_hour: -2300,
        arrival_rate_per_second: null,
        drain_rate_per_second: 0.639,
        estimated_clear_at: "2026-07-27T16:56:00Z",
      },
      {
        ...base,
        id: "72000000-0000-4000-8000-000000000002",
        resource: {
          kind: "store",
          key: "forecast/timer_backlog",
          display_name: "timer_backlog",
        },
        backlog_kind: "timer_backlog",
        window: "thirty_days",
        trend: "increasing",
        current_value: 6800,
        slope_per_hour: 410,
        arrival_rate_per_second: 0.114,
        drain_rate_per_second: null,
        estimated_clear_at: null,
      },
    ],
    baselines: [
      {
        id: "73000000-0000-4000-8000-000000000001",
        tenant_id: tenantId,
        cluster_id: clusterId,
        resource: {
          kind: "proxy",
          key: "forecast/proxy_capacity",
          display_name: "proxy_capacity",
        },
        metric: "rocketmq_proxy_capacity_utilization",
        seasonality: "daily",
        period_seconds: 86_400,
        median: 0.49,
        median_absolute_deviation: 0.025,
        sample_count: 28,
        coverage_ratio: 0.93,
        algorithm_version: "rocketmq-sre.explainable-forecast.v1",
        valid_from: "2026-06-27T09:00:00Z",
        valid_until: "2026-07-28T09:00:00Z",
      },
    ],
    anomalies: [
      {
        tenant_id: tenantId,
        cluster_id: clusterId,
        resource: {
          kind: "store",
          key: "forecast/timer_backlog",
          display_name: "timer_backlog",
        },
        metric: "rocketmq_timer_backlog",
        seasonality: "hourly",
        status: "ready",
        observed_value: 6800,
        baseline_median: 2400,
        robust_z_score: 5.7,
        empirical_quantile: 1,
        anomaly: true,
        evidence_ids: [EVIDENCE_ID],
        observed_at: OBSERVED_AT,
      },
    ],
    change_points: [
      {
        id: "74000000-0000-4000-8000-000000000001",
        tenant_id: tenantId,
        cluster_id: clusterId,
        resource: {
          kind: "store",
          key: "forecast/timer_backlog",
          display_name: "timer_backlog",
        },
        metric: "rocketmq_timer_backlog",
        detected_at: "2026-07-27T06:00:00Z",
        before_value: 2300,
        after_value: 6100,
        score: 4.8,
        algorithm_version: "rocketmq-sre.explainable-forecast.v1",
        evidence_ids: [EVIDENCE_ID],
      },
    ],
    accuracy: [
      {
        metric: "rocketmq_broker_disk_used_ratio",
        window: "seven_days",
        evaluated_points: 46,
        mean_absolute_error: 0.019,
        bias: -0.004,
        interval_coverage_ratio: 0.913,
        observed_at: OBSERVED_AT,
      },
    ],
    partial: false,
    warnings: [],
    execution_eligible: false,
    observed_at: OBSERVED_AT,
  };
}

export function demoSimulation(
  tenantId: string,
  input: WhatIfSimulationRequest,
): WhatIfSimulation {
  const current = input.current_utilization ?? 0.64;
  const factor =
    input.kind === "traffic_increase"
      ? 1 + (input.traffic_increase_percent ?? 25) / 100
      : input.kind === "broker_offline" ||
          input.kind === "proxy_offline"
        ? (input.current_instances ?? 3) /
          Math.max(1, (input.current_instances ?? 3) - 1)
        : 1;
  const projected = current * factor;
  return {
    id: crypto.randomUUID(),
    tenant_id: tenantId,
    cluster_id: input.cluster_id,
    kind: input.kind,
    status: "completed",
    input,
    assumptions: ["capacity_remains_constant"],
    projected_utilization: {
      current,
      projected,
      unit: "ratio",
    },
    bottlenecks:
      projected >= 0.8 ? ["projected_high_utilization"] : [],
    blast_radius:
      input.affected_resource_keys ?? ["topic:orders", "group:fulfillment"],
    missing_assumptions: [],
    evidence_ids: input.evidence_ids ?? [],
    algorithm_version: "rocketmq-sre.explainable-forecast.v1",
    created_by: "rocketmq-sre-demo",
    execution_eligible: false,
    created_at: new Date().toISOString(),
  };
}

export function demoUpgradeReadiness(
  tenantId: string,
  clusterId: string,
  targetVersion: string,
): UpgradeReadinessReport {
  return {
    id: crypto.randomUUID(),
    tenant_id: tenantId,
    cluster_id: clusterId,
    target_version: targetVersion,
    status: "ready_with_warnings",
    findings: [
      {
        code: "capacity_runway_acceptable_missing",
        severity: "warning",
        component: "capacity",
        summary: "一个容量指标仍在积累 30 天样本",
        evidence_ids: [EVIDENCE_ID],
        remediation_hint: "collect_capacity_runway_evidence",
      },
    ],
    pack_versions: [
      "upgrade-readiness.v1",
      "capacity-runway.v1",
      "broker-ha.v1",
    ],
    execution_eligible: false,
    observed_at: OBSERVED_AT,
    expires_at: "2026-07-27T10:00:00Z",
  };
}

export function demoDrReadiness(
  tenantId: string,
  clusterId: string,
  targetRegion?: string,
): DrReadinessReport {
  return {
    id: crypto.randomUUID(),
    tenant_id: tenantId,
    cluster_id: clusterId,
    target_region: targetRegion ?? "cn-shanghai",
    requested_rto_seconds: 3600,
    requested_rpo_seconds: 300,
    status: "blocked",
    findings: [
      {
        code: "recovery_verified_failed",
        severity: "blocker",
        component: "store",
        summary: "最近一次备份恢复验证已过期",
        evidence_ids: [],
        remediation_hint: "resolve_recovery_verified",
      },
    ],
    execution_eligible: false,
    observed_at: OBSERVED_AT,
    expires_at: "2026-07-27T10:00:00Z",
  };
}
