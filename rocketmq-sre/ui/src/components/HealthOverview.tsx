import {
  Activity,
  AlertTriangle,
  Gauge,
  ShieldCheck,
} from "lucide-react";

import type {
  ClusterHealthReport,
  FleetHealthReport,
  HealthDataQuality,
  HealthOperationalState,
  HealthStatus,
  SloDimension,
} from "@/api/types";
import { Badge } from "@/components/ui/badge";

const statusLabels: Record<HealthStatus, string> = {
  healthy: "健康",
  degraded: "降级",
  critical: "严重",
  unknown: "未知",
};

const statusVariants = {
  healthy: "success",
  degraded: "warning",
  critical: "destructive",
  unknown: "secondary",
} as const;

const qualityLabels: Record<HealthDataQuality, string> = {
  complete: "完整",
  partial: "部分",
  stale: "过期",
  missing: "缺失",
};

const operationalLabels: Record<HealthOperationalState, string> = {
  normal: "正常运行",
  maintenance: "维护窗口",
  fault_drill: "故障演练",
};

const dimensionLabels: Record<SloDimension, string> = {
  traffic: "流量",
  consumer: "消费",
  broker: "Broker",
  store: "存储",
  ha_controller: "HA / Controller",
  routing_proxy: "路由 / Proxy",
  security: "安全",
  platform: "平台",
};

export function ClusterHealthOverview({
  report,
}: {
  report: ClusterHealthReport;
}) {
  const triggered = report.slis.filter(
    (sli) =>
      sli.status !== "healthy" ||
      sli.windows.some((window) => window.triggered),
  );
  const change = report.recent_changes?.at(0);

  return (
    <section
      aria-label="集群 SLO 与健康评分"
      className="data-surface health-surface"
    >
      <div className="surface-heading">
        <div>
          <h2>确定性 SLO 与集群健康</h2>
          <p>
            三组 burn-rate 窗口与八维规则评分；模型不参与改分。
          </p>
        </div>
        <HealthStatusBadge status={report.status} />
      </div>

      <div className="health-command-grid">
        <div className={`health-score-card health-${report.status}`}>
          <div className="health-score-icon" aria-hidden="true">
            <Gauge size={19} />
          </div>
          <span>Cluster Health Score</span>
          <strong>{formatScore(report.score)}</strong>
          <progress
            aria-label="集群健康分"
            max="100"
            value={report.score ?? 0}
          />
          <small>
            {report.score === null || report.score === undefined
              ? "数据不足，不以 0 伪装"
              : `${statusLabels[report.status]} · ${qualityLabels[report.data_quality]}`}
          </small>
        </div>

        <div className="health-facts-grid">
          <HealthFact
            label="数据质量"
            value={qualityLabels[report.data_quality]}
          />
          <HealthFact
            label="运行上下文"
            value={operationalLabels[report.operational_state]}
          />
          <HealthFact
            label="活跃 Incident"
            value={String(report.incident_summary.active_incidents)}
          />
          <HealthFact
            label="严重 Incident"
            value={String(report.incident_summary.critical_incidents)}
            warning={report.incident_summary.critical_incidents > 0}
          />
          <HealthFact
            label="最近变化"
            value={
              change?.score_delta === null ||
              change?.score_delta === undefined
                ? "无分数变化"
                : `${change.score_delta > 0 ? "+" : ""}${change.score_delta} 分`
            }
          />
          <HealthFact
            label="证据快照"
            value={`${report.evidence_ids?.length ?? 0} 份`}
          />
        </div>
      </div>

      <div className="dimension-grid" aria-label="八维健康评分">
        {report.dimensions.map((dimension) => (
          <article
            className={`dimension-card health-${dimension.status}`}
            key={dimension.dimension}
          >
            <div>
              <span>{dimensionLabels[dimension.dimension]}</span>
              <HealthStatusBadge compact status={dimension.status} />
            </div>
            <strong>{formatScore(dimension.score)}</strong>
            <progress
              aria-label={`${dimensionLabels[dimension.dimension]}健康分`}
              max="100"
              value={dimension.score ?? 0}
            />
            <small>
              权重 {dimension.weight}% ·{" "}
              {qualityLabels[dimension.data_quality]}
            </small>
            <p>
              {dimension.triggered_sli_ids?.length
                ? dimension.triggered_sli_ids.join(" · ")
                : dimension.reason_codes?.at(0) ?? "未触发 SLI"}
            </p>
          </article>
        ))}
      </div>

      <div className="health-detail-grid">
        <div className="health-detail-panel">
          <div className="health-panel-title">
            <Activity aria-hidden="true" size={15} />
            <strong>已触发 SLI 与 burn-rate</strong>
            <span>{triggered.length}</span>
          </div>
          {triggered.length > 0 ? (
            <div className="burn-rate-list">
              {triggered.map((sli) => (
                <article className="burn-rate-item" key={sli.id}>
                  <div>
                    <strong>{sli.display_name}</strong>
                    <span>{dimensionLabels[sli.dimension]}</span>
                    <HealthStatusBadge compact status={sli.status} />
                  </div>
                  <div className="burn-window-grid">
                    {sli.windows.map((window) => (
                      <div
                        className={window.triggered ? "triggered" : undefined}
                        key={window.window_id}
                      >
                        <span>
                          {formatDuration(window.short_window_seconds)} /{" "}
                          {formatDuration(window.long_window_seconds)}
                        </span>
                        <strong>
                          {formatBurnRate(window.short_burn_rate)} /{" "}
                          {formatBurnRate(window.long_burn_rate)}
                        </strong>
                        <small>
                          阈值 {window.threshold.toFixed(1)} ·{" "}
                          {window.triggered ? "已触发" : "未触发"}
                        </small>
                      </div>
                    ))}
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="health-empty">
              <ShieldCheck aria-hidden="true" size={18} />
              当前没有触发多窗口 SLI。
            </div>
          )}
        </div>

        <div className="health-detail-panel health-policy-panel">
          <div className="health-panel-title">
            <ShieldCheck aria-hidden="true" size={15} />
            <strong>评分边界</strong>
          </div>
          <dl>
            <div>
              <dt>算法版本</dt>
              <dd>{report.algorithm_version}</dd>
            </div>
            <div>
              <dt>模型改分</dt>
              <dd>{report.model_adjustment_supported ? "允许" : "禁止"}</dd>
            </div>
            <div>
              <dt>执行资格</dt>
              <dd>{report.execution_eligible ? "可执行" : "只读"}</dd>
            </div>
            <div>
              <dt>观测时间</dt>
              <dd>{formatTime(report.observed_at)}</dd>
            </div>
          </dl>
          <p>
            缺失、过期和部分数据均保留显式状态；维护与演练不会隐藏底层严重度。
          </p>
        </div>
      </div>
    </section>
  );
}

export function FleetHealthOverview({
  report,
}: {
  report: FleetHealthReport;
}) {
  const clusters = [...report.clusters].sort(
    (left, right) =>
      statusRank(right.status) - statusRank(left.status) ||
      (left.score ?? -1) - (right.score ?? -1),
  );

  return (
    <section
      aria-label="Fleet 健康总览"
      className="data-surface fleet-health-surface"
    >
      <div className="surface-heading">
        <div>
          <h2>Fleet 健康总览</h2>
          <p>
            按租户与区域只读聚合，最严重集群直接决定 Fleet 状态。
          </p>
        </div>
        <div className="fleet-heading-status">
          <span>最差分 {formatScore(report.score)}</span>
          <HealthStatusBadge status={report.status} />
        </div>
      </div>

      <div className="fleet-health-layout">
        <div className="fleet-stat-grid">
          <HealthFact label="集群" value={String(report.cluster_count)} />
          <HealthFact
            label="健康"
            value={String(report.healthy_clusters)}
          />
          <HealthFact
            label="降级"
            value={String(report.degraded_clusters)}
            warning={report.degraded_clusters > 0}
          />
          <HealthFact
            label="严重"
            value={String(report.critical_clusters.length)}
            warning={report.critical_clusters.length > 0}
          />
          <HealthFact
            label="未知"
            value={String(report.unknown_clusters.length)}
            warning={report.unknown_clusters.length > 0}
          />
          <HealthFact
            label="数据质量"
            value={qualityLabels[report.data_quality]}
          />
        </div>

        <div className="fleet-cluster-list">
          <div className="fleet-cluster-header">
            <span>集群</span>
            <span>状态</span>
            <span>分数</span>
            <span>关键 SLI</span>
          </div>
          {clusters.map((cluster) => (
            <div
              className={
                cluster.cluster_id === report.worst_cluster_id
                  ? "fleet-cluster-row worst"
                  : "fleet-cluster-row"
              }
              key={cluster.cluster_id}
            >
              <span>
                <strong>{cluster.external_cluster_key}</strong>
                <small>{cluster.region}</small>
              </span>
              <span>
                <HealthStatusBadge compact status={cluster.status} />
                {cluster.operational_state !== "normal" && (
                  <small>
                    {operationalLabels[cluster.operational_state]}
                  </small>
                )}
              </span>
              <strong>{formatScore(cluster.score)}</strong>
              <span>
                {cluster.triggered_sli_ids?.length
                  ? cluster.triggered_sli_ids.join(" · ")
                  : cluster.data_quality === "missing"
                    ? "数据缺失"
                    : "未触发"}
              </span>
            </div>
          ))}
          {clusters.length === 0 && (
            <div className="health-empty">
              当前租户或区域没有可聚合的集群。
            </div>
          )}
        </div>
      </div>

      <div className="fleet-aggregation-note">
        <AlertTriangle aria-hidden="true" size={15} />
        <span>
          聚合策略：<code>{report.aggregation}</code>。不会使用平均值掩盖严重集群；
          缺失数据时 Fleet 分数保持未知。
        </span>
      </div>
    </section>
  );
}

function HealthStatusBadge({
  status,
  compact = false,
}: {
  status: HealthStatus;
  compact?: boolean;
}) {
  return (
    <Badge
      className={compact ? "health-status-badge compact" : "health-status-badge"}
      variant={statusVariants[status]}
    >
      {statusLabels[status]}
    </Badge>
  );
}

function HealthFact({
  label,
  value,
  warning = false,
}: {
  label: string;
  value: string;
  warning?: boolean;
}) {
  return (
    <div className={warning ? "health-fact warning" : "health-fact"}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function formatScore(score?: number | null) {
  return score === null || score === undefined ? "—" : String(score);
}

function formatBurnRate(value?: number | null) {
  return value === null || value === undefined ? "—" : value.toFixed(1);
}

function formatDuration(seconds: number) {
  if (seconds % 86400 === 0) {
    return `${seconds / 86400}d`;
  }
  if (seconds % 3600 === 0) {
    return `${seconds / 3600}h`;
  }
  return `${seconds / 60}m`;
}

function formatTime(value: string) {
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

function statusRank(status: HealthStatus) {
  return {
    healthy: 0,
    unknown: 1,
    degraded: 2,
    critical: 3,
  }[status];
}
