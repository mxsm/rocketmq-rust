import {
  Activity,
  AlertTriangle,
  ArrowDownRight,
  ArrowRight,
  ArrowUpRight,
  CalendarClock,
  ChartNoAxesCombined,
  CircleGauge,
  ShieldCheck,
  TimerReset,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import type {
  ClusterForecastReport,
  ClusterSummary,
  DrReadinessReport,
  UpgradeReadinessReport,
  WhatIfSimulation,
  WhatIfSimulationRequest,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { useSreData } from "@/data/SreDataContext";
import {
  ReadinessTab,
  SimulationTab,
} from "@/pages/ForecastScenarioPanels";
import {
  forecastSummary,
  formatDate,
  formatNumber,
  formatPercent,
  formatRunway,
  formatSigned,
  statusLabel,
  windowLabel,
} from "@/pages/forecastFormat";

type Forecast = ClusterForecastReport["forecasts"][number];
type Backlog = ClusterForecastReport["backlog_etas"][number];

export function ForecastPage() {
  const { api, clusters, loading } = useSreData();
  const [clusterId, setClusterId] = useState("");
  const [report, setReport] = useState<ClusterForecastReport>();
  const [upgrade, setUpgrade] = useState<UpgradeReadinessReport>();
  const [dr, setDr] = useState<DrReadinessReport>();
  const [simulation, setSimulation] = useState<WhatIfSimulation>();
  const [simulationKind, setSimulationKind] =
    useState<WhatIfSimulationRequest["kind"]>("traffic_increase");
  const [trafficPercent, setTrafficPercent] = useState("50");
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!clusterId && clusters[0]) {
      setClusterId(clusters[0].id);
    }
  }, [clusterId, clusters]);

  const cluster = clusters.find((item) => item.id === clusterId);

  useEffect(() => {
    if (!cluster) {
      return;
    }
    const controller = new AbortController();
    setError(undefined);
    setReport(undefined);
    setUpgrade(undefined);
    setDr(undefined);
    setSimulation(undefined);
    void Promise.allSettled([
      api.getClusterForecasts(cluster.id, controller.signal),
      api.getUpgradeReadiness(
        cluster.id,
        cluster.rocketmq_version,
        controller.signal,
      ),
      api.getDrReadiness(cluster.id, cluster.region, controller.signal),
    ]).then(([forecastResult, upgradeResult, drResult]) => {
      if (forecastResult.status === "fulfilled") {
        setReport(forecastResult.value);
      } else if (!controller.signal.aborted) {
        setError(errorMessage(forecastResult.reason));
      }
      if (upgradeResult.status === "fulfilled") {
        setUpgrade(upgradeResult.value);
      }
      if (drResult.status === "fulfilled") {
        setDr(drResult.value);
      }
    });
    return () => controller.abort();
  }, [api, cluster]);

  const runSimulation = async () => {
    if (!cluster) {
      return;
    }
    setBusy(true);
    setError(undefined);
    const request: WhatIfSimulationRequest = {
      cluster_id: cluster.id,
      kind: simulationKind,
      traffic_increase_percent:
        simulationKind === "traffic_increase"
          ? Number(trafficPercent)
          : undefined,
      instance_delta:
        simulationKind === "broker_scale_out" ||
        simulationKind === "proxy_scale_out"
          ? 1
          : undefined,
      queue_delta:
        simulationKind === "topic_queue_expand" ? 4 : undefined,
      target_version:
        simulationKind === "version_upgrade"
          ? cluster.rocketmq_version
          : undefined,
      configuration_changes:
        simulationKind === "configuration_diff"
          ? ["brokerRole digest changed"]
          : [],
      affected_resource_keys: [],
      evidence_ids: [],
    };
    try {
      setSimulation(await api.runSimulation(request));
    } catch (cause) {
      setError(errorMessage(cause));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="page forecast-page">
      <PageHeader
        eyebrow="P2-06 · PREVENTION"
        title="容量预测与 Readiness"
        description="用可解释趋势、季节基线和确定性 What-if 提前识别容量、积压与到期风险。所有结果仅供诊断，不触发自动扩容或 RocketMQ 变更。"
        actions={
          <div className="forecast-header-actions">
            <Select value={clusterId} onValueChange={setClusterId}>
              <SelectTrigger
                aria-label="选择预测集群"
                className="cluster-select"
              >
                <SelectValue placeholder="选择集群" />
              </SelectTrigger>
              <SelectContent>
                {clusters.map((item) => (
                  <SelectItem key={item.id} value={item.id}>
                    {item.external_cluster_key} · {item.region}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Badge variant="outline">
              <ShieldCheck size={14} />
              advisory only
            </Badge>
          </div>
        }
      />

      {loading && !cluster ? (
        <div className="state-panel">正在加载集群范围…</div>
      ) : !cluster ? (
        <div className="state-panel">当前租户范围内没有可预测的集群。</div>
      ) : !report ? (
        <div className="state-panel">
          <ChartNoAxesCombined size={24} />
          <div>
            <strong>{error ?? "正在读取预测结果…"}</strong>
            <p>预测 worker 首次轮转后会显示 7 天和 30 天结果。</p>
          </div>
        </div>
      ) : (
        <ForecastWorkspace
          cluster={cluster}
          report={report}
          upgrade={upgrade}
          dr={dr}
          simulation={simulation}
          simulationKind={simulationKind}
          trafficPercent={trafficPercent}
          busy={busy}
          onSimulationKindChange={setSimulationKind}
          onTrafficPercentChange={setTrafficPercent}
          onRunSimulation={() => void runSimulation()}
        />
      )}
      {error && report && <div className="inline-alert">{error}</div>}
    </div>
  );
}

export function ForecastWorkspace({
  cluster,
  report,
  upgrade,
  dr,
  simulation,
  simulationKind,
  trafficPercent,
  busy,
  onSimulationKindChange,
  onTrafficPercentChange,
  onRunSimulation,
}: {
  cluster: ClusterSummary;
  report: ClusterForecastReport;
  upgrade?: UpgradeReadinessReport;
  dr?: DrReadinessReport;
  simulation?: WhatIfSimulation;
  simulationKind: WhatIfSimulationRequest["kind"];
  trafficPercent: string;
  busy: boolean;
  onSimulationKindChange: (
    value: WhatIfSimulationRequest["kind"],
  ) => void;
  onTrafficPercentChange: (value: string) => void;
  onRunSimulation: () => void;
}) {
  const summary = useMemo(() => forecastSummary(report), [report]);
  return (
    <>
      <section className="forecast-summary-grid" aria-label="预测摘要">
        <SummaryCard
          icon={CalendarClock}
          label="阈值风险"
          value={`${summary.thresholdRisks}`}
          detail="未来 30 天内预计到达阈值"
          tone={summary.thresholdRisks > 0 ? "warning" : "normal"}
        />
        <SummaryCard
          icon={TimerReset}
          label="可清空积压"
          value={`${summary.clearableBacklogs}/${report.backlog_etas.length}`}
          detail="已有净排空速率与 ETA"
        />
        <SummaryCard
          icon={Activity}
          label="季节异常"
          value={`${summary.anomalies}`}
          detail={`${report.change_points.length} 条非因果变点提示`}
          tone={summary.anomalies > 0 ? "warning" : "normal"}
        />
        <SummaryCard
          icon={CircleGauge}
          label="实际回测"
          value={formatPercent(summary.coverage)}
          detail={`MAE ${formatNumber(summary.mae)} · bias ${formatSigned(summary.bias)}`}
        />
      </section>

      {(report.partial || Boolean(report.warnings?.length)) && (
        <div className="forecast-quality-banner">
          <AlertTriangle size={17} />
          <div>
            <strong>部分预测暂不可用</strong>
            <span>
              {(report.warnings ?? []).join(" · ") ||
                "样本、覆盖率、波动或时效性未达到配置阈值。"}
            </span>
          </div>
        </div>
      )}

      <Tabs defaultValue="capacity">
        <TabsList className="forecast-tabs">
          <TabsTrigger value="capacity">Capacity / ETA</TabsTrigger>
          <TabsTrigger value="baseline">Baseline / 变点</TabsTrigger>
          <TabsTrigger value="simulation">What-if</TabsTrigger>
          <TabsTrigger value="readiness">Upgrade / DR</TabsTrigger>
        </TabsList>

        <TabsContent value="capacity">
          <CapacityTab report={report} />
        </TabsContent>
        <TabsContent value="baseline">
          <BaselineTab report={report} />
        </TabsContent>
        <TabsContent value="simulation">
          <SimulationTab
            cluster={cluster}
            result={simulation}
            kind={simulationKind}
            trafficPercent={trafficPercent}
            busy={busy}
            onKindChange={onSimulationKindChange}
            onTrafficPercentChange={onTrafficPercentChange}
            onRun={onRunSimulation}
          />
        </TabsContent>
        <TabsContent value="readiness">
          <ReadinessTab upgrade={upgrade} dr={dr} />
        </TabsContent>
      </Tabs>

      <div className="read-only-footnote">
        <ShieldCheck size={16} />
        <span>
          execution_eligible={String(report.execution_eligible)} ·
          预测模型不能创建执行请求，扩容、下线、队列扩展和容灾切换均需在外部受控流程完成。
        </span>
      </div>
    </>
  );
}

function CapacityTab({ report }: { report: ClusterForecastReport }) {
  return (
    <div className="forecast-tab-stack">
      <section className="data-surface">
        <div className="surface-heading">
          <div>
            <h2>容量与到期趋势</h2>
            <p>按资源展示 7d/30d 小时数据、斜率、波动、阈值和耗尽时间。</p>
          </div>
          <Badge variant="outline">{report.forecasts.length} forecasts</Badge>
        </div>
        <div className="forecast-table">
          <div className="forecast-row forecast-row-header">
            <span>资源 / 指标</span>
            <span>窗口 / 趋势</span>
            <span>覆盖 / 质量</span>
            <span>斜率 / 波动</span>
            <span>阈值时间</span>
            <span>趋势</span>
          </div>
          {report.forecasts.map((forecast) => (
            <ForecastRow forecast={forecast} key={forecast.id} />
          ))}
        </div>
      </section>

      <section className="data-surface">
        <div className="surface-heading">
          <div>
            <h2>Backlog ETA</h2>
            <p>Lag、Retry、DLQ、POP 与 Timer 的净增长或预计清空时间。</p>
          </div>
        </div>
        <div className="backlog-grid">
          {report.backlog_etas.map((backlog) => (
            <BacklogCard backlog={backlog} key={backlog.id} />
          ))}
          {report.backlog_etas.length === 0 && (
            <div className="empty-state compact">暂无积压预测。</div>
          )}
        </div>
      </section>

      <AccuracyTable report={report} />
    </div>
  );
}

function ForecastRow({ forecast }: { forecast: Forecast }) {
  return (
    <div className="forecast-row">
      <div className="forecast-resource">
        <strong>
          {forecast.resource.display_name ?? forecast.resource.key}
        </strong>
        <code>{forecast.metric}</code>
      </div>
      <div>
        <strong>{windowLabel(forecast.window)}</strong>
        <TrendBadge trend={forecast.trend} />
      </div>
      <div>
        <strong>{formatPercent(forecast.coverage_ratio)}</strong>
        <small>
          {forecast.quality} · {statusLabel(forecast.status)}
        </small>
      </div>
      <div>
        <strong>{formatSigned(forecast.slope_per_hour)}</strong>
        <small>σ {formatNumber(forecast.volatility)}</small>
      </div>
      <div>
        <strong>{formatRunway(forecast.exhaustion_at)}</strong>
        <small>threshold {formatNumber(forecast.threshold)}</small>
      </div>
      <Sparkline points={forecast.points} />
    </div>
  );
}

function BacklogCard({ backlog }: { backlog: Backlog }) {
  return (
    <article className="backlog-card">
      <div>
        <span>{backlog.backlog_kind}</span>
        <TrendBadge trend={backlog.trend} />
      </div>
      <strong>{formatNumber(backlog.current_value)}</strong>
      <dl>
        <div>
          <dt>净斜率 / h</dt>
          <dd>{formatSigned(backlog.slope_per_hour)}</dd>
        </div>
        <div>
          <dt>覆盖率</dt>
          <dd>{formatPercent(backlog.coverage_ratio)}</dd>
        </div>
        <div>
          <dt>预计清空</dt>
          <dd>{formatRunway(backlog.estimated_clear_at)}</dd>
        </div>
      </dl>
      {backlog.status !== "ready" && (
        <p className="insufficient-note">
          insufficient_data：不生成排空结论
        </p>
      )}
    </article>
  );
}

function AccuracyTable({ report }: { report: ClusterForecastReport }) {
  const rows =
    report.accuracy.length > 0
      ? report.accuracy
      : report.forecasts.map((forecast) => ({
          metric: forecast.metric,
          window: forecast.window,
          ...forecast.backtest,
          observed_at: forecast.observed_at,
        }));
  return (
    <section className="data-surface">
      <div className="surface-heading">
        <div>
          <h2>预测回测</h2>
          <p>优先展示已保存实际结果；尚未命中未来点时展示固定尾部留出集。</p>
        </div>
      </div>
      <div className="accuracy-grid">
        {rows.slice(0, 8).map((row) => (
          <article key={`${row.metric}-${row.window}`}>
            <code>{row.metric}</code>
            <span>{windowLabel(row.window)}</span>
            <strong>MAE {formatNumber(row.mean_absolute_error)}</strong>
            <small>
              bias {formatSigned(row.bias)} · coverage{" "}
              {formatPercent(row.interval_coverage_ratio)}
            </small>
          </article>
        ))}
      </div>
    </section>
  );
}

function BaselineTab({ report }: { report: ClusterForecastReport }) {
  return (
    <div className="forecast-tab-stack">
      <section className="data-surface">
        <div className="surface-heading">
          <div>
            <h2>季节性异常</h2>
            <p>
              小时、日、周 baseline 使用 median/MAD、robust z-score 与经验分位数。
            </p>
          </div>
          <Badge variant="outline">
            {report.baselines.length} baselines
          </Badge>
        </div>
        <div className="anomaly-grid">
          {report.anomalies.map((anomaly) => (
            <article
              className={anomaly.anomaly ? "anomaly-card active" : "anomaly-card"}
              key={`${anomaly.metric}-${anomaly.seasonality}`}
            >
              <div>
                <Badge variant={anomaly.anomaly ? "destructive" : "outline"}>
                  {anomaly.anomaly ? "ANOMALY" : "NORMAL"}
                </Badge>
                <span>{anomaly.seasonality}</span>
              </div>
              <strong>
                {anomaly.resource.display_name ?? anomaly.resource.key}
              </strong>
              <code>{anomaly.metric}</code>
              <dl>
                <div>
                  <dt>observed</dt>
                  <dd>{formatNumber(anomaly.observed_value)}</dd>
                </div>
                <div>
                  <dt>median</dt>
                  <dd>{formatNumber(anomaly.baseline_median)}</dd>
                </div>
                <div>
                  <dt>robust z</dt>
                  <dd>{formatSigned(anomaly.robust_z_score)}</dd>
                </div>
                <div>
                  <dt>quantile</dt>
                  <dd>{formatPercent(anomaly.empirical_quantile)}</dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      </section>

      <section className="data-surface">
        <div className="surface-heading">
          <div>
            <h2>变点调查提示</h2>
            <p>变点不直接判定根因，只提示需要关联部署、配置和事件时间线。</p>
          </div>
        </div>
        <div className="change-point-list">
          {report.change_points.map((point) => (
            <article key={point.id}>
              <span className="change-point-marker" />
              <div>
                <strong>
                  {point.resource.display_name ?? point.resource.key}
                </strong>
                <code>{point.metric}</code>
              </div>
              <div>
                <span>{formatNumber(point.before_value)}</span>
                <ArrowRight size={15} />
                <strong>{formatNumber(point.after_value)}</strong>
              </div>
              <div>
                <strong>score {point.score.toFixed(2)}</strong>
                <small>{formatDate(point.detected_at)}</small>
              </div>
            </article>
          ))}
          {report.change_points.length === 0 && (
            <div className="empty-state compact">未发现稳定变点。</div>
          )}
        </div>
      </section>
    </div>
  );
}

function SummaryCard({
  icon: Icon,
  label,
  value,
  detail,
  tone = "normal",
}: {
  icon: typeof CalendarClock;
  label: string;
  value: string;
  detail: string;
  tone?: "normal" | "warning";
}) {
  return (
    <article className={`forecast-summary-card ${tone}`}>
      <span className="forecast-summary-icon">
        <Icon size={18} />
      </span>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
        <small>{detail}</small>
      </div>
    </article>
  );
}

function TrendBadge({ trend }: { trend: Forecast["trend"] }) {
  const Icon =
    trend === "increasing"
      ? ArrowUpRight
      : trend === "decreasing"
        ? ArrowDownRight
        : ArrowRight;
  return (
    <span className={`trend-badge ${trend}`}>
      <Icon size={13} />
      {trend}
    </span>
  );
}

function Sparkline({ points }: { points: Forecast["points"] }) {
  const values = points.map((point) => point.value).filter(Number.isFinite);
  if (values.length < 2) {
    return <span className="sparkline-empty">insufficient</span>;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, Number.EPSILON);
  const path = values
    .map((value, index) => {
      const x = (index / (values.length - 1)) * 116 + 2;
      const y = 34 - ((value - min) / range) * 30;
      return `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const split = points.findIndex((point) => point.projected);
  return (
    <svg
      aria-label="观测与预测趋势"
      className="forecast-sparkline"
      role="img"
      viewBox="0 0 120 38"
    >
      <path d={path} />
      {split > 0 && (
        <line
          className="projection-boundary"
          x1={(split / (values.length - 1)) * 116 + 2}
          x2={(split / (values.length - 1)) * 116 + 2}
          y1="2"
          y2="36"
        />
      )}
    </svg>
  );
}

function errorMessage(cause: unknown) {
  return cause instanceof Error ? cause.message : "预测数据暂不可用";
}
