import {
  AlertTriangle,
  Bot,
  CheckCircle2,
  Clock3,
  Coins,
  FileClock,
  Gauge,
  Hand,
  RefreshCw,
  ShieldCheck,
  Sparkles,
  TimerReset,
  TriangleAlert,
} from "lucide-react";
import {
  type ReactNode,
  useCallback,
  useState,
} from "react";

import type {
  AutonomyOperationalReport,
  AutonomyOutcome,
  AutonomyOutcomeClass,
  AutonomyReportPeriod,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import {
  DataState,
  DataSurface,
  formatTime,
} from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import { useAsyncResource } from "@/hooks/useAsyncResource";

const ALL_CLUSTERS = "all-clusters";
const ALL_OUTCOMES = "all-outcomes";

export function AutonomyOperationsPage() {
  const { api, clusters } = useSreData();
  const [period, setPeriod] =
    useState<AutonomyReportPeriod>("weekly");
  const [clusterId, setClusterId] = useState(ALL_CLUSTERS);
  const [outcomeClass, setOutcomeClass] = useState<
    AutonomyOutcomeClass | typeof ALL_OUTCOMES
  >(ALL_OUTCOMES);
  const effectiveClusterId =
    clusterId === ALL_CLUSTERS ? undefined : clusterId;
  const effectiveOutcomeClass =
    outcomeClass === ALL_OUTCOMES ? undefined : outcomeClass;
  const load = useCallback(
    async (signal: AbortSignal) => {
      const [report, outcomes] = await Promise.all([
        api.getAutonomyOperationalReport(
          {
            period,
            clusterId: effectiveClusterId,
          },
          signal,
        ),
        api.listAutonomyOutcomes(
          {
            clusterId: effectiveClusterId,
            class: effectiveOutcomeClass,
            limit: 100,
          },
          signal,
        ),
      ]);
      return { outcomes, report };
    },
    [
      api,
      effectiveClusterId,
      effectiveOutcomeClass,
      period,
    ],
  );
  const resource = useAsyncResource(load);
  const report = resource.data?.report;
  const outcomes = resource.data?.outcomes;
  const successRate = report
    ? ratioBasisPoints(
        report.outcomes.successes,
        report.outcomes.successes +
          report.outcomes.execution_failures,
      )
    : null;

  return (
    <div className="page autonomy-operations-page">
      <PageHeader
        eyebrow="Bounded autonomy / operations"
        title="自治运营与成本"
        description="用可追溯 Outcome 衡量自治质量、响应效率与模型成本；优化候选只能进入人工评审，不能在此直接发布。"
        actions={
          <div className="autonomy-page-controls">
            <Select
              value={period}
              onValueChange={(value) =>
                setPeriod(value as AutonomyReportPeriod)
              }
            >
              <SelectTrigger aria-label="报告周期">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="weekly">本周运营</SelectItem>
                <SelectItem value="monthly">本月运营</SelectItem>
              </SelectContent>
            </Select>
            <Select value={clusterId} onValueChange={setClusterId}>
              <SelectTrigger aria-label="集群范围">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL_CLUSTERS}>
                  全部授权集群
                </SelectItem>
                {clusters.map((cluster) => (
                  <SelectItem key={cluster.id} value={cluster.id}>
                    {cluster.external_cluster_key}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              aria-label="刷新自治运营数据"
              onClick={resource.reload}
              size="sm"
              variant="outline"
            >
              <RefreshCw
                className={resource.loading ? "spin" : undefined}
                size={14}
              />
              刷新
            </Button>
          </div>
        }
      />

      <section className="autonomy-operations-boundary">
        <ShieldCheck aria-hidden="true" size={20} />
        <div>
          <strong>运营观察面与生产发布面严格分离</strong>
          <p>
            报表、预算告警和优化候选均为只读结果；模型和周期任务不能修改
            Policy、ActionDescriptor、Provider 路由或自治状态。
          </p>
        </div>
        <Badge variant="success">cluster mutation = false</Badge>
        <Badge variant="outline">人工评审后发布</Badge>
      </section>

      <DataState
        empty={!resource.loading && !resource.error && !report}
        emptyDescription="当前授权范围没有可展示的自治运营报告。"
        emptyTitle="暂无自治运营数据"
        error={resource.error}
        loading={resource.loading && !report}
        onRetry={resource.reload}
      />

      {report && (
        <>
          <ReportWindow report={report} />
          <section
            aria-label="自治运营核心指标"
            className="autonomy-metric-grid"
          >
            <MetricCard
              detail={`${formatCount(report.outcomes.denied)} denied`}
              icon={<Gauge size={17} />}
              label="候选 / Eligible"
              value={`${formatCount(report.outcomes.candidates)} / ${formatCount(report.outcomes.eligible)}`}
            />
            <MetricCard
              detail={`${formatCount(report.outcomes.execution_failures)} execution failures`}
              icon={<CheckCircle2 size={17} />}
              label="执行成功率"
              tone="success"
              value={formatBasisPoints(successRate)}
            />
            <MetricCard
              detail={`${formatCount(report.outcomes.unknown_effects)} unknown`}
              icon={<TimerReset size={17} />}
              label="回滚 / 人工接管"
              tone={
                report.outcomes.unknown_effects > 0
                  ? "warning"
                  : "neutral"
              }
              value={`${formatCount(report.outcomes.rollbacks)} / ${formatCount(report.outcomes.human_handoffs)}`}
            />
            <MetricCard
              detail={`${formatCount(report.model_usage.calls)} model calls`}
              icon={<Coins size={17} />}
              label="已知模型成本"
              value={formatCost(report.model_usage.cost_micros)}
            />
            <MetricCard
              detail={report.savings.estimate_method}
              icon={<Clock3 size={17} />}
              label="保守节省工时"
              tone="success"
              value={formatMinutes(report.savings.estimated_minutes_saved)}
            />
            <MetricCard
              detail={`${formatCount(report.quality.raw_alert_occurrences)} raw alerts`}
              icon={<Sparkles size={17} />}
              label="告警降噪率"
              value={formatBasisPoints(
                report.quality.noise_reduction_basis_points,
              )}
            />
            <MetricCard
              detail={`${formatCount(report.quality.routed_incidents)} routed incidents`}
              icon={<Hand size={17} />}
              label="Owner 命中率"
              value={formatBasisPoints(
                report.quality.owner_routing_hit_basis_points,
              )}
            />
            <MetricCard
              detail={`${formatCount(report.quality.recurrent_incidents)} recurrent incidents`}
              icon={<TriangleAlert size={17} />}
              label="相同根因复发率"
              tone={
                (report.quality.recurrence_basis_points ?? 0) > 1_500
                  ? "warning"
                  : "neutral"
              }
              value={formatBasisPoints(
                report.quality.recurrence_basis_points,
              )}
            />
          </section>

          <Tabs
            className="autonomy-operations-tabs"
            defaultValue="effectiveness"
          >
            <TabsList aria-label="自治运营视图">
              <TabsTrigger value="effectiveness">
                质量与效率
              </TabsTrigger>
              <TabsTrigger value="cost">模型与成本</TabsTrigger>
              <TabsTrigger value="outcomes">
                Outcome 明细
              </TabsTrigger>
              <TabsTrigger value="candidates">
                人工候选
                {report.optimization_candidates.length > 0 && (
                  <span className="tab-count">
                    {report.optimization_candidates.length}
                  </span>
                )}
              </TabsTrigger>
            </TabsList>

            <TabsContent value="effectiveness">
              <EffectivenessView report={report} />
            </TabsContent>
            <TabsContent value="cost">
              <CostView report={report} />
            </TabsContent>
            <TabsContent value="outcomes">
              <OutcomeView
                items={outcomes?.items ?? []}
                outcomeClass={outcomeClass}
                setOutcomeClass={setOutcomeClass}
                truncated={outcomes?.truncated ?? false}
              />
            </TabsContent>
            <TabsContent value="candidates">
              <CandidateView report={report} />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
}

function ReportWindow({
  report,
}: {
  report: AutonomyOperationalReport;
}) {
  return (
    <div className="autonomy-report-window">
      <div>
        <FileClock aria-hidden="true" size={16} />
        <span>
          {report.window.period === "weekly" ? "周报" : "月报"} ·{" "}
          {formatDate(report.window.start)}—{formatDate(report.window.end)}
        </span>
      </div>
      <Badge variant={report.window.complete ? "success" : "info"}>
        {report.window.complete ? "周期已归档" : "周期进行中"}
      </Badge>
      <span>
        生成于 {formatTime(report.generated_at)} ·{" "}
        {report.cluster_ids.length} 个集群
      </span>
    </div>
  );
}

function MetricCard({
  detail,
  icon,
  label,
  tone = "neutral",
  value,
}: {
  detail: string;
  icon: ReactNode;
  label: string;
  tone?: "neutral" | "success" | "warning";
  value: string;
}) {
  return (
    <article className={`autonomy-metric-card tone-${tone}`}>
      <span className="autonomy-metric-icon">{icon}</span>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
        <small title={detail}>{detail}</small>
      </div>
    </article>
  );
}

function EffectivenessView({
  report,
}: {
  report: AutonomyOperationalReport;
}) {
  return (
    <div className="autonomy-effectiveness-layout">
      <div className="autonomy-effectiveness-column">
        <DataSurface
          description="缺少生命周期时间戳时保持 missing，不以 0 填充。"
          title="响应、诊断与恢复"
        >
          <div className="autonomy-stat-list">
            <StatRow
              detail={`${formatCount(report.durations.acknowledged_incidents)} incidents`}
              label="MTTA"
              value={formatDuration(
                report.durations.mean_time_to_acknowledge_seconds,
              )}
            />
            <StatRow
              detail={`${formatCount(report.durations.resolved_incidents)} incidents`}
              label="MTTR"
              value={formatDuration(
                report.durations.mean_time_to_resolve_seconds,
              )}
            />
            <StatRow
              detail={`${formatCount(report.durations.diagnosed_incidents)} diagnoses`}
              label="平均诊断耗时"
              value={formatDuration(
                report.durations.average_diagnosis_seconds,
              )}
            />
            <StatRow
              detail={`${formatCount(report.durations.completed_executions)} executions`}
              label="平均执行耗时"
              value={formatDuration(
                report.durations.average_execution_seconds,
              )}
            />
            <StatRow
              detail="从失败到验证恢复"
              label="平均恢复耗时"
              value={formatDuration(
                report.durations.average_recovery_seconds,
              )}
            />
          </div>
        </DataSurface>

        <DataSurface
          description="人工反馈不会直接改变生产规则。"
          title="采纳与人工反馈"
        >
          <div className="autonomy-stat-list">
            <StatRow
              detail={`${formatCount(report.feedback.adopted)} / ${formatCount(report.feedback.total)}`}
              label="采纳率"
              value={formatBasisPoints(
                report.feedback.adoption_basis_points,
              )}
            />
            <StatRow
              detail={`${formatCount(report.feedback.modified)} modified`}
              label="人工修改率"
              value={formatBasisPoints(
                report.feedback.modification_basis_points,
              )}
            />
            <StatRow
              detail={`${formatCount(report.feedback.rejected)} rejected`}
              label="拒绝率"
              value={formatBasisPoints(
                report.feedback.rejection_basis_points,
              )}
            />
            <StatRow
              detail={`${formatCount(report.quality.overdue_action_items)} overdue`}
              label="Action Item 逾期"
              value={formatCount(
                report.quality.overdue_action_items,
              )}
            />
            <StatRow
              detail={`${formatCount(report.quality.post_close_recurrences)} after close`}
              label="关闭后复发"
              value={formatCount(
                report.quality.post_close_recurrences,
              )}
            />
          </div>
        </DataSurface>
      </div>

      <DataSurface
        className="autonomy-action-surface"
        description="按 action、版本和集群拆分；Unknown 不并入 success。"
        meta={
          <Badge variant="outline">
            health Δ{" "}
            {formatDelta(report.quality.health_score_delta)}
          </Badge>
        }
        title="Action 成功与回滚"
      >
        <div className="table-scroll">
          <table className="autonomy-data-table">
            <thead>
              <tr>
                <th>Action / Cluster</th>
                <th>Candidate</th>
                <th>Eligible</th>
                <th>Success</th>
                <th>Failure</th>
                <th>Rollback</th>
                <th>Unknown</th>
                <th>平均执行</th>
              </tr>
            </thead>
            <tbody>
              {report.action_breakdown.map((item) => (
                <tr
                  key={`${item.cluster_id}:${item.action_id}:${item.action_version}`}
                >
                  <td>
                    <strong>{item.action_id}</strong>
                    <span>
                      {item.action_version} ·{" "}
                      {shortId(item.cluster_id)}
                    </span>
                  </td>
                  <td>{formatCount(item.outcomes.candidates)}</td>
                  <td>{formatCount(item.outcomes.eligible)}</td>
                  <td className="tone-success-text">
                    {formatCount(item.outcomes.successes)}
                  </td>
                  <td>
                    {formatCount(
                      item.outcomes.execution_failures,
                    )}
                  </td>
                  <td>{formatCount(item.outcomes.rollbacks)}</td>
                  <td>
                    {item.outcomes.unknown_effects > 0 ? (
                      <Badge variant="warning">
                        {item.outcomes.unknown_effects}
                      </Badge>
                    ) : (
                      "0"
                    )}
                  </td>
                  <td>
                    {formatDuration(item.average_execution_seconds)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </DataSurface>
    </div>
  );
}

function CostView({
  report,
}: {
  report: AutonomyOperationalReport;
}) {
  return (
    <div className="autonomy-cost-layout">
      <section className="autonomy-coverage-strip">
        <Bot aria-hidden="true" size={18} />
        <div>
          <strong>模型用量覆盖</strong>
          <span>
            Token{" "}
            {formatBasisPoints(
              report.model_usage.usage_coverage_basis_points,
            )}{" "}
            · 成本{" "}
            {formatBasisPoints(
              report.model_usage.cost_coverage_basis_points,
            )}
          </span>
        </div>
        <div>
          <span>
            {formatCount(report.model_usage.input_tokens)} input
          </span>
          <span>
            {formatCount(report.model_usage.output_tokens)} output
          </span>
        </div>
        <Badge
          variant={
            report.model_usage.calls_missing_cost > 0
              ? "warning"
              : "success"
          }
        >
          {report.model_usage.calls_missing_cost} calls 缺成本
        </Badge>
      </section>

      {report.budget_alerts.length > 0 && (
        <div className="autonomy-budget-alerts" role="status">
          {report.budget_alerts.map((alert) => (
            <article key={`${alert.scope_kind}:${alert.scope_id}`}>
              <AlertTriangle aria-hidden="true" size={17} />
              <div>
                <strong>
                  {alert.scope_kind} · {alert.scope_id} 超出预算
                </strong>
                <span>
                  {formatCost(alert.observed_cost_micros)} /{" "}
                  {formatCost(alert.budget_micros)} ·{" "}
                  {alert.recommended_degradation}
                </span>
              </div>
              <Badge variant="warning">
                自动路由变更：否
              </Badge>
            </article>
          ))}
        </div>
      )}

      <DataSurface
        description="仅汇总已记录成本；missing 调用保持显式可见。"
        title="Provider / Model 成本"
      >
        <div className="table-scroll">
          <table className="autonomy-data-table autonomy-cost-table">
            <thead>
              <tr>
                <th>Provider / Model</th>
                <th>Calls</th>
                <th>Tokens</th>
                <th>Failed</th>
                <th>Fallback</th>
                <th>已知成本</th>
                <th>成本覆盖</th>
              </tr>
            </thead>
            <tbody>
              {report.model_breakdown.map((item) => (
                <tr key={item.actual_profile_id}>
                  <td>
                    <strong>{item.provider_family}</strong>
                    <span>
                      {item.model_family} · {item.model_revision}
                    </span>
                  </td>
                  <td>{formatCount(item.usage.calls)}</td>
                  <td>
                    {formatCompact(
                      item.usage.input_tokens +
                        item.usage.output_tokens,
                    )}
                  </td>
                  <td>{formatCount(item.usage.failed_calls)}</td>
                  <td>{formatCount(item.usage.fallback_calls)}</td>
                  <td>{formatCost(item.usage.cost_micros)}</td>
                  <td>
                    <span>
                      {formatBasisPoints(
                        item.usage.cost_coverage_basis_points,
                      )}
                    </span>
                    {item.usage.calls_missing_cost > 0 && (
                      <small>
                        {item.usage.calls_missing_cost} missing
                      </small>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </DataSurface>

      <DataSurface
        description="比较 Pack、Action、Prompt、Model 和 Policy 的实际版本效果。"
        title="版本效果对比"
      >
        <div className="table-scroll">
          <table className="autonomy-data-table autonomy-version-table">
            <thead>
              <tr>
                <th>维度</th>
                <th>实际版本</th>
                <th>样本</th>
                <th>成功率</th>
                <th>已知成本</th>
              </tr>
            </thead>
            <tbody>
              {report.version_effects.map((item) => (
                <tr key={`${item.dimension}:${item.version}`}>
                  <td>
                    <Badge variant="outline">
                      {item.dimension}
                    </Badge>
                  </td>
                  <td>
                    <code>{item.version}</code>
                  </td>
                  <td>{formatCount(item.samples)}</td>
                  <td>
                    {formatBasisPoints(
                      item.success_basis_points,
                    )}
                  </td>
                  <td>{formatCost(item.cost_micros)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </DataSurface>

      {report.warnings.length > 0 && (
        <div className="autonomy-report-warnings">
          <TriangleAlert aria-hidden="true" size={17} />
          <div>
            <strong>数据完整性提示</strong>
            {report.warnings.map((warning) => (
              <span key={warning}>{warning}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function OutcomeView({
  items,
  outcomeClass,
  setOutcomeClass,
  truncated,
}: {
  items: AutonomyOutcome[];
  outcomeClass: AutonomyOutcomeClass | typeof ALL_OUTCOMES;
  setOutcomeClass: (
    value: AutonomyOutcomeClass | typeof ALL_OUTCOMES,
  ) => void;
  truncated: boolean;
}) {
  return (
    <DataSurface
      description="终态与拒绝结果按 occurred_at 倒序；计划 hash 和完整标识不在列表中展开。"
      meta={
        <Select
          value={outcomeClass}
          onValueChange={(value) =>
            setOutcomeClass(
              value as
                | AutonomyOutcomeClass
                | typeof ALL_OUTCOMES,
            )
          }
        >
          <SelectTrigger
            aria-label="Outcome 分类"
            className="autonomy-outcome-filter"
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL_OUTCOMES}>全部结果</SelectItem>
            <SelectItem value="success">成功</SelectItem>
            <SelectItem value="expected_deny">
              预期拒绝
            </SelectItem>
            <SelectItem value="autonomous_execution_failure">
              自治执行失败
            </SelectItem>
          </SelectContent>
        </Select>
      }
      title="最近 Outcome"
    >
      {truncated && (
        <div className="autonomy-inline-warning">
          <AlertTriangle size={14} />
          结果已按 API 上限截断，请缩小集群或分类范围。
        </div>
      )}
      {items.length === 0 ? (
        <div className="autonomy-empty-inline">
          当前筛选范围没有 Outcome。
        </div>
      ) : (
        <div className="table-scroll">
          <table className="autonomy-data-table autonomy-outcome-table">
            <thead>
              <tr>
                <th>时间</th>
                <th>Action</th>
                <th>结果</th>
                <th>原因 / Failure</th>
                <th>Incident</th>
                <th>Execution</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.id}>
                  <td>{formatTime(item.occurred_at)}</td>
                  <td>
                    <strong>{item.action}</strong>
                    <span>{item.action_version}</span>
                  </td>
                  <td>
                    <Badge variant={outcomeVariant(item.class)}>
                      {outcomeLabel(item.class)}
                    </Badge>
                  </td>
                  <td>
                    <strong>{item.failure ?? "—"}</strong>
                    <span>{item.reason_codes.join(" · ")}</span>
                  </td>
                  <td>
                    <code>{shortId(item.incident_id)}</code>
                  </td>
                  <td>
                    {item.execution_id ? (
                      <code>{shortId(item.execution_id)}</code>
                    ) : (
                      <span>未进入执行</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </DataSurface>
  );
}

function CandidateView({
  report,
}: {
  report: AutonomyOperationalReport;
}) {
  return (
    <div className="autonomy-candidate-layout">
      <section className="autonomy-review-boundary">
        <ShieldCheck aria-hidden="true" size={20} />
        <div>
          <strong>候选不等于发布</strong>
          <span>
            页面没有直接发布按钮；代码或配置变更必须进入独立人工评审流程。
          </span>
        </div>
        <Badge variant="success">publication_allowed = false</Badge>
      </section>

      {report.optimization_candidates.length === 0 ? (
        <div className="autonomy-empty-inline">
          当前周期没有需要人工评审的优化候选。
        </div>
      ) : (
        <div className="autonomy-candidate-grid">
          {report.optimization_candidates.map((candidate) => (
            <article key={candidate.id}>
              <header>
                <div>
                  <Badge variant="info">
                    {candidate.category}
                  </Badge>
                  <strong>{candidate.scope}</strong>
                </div>
                <Badge variant="warning">待人工评审</Badge>
              </header>
              <code>{candidate.reason_code}</code>
              <p>{candidate.evidence_summary}</p>
              <footer>
                <span>
                  requires_human_review=
                  {String(candidate.requires_human_review)}
                </span>
                <span>
                  publication_allowed=
                  {String(candidate.publication_allowed)}
                </span>
              </footer>
            </article>
          ))}
        </div>
      )}
    </div>
  );
}

function StatRow({
  detail,
  label,
  value,
}: {
  detail: string;
  label: string;
  value: string;
}) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function formatCount(value: number) {
  return new Intl.NumberFormat("zh-CN").format(value);
}

function formatCompact(value: number) {
  return new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: 1,
    notation: "compact",
  }).format(value);
}

function formatBasisPoints(value: number | null) {
  return value === null ? "缺失" : `${(value / 100).toFixed(1)}%`;
}

function ratioBasisPoints(numerator: number, denominator: number) {
  return denominator === 0
    ? null
    : Math.round((numerator * 10_000) / denominator);
}

function formatCost(micros: number) {
  return new Intl.NumberFormat("zh-CN", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(micros / 1_000_000);
}

function formatDuration(seconds: number | null) {
  if (seconds === null) {
    return "缺失";
  }
  if (seconds < 60) {
    return `${Math.round(seconds)} 秒`;
  }
  if (seconds < 3_600) {
    return `${(seconds / 60).toFixed(1)} 分`;
  }
  return `${(seconds / 3_600).toFixed(1)} 小时`;
}

function formatMinutes(minutes: number) {
  return minutes < 60
    ? `${minutes} 分钟`
    : `${(minutes / 60).toFixed(1)} 小时`;
}

function formatDate(value: string) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    timeZone: "UTC",
  }).format(new Date(value));
}

function formatDelta(value: number | null) {
  if (value === null) {
    return "missing";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}`;
}

function shortId(value: string) {
  return value.length > 12 ? `${value.slice(0, 8)}…` : value;
}

function outcomeLabel(value: AutonomyOutcomeClass) {
  switch (value) {
    case "success":
      return "成功";
    case "expected_deny":
      return "预期拒绝";
    case "autonomous_execution_failure":
      return "自治执行失败";
  }
}

function outcomeVariant(value: AutonomyOutcomeClass) {
  switch (value) {
    case "success":
      return "success" as const;
    case "expected_deny":
      return "warning" as const;
    case "autonomous_execution_failure":
      return "destructive" as const;
  }
}
