import {
  BarChart3,
  CheckCircle2,
  Clock3,
  Coins,
  Search,
  Sparkles,
} from "lucide-react";
import type { FormEvent, ReactNode } from "react";

import type { OperationsAnalyticsReport } from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export interface OperationsAnalyticsDraft {
  scenario: string;
  providerFamily: string;
  modelFamily: string;
  actionId: string;
}

interface OperationsAnalyticsPanelProps {
  draft: OperationsAnalyticsDraft;
  loading: boolean;
  onApply: () => void;
  onChange: (
    field: keyof OperationsAnalyticsDraft,
    value: string,
  ) => void;
  report?: OperationsAnalyticsReport;
}

export function OperationsAnalyticsPanel({
  draft,
  loading,
  onApply,
  onChange,
  report,
}: OperationsAnalyticsPanelProps) {
  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    onApply();
  };

  return (
    <section
      aria-label="多维运维分析"
      className="operations-analytics-panel"
    >
      <header>
        <div>
          <span className="operations-analytics-icon">
            <BarChart3 aria-hidden="true" size={17} />
          </span>
          <div>
            <strong>多维运维分析</strong>
            <p>
              租户范围来自登录身份，可按集群、场景、模型和动作交叉查询，
              不允许从查询参数切换租户。
            </p>
          </div>
        </div>
        <Badge variant="outline">read-only analytics</Badge>
      </header>

      <form className="operations-analytics-filters" onSubmit={submit}>
        <label>
          <span>场景 / symptom family</span>
          <Input
            aria-label="运维场景"
            maxLength={128}
            onChange={(event) =>
              onChange("scenario", event.target.value)
            }
            placeholder="consumer_lag"
            value={draft.scenario}
          />
        </label>
        <label>
          <span>Provider</span>
          <Input
            aria-label="模型 Provider"
            maxLength={128}
            onChange={(event) =>
              onChange("providerFamily", event.target.value)
            }
            placeholder="deepseek"
            value={draft.providerFamily}
          />
        </label>
        <label>
          <span>模型族</span>
          <Input
            aria-label="模型族"
            maxLength={128}
            onChange={(event) =>
              onChange("modelFamily", event.target.value)
            }
            placeholder="deepseek"
            value={draft.modelFamily}
          />
        </label>
        <label>
          <span>动作 ID</span>
          <Input
            aria-label="动作 ID"
            maxLength={128}
            onChange={(event) =>
              onChange("actionId", event.target.value)
            }
            placeholder="observability.logger_level_ttl.v1"
            value={draft.actionId}
          />
        </label>
        <Button disabled={loading} size="sm" type="submit">
          <Search aria-hidden="true" size={14} />
          应用维度
        </Button>
      </form>

      {!report ? (
        <div className="operations-analytics-empty">
          正在读取当前维度的运维指标…
        </div>
      ) : (
        <>
          <div className="operations-analytics-scope">
            <span>当前范围</span>
            <Badge variant="secondary">
              {report.filters.cluster_ids.length} 个集群
            </Badge>
            {dimensionBadges(report).map((value) => (
              <Badge key={value} variant="outline">
                {value}
              </Badge>
            ))}
          </div>

          <div className="operations-analytics-metrics">
            <AnalyticsMetric
              detail={report.mttd_definition}
              icon={<Clock3 size={15} />}
              label="MTTD"
              value={formatDuration(
                report.incidents.mean_time_to_detect_seconds,
              )}
            />
            <AnalyticsMetric
              detail={report.mttr_definition}
              icon={<Clock3 size={15} />}
              label="MTTR"
              value={formatDuration(
                report.incidents.mean_time_to_resolve_seconds,
              )}
            />
            <AnalyticsMetric
              detail={`${formatCount(report.model_usage.calls)} 次模型调用`}
              icon={<Sparkles size={15} />}
              label="Token"
              value={formatCount(
                report.model_usage.input_tokens +
                  report.model_usage.output_tokens,
              )}
            />
            <AnalyticsMetric
              detail={`${formatBasisPoints(report.model_usage.cost_coverage_basis_points)} 成本覆盖`}
              icon={<Coins size={15} />}
              label="模型费用"
              value={formatCost(report.model_usage.cost_micros)}
            />
            <AnalyticsMetric
              detail={`${formatCount(report.recommendation_feedback.total)} 条建议/计划反馈`}
              icon={<CheckCircle2 size={15} />}
              label="建议采纳率"
              value={formatBasisPoints(
                report.recommendation_feedback.adoption_basis_points,
              )}
            />
            <AnalyticsMetric
              detail={`${formatCount(report.executions.terminal)} 个终态执行`}
              icon={<CheckCircle2 size={15} />}
              label="执行成功率"
              value={formatBasisPoints(
                report.executions.success_basis_points,
              )}
            />
            <AnalyticsMetric
              detail={`${formatCount(report.savings.successful_autonomous_actions)} 个自治动作`}
              icon={<Clock3 size={15} />}
              label="自治节省工时"
              value={formatMinutes(
                report.savings.estimated_minutes_saved,
              )}
            />
          </div>

          {report.warnings.length > 0 && (
            <div className="operations-analytics-warnings">
              {report.warnings.map((warning) => (
                <span key={warning}>{warning}</span>
              ))}
            </div>
          )}
        </>
      )}
    </section>
  );
}

function AnalyticsMetric({
  detail,
  icon,
  label,
  value,
}: {
  detail: string;
  icon: ReactNode;
  label: string;
  value: string;
}) {
  return (
    <article>
      <span>{icon}</span>
      <div>
        <small>{label}</small>
        <strong>{value}</strong>
        <p title={detail}>{detail}</p>
      </div>
    </article>
  );
}

function dimensionBadges(report: OperationsAnalyticsReport) {
  return [
    report.filters.scenario
      ? `场景 ${report.filters.scenario}`
      : null,
    report.filters.provider_family
      ? `Provider ${report.filters.provider_family}`
      : null,
    report.filters.model_family
      ? `模型 ${report.filters.model_family}`
      : null,
    report.filters.action_id
      ? `动作 ${report.filters.action_id}`
      : null,
  ].filter((value): value is string => Boolean(value));
}

function formatDuration(value: number | null) {
  if (value === null) {
    return "缺少样本";
  }
  if (value < 60) {
    return `${Math.round(value)} 秒`;
  }
  if (value < 3_600) {
    return `${(value / 60).toFixed(1)} 分钟`;
  }
  return `${(value / 3_600).toFixed(1)} 小时`;
}

function formatCount(value: number) {
  return new Intl.NumberFormat("zh-CN").format(value);
}

function formatBasisPoints(value: number | null) {
  return value === null ? "缺少样本" : `${(value / 100).toFixed(1)}%`;
}

function formatCost(value: number) {
  return new Intl.NumberFormat("zh-CN", {
    style: "currency",
    currency: "USD",
  }).format(value / 1_000_000);
}

function formatMinutes(value: number) {
  if (value < 60) {
    return `${value} 分钟`;
  }
  return `${(value / 60).toFixed(1)} 小时`;
}
