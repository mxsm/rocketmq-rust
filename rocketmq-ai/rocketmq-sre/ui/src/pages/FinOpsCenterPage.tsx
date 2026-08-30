import {
  Banknote,
  ChartNoAxesCombined,
  CircleDollarSign,
  Gauge,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";
import { useMemo } from "react";
import { Link } from "react-router-dom";

import {
  EnterpriseBoundary,
  EnterpriseMetric,
  EnterpriseScopeBar,
  EnterpriseStatus,
} from "@/components/EnterprisePrimitives";
import { PageHeader } from "@/components/PageHeader";
import { DataState, DataSurface } from "@/components/Phase1Primitives";
import { exportCsv, useEnterpriseData } from "@/hooks/useEnterpriseData";

export function FinOpsCenterPage() {
  const resource = useEnterpriseData();
  const snapshot = resource.data;
  const report = snapshot?.finops;
  const rows = useMemo(() => {
    const search = resource.filters.search.trim().toLowerCase();
    return (report?.rows ?? []).filter(
      (row) =>
        (!resource.filters.region ||
          row.dimensions.region_id === resource.filters.region) &&
        (!search ||
          Object.values(row.dimensions).some((value) =>
            value.toLowerCase().includes(search),
          )),
    );
  }, [report, resource.filters.region, resource.filters.search]);
  const totalTokens = rows.reduce(
    (sum, row) => sum + row.input_tokens + row.output_tokens,
    0,
  );
  const successfulOutcomes = rows.reduce(
    (sum, row) => sum + row.successful_outcomes,
    0,
  );
  const minutesSaved = rows.reduce(
    (sum, row) => sum + row.estimated_minutes_saved,
    0,
  );
  const forecast = report?.forecasts.at(0);

  const download = () => {
    exportCsv(
      `rocketmq-finops-${new Date().toISOString().slice(0, 10)}.csv`,
      [
        "region",
        "cluster",
        "provider",
        "model",
        "workload",
        "requests",
        "input_tokens",
        "output_tokens",
        "errors",
        "latency_ms",
        "cost_usd",
        "successful_outcomes",
        "minutes_saved",
      ],
      rows.map((row) => [
        row.dimensions.region_id,
        row.dimensions.cluster_id,
        row.dimensions.provider,
        row.dimensions.model,
        row.dimensions.workload,
        row.request_count,
        row.input_tokens,
        row.output_tokens,
        row.error_count,
        row.average_latency_millis,
        microsToUsd(row.cost_micros),
        row.successful_outcomes,
        row.estimated_minutes_saved,
      ]),
    );
  };

  return (
    <div className="page enterprise-page finops-center-page">
      <PageHeader
        actions={
          <EnterpriseBoundary>
            预算只可降级低优先级模型工作；Safety、Audit、Verification 与
            Rollback 永不因成本被禁用。
          </EnterpriseBoundary>
        }
        description="把 Provider/Model Token、基础设施成本、质量、结果、SLO 覆盖与节省工时放在同一 showback 视图；缺失归因不会被伪造成 0。"
        eyebrow="PLATFORM / MODEL & FINOPS"
        title="模型成本与 FinOps"
      />

      <DataState
        empty={!resource.loading && !report}
        error={resource.error}
        loading={resource.loading && !report}
        onRetry={resource.reload}
      />

      {snapshot && report && (
        <>
          <section className="enterprise-metric-grid">
            <EnterpriseMetric
              detail={`${report.ledger_entries.toLocaleString()} ledger entries`}
              icon={<CircleDollarSign size={18} />}
              label="总成本"
              value={formatUsd(report.total_cost_micros)}
            />
            <EnterpriseMetric
              detail={`${formatCompact(totalTokens)} tokens`}
              icon={<ChartNoAxesCombined size={18} />}
              label="模型请求"
              value={formatCompact(
                rows.reduce((sum, row) => sum + row.request_count, 0),
              )}
            />
            <EnterpriseMetric
              detail={`${minutesSaved.toLocaleString()} minutes saved`}
              icon={<Gauge size={18} />}
              label="成功结果"
              tone="success"
              value={successfulOutcomes.toLocaleString()}
            />
            <EnterpriseMetric
              detail={`${report.entries_missing_cost} entries missing cost`}
              icon={<Banknote size={18} />}
              label="成本覆盖"
              tone={
                (report.cost_coverage_basis_points ?? 0) >= 9_900
                  ? "success"
                  : "warning"
              }
              value={
                report.cost_coverage_basis_points === undefined
                  ? "missing"
                  : `${(report.cost_coverage_basis_points / 100).toFixed(2)}%`
              }
            />
          </section>

          <EnterpriseScopeBar
            filters={resource.filters}
            onExport={download}
            onFilter={resource.setFilter}
            onReset={resource.resetFilters}
            owners={[]}
            regions={snapshot.fleet.regions.map((region) => ({
              id: region.id,
              label: region.display_name,
            }))}
            showHealth={false}
          />

          <section className="finops-budget-strip">
            <div>
              <span>ALLOCATION MODE</span>
              <strong>{report.allocation_mode}</strong>
              <small>
                chargeback {report.chargeback_enabled ? "enabled" : "disabled"}
              </small>
            </div>
            <div>
              <span>PERIOD FORECAST</span>
              <strong>
                {forecast ? formatUsd(forecast.projected_cost_micros) : "missing"}
              </strong>
              <small>
                {forecast
                  ? `${(
                      forecast.coverage_basis_points / 100
                    ).toFixed(2)}% coverage`
                  : "no forecast sample"}
              </small>
            </div>
            <div>
              <span>HARD LIMIT</span>
              <strong>
                {forecast ? formatUsd(forecast.hard_limit_micros) : "missing"}
              </strong>
              <small>
                {forecast?.projected_over_budget
                  ? "projected over budget"
                  : "within bounded envelope"}
              </small>
            </div>
            <div className="finops-protected-paths">
              <span>
                <ShieldCheck size={14} />
                PROTECTED CONTROLS
              </span>
              <strong>Safety · Audit · Verify · Rollback</strong>
              <small>degradation=none · fail closed</small>
            </div>
          </section>

          <section className="enterprise-split-grid finops-primary-grid">
            <DataSurface
              className="finops-showback-surface"
              description="维度可按 Region、Cluster、Provider、Model 和 Workload 导出。"
              meta={<span>{rows.length} allocation rows</span>}
              title="Cost / Quality / Outcome"
            >
              <div className="enterprise-table-scroll">
                <table className="enterprise-table finops-table">
                  <thead>
                    <tr>
                      <th>Provider / Model</th>
                      <th>Cluster / Workload</th>
                      <th>Tokens</th>
                      <th>Latency</th>
                      <th>Outcome</th>
                      <th>Cost</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => (
                      <tr
                        key={`${row.dimensions.cluster_id}:${row.dimensions.provider}:${row.dimensions.workload}`}
                      >
                        <td>
                          <strong>{row.dimensions.provider}</strong>
                          <small>{row.dimensions.model}</small>
                        </td>
                        <td>
                          <Link
                            to={`/clusters/${encodeURIComponent(row.dimensions.cluster_id)}`}
                          >
                            {shortId(row.dimensions.cluster_id)}
                          </Link>
                          <small>{row.dimensions.workload}</small>
                        </td>
                        <td>
                          <strong>
                            {formatCompact(
                              row.input_tokens + row.output_tokens,
                            )}
                          </strong>
                          <small>
                            {formatCompact(row.input_tokens)} in /{" "}
                            {formatCompact(row.output_tokens)} out
                          </small>
                        </td>
                        <td>
                          <strong>
                            {row.average_latency_millis ?? "missing"} ms
                          </strong>
                          <small>{row.error_count} errors</small>
                        </td>
                        <td>
                          <strong>{row.successful_outcomes}</strong>
                          <small>
                            {row.estimated_minutes_saved} min saved
                          </small>
                        </td>
                        <td>
                          <strong>{formatUsd(row.cost_micros)}</strong>
                          <small>{row.request_count} requests</small>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </DataSurface>

            <div className="finops-side-stack">
              <DataSurface
                description="简单窗口对比，不把异常直接解释为根因。"
                title="Cost anomalies"
              >
                <div className="finops-anomaly-list">
                  {report.anomalies.map((anomaly) => (
                    <article key={`${anomaly.scope_kind}:${anomaly.scope_key}`}>
                      <header>
                        <span>
                          <TriangleAlert size={14} />
                          {anomaly.reason_code.replaceAll("_", " ")}
                        </span>
                        <EnterpriseStatus value="warning" />
                      </header>
                      <strong>{shortId(anomaly.scope_key)}</strong>
                      <div>
                        <span>{formatUsd(anomaly.current_cost_micros)}</span>
                        <small>
                          baseline {formatUsd(anomaly.baseline_cost_micros)}
                        </small>
                      </div>
                      <footer>
                        {anomaly.change_basis_points === undefined
                          ? "change missing"
                          : `+${(
                              anomaly.change_basis_points / 100
                            ).toFixed(2)}%`}
                      </footer>
                    </article>
                  ))}
                </div>
              </DataSurface>

              <DataSurface
                description="Coverage 缺口保留原始 warning，禁止补零。"
                title="Attribution warnings"
              >
                <div className="finops-warning-list">
                  {report.warnings.map((warning) => (
                    <div key={warning}>
                      <TriangleAlert size={14} />
                      <span>{warning}</span>
                    </div>
                  ))}
                  {report.warnings.length === 0 && (
                    <div>
                      <ShieldCheck size={14} />
                      <span>当前报告没有归因告警。</span>
                    </div>
                  )}
                </div>
              </DataSurface>
            </div>
          </section>
        </>
      )}
    </div>
  );
}

function formatUsd(micros: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(micros / 1_000_000);
}

function microsToUsd(micros: number) {
  return (micros / 1_000_000).toFixed(6);
}

function formatCompact(value: number) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function shortId(value: string) {
  return value.length > 16
    ? `${value.slice(0, 8)}…${value.slice(-5)}`
    : value;
}
