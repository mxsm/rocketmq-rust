import { ArrowRight, FlaskConical } from "lucide-react";

import type {
  ClusterSummary,
  DrReadinessReport,
  UpgradeReadinessReport,
  WhatIfSimulation,
  WhatIfSimulationRequest,
} from "@/api/types";
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
  formatDate,
  formatPercent,
  readinessVariant,
} from "@/pages/forecastFormat";

export function SimulationTab({
  cluster,
  result,
  kind,
  trafficPercent,
  busy,
  onKindChange,
  onTrafficPercentChange,
  onRun,
}: {
  cluster: ClusterSummary;
  result?: WhatIfSimulation;
  kind: WhatIfSimulationRequest["kind"];
  trafficPercent: string;
  busy: boolean;
  onKindChange: (value: WhatIfSimulationRequest["kind"]) => void;
  onTrafficPercentChange: (value: string) => void;
  onRun: () => void;
}) {
  const utilization = result?.projected_utilization as
    | { current?: number; projected?: number }
    | undefined;
  return (
    <div className="simulation-layout">
      <section className="data-surface simulation-config">
        <div className="surface-heading">
          <div>
            <h2>确定性场景</h2>
            <p>使用当前 Inventory、容量与依赖图，不调用模型自由推断。</p>
          </div>
          <FlaskConical size={20} />
        </div>
        <div className="simulation-form-grid">
          <label>
            <span>场景</span>
            <Select
              value={kind}
              onValueChange={(value) =>
                onKindChange(value as WhatIfSimulationRequest["kind"])
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="broker_offline">下线一个 Broker</SelectItem>
                <SelectItem value="proxy_offline">下线一个 Proxy</SelectItem>
                <SelectItem value="traffic_increase">流量增长</SelectItem>
                <SelectItem value="broker_scale_out">Broker 扩容</SelectItem>
                <SelectItem value="proxy_scale_out">Proxy 扩容</SelectItem>
                <SelectItem value="topic_queue_expand">
                  Topic Queue expand-only
                </SelectItem>
                <SelectItem value="version_upgrade">版本升级</SelectItem>
                <SelectItem value="configuration_diff">
                  已知配置 diff
                </SelectItem>
              </SelectContent>
            </Select>
          </label>
          {kind === "traffic_increase" && (
            <label>
              <span>流量增幅</span>
              <Select
                value={trafficPercent}
                onValueChange={onTrafficPercentChange}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="25">+25%</SelectItem>
                  <SelectItem value="50">+50%</SelectItem>
                  <SelectItem value="100">+100%</SelectItem>
                </SelectContent>
              </Select>
            </label>
          )}
          <div className="simulation-context">
            <span>集群</span>
            <strong>{cluster.external_cluster_key}</strong>
            <small>
              {cluster.region} · {cluster.deployment_mode}
            </small>
          </div>
        </div>
        <Button disabled={busy} onClick={onRun}>
          <FlaskConical size={15} />
          {busy ? "正在计算…" : "运行只读模拟"}
        </Button>
      </section>

      <section className="data-surface simulation-result">
        <div className="surface-heading">
          <div>
            <h2>模拟结果</h2>
            <p>输出预计利用率、瓶颈、blast radius 和缺失假设。</p>
          </div>
          <Badge
            variant={result?.status === "completed" ? "success" : "outline"}
          >
            {result?.status ?? "WAITING"}
          </Badge>
        </div>
        {result ? (
          <>
            <div className="utilization-comparison">
              <div>
                <span>当前利用率</span>
                <strong>{formatPercent(utilization?.current)}</strong>
              </div>
              <ArrowRight size={21} />
              <div>
                <span>预计利用率</span>
                <strong>{formatPercent(utilization?.projected)}</strong>
              </div>
            </div>
            <ResultList title="Bottleneck" values={result.bottlenecks} />
            <ResultList title="Blast radius" values={result.blast_radius} />
            <ResultList
              title="缺失假设"
              values={result.missing_assumptions}
            />
            <Badge variant="outline">
              execution_eligible={String(result.execution_eligible)}
            </Badge>
          </>
        ) : (
          <div className="empty-state compact">
            选择场景并运行后显示结果；不会创建执行请求。
          </div>
        )}
      </section>
    </div>
  );
}

export function ReadinessTab({
  upgrade,
  dr,
}: {
  upgrade?: UpgradeReadinessReport;
  dr?: DrReadinessReport;
}) {
  return (
    <div className="readiness-grid">
      <ReadinessCard
        title="Upgrade Readiness"
        subtitle={
          upgrade ? `目标 ${upgrade.target_version}` : "正在读取升级准备度"
        }
        report={upgrade}
      />
      <ReadinessCard
        title="DR Readiness"
        subtitle={
          dr?.target_region ? `目标区域 ${dr.target_region}` : "正在读取容灾准备度"
        }
        report={dr}
      />
    </div>
  );
}

function ReadinessCard({
  title,
  subtitle,
  report,
}: {
  title: string;
  subtitle: string;
  report?: UpgradeReadinessReport | DrReadinessReport;
}) {
  return (
    <section className="data-surface readiness-card">
      <div className="surface-heading">
        <div>
          <h2>{title}</h2>
          <p>{subtitle}</p>
        </div>
        <Badge variant={readinessVariant(report?.status)}>
          {report?.status ?? "LOADING"}
        </Badge>
      </div>
      <div className="readiness-findings">
        {report?.findings.map((finding) => (
          <article key={finding.code}>
            <span className={`finding-severity ${finding.severity}`} />
            <div>
              <strong>{finding.summary}</strong>
              <code>{finding.code}</code>
            </div>
            <div>
              <span>{finding.component}</span>
              <small>{finding.evidence_ids.length} evidence</small>
            </div>
          </article>
        ))}
        {report && report.findings.length === 0 && (
          <div className="empty-state compact">
            当前检查项均已满足；报告仍不执行升级或切换。
          </div>
        )}
      </div>
      {report && (
        <div className="readiness-footer">
          <span>有效期至 {formatDate(report.expires_at)}</span>
          <Badge variant="outline">
            execution_eligible={String(report.execution_eligible)}
          </Badge>
        </div>
      )}
    </section>
  );
}

function ResultList({
  title,
  values,
}: {
  title: string;
  values: string[];
}) {
  return (
    <div className="simulation-result-list">
      <span>{title}</span>
      {values.length ? (
        <div>
          {values.slice(0, 8).map((value) => (
            <Badge key={value} variant="outline">
              {value}
            </Badge>
          ))}
        </div>
      ) : (
        <strong>无</strong>
      )}
    </div>
  );
}
