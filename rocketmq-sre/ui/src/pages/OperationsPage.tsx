import {
  AlarmClockCheck,
  AlertTriangle,
  ArrowUpRight,
  CalendarDays,
  CheckCircle2,
  ClipboardCheck,
  Download,
  FileClock,
  GitMerge,
  LoaderCircle,
  RotateCcw,
  ShieldCheck,
  Split,
  UserRoundCheck,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import type {
  IncidentOperationRequest,
  IncidentOperationsState,
  OperationsReport,
  OperationsReportWindow,
  ShiftHandoffSummary,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
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

type Finding = ShiftHandoffSummary["unresolved_incidents"][number];
type OperationKind = IncidentOperationRequest["action"];

const operationLabels: Record<OperationKind, string> = {
  acknowledge: "确认事件",
  assign: "分派 Owner",
  merge: "合并事件",
  split: "拆分事件",
  suppress: "限时抑制",
  reopen: "重开终态事件",
};

export function OperationsPage() {
  const { api, clusters, loading } = useSreData();
  const [clusterId, setClusterId] = useState("all");
  const [handoff, setHandoff] = useState<ShiftHandoffSummary>();
  const [daily, setDaily] = useState<OperationsReport>();
  const [weekly, setWeekly] = useState<OperationsReport>();
  const [reportWindow, setReportWindow] =
    useState<OperationsReportWindow>("daily");
  const [error, setError] = useState<string>();
  const [refreshToken, setRefreshToken] = useState(0);
  const requestedCluster = clusterId === "all" ? undefined : clusterId;

  useEffect(() => {
    const controller = new AbortController();
    setError(undefined);
    void Promise.all([
      api.getShiftHandoff(requestedCluster, controller.signal),
      api.getOperationsReport(
        "daily",
        requestedCluster,
        controller.signal,
      ),
      api.getOperationsReport(
        "weekly",
        requestedCluster,
        controller.signal,
      ),
    ])
      .then(([nextHandoff, nextDaily, nextWeekly]) => {
        setHandoff(nextHandoff);
        setDaily(nextDaily);
        setWeekly(nextWeekly);
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error
              ? cause.message
              : "运营摘要暂不可用。",
          );
        }
      });
    return () => controller.abort();
  }, [api, refreshToken, requestedCluster]);

  const report = reportWindow === "daily" ? daily : weekly;

  const downloadReport = async (format: "markdown" | "html") => {
    setError(undefined);
    try {
      const blob = await api.downloadOperationsReport(
        reportWindow,
        format,
        requestedCluster,
      );
      const href = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = href;
      anchor.download = `rocketmq-sre-${reportWindow}-operations.${
        format === "markdown" ? "md" : "html"
      }`;
      anchor.click();
      URL.revokeObjectURL(href);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "运营报告下载失败。",
      );
    }
  };

  return (
    <div className="page operations-page">
      <PageHeader
        eyebrow="P2-11 · DAILY OPERATIONS"
        title="值班运营与预防性工作台"
        description="汇总交接班风险、日报/周报和 Incident 元数据操作。集群资源始终只读；确认、分派、抑制、合并与拆分只写入 SRE 审计域。"
        actions={
          <div className="operations-header-actions">
            <Select value={clusterId} onValueChange={setClusterId}>
              <SelectTrigger
                aria-label="运营集群范围"
                className="cluster-select"
              >
                <SelectValue placeholder="全部授权集群" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部授权集群</SelectItem>
                {clusters.map((cluster) => (
                  <SelectItem key={cluster.id} value={cluster.id}>
                    {cluster.external_cluster_key} · {cluster.region}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Badge variant="outline">
              <ShieldCheck size={14} />
              cluster mutations = 0
            </Badge>
          </div>
        }
      />

      {loading && !handoff ? (
        <div className="state-panel" role="status">
          <LoaderCircle className="spin" size={24} />
          正在生成交接班摘要…
        </div>
      ) : error && !handoff ? (
        <div className="state-panel unavailable" role="alert">
          <AlertTriangle size={24} />
          <div>
            <strong>运营数据暂不可用</strong>
            <span>{error}</span>
          </div>
        </div>
      ) : handoff ? (
        <>
          <OperationsSummary handoff={handoff} report={report} />
          {(handoff.partial || report?.partial) && (
            <div className="forecast-quality-banner">
              <AlertTriangle size={17} />
              <div>
                <strong>报告包含部分结果</strong>
                <span>
                  {[...handoff.warnings, ...(report?.warnings ?? [])].join(
                    " · ",
                  )}
                </span>
              </div>
            </div>
          )}

          <Tabs defaultValue="handoff">
            <TabsList className="operations-tabs">
              <TabsTrigger value="handoff">交接班</TabsTrigger>
              <TabsTrigger value="reports">日报 / 周报</TabsTrigger>
              <TabsTrigger value="incident-ops">Incident 运营</TabsTrigger>
            </TabsList>
            <TabsContent value="handoff">
              <HandoffWorkspace handoff={handoff} />
            </TabsContent>
            <TabsContent value="reports">
              <ReportWorkspace
                report={report}
                window={reportWindow}
                onWindowChange={setReportWindow}
                onDownload={downloadReport}
              />
            </TabsContent>
            <TabsContent value="incident-ops">
              <IncidentOperationsPanel
                initialIncidentId={
                  handoff.unresolved_incidents[0]?.incident_id ?? ""
                }
                onApplied={() => setRefreshToken((value) => value + 1)}
              />
            </TabsContent>
          </Tabs>
        </>
      ) : (
        <div className="state-panel">当前范围内尚未生成运营数据。</div>
      )}
      {error && handoff && (
        <div className="inline-alert warning">{error}</div>
      )}
    </div>
  );
}

function OperationsSummary({
  handoff,
  report,
}: {
  handoff: ShiftHandoffSummary;
  report?: OperationsReport;
}) {
  const cards = [
    {
      label: "未解决 Incident",
      value: handoff.unresolved_incidents.length,
      detail: `${handoff.new_incidents.length} 条本班次新增`,
      icon: FileClock,
      warning: handoff.unresolved_incidents.length > 0,
    },
    {
      label: "容量 / 到期风险",
      value:
        handoff.capacity_risks.length +
        handoff.expiring_certificates.length,
      detail: "未来 30 天阈值",
      icon: AlarmClockCheck,
      warning: handoff.capacity_risks.length > 0,
    },
    {
      label: "逾期 Action Item",
      value: handoff.overdue_action_items.length,
      detail: "需明确 Owner 与到期时间",
      icon: ClipboardCheck,
      warning: handoff.overdue_action_items.length > 0,
    },
    {
      label: "预测 MAE",
      value:
        report?.forecast_mean_absolute_error == null
          ? "N/A"
          : report.forecast_mean_absolute_error.toFixed(3),
      detail: `${report?.forecast_errors.length ?? 0} 个回测样本`,
      icon: CalendarDays,
      warning: false,
    },
  ];
  return (
    <section className="operations-summary-grid" aria-label="运营摘要">
      {cards.map(({ icon: Icon, ...card }) => (
        <article
          className={`operations-summary-card${
            card.warning ? " warning" : ""
          }`}
          key={card.label}
        >
          <span className="operations-summary-icon">
            <Icon size={17} />
          </span>
          <div>
            <span>{card.label}</span>
            <strong>{card.value}</strong>
            <small>{card.detail}</small>
          </div>
        </article>
      ))}
    </section>
  );
}

function HandoffWorkspace({
  handoff,
}: {
  handoff: ShiftHandoffSummary;
}) {
  return (
    <div className="operations-section-grid">
      <FindingSection
        description="新建与仍在处理的 Incident，已排除当前有效抑制和合并源事件。"
        findings={handoff.unresolved_incidents}
        title="未解决 Incident"
      />
      <FindingSection
        description="近期变点与部署、配置、证书和运营状态变化。"
        findings={[...handoff.risk_trends, ...handoff.recent_changes]}
        title="趋势与近期变化"
      />
      <FindingSection
        description="证书到期、容量阈值和必须由人工收口的逾期任务。"
        findings={[
          ...handoff.expiring_certificates,
          ...handoff.capacity_risks,
          ...handoff.overdue_action_items,
        ]}
        title="预防性风险"
      />
      <FindingSection
        description="未达到 queryable 的数据源不会伪装为正常或零值。"
        findings={handoff.source_gaps}
        title="数据源缺口"
      />
    </div>
  );
}

function ReportWorkspace({
  report,
  window,
  onWindowChange,
  onDownload,
}: {
  report?: OperationsReport;
  window: OperationsReportWindow;
  onWindowChange: (value: OperationsReportWindow) => void;
  onDownload: (format: "markdown" | "html") => Promise<void>;
}) {
  if (!report) {
    return <div className="state-panel">正在生成运营报告…</div>;
  }
  return (
    <div className="operations-report-stack">
      <div className="operations-report-toolbar">
        <div>
          <strong>运营报告</strong>
          <span>
            {formatTime(report.window_start)} →{" "}
            {formatTime(report.window_end)}
          </span>
        </div>
        <div>
          <Select
            value={window}
            onValueChange={(value) =>
              onWindowChange(value as OperationsReportWindow)
            }
          >
            <SelectTrigger aria-label="报告周期">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="daily">日报 · 24h</SelectItem>
              <SelectItem value="weekly">周报 · 7d</SelectItem>
            </SelectContent>
          </Select>
          <Button
            onClick={() => void onDownload("markdown")}
            size="sm"
            variant="outline"
          >
            <Download size={14} />
            Markdown
          </Button>
          <Button
            onClick={() => void onDownload("html")}
            size="sm"
            variant="outline"
          >
            <Download size={14} />
            HTML
          </Button>
        </div>
      </div>
      <div className="operations-section-grid">
        <FindingSection
          findings={report.worst_clusters}
          title="健康最差集群"
        />
        <FindingSection findings={report.slo_burns} title="SLO Burn" />
        <FindingSection
          findings={report.diagnostic_pack_findings}
          title="DiagnosticPack Finding"
        />
        <FindingSection
          findings={report.repeat_incidents}
          title="重复 Incident"
        />
        <FindingSection
          findings={report.forecast_errors}
          title="预测误差"
        />
        <FindingSection
          findings={report.source_gaps}
          title="数据源缺口与建议 Owner"
        />
      </div>
    </div>
  );
}

function FindingSection({
  title,
  description,
  findings,
}: {
  title: string;
  description?: string;
  findings: Finding[];
}) {
  return (
    <section className="operations-finding-section">
      <header>
        <div>
          <h2>{title}</h2>
          {description && <p>{description}</p>}
        </div>
        <Badge variant="outline">{findings.length}</Badge>
      </header>
      {findings.length === 0 ? (
        <div className="operations-empty">
          <CheckCircle2 size={17} />
          当前时间窗没有需要交接的条目。
        </div>
      ) : (
        <div className="operations-finding-list">
          {findings.map((finding, index) => (
            <article
              key={`${finding.category}-${finding.cluster_id}-${finding.incident_id ?? index}`}
            >
              <span
                className={`operations-severity ${finding.severity}`}
                aria-hidden="true"
              />
              <div>
                <strong>{finding.title}</strong>
                <p>{finding.detail}</p>
                <small>
                  {finding.category} · {finding.suggested_owner} ·{" "}
                  {formatTime(finding.observed_at)}
                </small>
              </div>
              <Link to={finding.deep_link}>
                查看
                <ArrowUpRight size={13} />
              </Link>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function IncidentOperationsPanel({
  initialIncidentId,
  onApplied,
}: {
  initialIncidentId: string;
  onApplied: () => void;
}) {
  const { api } = useSreData();
  const [incidentId, setIncidentId] = useState(initialIncidentId);
  const [operation, setOperation] =
    useState<OperationKind>("acknowledge");
  const [owner, setOwner] = useState("platform-sre");
  const [reason, setReason] = useState("值班工程师确认");
  const [targetIncidentId, setTargetIncidentId] = useState("");
  const [splitTitle, setSplitTitle] = useState("拆分出的独立症状");
  const [symptomFamily, setSymptomFamily] = useState("operator_split");
  const [state, setState] = useState<IncidentOperationsState>();
  const [message, setMessage] = useState<string>();
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!incidentId && initialIncidentId) {
      setIncidentId(initialIncidentId);
    }
  }, [incidentId, initialIncidentId]);

  useEffect(() => {
    if (!incidentId) {
      setState(undefined);
      return;
    }
    const controller = new AbortController();
    void api
      .getIncidentOperations(incidentId, controller.signal)
      .then(setState)
      .catch(() => setState(undefined));
    return () => controller.abort();
  }, [api, incidentId]);

  const request = useMemo<IncidentOperationRequest>(() => {
    switch (operation) {
      case "acknowledge":
        return { action: "acknowledge", note: reason };
      case "assign":
        return { action: "assign", owner, reason };
      case "merge":
        return {
          action: "merge",
          target_incident_id: targetIncidentId,
          reason,
        };
      case "split":
        return {
          action: "split",
          title: splitTitle,
          symptom_family: symptomFamily,
          reason,
        };
      case "suppress": {
        const until = new Date(Date.now() + 2 * 60 * 60 * 1_000);
        return { action: "suppress", until: until.toISOString(), reason };
      }
      case "reopen":
        return { action: "reopen", reason };
    }
  }, [
    operation,
    owner,
    reason,
    splitTitle,
    symptomFamily,
    targetIncidentId,
  ]);

  const apply = async () => {
    if (!incidentId) {
      return;
    }
    setBusy(true);
    setMessage(undefined);
    try {
      const result = await api.applyIncidentOperation(
        incidentId,
        request,
      );
      setState(result.state);
      setMessage(
        result.related_incident_id
          ? `操作完成，关联 Incident：${result.related_incident_id}`
          : "操作已写入 SRE 时间线；RocketMQ 集群未发生变更。",
      );
      onApplied();
    } catch (cause) {
      setMessage(
        cause instanceof Error ? cause.message : "事件运营操作失败。",
      );
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="incident-operations-grid">
      <section className="operations-finding-section operation-form">
        <header>
          <div>
            <h2>Incident 元数据操作</h2>
            <p>
              每次操作写入 append-only 时间线；reopen 会创建新 Incident，
              不逆转终态。
            </p>
          </div>
          <Badge variant="outline">SRE metadata only</Badge>
        </header>
        <label>
          Incident ID
          <input
            onChange={(event) => setIncidentId(event.target.value.trim())}
            placeholder="UUID"
            value={incidentId}
          />
        </label>
        <label>
          操作
          <Select
            value={operation}
            onValueChange={(value) =>
              setOperation(value as OperationKind)
            }
          >
            <SelectTrigger aria-label="Incident 操作">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {Object.entries(operationLabels).map(([value, label]) => (
                <SelectItem key={value} value={value}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        {operation === "assign" && (
          <label>
            Owner
            <input
              onChange={(event) => setOwner(event.target.value)}
              value={owner}
            />
          </label>
        )}
        {operation === "merge" && (
          <label>
            目标 Incident
            <input
              onChange={(event) =>
                setTargetIncidentId(event.target.value.trim())
              }
              placeholder="同集群 Incident UUID"
              value={targetIncidentId}
            />
          </label>
        )}
        {operation === "split" && (
          <div className="operation-inline-fields">
            <label>
              新 Incident 标题
              <input
                onChange={(event) => setSplitTitle(event.target.value)}
                value={splitTitle}
              />
            </label>
            <label>
              Symptom family
              <input
                onChange={(event) =>
                  setSymptomFamily(event.target.value)
                }
                value={symptomFamily}
              />
            </label>
          </div>
        )}
        <label>
          原因 / 备注
          <textarea
            maxLength={2048}
            onChange={(event) => setReason(event.target.value)}
            rows={3}
            value={reason}
          />
        </label>
        <div className="operation-submit">
          <Button
            disabled={
              busy ||
              !incidentId ||
              !reason.trim() ||
              (operation === "merge" && !targetIncidentId)
            }
            onClick={() => void apply()}
            type="button"
          >
            {operationIcon(operation)}
            {busy ? "正在写入审计…" : operationLabels[operation]}
          </Button>
          <span>不会调用 Admin mutation、Executor 或 Agent。</span>
        </div>
        {message && <div className="inline-alert">{message}</div>}
      </section>
      <IncidentStateCard state={state} />
    </div>
  );
}

function IncidentStateCard({
  state,
}: {
  state?: IncidentOperationsState;
}) {
  return (
    <section className="operations-finding-section incident-state-card">
      <header>
        <div>
          <h2>运营状态与 SLA</h2>
          <p>确认时间、Owner、抑制窗口和关联状态均来自 PostgreSQL。</p>
        </div>
        <Badge variant={state ? "success" : "outline"}>
          {state ? "loaded" : "not loaded"}
        </Badge>
      </header>
      {!state ? (
        <div className="operations-empty">
          输入授权范围内的 Incident ID 读取状态。
        </div>
      ) : (
        <dl className="operations-state-list">
          <div>
            <dt>Owner</dt>
            <dd>{state.owner}</dd>
          </div>
          <div>
            <dt>Acknowledged by</dt>
            <dd>{state.acknowledged_by ?? "未确认"}</dd>
          </div>
          <div>
            <dt>Ack SLA</dt>
            <dd className={state.sla.acknowledgement_breached ? "risk" : ""}>
              {state.sla.acknowledgement_breached ? "已超时" : "正常"} ·{" "}
              {formatTime(state.sla.acknowledgement_due_at)}
            </dd>
          </div>
          <div>
            <dt>Resolve SLA</dt>
            <dd className={state.sla.resolution_breached ? "risk" : ""}>
              {state.sla.resolution_breached ? "已超时" : "正常"} ·{" "}
              {formatTime(state.sla.resolution_due_at)}
            </dd>
          </div>
          <div>
            <dt>Suppressed until</dt>
            <dd>
              {state.suppressed_until
                ? formatTime(state.suppressed_until)
                : "未抑制"}
            </dd>
          </div>
          <div>
            <dt>Merged into</dt>
            <dd className="mono">
              {state.merged_into_incident_id ?? "—"}
            </dd>
          </div>
          <div>
            <dt>Split children</dt>
            <dd>{state.split_incident_ids.length}</dd>
          </div>
          <div>
            <dt>Updated</dt>
            <dd>{formatTime(state.updated_at)}</dd>
          </div>
        </dl>
      )}
    </section>
  );
}

function operationIcon(operation: OperationKind) {
  switch (operation) {
    case "acknowledge":
      return <CheckCircle2 size={15} />;
    case "assign":
      return <UserRoundCheck size={15} />;
    case "merge":
      return <GitMerge size={15} />;
    case "split":
      return <Split size={15} />;
    case "suppress":
      return <AlarmClockCheck size={15} />;
    case "reopen":
      return <RotateCcw size={15} />;
  }
}

function formatTime(value: string) {
  return new Date(value).toLocaleString("zh-CN", {
    hour12: false,
    timeZone: "Asia/Shanghai",
  });
}
