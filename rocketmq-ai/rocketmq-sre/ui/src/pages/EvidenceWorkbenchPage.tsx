import {
  CheckCircle2,
  Circle,
  CircleAlert,
  Clock3,
  Copy,
  Database,
  RefreshCw,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import type {
  CapabilitySnapshot,
  EvidenceCollectionStatus,
  EvidenceRow,
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
import { useSreData } from "@/data/SreDataContext";
import { demoEvidence } from "@/data/demo";

const sourceLabels: Record<string, string> = {
  broker: "Broker 运行时",
  nameserver: "Topic / 路由清单",
  controller: "集群概览",
  proxy: "代理运行态",
  mcp: "MCP 运行时",
  runtime: "Runtime diagnostics",
};

export function EvidenceWorkbenchPage() {
  const { clusters, capability, demoMode } = useSreData();
  const [clusterId, setClusterId] = useState("");
  const [snapshot, setSnapshot] = useState<CapabilitySnapshot>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!clusterId && clusters.length > 0) {
      setClusterId(clusters[0].id);
    }
  }, [clusterId, clusters]);

  const collect = async (id = clusterId) => {
    if (!id) {
      return;
    }
    setLoading(true);
    setError(undefined);
    try {
      setSnapshot(await capability(id));
    } catch (cause) {
      setSnapshot(undefined);
      setError(cause instanceof Error ? cause.message : "证据源不可用");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void collect(clusterId);
    // collect is deliberately scoped to the selected cluster.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clusterId]);

  const cluster = clusters.find((item) => item.id === clusterId);
  const evidence = useMemo(
    () => (demoMode ? demoEvidence : evidenceFromCapability(snapshot)),
    [demoMode, snapshot],
  );

  return (
    <div className="page evidence-page">
      <PageHeader
        eyebrow="CANONICAL EVIDENCE"
        title="集群证据链"
        description="按来源顺序展示只读查询结果；兼容性由最薄弱证据决定。"
        actions={
          <>
            <Select value={clusterId} onValueChange={setClusterId}>
              <SelectTrigger aria-label="选择集群" className="cluster-select">
                <Database aria-hidden="true" size={15} />
                <SelectValue placeholder="选择集群" />
              </SelectTrigger>
              <SelectContent>
                {clusters.map((item) => (
                  <SelectItem key={item.id} value={item.id}>
                    {item.external_cluster_key}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              disabled={!clusterId || loading}
              onClick={() => void collect()}
            >
              <RefreshCw
                aria-hidden="true"
                className={loading ? "spin" : undefined}
                size={15}
              />
              重新采集
            </Button>
          </>
        }
      />

      {error && (
        <div className="inline-alert warning">
          <TriangleAlert aria-hidden="true" size={16} />
          {error}
        </div>
      )}

      <div className="evidence-layout">
        <section className="evidence-chain" aria-label="证据查询结果">
          <div className="evidence-header evidence-grid">
            <span>顺序 / 来源</span>
            <span>查询状态</span>
            <span>观测时间</span>
            <span>时效性</span>
            <span>覆盖度</span>
            <span>完整性</span>
            <span>证据哈希</span>
          </div>
          {evidence.length === 0 ? (
            <div className="empty-state">
              <h3>尚无 Canonical Evidence</h3>
              <p>Connector 握手完成后，可从只读 MCP 查询生成证据。</p>
            </div>
          ) : (
            <ol>
              {evidence.map((item, index) => (
                <li className={`evidence-entry ${item.status}`} key={item.id}>
                  <div className="evidence-grid">
                    <div className="evidence-source">
                      <span className="evidence-sequence">
                        {String(index + 1).padStart(2, "0")}
                      </span>
                      <StatusIcon status={item.status} />
                      <span>
                        <strong>{item.sourceLabel}</strong>
                        <small>{item.source}</small>
                      </span>
                    </div>
                    <div>
                      <StatusText status={item.status} />
                      {item.errorCode && <small>{item.errorCode}</small>}
                    </div>
                    <div>
                      {formatTime(item.observedAt)}
                      <small>{relativeTime(item.observedAt)}</small>
                    </div>
                    <div>
                      {item.freshnessSeconds === undefined
                        ? "未知"
                        : formatFreshness(item.freshnessSeconds)}
                    </div>
                    <div>
                      {item.coveragePercent === undefined
                        ? "无数据"
                        : `${item.coveragePercent}%`}
                    </div>
                    <div>
                      {item.status === "complete"
                        ? "完整"
                        : item.status === "partial"
                          ? "部分"
                          : "不可用"}
                      {item.warning && <small>{item.warning}</small>}
                    </div>
                    <div className="hash-cell">
                      <code>{item.hash ?? "未生成"}</code>
                      {item.hash && (
                        <Button
                          aria-label={`复制 ${item.sourceLabel} 哈希`}
                          onClick={() =>
                            void navigator.clipboard.writeText(item.hash ?? "")
                          }
                          size="icon"
                          variant="ghost"
                        >
                          <Copy size={13} />
                        </Button>
                      )}
                    </div>
                  </div>
                </li>
              ))}
            </ol>
          )}
          <footer className="evidence-footer">
            <CircleAlert aria-hidden="true" size={15} />
            未采集、缺失和未生产验证不会被转换成数值 0。
          </footer>
        </section>

        <aside className="compatibility-inspector">
          <div className="inspector-title">
            <div>
              <h2>兼容性检查器</h2>
              <p>当前集群与 MCP 上线要求的只读匹配结果</p>
            </div>
            <Badge variant="outline">只读</Badge>
          </div>
          <InspectorGroup title="基本信息">
            <InspectorRow label="租户" value={cluster?.tenant_id ?? "未选择"} />
            <InspectorRow
              label="集群"
              value={cluster?.external_cluster_key ?? "未选择"}
            />
            <InspectorRow
              label="集群允许列表"
              value={cluster ? "允许" : "未验证"}
              status={cluster ? "success" : undefined}
            />
          </InspectorGroup>
          <InspectorGroup title="协议与版本">
            <InspectorRow
              label="MCP 协议"
              value={snapshot?.protocol_version ?? "未采集"}
            />
            <InspectorRow
              label="业务 Schema"
              value={snapshot?.schema_version ?? "未采集"}
            />
            <InspectorRow
              label="mutation_supported"
              value={
                snapshot?.mutation_supported === false ? "false" : "未验证"
              }
              status={
                snapshot?.mutation_supported === false ? "success" : undefined
              }
            />
          </InspectorGroup>
          <InspectorGroup title="工具与模式摘要">
            <InspectorRow
              label="工具面摘要"
              mono
              value={
                snapshot?.manifest?.tool_surface_digest?.replace(
                  "sha256:",
                  "",
                ) ?? "未采集"
              }
            />
            <InspectorRow
              label="资源面摘要"
              mono
              value={
                snapshot?.manifest?.resource_surface_digest?.replace(
                  "sha256:",
                  "",
                ) ?? "未采集"
              }
            />
            <InspectorRow
              label="Schema 摘要"
              mono
              value={snapshot?.digest.replace("sha256:", "") ?? "未采集"}
            />
          </InspectorGroup>
          <InspectorGroup title="所需来源">
            {evidence.map((item) => (
              <InspectorRow
                key={item.id}
                label={item.sourceLabel}
                status={
                  item.status === "complete"
                    ? "success"
                    : item.status === "partial"
                      ? "warning"
                      : "danger"
                }
                value={
                  item.status === "complete"
                    ? "可用"
                    : item.status === "partial"
                      ? "部分"
                      : "不可用"
                }
              />
            ))}
          </InspectorGroup>
          <div className="inspector-note">
            <ShieldCheck aria-hidden="true" size={15} />
            右侧仅展示当前观测结果，不执行任何集群操作。
          </div>
        </aside>
      </div>
    </div>
  );
}

function evidenceFromCapability(
  capability?: CapabilitySnapshot,
): EvidenceRow[] {
  if (!capability) {
    return [];
  }
  return capability.data_sources.map((source) => {
    const status: EvidenceCollectionStatus =
      source.availability === "queryable"
        ? "complete"
        : source.availability === "missing_instrumentation"
          ? "unavailable"
          : "partial";
    return {
      id: source.id,
      source: source.id,
      sourceLabel: sourceLabels[source.id] ?? source.id,
      status,
      observedAt: capability.observed_at,
      freshnessSeconds:
        source.freshness_ms === undefined
          ? undefined
          : Math.ceil(source.freshness_ms / 1000),
      warning:
        status === "partial"
          ? (source.detail ?? source.availability)
          : undefined,
      errorCode:
        status === "unavailable" ? "source_unavailable" : undefined,
    };
  });
}

function StatusIcon({ status }: { status: EvidenceCollectionStatus }) {
  if (status === "complete") {
    return <CheckCircle2 aria-hidden="true" size={21} />;
  }
  if (status === "partial") {
    return <TriangleAlert aria-hidden="true" size={21} />;
  }
  return <CircleAlert aria-hidden="true" size={21} />;
}

function StatusText({ status }: { status: EvidenceCollectionStatus }) {
  const value =
    status === "complete" ? "成功" : status === "partial" ? "部分" : "不可用";
  return (
    <span className={`evidence-status ${status}`}>
      <Clock3 aria-hidden="true" size={12} />
      {value}
    </span>
  );
}

function InspectorGroup({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="inspector-group">
      <h3>{title}</h3>
      {children}
    </section>
  );
}

function InspectorRow({
  label,
  value,
  status,
  mono = false,
}: {
  label: string;
  value: string;
  status?: "success" | "warning" | "danger";
  mono?: boolean;
}) {
  return (
    <div className="inspector-row">
      <span>{label}</span>
      <strong className={`${status ?? ""}${mono ? " mono" : ""}`}>
        {status && (
          <Circle aria-hidden="true" fill="currentColor" size={7} />
        )}
        {value}
      </strong>
    </div>
  );
}

function formatTime(value: string) {
  return new Date(value).toLocaleTimeString("zh-CN", {
    hour12: false,
    timeZone: "Asia/Shanghai",
  });
}

function relativeTime(value: string) {
  const seconds = Math.max(
    0,
    Math.round((Date.now() - new Date(value).getTime()) / 1000),
  );
  if (seconds < 60) {
    return `${seconds} 秒前`;
  }
  return `${Math.ceil(seconds / 60)} 分钟前`;
}

function formatFreshness(seconds: number) {
  if (seconds < 60) {
    return `${seconds}s`;
  }
  return `${Math.ceil(seconds / 60)}m`;
}
