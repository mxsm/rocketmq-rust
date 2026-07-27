import {
  ChevronRight,
  Circle,
  CircleDot,
  Copy,
  RefreshCw,
  ShieldCheck,
  TriangleAlert,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import type {
  CapabilitySnapshot,
  ClusterSummary,
  FleetHealthReport,
} from "@/api/types";
import { FleetHealthOverview } from "@/components/HealthOverview";
import { PageHeader } from "@/components/PageHeader";
import { ReadOnlyBoundary } from "@/components/ReadOnlyBoundary";
import {
  AvailabilityBadge,
  StatusBadge,
} from "@/components/StatusBadge";
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

export function OverviewPage() {
  const { clusters, loading, error, refresh, capability, api } = useSreData();
  const [environment, setEnvironment] = useState("all");
  const [selectedId, setSelectedId] = useState<string>();
  const [capabilityMap, setCapabilityMap] = useState<
    Record<string, CapabilitySnapshot>
  >({});
  const [capabilityError, setCapabilityError] = useState<string>();
  const [fleetHealth, setFleetHealth] = useState<FleetHealthReport>();
  const [fleetHealthError, setFleetHealthError] = useState<string>();

  const filteredClusters = useMemo(
    () =>
      environment === "all"
        ? clusters
        : clusters.filter((cluster) => cluster.environment === environment),
    [clusters, environment],
  );
  const environments = useMemo(
    () => [...new Set(clusters.map((cluster) => cluster.environment))],
    [clusters],
  );

  useEffect(() => {
    if (
      filteredClusters.length > 0 &&
      !filteredClusters.some((cluster) => cluster.id === selectedId)
    ) {
      setSelectedId(filteredClusters[0].id);
    }
  }, [filteredClusters, selectedId]);

  useEffect(() => {
    if (filteredClusters.length === 0) {
      setCapabilityMap({});
      return;
    }
    const controller = new AbortController();
    setCapabilityError(undefined);
    void Promise.allSettled(
      filteredClusters.map(async (cluster) => ({
        id: cluster.id,
        snapshot: await capability(cluster.id, controller.signal),
      })),
    ).then((results) => {
      const next: Record<string, CapabilitySnapshot> = {};
      let unavailable = false;
      for (const result of results) {
        if (result.status === "fulfilled") {
          next[result.value.id] = result.value.snapshot;
        } else if (
          !(result.reason instanceof DOMException &&
            result.reason.name === "AbortError")
        ) {
          unavailable = true;
        }
      }
      if (!controller.signal.aborted) {
        setCapabilityMap(next);
        if (unavailable) {
          setCapabilityError(
            "部分集群尚无能力快照；对应字段保持“未采集”。",
          );
        }
      }
    });
    return () => controller.abort();
  }, [capability, filteredClusters]);

  useEffect(() => {
    const controller = new AbortController();
    setFleetHealthError(undefined);
    void api
      .getFleetHealth(undefined, controller.signal)
      .then(setFleetHealth)
      .catch((cause: unknown) => {
        if (
          !(cause instanceof DOMException && cause.name === "AbortError")
        ) {
          setFleetHealthError(
            cause instanceof Error
              ? cause.message
              : "Fleet 健康评分暂不可用",
          );
        }
      });
    return () => controller.abort();
  }, [api]);

  const selected = filteredClusters.find(
    (cluster) => cluster.id === selectedId,
  );
  const selectedCapability = selectedId
    ? capabilityMap[selectedId]
    : undefined;
  const summary = summarize(filteredClusters, selectedCapability);

  return (
    <div className="page command-center-page">
      <PageHeader
        eyebrow="CLUSTER COMMAND CENTER"
        title="集群战情台"
        description="汇总只读接入、安全握手和证据覆盖状态，缺失数据始终显式标记。"
        actions={
          <>
            <Select value={environment} onValueChange={setEnvironment}>
              <SelectTrigger aria-label="环境筛选">
                <SelectValue placeholder="全部环境" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部环境</SelectItem>
                {environments.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button disabled={loading} onClick={() => void refresh()}>
              <RefreshCw
                aria-hidden="true"
                className={loading ? "spin" : undefined}
                size={15}
              />
              刷新证据
            </Button>
          </>
        }
      />

      <ReadOnlyBoundary />

      {error && (
        <div className="inline-alert warning" role="status">
          <TriangleAlert aria-hidden="true" size={16} />
          {error}
        </div>
      )}

      <section className="summary-strip" aria-label="集群状态摘要">
        <SummaryItem label="集群总数" value={String(summary.total)} />
        <SummaryItem
          label="安全状态"
          value={`${summary.ready} 就绪 · ${summary.degraded} 降级 · ${summary.rejected} 拒绝`}
        />
        <SummaryItem label="最近证据" value={summary.freshness} />
        <SummaryItem label="当前覆盖率" value={summary.coverage} />
        <SummaryItem
          label="MCP 协议"
          value={selectedCapability?.protocol_version ?? "未采集"}
        />
        <SummaryItem
          label="业务 Schema"
          value={selectedCapability?.schema_version ?? "未采集"}
        />
        <SummaryItem
          label="变更能力"
          value={
            selectedCapability?.mutation_supported === false
              ? "不可用"
              : "未认证"
          }
          safe={selectedCapability?.mutation_supported === false}
        />
      </section>

      {fleetHealth ? (
        <FleetHealthOverview report={fleetHealth} />
      ) : (
        <section className="data-surface health-loading-surface">
          <div className="state-message">
            {fleetHealthError ?? "正在读取 Fleet 健康评分…"}
          </div>
        </section>
      )}

      <section className="data-surface cluster-surface">
        <div className="surface-heading">
          <div>
            <h2>只读集群</h2>
            <p>选择一行查看最新 capability snapshot 和数据源状态。</p>
          </div>
          <span>{filteredClusters.length} 个结果</span>
        </div>
        {loading && clusters.length === 0 ? (
          <div className="state-message">正在读取集群状态…</div>
        ) : filteredClusters.length === 0 ? (
          <div className="empty-state">
            <h3>暂无已接入集群</h3>
            <p>使用 Control Plane onboarding API 注册第一个只读 Connector。</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table className="cluster-table">
              <thead>
                <tr>
                  <th>集群</th>
                  <th>环境</th>
                  <th>安全状态</th>
                  <th>MCP 协议</th>
                  <th>业务 Schema</th>
                  <th>变更能力</th>
                  <th>证据新鲜度</th>
                  <th>覆盖率</th>
                  <th>关键缺失项</th>
                  <th aria-label="查看详情" />
                </tr>
              </thead>
              <tbody>
                {filteredClusters.map((cluster) => {
                  const isSelected = cluster.id === selectedId;
                  const current = capabilityMap[cluster.id];
                  const rowSummary = summarizeCapability(current);
                  return (
                    <tr
                      aria-selected={isSelected}
                      className={isSelected ? "selected" : undefined}
                      key={cluster.id}
                    >
                      <td>
                        <button
                          className="cluster-name-button"
                          onClick={() => setSelectedId(cluster.id)}
                          type="button"
                        >
                          {isSelected ? (
                            <CircleDot
                              aria-hidden="true"
                              className="selection-icon selected"
                              size={16}
                            />
                          ) : (
                            <Circle
                              aria-hidden="true"
                              className="selection-icon"
                              size={16}
                            />
                          )}
                          <span>
                            <strong>{cluster.external_cluster_key}</strong>
                            <small>{cluster.tenant_id}</small>
                          </span>
                        </button>
                      </td>
                      <td>
                        {cluster.environment}
                        <small>{cluster.region}</small>
                      </td>
                      <td>
                        <StatusBadge state={cluster.state} />
                      </td>
                      <td>{current?.protocol_version ?? "—"}</td>
                      <td>{current?.schema_version ?? "—"}</td>
                      <td>
                        {current?.mutation_supported === false ? (
                          <>
                            false
                            <small>不支持</small>
                          </>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td>{rowSummary.freshness}</td>
                      <td>{rowSummary.coverage}</td>
                      <td>{rowSummary.gap}</td>
                      <td>
                        <Button
                          aria-label={`查看 ${cluster.external_cluster_key}`}
                          onClick={() => setSelectedId(cluster.id)}
                          size="icon"
                          variant="ghost"
                        >
                          <ChevronRight size={16} />
                        </Button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {selected && (
          <div className="cluster-inspector">
            <Tabs defaultValue="detail">
              <TabsList>
                <TabsTrigger value="detail">集群详情</TabsTrigger>
                <TabsTrigger value="sources">数据源状态</TabsTrigger>
                <TabsTrigger value="digest">契约与摘要</TabsTrigger>
              </TabsList>
              <TabsContent value="detail">
                <div className="inspector-grid">
                  <Definition label="集群名称" value={selected.external_cluster_key} />
                  <Definition label="租户" value={selected.tenant_id} />
                  <Definition label="部署模式" value={selected.deployment_mode} />
                  <Definition label="RocketMQ" value={selected.rocketmq_version} />
                  <Definition label="Owner" value={selected.owner} />
                  <Definition
                    label="有效权限"
                    value={selected.effective_access_profile}
                  />
                </div>
              </TabsContent>
              <TabsContent value="sources">
                {selectedCapability?.data_sources.length ? (
                  <div className="source-table">
                    <div className="source-row source-header">
                      <span>数据源</span>
                      <span>可用性</span>
                      <span>新鲜度</span>
                      <span>说明</span>
                    </div>
                    {selectedCapability.data_sources.map((source) => (
                      <div className="source-row" key={source.id}>
                        <strong>{source.id}</strong>
                        <AvailabilityBadge availability={source.availability} />
                        <span>
                          {source.freshness_ms === undefined
                            ? "未提供"
                            : `${Math.ceil(source.freshness_ms / 60_000)} 分钟前`}
                        </span>
                        <span>{source.detail ?? "—"}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="state-message">
                    {capabilityError ?? "尚无数据源状态。"}
                  </div>
                )}
              </TabsContent>
              <TabsContent value="digest">
                <div className="digest-panel">
                  <Definition
                    label="Schema Digest"
                    value={selectedCapability?.digest ?? "未采集"}
                    mono
                  />
                  <Definition
                    label="Tool Surface"
                    value={
                      selectedCapability?.manifest?.tool_surface_digest ??
                      "未采集"
                    }
                    mono
                  />
                  <Definition
                    label="Resource Surface"
                    value={
                      selectedCapability?.manifest?.resource_surface_digest ??
                      "未采集"
                    }
                    mono
                  />
                  <Button
                    disabled={!selectedCapability?.digest}
                    onClick={() => {
                      if (selectedCapability?.digest) {
                        void navigator.clipboard.writeText(
                          selectedCapability.digest,
                        );
                      }
                    }}
                    size="sm"
                    variant="outline"
                  >
                    <Copy size={14} />
                    复制摘要
                  </Button>
                </div>
              </TabsContent>
            </Tabs>
            {selected.state === "ready_read_only" && (
              <div className="production-ready">
                <ShieldCheck aria-hidden="true" size={15} />
                只读就绪
              </div>
            )}
          </div>
        )}
      </section>
    </div>
  );
}

function summarize(
  clusters: ClusterSummary[],
  capability?: CapabilitySnapshot,
) {
  const current = summarizeCapability(capability);
  return {
    total: clusters.length,
    ready: clusters.filter((item) => item.state === "ready_read_only").length,
    degraded: clusters.filter((item) => item.state === "read_only_degraded")
      .length,
    rejected: clusters.filter((item) => item.state === "rejected").length,
    freshness: current.freshness,
    coverage: current.coverage,
  };
}

function summarizeCapability(capability?: CapabilitySnapshot) {
  if (!capability || capability.data_sources.length === 0) {
    return { freshness: "未采集", coverage: "未采集", gap: "未采集" };
  }
  const queryable = capability.data_sources.filter(
    (source) => source.availability === "queryable",
  ).length;
  const coverage = Math.round(
    (queryable / capability.data_sources.length) * 100,
  );
  const ages = capability.data_sources
    .map((source) => source.freshness_ms)
    .filter((value): value is number => value !== undefined);
  const gap = capability.data_sources.find(
    (source) => source.availability !== "queryable",
  );
  return {
    freshness:
      ages.length === 0
        ? "未知"
        : `${Math.ceil(Math.max(...ages) / 60_000)} 分钟前`,
    coverage: `${coverage}%`,
    gap: gap?.detail ?? "无",
  };
}

function SummaryItem({
  label,
  value,
  safe = false,
}: {
  label: string;
  value: string;
  safe?: boolean;
}) {
  return (
    <div className="summary-item">
      <span>{label}</span>
      <strong className={safe ? "safe-text" : undefined}>{value}</strong>
    </div>
  );
}

function Definition({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="definition">
      <span>{label}</span>
      <strong className={mono ? "mono" : undefined}>{value}</strong>
    </div>
  );
}
