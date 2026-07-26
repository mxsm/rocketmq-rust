import {
  ArrowLeft,
  CheckCircle2,
  CircleSlash2,
  DatabaseZap,
} from "lucide-react";
import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

import type { CapabilitySnapshot } from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import {
  AvailabilityBadge,
  StatusBadge,
} from "@/components/StatusBadge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";

export function ClusterDetailPage() {
  const { clusterId = "" } = useParams();
  const { clusters, capability } = useSreData();
  const cluster = clusters.find((item) => item.id === clusterId);
  const [snapshot, setSnapshot] = useState<CapabilitySnapshot>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!clusterId) {
      return;
    }
    const controller = new AbortController();
    void capability(clusterId, controller.signal)
      .then(setSnapshot)
      .catch((cause: unknown) =>
        setError(
          cause instanceof Error ? cause.message : "能力快照暂不可用",
        ),
      );
    return () => controller.abort();
  }, [capability, clusterId]);

  if (!cluster) {
    return (
      <div className="page">
        <div className="state-message">集群不存在或已不在当前租户范围内。</div>
      </div>
    );
  }

  return (
    <div className="page">
      <Button asChild size="sm" variant="ghost">
        <Link className="back-link" to="/clusters">
          <ArrowLeft size={15} />
          返回集群
        </Link>
      </Button>
      <PageHeader
        eyebrow={cluster.tenant_id}
        title={cluster.external_cluster_key}
        description={`${cluster.region} · ${cluster.environment} · ${cluster.rocketmq_version}`}
        actions={<StatusBadge state={cluster.state} />}
      />

      <section className="definition-strip">
        <Definition label="Owner" value={cluster.owner} />
        <Definition label="部署模式" value={cluster.deployment_mode} />
        <Definition
          label="有效权限"
          value={cluster.effective_access_profile}
        />
        <Definition
          label="最近更新"
          value={new Date(cluster.updated_at).toLocaleString("zh-CN", {
            hour12: false,
          })}
        />
      </section>

      <section className="data-surface">
        <div className="surface-heading">
          <div>
            <h2>MCP 能力握手</h2>
            <p>协议、业务 schema、摘要和只读能力声明。</p>
          </div>
          {snapshot?.mutation_supported === false ? (
            <Badge variant="success">
              <CheckCircle2 size={14} />
              mutation_supported=false
            </Badge>
          ) : (
            <Badge variant="destructive">
              <CircleSlash2 size={14} />
              尚未认证
            </Badge>
          )}
        </div>
        {snapshot ? (
          <>
            <div className="definition-strip embedded">
              <Definition label="MCP Protocol" value={snapshot.protocol_version} />
              <Definition
                label="Business Schema"
                value={snapshot.schema_version}
              />
              <Definition label="Digest" value={snapshot.digest} mono />
              <Definition
                label="采集时间"
                value={new Date(snapshot.observed_at).toLocaleString("zh-CN", {
                  hour12: false,
                })}
              />
            </div>
            <div className="source-table">
              <div className="source-row source-header">
                <span>数据源</span>
                <span>可用性</span>
                <span>新鲜度</span>
                <span>说明</span>
              </div>
              {snapshot.data_sources.map((source) => (
                <div className="source-row" key={source.id}>
                  <strong>{source.id}</strong>
                  <AvailabilityBadge availability={source.availability} />
                  <span>
                    {source.freshness_ms === undefined
                      ? "未知"
                      : `${Math.ceil(source.freshness_ms / 60_000)} 分钟`}
                  </span>
                  <span>{source.detail ?? "—"}</span>
                </div>
              ))}
            </div>
          </>
        ) : (
          <div className="empty-state">
            <DatabaseZap size={28} />
            <h3>没有 capability snapshot</h3>
            <p>{error ?? "Connector 完成握手后会在这里显示只读能力。"}</p>
          </div>
        )}
      </section>
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
