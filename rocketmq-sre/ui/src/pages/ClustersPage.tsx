import { ChevronRight, RefreshCw, ServerCrash } from "lucide-react";
import { Link } from "react-router-dom";

import { PageHeader } from "@/components/PageHeader";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";

export function ClustersPage() {
  const { clusters, loading, error, refresh } = useSreData();

  return (
    <div className="page">
      <PageHeader
        eyebrow="CLUSTER ONBOARDING"
        title="集群接入"
        description="展示只读身份、MCP 契约和数据源握手结果。"
        actions={
          <Button
            disabled={loading}
            onClick={() => void refresh()}
            variant="outline"
          >
            <RefreshCw
              className={loading ? "spin" : undefined}
              size={15}
            />
            刷新
          </Button>
        }
      />

      {error && <div className="inline-alert warning">{error}</div>}
      <section className="data-surface">
        <div className="surface-heading">
          <div>
            <h2>Onboarding 状态</h2>
            <p>注册、握手、降级、拒绝与安全下线均保留历史。</p>
          </div>
          <span>{clusters.length} 个集群</span>
        </div>
        {loading && clusters.length === 0 ? (
          <div className="state-message">正在加载集群状态…</div>
        ) : clusters.length === 0 ? (
          <div className="empty-state">
            <ServerCrash size={30} />
            <h3>尚未接入集群</h3>
            <p>通过 Control Plane API 注册第一个只读 Connector。</p>
          </div>
        ) : (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>集群</th>
                  <th>租户 / 环境</th>
                  <th>版本 / 部署</th>
                  <th>区域</th>
                  <th>Owner</th>
                  <th>状态</th>
                  <th>最近更新</th>
                  <th aria-label="详情" />
                </tr>
              </thead>
              <tbody>
                {clusters.map((cluster) => (
                  <tr key={cluster.id}>
                    <td>
                      <Link
                        className="table-link"
                        to={`/clusters/${cluster.id}`}
                      >
                        {cluster.external_cluster_key}
                      </Link>
                    </td>
                    <td>
                      {cluster.tenant_id}
                      <small>{cluster.environment}</small>
                    </td>
                    <td>
                      {cluster.rocketmq_version}
                      <small>{cluster.deployment_mode}</small>
                    </td>
                    <td>{cluster.region}</td>
                    <td>{cluster.owner}</td>
                    <td>
                      <StatusBadge state={cluster.state} />
                    </td>
                    <td>
                      {new Date(cluster.updated_at).toLocaleString("zh-CN", {
                        hour12: false,
                      })}
                    </td>
                    <td>
                      <Button asChild size="icon" variant="ghost">
                        <Link
                          aria-label={`查看 ${cluster.external_cluster_key}`}
                          to={`/clusters/${cluster.id}`}
                        >
                          <ChevronRight size={16} />
                        </Link>
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
