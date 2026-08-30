import { Network, TriangleAlert } from "lucide-react";

import type { IncidentTopologyView } from "@/api/types";
import { DataState } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import type { AsyncResource } from "@/hooks/useAsyncResource";

export function IncidentTopology({
  topology,
}: {
  topology: AsyncResource<IncidentTopologyView>;
}) {
  return (
    <>
      <DataState
        loading={topology.loading}
        error={topology.error}
        empty={!topology.loading && !topology.data}
        onRetry={topology.reload}
        emptyTitle="暂无拓扑"
        emptyDescription="当前 Incident 尚未关联可显示的资源关系。"
      />
      {topology.data?.partial && (
        <div className="inline-alert warning">
          <TriangleAlert aria-hidden="true" size={15} />
          拓扑为部分结果：
          {topology.data.warnings.join("；") || "部分数据源不可用"}
        </div>
      )}
      {topology.data && topology.data.nodes.length > 0 && (
        <div className="incident-topology">
          <div className="topology-node-list">
            <div className="topology-panel-title">
              <Network aria-hidden="true" size={15} />
              <strong>节点</strong>
            </div>
            {topology.data.nodes.map((node) => (
              <article key={node.key}>
                <div>
                  <strong>{node.display_name}</strong>
                  <small>{node.kind}</small>
                </div>
                <Badge
                  variant={node.alert_count > 0 ? "warning" : "outline"}
                >
                  {node.alert_count} alerts
                </Badge>
              </article>
            ))}
          </div>
          <div className="topology-edge-list">
            <div className="topology-panel-title">
              <Network aria-hidden="true" size={15} />
              <strong>依赖关系</strong>
            </div>
            {topology.data.edges.map((edge, index) => (
              <article
                key={`${edge.from}-${edge.relation}-${edge.to}-${index}`}
              >
                <code>{edge.from}</code>
                <span>→ {edge.relation} →</span>
                <code>{edge.to}</code>
              </article>
            ))}
            {topology.data.edges.length === 0 && (
              <div className="state-message">尚无依赖边。</div>
            )}
          </div>
        </div>
      )}
    </>
  );
}
