import {
  Boxes,
  ChevronRight,
  GitBranch,
  Search,
  ShieldCheck,
} from "lucide-react";
import {
  type FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Link, useNavigate } from "react-router-dom";

import { ApiError } from "@/api/client";
import type { AssetKind, OnboardClusterRequest } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import {
  ClusterScopeSelect,
  DataState,
  DataSurface,
  DefinitionGrid,
  PartialNotice,
  formatTime,
} from "@/components/Phase1Primitives";
import { ReadOnlyBoundary } from "@/components/ReadOnlyBoundary";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useClusterScope } from "@/hooks/useClusterScope";

const kindLabels: Record<AssetKind, string> = {
  name_server: "NameServer",
  controller: "Controller",
  broker: "Broker",
  proxy: "Proxy",
  store: "Store",
  pod: "Pod",
  node: "Node",
  persistent_volume_claim: "PVC",
  pod_disruption_budget: "PDB",
  topic: "Topic",
  queue: "Queue",
  producer: "Producer",
  consumer: "Consumer",
  connection: "Connection",
};

export function AssetsPage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const appliedContext = useRef("");
  const [search, setSearch] = useState("");
  const [kind, setKind] = useState<AssetKind | "all">("all");
  useEffect(() => {
    const context = scope.urlContext;
    if (
      !context?.resourceKind ||
      context.resourceKind === "cluster" ||
      !context.resourceKey
    ) {
      return;
    }
    const fingerprint = `${context.clusterId}:${context.resourceKind}:${context.resourceKey}`;
    if (appliedContext.current === fingerprint) {
      return;
    }
    appliedContext.current = fingerprint;
    setKind(context.resourceKind);
    setSearch(context.resourceKey);
  }, [scope.urlContext]);
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listAssets(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: new Date().toISOString(),
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  const filtered = useMemo(
    () =>
      (resource.data?.items ?? []).filter((asset) => {
        const query = search.trim().toLocaleLowerCase();
        return (
          (kind === "all" || asset.kind === kind) &&
          (!query ||
            asset.display_name.toLocaleLowerCase().includes(query) ||
            asset.external_key.toLocaleLowerCase().includes(query))
        );
      }),
    [kind, resource.data?.items, search],
  );

  return (
    <div className="page">
      <PageHeader
        eyebrow="ASSET INVENTORY"
        title="资产视图"
        description="将 RocketMQ 与 Kubernetes 资产归一化为带版本、来源、新鲜度和 partial 语义的只读快照。"
        actions={
          <ClusterScopeSelect
            clusters={scope.clusters}
            value={scope.clusterId}
            onChange={scope.setClusterId}
          />
        }
      />
      <ReadOnlyBoundary compact />
      <PartialNotice envelope={resource.data} />
      <DataSurface
        title="资产清单"
        description="属性只展示脱敏摘要；连接地址、凭据和消息正文均不进入 UI。"
        meta={<span>{filtered.length} 个结果</span>}
      >
        <div className="filter-bar">
          <label className="search-field">
            <Search size={14} />
            <span className="sr-only">搜索资产</span>
            <input
              onChange={(event) => setSearch(event.target.value)}
              placeholder="搜索名称或 external key"
              type="search"
              value={search}
            />
          </label>
          <label>
            <span className="sr-only">资产类型</span>
            <select
              className="native-select"
              onChange={(event) =>
                setKind(event.target.value as AssetKind | "all")
              }
              value={kind}
            >
              <option value="all">全部类型</option>
              {Object.entries(kindLabels).map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </div>
        <DataState
          loading={resource.loading}
          error={resource.error}
          empty={!resource.loading && filtered.length === 0}
          onRetry={resource.reload}
          emptyTitle="没有匹配的资产"
          emptyDescription="调整筛选条件，或等待 Connector 完成下一轮资产快照。"
        />
        {!resource.loading && !resource.error && filtered.length > 0 && (
          <div className="table-scroll">
            <table className="phase1-table">
              <thead>
                <tr>
                  <th>资产</th>
                  <th>类型</th>
                  <th>来源</th>
                  <th>状态摘要</th>
                  <th>区域</th>
                  <th>新鲜度</th>
                  <th>完整性</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((asset) => (
                  <tr key={asset.id}>
                    <td>
                      <strong>{asset.display_name}</strong>
                      <small>{asset.external_key}</small>
                    </td>
                    <td>{kindLabels[asset.kind]}</td>
                    <td>{asset.source}</td>
                    <td>{String(asset.attributes.state ?? "未采集")}</td>
                    <td>{String(asset.attributes.zone ?? "未采集")}</td>
                    <td>{asset.freshness_seconds}s</td>
                    <td>
                      <Badge variant={asset.partial ? "warning" : "success"}>
                        {asset.partial ? "partial" : "complete"}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </DataSurface>
    </div>
  );
}

export function TopologyPage() {
  const { api } = useSreData();
  const scope = useClusterScope();
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.getTopology(scope.clusterId, signal)
        : Promise.resolve({
            assets: [],
            edges: [],
            observed_at: new Date().toISOString(),
            partial: false,
            warnings: [],
          }),
    [api, scope.clusterId],
  );
  const resource = useAsyncResource(load);
  const assetNames = useMemo(
    () =>
      new Map(
        resource.data?.assets.map((asset) => [
          asset.external_key,
          asset.display_name,
        ]) ?? [],
      ),
    [resource.data?.assets],
  );

  return (
    <div className="page">
      <PageHeader
        eyebrow="VERSIONED TOPOLOGY"
        title="拓扑关系"
        description="按来源展示路由、运行、存储、生产与消费关系；缺边和旧快照不会被推断补齐。"
        actions={
          <ClusterScopeSelect
            clusters={scope.clusters}
            value={scope.clusterId}
            onChange={scope.setClusterId}
          />
        }
      />
      {resource.data?.partial && (
        <div className="partial-notice">
          <GitBranch size={15} />
          <div>
            <strong>拓扑为部分视图</strong>
            <span>{resource.data.warnings.join("；")}</span>
          </div>
        </div>
      )}
      <DataState
        loading={resource.loading}
        error={resource.error}
        empty={
          !resource.loading &&
          (resource.data?.assets.length ?? 0) === 0
        }
        onRetry={resource.reload}
        emptyTitle="尚无拓扑快照"
      />
      {resource.data && resource.data.assets.length > 0 && (
        <div className="phase1-two-column">
          <DataSurface
            title="组件分布"
            description="同一资产在表格与关系中使用稳定 external key。"
            meta={<span>{resource.data.assets.length} 资产</span>}
          >
            <div className="asset-kind-grid">
              {Object.entries(
                resource.data.assets.reduce<Record<string, number>>(
                  (counts, asset) => ({
                    ...counts,
                    [asset.kind]: (counts[asset.kind] ?? 0) + 1,
                  }),
                  {},
                ),
              ).map(([kind, count]) => (
                <div key={kind}>
                  <Boxes size={16} />
                  <strong>{kindLabels[kind as AssetKind]}</strong>
                  <span>{count}</span>
                </div>
              ))}
            </div>
          </DataSurface>
          <DataSurface
            title="快照信息"
            description="Topology diff 使用该时间点作为当前版本。"
          >
            <DefinitionGrid
              items={[
                {
                  label: "集群",
                  value:
                    scope.cluster?.external_cluster_key ?? "未选择",
                },
                {
                  label: "观测时间",
                  value: formatTime(resource.data.observed_at),
                },
                {
                  label: "关系数",
                  value: resource.data.edges.length,
                },
                {
                  label: "完整性",
                  value: resource.data.partial ? "partial" : "complete",
                },
              ]}
            />
          </DataSurface>
        </div>
      )}
      {resource.data && resource.data.edges.length > 0 && (
        <DataSurface
          title="关系清单"
          description="采用可审计表格表达拓扑，不用未经证据支持的视觉推断。"
          meta={<span>{resource.data.edges.length} 条关系</span>}
        >
          <div className="table-scroll">
            <table className="phase1-table">
              <thead>
                <tr>
                  <th>来源资产</th>
                  <th>关系</th>
                  <th>目标资产</th>
                  <th>证据来源</th>
                  <th>观测时间</th>
                  <th>完整性</th>
                </tr>
              </thead>
              <tbody>
                {resource.data.edges.map((edge) => (
                  <tr key={edge.id}>
                    <td>
                      <strong>
                        {assetNames.get(edge.from_key) ?? edge.from_key}
                      </strong>
                      <small>{edge.from_key}</small>
                    </td>
                    <td>
                      <Badge variant="outline">{edge.relation}</Badge>
                    </td>
                    <td>
                      <strong>
                        {assetNames.get(edge.to_key) ?? edge.to_key}
                      </strong>
                      <small>{edge.to_key}</small>
                    </td>
                    <td>{edge.source}</td>
                    <td>{formatTime(edge.observed_at)}</td>
                    <td>{edge.partial ? "partial" : "complete"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </DataSurface>
      )}
    </div>
  );
}

export function OnboardingPage() {
  const { api, demoMode, refresh } = useSreData();
  const auth = useAuth();
  const navigate = useNavigate();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string>();
  const canOnboard =
    auth.mode === "development" ||
    auth.session?.roles.includes("rocketmq:onboard");
  const [input, setInput] = useState<OnboardClusterRequest>({
    tenant_id: auth.session?.tenantId ?? "",
    external_cluster_key: "",
    environment: "预发",
    region: "cn-shanghai",
    rocketmq_version: "5.3.2-rust",
    deployment_mode: "controller",
    owner: auth.session?.subject ?? "",
    actor_subject: auth.session?.subject ?? "",
  });

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!canOnboard) {
      return;
    }
    setSubmitting(true);
    setError(undefined);
    try {
      const result = await api.onboardCluster(input);
      await refresh();
      navigate(`/clusters/${result.cluster.id}`);
    } catch {
      setError(
        "只读接入登记失败。请检查 tenant、内部 onboarding scope 与 Control Plane 状态。",
      );
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="page">
      <PageHeader
        eyebrow="READ-ONLY ONBOARDING"
        title="登记只读集群"
        description="仅登记 Control Plane 元数据并启动 capability handshake，不创建 Topic、不修改 Broker 配置。"
      />
      <ReadOnlyBoundary />
      {!canOnboard ? (
        <DataState
          loading={false}
          error={
            new ApiError(
              403,
              "unauthorized_scope",
              "缺少 rocketmq:onboard scope",
            )
          }
          empty={false}
        />
      ) : (
        <div className="phase1-two-column onboarding-layout">
          <DataSurface
            title="集群身份"
            description="凭据只通过服务端 secret 引用配置，不在浏览器表单中输入。"
          >
            <form className="phase1-form" onSubmit={(event) => void submit(event)}>
              <FormField
                label="外部集群标识"
                value={input.external_cluster_key}
                onChange={(value) =>
                  setInput((current) => ({
                    ...current,
                    external_cluster_key: value,
                  }))
                }
                placeholder="rmq-staging-cn"
                required
              />
              <FormField
                label="环境"
                value={input.environment}
                onChange={(value) =>
                  setInput((current) => ({ ...current, environment: value }))
                }
                required
              />
              <FormField
                label="区域"
                value={input.region}
                onChange={(value) =>
                  setInput((current) => ({ ...current, region: value }))
                }
                required
              />
              <FormField
                label="RocketMQ 版本"
                value={input.rocketmq_version}
                onChange={(value) =>
                  setInput((current) => ({
                    ...current,
                    rocketmq_version: value,
                  }))
                }
                required
              />
              <FormField
                label="部署模式"
                value={input.deployment_mode}
                onChange={(value) =>
                  setInput((current) => ({
                    ...current,
                    deployment_mode: value,
                  }))
                }
                required
              />
              <FormField
                label="Owner"
                value={input.owner}
                onChange={(value) =>
                  setInput((current) => ({ ...current, owner: value }))
                }
                required
              />
              {error && <div className="inline-alert warning">{error}</div>}
              <div className="form-actions">
                <Button asChild variant="ghost">
                  <Link to="/clusters">取消</Link>
                </Button>
                <Button disabled={submitting} type="submit">
                  <ShieldCheck size={15} />
                  {submitting ? "正在登记…" : "验证并登记只读接入"}
                </Button>
              </div>
            </form>
          </DataSurface>
          <DataSurface
            title="接入边界"
            description="提交后仍需 Connector 完成协议、schema、tool digest 和 mutation=false 校验。"
          >
            <ol className="onboarding-steps">
              {[
                "登记 tenant 与 cluster scope",
                "验证 MCP TLS 与 OAuth 身份",
                "固定协议、schema 和 Tool digest",
                "确认 mutation_supported=false",
                "建立只读证据来源状态",
              ].map((step, index) => (
                <li key={step}>
                  <span>{index + 1}</span>
                  <div>
                    <strong>{step}</strong>
                    <small>失败时进入 degraded/rejected，不匿名降级。</small>
                  </div>
                  <ChevronRight size={14} />
                </li>
              ))}
            </ol>
            {demoMode && (
              <div className="partial-notice">
                当前为 Mock API，提交只更新本地会话中的示例状态。
              </div>
            )}
          </DataSurface>
        </div>
      )}
    </div>
  );
}

function FormField({
  label,
  value,
  onChange,
  placeholder,
  required = false,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  required?: boolean;
}) {
  return (
    <label className="form-field">
      <span>{label}</span>
      <input
        className="text-input"
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        required={required}
        value={value}
      />
    </label>
  );
}
