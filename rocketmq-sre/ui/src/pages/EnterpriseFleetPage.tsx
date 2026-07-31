import {
  Activity,
  Boxes,
  Gauge,
  Globe2,
  Layers3,
  ServerCog,
  ShieldCheck,
} from "lucide-react";
import { useMemo } from "react";

import type { FleetRegistration } from "@/api/enterpriseTypes";
import {
  EnterpriseBoundary,
  EnterpriseMetric,
  EnterprisePageFooter,
  EnterpriseScopeBar,
  EnterpriseStatus,
} from "@/components/EnterprisePrimitives";
import { PageHeader } from "@/components/PageHeader";
import { DataState, DataSurface } from "@/components/Phase1Primitives";
import {
  exportCsv,
  useEnterpriseData,
  useVirtualRows,
} from "@/hooks/useEnterpriseData";

const PAGE_SIZE = 50;
const ROW_HEIGHT = 58;
const VIEWPORT_HEIGHT = 464;

export function EnterpriseFleetPage() {
  const resource = useEnterpriseData();
  const snapshot = resource.data;
  const filteredRegistrations = useMemo(
    () =>
      snapshot
        ? filterRegistrations(
            snapshot.fleet.registrations,
            resource.filters,
          )
        : [],
    [resource.filters, snapshot],
  );
  const pageCount = Math.max(
    1,
    Math.ceil(filteredRegistrations.length / PAGE_SIZE),
  );
  const currentPage = Math.min(resource.filters.page, pageCount);
  const pageRows = filteredRegistrations.slice(
    (currentPage - 1) * PAGE_SIZE,
    currentPage * PAGE_SIZE,
  );
  const virtual = useVirtualRows(
    pageRows.length,
    ROW_HEIGHT,
    VIEWPORT_HEIGHT,
  );
  const owners = useMemo(
    () =>
      snapshot
        ? [...new Set(snapshot.fleet.registrations.map((item) => item.owner))].sort()
        : [],
    [snapshot],
  );
  const health = useMemo(
    () => fleetHealth(snapshot?.fleet.registrations ?? []),
    [snapshot],
  );
  const runningInspection = snapshot?.inspections.items.find(
    (inspection) => inspection.state === "running",
  );

  const download = () => {
    exportCsv(
      `rocketmq-fleet-${new Date().toISOString().slice(0, 10)}.csv`,
      [
        "cluster",
        "tenant",
        "region",
        "environment",
        "owner",
        "state",
        "revision",
        "updated_at",
      ],
      filteredRegistrations.map((registration) => [
        registration.external_cluster_key,
        registration.tenant_id,
        registration.region_id,
        registration.environment,
        registration.owner,
        registration.state,
        registration.lifecycle_revision,
        registration.updated_at,
      ]),
    );
  };

  return (
    <div className="page enterprise-page fleet-command-page">
      <PageHeader
        eyebrow="ENTERPRISE FLEET / COMMAND DECK"
        title="Fleet 全局态势"
        description="按 Tenant、Region 与 Cluster 汇总 100 集群状态；查询条件可通过 URL 分享，批量操作继续受配额、审批和 Agent 围栏约束。"
        actions={
          <EnterpriseBoundary>
            页面只读取 Fleet 索引；任何发布或变更都跳转到类型化 Plan。
          </EnterpriseBoundary>
        }
      />

      <DataState
        empty={!resource.loading && !snapshot}
        error={resource.error}
        loading={resource.loading && !snapshot}
        onRetry={resource.reload}
      />

      {snapshot && (
        <>
          <section className="enterprise-metric-grid">
            <EnterpriseMetric
              detail={`${snapshot.fleet.tenant.name} · ${snapshot.fleet.fleet.owner}`}
              icon={<Globe2 size={18} />}
              label="纳管集群"
              value={snapshot.fleet.registrations.length}
            />
            <EnterpriseMetric
              detail={`${health.degraded} degraded · ${health.onboarding} onboarding`}
              icon={<Activity size={18} />}
              label="Active"
              tone={health.degraded > 0 ? "warning" : "success"}
              value={health.active}
            />
            <EnterpriseMetric
              detail="驻留标签随查询与导出保留"
              icon={<Layers3 size={18} />}
              label="Region"
              tone="success"
              value={snapshot.fleet.regions.length}
            />
            <EnterpriseMetric
              detail={
                runningInspection
                  ? `${runningInspection.completed_clusters}/${runningInspection.cluster_ids.length} clusters`
                  : "当前无运行中的批量巡检"
              }
              icon={<Gauge size={18} />}
              label="巡检进度"
              tone={runningInspection ? "warning" : "success"}
              value={
                runningInspection
                  ? `${Math.round(
                      (runningInspection.completed_clusters /
                        runningInspection.cluster_ids.length) *
                        100,
                    )}%`
                  : "IDLE"
              }
            />
          </section>

          <EnterpriseScopeBar
            filters={resource.filters}
            onExport={download}
            onFilter={resource.setFilter}
            onReset={resource.resetFilters}
            owners={owners}
            regions={snapshot.fleet.regions.map((region) => ({
              id: region.id,
              label: region.display_name,
            }))}
          />

          <section className="fleet-region-strip" aria-label="区域状态">
            {snapshot.fleet.regions.map((region) => {
              const regionClusters = snapshot.fleet.registrations.filter(
                (registration) => registration.region_id === region.id,
              );
              const degraded = regionClusters.filter(
                (registration) =>
                  registration.state === "read_only_degraded",
              ).length;
              return (
                <button
                  className={
                    resource.filters.region === region.id ? "selected" : ""
                  }
                  key={region.id}
                  onClick={() =>
                    resource.setFilter(
                      "region",
                      resource.filters.region === region.id ? "" : region.id,
                    )
                  }
                  type="button"
                >
                  <span>
                    <Globe2 size={14} />
                    {region.display_name}
                  </span>
                  <strong>{regionClusters.length}</strong>
                  <small>
                    {degraded > 0 ? `${degraded} degraded` : "all nominal"} ·{" "}
                    {region.residency_tags.join(" / ")}
                  </small>
                </button>
              );
            })}
          </section>

          <section className="enterprise-split-grid fleet-primary-grid">
            <DataSurface
              className="fleet-registry-surface"
              description="服务端分页上限 200；当前页进一步采用固定行高虚拟渲染。"
              meta={
                <span className="surface-meta">
                  {filteredRegistrations.length} matched
                </span>
              }
              title="Cluster Registry"
            >
              <div className="fleet-table-header" role="row">
                <span>Cluster</span>
                <span>Region / Env</span>
                <span>Owner</span>
                <span>State</span>
                <span>Revision</span>
                <span>Observed</span>
              </div>
              <div
                className="fleet-virtual-viewport"
                onScroll={virtual.onScroll}
                role="table"
                style={{ height: VIEWPORT_HEIGHT }}
                tabIndex={0}
              >
                <div
                  className="fleet-virtual-spacer"
                  style={{ height: virtual.totalHeight }}
                >
                  <div
                    className="fleet-virtual-window"
                    style={{ transform: `translateY(${virtual.offsetTop}px)` }}
                  >
                    {pageRows
                      .slice(virtual.start, virtual.end)
                      .map((registration) => (
                        <FleetRow
                          key={registration.cluster_id}
                          registration={registration}
                          region={
                            snapshot.fleet.regions.find(
                              (region) =>
                                region.id === registration.region_id,
                            )?.display_name ?? registration.region_id
                          }
                        />
                      ))}
                  </div>
                </div>
              </div>
              <EnterprisePageFooter
                onPage={(page) => resource.setFilter("page", page)}
                page={currentPage}
                pageSize={PAGE_SIZE}
                total={filteredRegistrations.length}
              />
            </DataSurface>

            <div className="fleet-side-stack">
              <DataSurface
                description="状态排序包含文字与图标，不依赖颜色识别。"
                title="Worst clusters"
              >
                <div className="fleet-worst-list">
                  {worstRegistrations(snapshot.fleet.registrations).map(
                    (registration) => (
                      <article key={registration.cluster_id}>
                        <span className="fleet-worst-icon">
                          <ServerCog size={15} />
                        </span>
                        <div>
                          <strong>{registration.external_cluster_key}</strong>
                          <small>
                            {registration.owner} · revision{" "}
                            {registration.lifecycle_revision}
                          </small>
                        </div>
                        <EnterpriseStatus value={registration.state} />
                      </article>
                    ),
                  )}
                </div>
              </DataSurface>

              <DataSurface
                description="运行中任务受并发、Token、Evidence bytes 和超时预算约束。"
                title="Inspection capacity"
              >
                <div className="fleet-inspection-list">
                  {snapshot.inspections.items.map((inspection) => (
                    <article key={inspection.id}>
                      <header>
                        <span>
                          <Boxes size={14} />
                          {inspection.pack_ids.join(" + ")}
                        </span>
                        <EnterpriseStatus value={inspection.state} />
                      </header>
                      <div className="fleet-progress-track">
                        <span
                          style={{
                            width: `${Math.min(
                              100,
                              (inspection.completed_clusters /
                                inspection.cluster_ids.length) *
                                100,
                            )}%`,
                          }}
                        />
                      </div>
                      <footer>
                        <span>
                          {inspection.completed_clusters}/
                          {inspection.cluster_ids.length} clusters
                        </span>
                        <span>
                          max {inspection.max_concurrency} ·{" "}
                          {Math.round(inspection.model_token_budget / 1_000)}K
                          tokens
                        </span>
                      </footer>
                    </article>
                  ))}
                </div>
              </DataSurface>
            </div>
          </section>

          <aside className="enterprise-assurance-strip">
            <ShieldCheck size={16} />
            <strong>Isolation confirmed</strong>
            <span>
              Tenant claim、Region residency 与 Cluster allowlist
              在服务端重新校验；客户端筛选不扩大授权范围。
            </span>
            <code>{snapshot.fleet.schema_version}</code>
          </aside>
        </>
      )}
    </div>
  );
}

function FleetRow({
  registration,
  region,
}: {
  registration: FleetRegistration;
  region: string;
}) {
  return (
    <div className="fleet-table-row" role="row">
      <span>
        <strong>{registration.external_cluster_key}</strong>
        <small>{registration.cluster_id}</small>
      </span>
      <span>
        <strong>{region}</strong>
        <small>{registration.environment}</small>
      </span>
      <span>{registration.owner}</span>
      <span>
        <EnterpriseStatus value={registration.state} />
      </span>
      <code>r{registration.lifecycle_revision}</code>
      <time dateTime={registration.updated_at}>
        {new Date(registration.updated_at).toLocaleTimeString("zh-CN", {
          hour12: false,
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        })}
      </time>
    </div>
  );
}

function filterRegistrations(
  registrations: FleetRegistration[],
  filters: ReturnType<typeof useEnterpriseData>["filters"],
) {
  const search = filters.search.trim().toLowerCase();
  return registrations.filter(
    (registration) =>
      (!filters.region || registration.region_id === filters.region) &&
      (!filters.environment ||
        registration.environment === filters.environment) &&
      (!filters.owner || registration.owner === filters.owner) &&
      (!filters.health ||
        (filters.health === "healthy"
          ? registration.state === "active"
          : filters.health === "degraded"
            ? registration.state === "read_only_degraded"
            : registration.state === filters.health)) &&
      (!search ||
        [
          registration.external_cluster_key,
          registration.cluster_id,
          registration.owner,
          registration.environment,
        ].some((value) => value.toLowerCase().includes(search))),
  );
}

function fleetHealth(registrations: FleetRegistration[]) {
  return {
    active: registrations.filter((item) => item.state === "active").length,
    degraded: registrations.filter(
      (item) => item.state === "read_only_degraded",
    ).length,
    onboarding: registrations.filter(
      (item) => item.state === "onboarding",
    ).length,
  };
}

function worstRegistrations(registrations: FleetRegistration[]) {
  const risk = (registration: FleetRegistration) =>
    registration.state === "read_only_degraded"
      ? 0
      : registration.state === "onboarding"
        ? 1
        : 2;
  return [...registrations]
    .sort(
      (left, right) =>
        risk(left) - risk(right) ||
        left.external_cluster_key.localeCompare(right.external_cluster_key),
    )
    .slice(0, 6);
}
