import {
  Boxes,
  FileWarning,
  Fingerprint,
  ScanSearch,
  ShieldCheck,
} from "lucide-react";
import { useMemo } from "react";
import { Link } from "react-router-dom";

import {
  EnterpriseBoundary,
  EnterpriseMetric,
  EnterprisePageFooter,
  EnterpriseScopeBar,
  EnterpriseStatus,
} from "@/components/EnterprisePrimitives";
import { PageHeader } from "@/components/PageHeader";
import { DataState, DataSurface } from "@/components/Phase1Primitives";
import { exportCsv, useEnterpriseData } from "@/hooks/useEnterpriseData";

const PAGE_SIZE = 25;

export function FleetCompliancePage() {
  const resource = useEnterpriseData();
  const snapshot = resource.data;
  const owners = useMemo(
    () =>
      snapshot
        ? [...new Set(snapshot.assets.items.map((item) => item.owner))].sort()
        : [],
    [snapshot],
  );
  const assets = useMemo(() => {
    if (!snapshot) {
      return [];
    }
    const search = resource.filters.search.trim().toLowerCase();
    return snapshot.assets.items.filter(
      (asset) =>
        (!resource.filters.region ||
          asset.region_id === resource.filters.region) &&
        (!resource.filters.environment ||
          asset.environment === resource.filters.environment) &&
        (!resource.filters.owner || asset.owner === resource.filters.owner) &&
        (!resource.filters.health ||
          asset.health === resource.filters.health) &&
        (!search ||
          [
            asset.cluster_id,
            asset.component,
            asset.component_version,
            asset.owner,
          ].some((value) => value.toLowerCase().includes(search))),
    );
  }, [resource.filters, snapshot]);
  const pageCount = Math.max(1, Math.ceil(assets.length / PAGE_SIZE));
  const currentPage = Math.min(resource.filters.page, pageCount);
  const pageAssets = assets.slice(
    (currentPage - 1) * PAGE_SIZE,
    currentPage * PAGE_SIZE,
  );
  const openFindings =
    snapshot?.compliance.items.filter(
      (finding) =>
        finding.state === "open" || finding.state === "acknowledged",
    ) ?? [];
  const criticalFindings = openFindings.filter(
    (finding) => finding.severity === "critical",
  );

  const download = () => {
    exportCsv(
      `rocketmq-compliance-${new Date().toISOString().slice(0, 10)}.csv`,
      [
        "finding_id",
        "cluster_id",
        "region_id",
        "category",
        "severity",
        "state",
        "owner",
        "recommendation",
        "observed_at",
      ],
      (snapshot?.compliance.items ?? []).map((finding) => [
        finding.id,
        finding.cluster_id,
        finding.region_id,
        finding.category,
        finding.severity,
        finding.state,
        finding.owner,
        finding.recommendation,
        finding.observed_at,
      ]),
    );
  };

  return (
    <div className="page enterprise-page compliance-page">
      <PageHeader
        actions={
          <EnterpriseBoundary>
            Finding 仅生成建议或 Action Item；不允许直接 Patch
            线上配置。
          </EnterpriseBoundary>
        }
        description="把组件版本、Feature/Config digest、健康状态与模板偏差放在同一只读视图，明确 missing、exception 与待处置项。"
        eyebrow="FLEET / ASSET & COMPLIANCE"
        title="资产与合规"
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
              detail={`${snapshot.assets.total} total server-side`}
              icon={<Boxes size={18} />}
              label="资产索引"
              value={assets.length}
            />
            <EnterpriseMetric
              detail="当前查询范围"
              icon={<ShieldCheck size={18} />}
              label="Healthy"
              tone="success"
              value={assets.filter((asset) => asset.health === "healthy").length}
            />
            <EnterpriseMetric
              detail={`${criticalFindings.length} critical`}
              icon={<FileWarning size={18} />}
              label="Open findings"
              tone={openFindings.length > 0 ? "warning" : "success"}
              value={openFindings.length}
            />
            <EnterpriseMetric
              detail="Expected vs live digest"
              icon={<Fingerprint size={18} />}
              label="模板覆盖"
              tone={snapshot.compliance.total > 0 ? "warning" : "success"}
              value={`${Math.max(
                0,
                Math.round(
                  (1 - snapshot.compliance.total / snapshot.assets.total) *
                    100,
                ),
              )}%`}
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

          <section className="enterprise-split-grid compliance-primary-grid">
            <DataSurface
              className="asset-index-surface"
              description="Digest 仅展示短指纹，完整值保留在 API 与 Evidence 中。"
              meta={<span>{assets.length} matched</span>}
              title="Fleet Asset Index"
            >
              <div className="enterprise-table-scroll">
                <table className="enterprise-table asset-index-table">
                  <thead>
                    <tr>
                      <th>Component</th>
                      <th>Cluster</th>
                      <th>Version</th>
                      <th>Config digest</th>
                      <th>Owner</th>
                      <th>Health</th>
                    </tr>
                  </thead>
                  <tbody>
                    {pageAssets.map((asset) => (
                      <tr
                        key={`${asset.cluster_id}:${asset.component}`}
                      >
                        <td>
                          <strong>{asset.component}</strong>
                          <small>{asset.environment}</small>
                        </td>
                        <td>
                          <Link
                            to={`/clusters/${encodeURIComponent(asset.cluster_id)}`}
                          >
                            {shortId(asset.cluster_id)}
                          </Link>
                          <small>{asset.region_id}</small>
                        </td>
                        <td>
                          <code>{asset.component_version}</code>
                        </td>
                        <td>
                          <code>
                            {asset.configuration_digest
                              ? shortDigest(asset.configuration_digest)
                              : "missing"}
                          </code>
                        </td>
                        <td>{asset.owner}</td>
                        <td>
                          <EnterpriseStatus value={asset.health} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <EnterprisePageFooter
                onPage={(page) => resource.setFilter("page", page)}
                page={currentPage}
                pageSize={PAGE_SIZE}
                total={assets.length}
              />
            </DataSurface>

            <DataSurface
              className="compliance-findings-surface"
              description="每条差异都保留 expected/live digest 与 Evidence 引用。"
              meta={<span>{openFindings.length} actionable</span>}
              title="Compliance Findings"
            >
              <div className="compliance-finding-list">
                {openFindings.slice(0, 12).map((finding) => (
                  <article key={finding.id}>
                    <header>
                      <span>
                        <ScanSearch size={15} />
                        {finding.category}
                      </span>
                      <EnterpriseStatus value={finding.severity} />
                    </header>
                    <strong>{shortId(finding.cluster_id)}</strong>
                    <p>{finding.recommendation}</p>
                    <dl>
                      <div>
                        <dt>Expected</dt>
                        <dd>{shortDigest(finding.expected_digest)}</dd>
                      </div>
                      <div>
                        <dt>Live</dt>
                        <dd>{shortDigest(finding.live_digest)}</dd>
                      </div>
                    </dl>
                    <footer>
                      <EnterpriseStatus value={finding.state} />
                      <span>{finding.owner}</span>
                      <span>{finding.evidence_ids.length} evidence</span>
                    </footer>
                  </article>
                ))}
                {openFindings.length === 0 && (
                  <div className="enterprise-empty-line">
                    <ShieldCheck size={17} />
                    当前范围没有未完成的合规差异。
                  </div>
                )}
              </div>
            </DataSurface>
          </section>
        </>
      )}
    </div>
  );
}

function shortId(value: string) {
  return value.length > 16
    ? `${value.slice(0, 8)}…${value.slice(-5)}`
    : value;
}

function shortDigest(value: string) {
  return value.length > 22
    ? `${value.slice(0, 14)}…${value.slice(-6)}`
    : value;
}
