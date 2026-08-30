import {
  BadgeCheck,
  FileKey2,
  GitCompareArrows,
  KeyRound,
  Network,
  ShieldCheck,
  ShieldX,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { loadGovernanceVersions } from "@/api/enterprise";
import type {
  GovernanceArtifact,
  GovernanceVersionPage,
} from "@/api/enterpriseTypes";
import { useAuth } from "@/auth/AuthContext";
import {
  EnterpriseBoundary,
  EnterpriseMetric,
  EnterpriseScopeBar,
  EnterpriseStatus,
} from "@/components/EnterprisePrimitives";
import { PageHeader } from "@/components/PageHeader";
import { DataState, DataSurface } from "@/components/Phase1Primitives";
import { useSreData } from "@/data/SreDataContext";
import { exportCsv, useEnterpriseData } from "@/hooks/useEnterpriseData";

export function GovernanceCenterPage() {
  const auth = useAuth();
  const { demoMode } = useSreData();
  const resource = useEnterpriseData();
  const snapshot = resource.data;
  const [selectedId, setSelectedId] = useState<string>();
  const [versions, setVersions] = useState<GovernanceVersionPage>();
  const [versionError, setVersionError] = useState<unknown>();
  const [versionLoading, setVersionLoading] = useState(false);
  const artifacts = useMemo(() => {
    const search = resource.filters.search.trim().toLowerCase();
    return (snapshot?.governanceArtifacts.items ?? []).filter(
      (artifact) =>
        (!resource.filters.owner ||
          artifact.owner === resource.filters.owner) &&
        (!search ||
          [
            artifact.logical_key,
            artifact.kind,
            artifact.owner,
            artifact.reviewer,
          ].some((value) => value.toLowerCase().includes(search))),
    );
  }, [resource.filters.owner, resource.filters.search, snapshot]);
  const owners = useMemo(
    () =>
      snapshot
        ? [
            ...new Set(
              snapshot.governanceArtifacts.items.map(
                (artifact) => artifact.owner,
              ),
            ),
          ].sort()
        : [],
    [snapshot],
  );
  const effectiveSelected =
    artifacts.find((artifact) => artifact.id === selectedId) ?? artifacts[0];

  useEffect(() => {
    if (!effectiveSelected) {
      setVersions(undefined);
      return;
    }
    const controller = new AbortController();
    setVersionLoading(true);
    setVersionError(undefined);
    void loadGovernanceVersions(
      effectiveSelected.id,
      auth.requestContext,
      demoMode,
      controller.signal,
    )
      .then((page) => {
        setVersions(page);
        setVersionLoading(false);
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) {
          setVersionError(error);
          setVersionLoading(false);
        }
      });
    return () => controller.abort();
  }, [auth.requestContext, demoMode, effectiveSelected]);

  const compliance = snapshot?.governanceCompliance;
  const download = () => {
    exportCsv(
      `rocketmq-governance-${new Date().toISOString().slice(0, 10)}.csv`,
      [
        "artifact_id",
        "kind",
        "logical_key",
        "owner",
        "reviewer",
        "current_version_id",
        "updated_at",
      ],
      artifacts.map((artifact) => [
        artifact.id,
        artifact.kind,
        artifact.logical_key,
        artifact.owner,
        artifact.reviewer,
        artifact.current_version_id,
        artifact.updated_at,
      ]),
    );
  };

  return (
    <div className="page enterprise-page governance-center-page">
      <PageHeader
        actions={
          <EnterpriseBoundary>
            模型仅可创建 Draft；Review、签名、Active、Quarantine 与 Retire
            必须由有权限的人类或服务执行。
          </EnterpriseBoundary>
        }
        description="集中管理 Data、Prompt、Knowledge、Model、Pack、Policy、Action、Runbook 与 Integration 的签名版本、依赖、影响和生命周期。"
        eyebrow="PLATFORM / GOVERNANCE CENTER"
        title="治理中心"
      />

      <DataState
        empty={!resource.loading && !snapshot}
        error={resource.error}
        loading={resource.loading && !snapshot}
        onRetry={resource.reload}
      />

      {snapshot && compliance && (
        <>
          <section className="enterprise-metric-grid">
            <EnterpriseMetric
              detail={`${artifacts.length} visible in current filter`}
              icon={<FileKey2 size={18} />}
              label="治理对象"
              value={snapshot.governanceArtifacts.items.length}
            />
            <EnterpriseMetric
              detail="Active versions without signature"
              icon={<KeyRound size={18} />}
              label="Unsigned"
              tone={compliance.unsigned_active > 0 ? "critical" : "success"}
              value={compliance.unsigned_active}
            />
            <EnterpriseMetric
              detail={`${compliance.overdue_review} overdue review`}
              icon={<GitCompareArrows size={18} />}
              label="Quarantined"
              tone={compliance.quarantined > 0 ? "warning" : "success"}
              value={compliance.quarantined}
            />
            <EnterpriseMetric
              detail={formatObserved(compliance.observed_at)}
              icon={
                compliance.compliant ? (
                  <ShieldCheck size={18} />
                ) : (
                  <ShieldX size={18} />
                )
              }
              label="Compliance"
              tone={compliance.compliant ? "success" : "warning"}
              value={compliance.compliant ? "PASS" : "ATTENTION"}
            />
          </section>

          <EnterpriseScopeBar
            filters={resource.filters}
            onExport={download}
            onFilter={resource.setFilter}
            onReset={resource.resetFilters}
            owners={owners}
            regions={[]}
            showHealth={false}
          />

          <section className="enterprise-split-grid governance-primary-grid">
            <DataSurface
              className="governance-artifact-surface"
              description="逻辑对象与内容版本分离；当前版本缺失会明确显示 missing。"
              meta={<span>{artifacts.length} artifacts</span>}
              title="Governed Registry"
            >
              <div className="governance-kind-matrix">
                {kindCounts(snapshot.governanceArtifacts.items).map(
                  ([kind, count]) => (
                    <span key={kind}>
                      <strong>{count}</strong>
                      {kind.replaceAll("_", " ")}
                    </span>
                  ),
                )}
              </div>
              <div className="governance-artifact-list">
                {artifacts.map((artifact) => (
                  <button
                    className={
                      artifact.id === effectiveSelected?.id ? "selected" : ""
                    }
                    key={artifact.id}
                    onClick={() => setSelectedId(artifact.id)}
                    type="button"
                  >
                    <span className="governance-artifact-icon">
                      <Network size={15} />
                    </span>
                    <span>
                      <strong>{artifact.logical_key}</strong>
                      <small>
                        {artifact.kind.replaceAll("_", " ")} ·{" "}
                        {artifact.owner}
                      </small>
                    </span>
                    <code>
                      {artifact.current_version_id
                        ? shortId(artifact.current_version_id)
                        : "missing"}
                    </code>
                  </button>
                ))}
              </div>
            </DataSurface>

            <DataSurface
              className="governance-version-surface"
              description="签名、期限、依赖和回滚目标均参与高权限 admission。"
              meta={
                effectiveSelected ? (
                  <code>{shortId(effectiveSelected.id)}</code>
                ) : undefined
              }
              title="Version Inspector"
            >
              <DataState
                empty={
                  !versionLoading &&
                  !versionError &&
                  (versions?.items.length ?? 0) === 0
                }
                emptyDescription="该治理对象还没有可读取的版本。"
                emptyTitle="版本缺失"
                error={versionError}
                loading={versionLoading}
              />
              {effectiveSelected && versions && versions.items.length > 0 && (
                <div className="governance-version-list">
                  {versions.items.map((version) => (
                    <article key={version.id}>
                      <header>
                        <div>
                          <span>VERSION {version.version}</span>
                          <h3>{effectiveSelected.logical_key}</h3>
                        </div>
                        <EnterpriseStatus value={version.state} />
                      </header>
                      <dl>
                        <Definition
                          label="Signature"
                          value={
                            version.signature
                              ? `${version.signature.algorithm} · ${version.signature.key_id}`
                              : "missing"
                          }
                          warning={!version.signature}
                        />
                        <Definition
                          label="Content digest"
                          value={shortDigest(version.content_digest)}
                        />
                        <Definition
                          label="Version range"
                          value={version.applicable_version_range}
                        />
                        <Definition
                          label="Review due"
                          value={formatObserved(version.review_due_at)}
                          warning={
                            Date.parse(version.review_due_at) < Date.now()
                          }
                        />
                        <Definition
                          label="Rollback"
                          value={
                            version.rollback_version_id
                              ? shortId(version.rollback_version_id)
                              : "not configured"
                          }
                        />
                        <Definition
                          label="Created by"
                          value={version.created_by}
                        />
                      </dl>
                      <section className="governance-dependencies">
                        <span>DEPENDENCIES</span>
                        {version.dependencies.length > 0 ? (
                          version.dependencies.map((dependency) => (
                            <code
                              key={`${dependency.kind}:${dependency.logical_key}:${dependency.version}`}
                            >
                              {dependency.kind}/{dependency.logical_key}@
                              {dependency.version}
                            </code>
                          ))
                        ) : (
                          <code>none</code>
                        )}
                      </section>
                      <footer>
                        <span>
                          <BadgeCheck size={14} />
                          {version.applicable_components.join(" / ")}
                        </span>
                        <code>{shortId(version.id)}</code>
                      </footer>
                    </article>
                  ))}
                </div>
              )}
            </DataSurface>
          </section>

          <aside className="governance-admission-strip">
            <div>
              <strong>High-privilege admission</strong>
              <span>
                unsigned · expired · overdue · quarantined → fail closed
              </span>
            </div>
            <div>
              <strong>Read-only admission</strong>
              <span>
                可在非 quarantine 情况下降级，并输出明确 reason code
              </span>
            </div>
            <code>{snapshot.governanceArtifacts.schema_version}</code>
          </aside>
        </>
      )}
    </div>
  );
}

function Definition({
  label,
  value,
  warning = false,
}: {
  label: string;
  value: string;
  warning?: boolean;
}) {
  return (
    <div>
      <dt>{label}</dt>
      <dd className={warning ? "warning" : undefined}>{value}</dd>
    </div>
  );
}

function kindCounts(artifacts: GovernanceArtifact[]) {
  const counts = new Map<string, number>();
  for (const artifact of artifacts) {
    counts.set(artifact.kind, (counts.get(artifact.kind) ?? 0) + 1);
  }
  return [...counts].sort(([left], [right]) => left.localeCompare(right));
}

function formatObserved(value: string) {
  return new Date(value).toLocaleString("zh-CN", {
    hour12: false,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
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
