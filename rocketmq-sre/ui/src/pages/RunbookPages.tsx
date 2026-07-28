import {
  BookOpenCheck,
  GitCompareArrows,
  Layers3,
  ShieldCheck,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { createChangeManagementApi } from "@/api/changeManagementClient";
import type { RunbookDefinition } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useSreData } from "@/data/SreDataContext";
import {
  ChangeClusterSelect,
  ChangeStatePanel,
  ChangeWorkspaceNav,
  RunbookDiff,
} from "@/features/change-management/ChangeWorkspace";
import { formatChangeTimestamp } from "@/features/change-management/changePresentation";
import { createMockChangeManagementApi } from "@/data/phase3ChangeDemo";

const EMPTY_RUNBOOK_VERSIONS: RunbookDefinition[] = [];

export function RunbooksPage() {
  const auth = useAuth();
  const { clusters, demoMode } = useSreData();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockChangeManagementApi(auth.requestContext)
          : createChangeManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const [clusterId, setClusterId] = useState("");
  const [runbooks, setRunbooks] = useState<RunbookDefinition[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [beforeVersion, setBeforeVersion] = useState("");
  const [afterVersion, setAfterVersion] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    if (!clusterId && clusters[0]) {
      setClusterId(clusters[0].id);
    }
  }, [clusterId, clusters]);

  useEffect(() => {
    if (!api || !clusterId) {
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void api
      .listRunbooks(clusterId, 256, controller.signal)
      .then((page) => {
        setRunbooks(page.items);
        const first = page.items[0];
        setSelectedId((current) =>
          page.items.some((item) => item.id === current)
            ? current
            : (first?.id ?? ""),
        );
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error
              ? cause.message
              : "Runbook 列表暂不可用",
          );
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [api, clusterId]);

  const families = useMemo(() => groupRunbooks(runbooks), [runbooks]);
  const versions = families.get(selectedId) ?? EMPTY_RUNBOOK_VERSIONS;

  useEffect(() => {
    const newest = versions.at(-1);
    const previous = versions.at(-2) ?? newest;
    setBeforeVersion((current) =>
      versions.some((item) => item.version === current)
        ? current
        : (previous?.version ?? ""),
    );
    setAfterVersion((current) =>
      versions.some((item) => item.version === current)
        ? current
        : (newest?.version ?? ""),
    );
  }, [versions]);

  const before = versions.find((item) => item.version === beforeVersion);
  const after = versions.find((item) => item.version === afterVersion);

  return (
    <div className="page change-page">
      <PageHeader
        eyebrow="P3-11 · COMPOSITE RUNBOOKS"
        title="Runbook 库与版本差异"
        description="审阅组合风险、类型化动作、依赖、人工门与补偿边。Runbook 不能包含 shell、raw RequestCode 或任意 patch。"
        actions={
          <ChangeClusterSelect
            clusters={clusters}
            value={clusterId}
            onValueChange={setClusterId}
          />
        }
      />
      <ChangeWorkspaceNav />

      {loading && runbooks.length === 0 ? (
        <ChangeStatePanel state="loading" title="正在加载 Runbook 库" />
      ) : error && runbooks.length === 0 ? (
        <ChangeStatePanel state="error" title="Runbook 加载失败" detail={error} />
      ) : runbooks.length === 0 ? (
        <ChangeStatePanel
          state="empty"
          title="当前集群尚无 Runbook 版本"
          detail="先通过版本化 Control Plane API 注册已验证的类型化 Runbook。"
        />
      ) : (
        <div className="runbook-workspace">
          <aside className="runbook-library">
            <header>
              <span className="section-kicker">LIBRARY</span>
              <strong>{families.size} 个 Runbook</strong>
            </header>
            <div className="runbook-library-list">
              {[...families.entries()].map(([id, items]) => {
                const latest = items.at(-1);
                return (
                  <button
                    className={selectedId === id ? "active" : ""}
                    key={id}
                    onClick={() => setSelectedId(id)}
                    type="button"
                  >
                    <BookOpenCheck aria-hidden="true" size={17} />
                    <span>
                      <strong>{latest?.name ?? id}</strong>
                      <small>
                        {items.length} 个版本 · {latest?.risk.toUpperCase()}
                      </small>
                    </span>
                    <code>{latest?.version}</code>
                  </button>
                );
              })}
            </div>
          </aside>

          <section className="runbook-inspector">
            <header className="runbook-inspector-header">
              <div>
                <span className="section-kicker">VERSION REVIEW</span>
                <h2>{after?.name}</h2>
                <p>{after?.description}</p>
              </div>
              <div className="runbook-version-pickers">
                <VersionSelect
                  label="基线版本"
                  value={beforeVersion}
                  versions={versions}
                  onValueChange={setBeforeVersion}
                />
                <GitCompareArrows aria-hidden="true" size={17} />
                <VersionSelect
                  label="目标版本"
                  value={afterVersion}
                  versions={versions}
                  onValueChange={setAfterVersion}
                />
              </div>
            </header>

            {after && <RunbookSummary runbook={after} />}
            {before && after && <RunbookDiff before={before} after={after} />}
            {after && <RunbookSteps runbook={after} />}
          </section>
        </div>
      )}
    </div>
  );
}

function VersionSelect({
  label,
  value,
  versions,
  onValueChange,
}: {
  label: string;
  value: string;
  versions: RunbookDefinition[];
  onValueChange: (value: string) => void;
}) {
  return (
    <label className="compact-field">
      <span>{label}</span>
      <Select value={value} onValueChange={onValueChange}>
        <SelectTrigger aria-label={label}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {versions.map((runbook) => (
            <SelectItem key={runbook.version} value={runbook.version}>
              {runbook.version}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </label>
  );
}

function RunbookSummary({ runbook }: { runbook: RunbookDefinition }) {
  return (
    <div className="runbook-summary-strip">
      <div>
        <ShieldCheck size={15} />
        <span>组合风险</span>
        <strong>{runbook.risk.toUpperCase()}</strong>
      </div>
      <div>
        <Layers3 size={15} />
        <span>步骤 / 补偿边</span>
        <strong>
          {runbook.steps.length} / {runbook.compensation_edges.length}
        </strong>
      </div>
      <div>
        <span>最大并发</span>
        <strong>{runbook.max_parallelism}</strong>
      </div>
      <div>
        <span>Owner</span>
        <strong>{runbook.owner}</strong>
      </div>
      <div>
        <span>创建时间</span>
        <strong>{formatChangeTimestamp(runbook.created_at)}</strong>
      </div>
    </div>
  );
}

function RunbookSteps({ runbook }: { runbook: RunbookDefinition }) {
  return (
    <section className="runbook-steps-panel">
      <header>
        <div>
          <span className="section-kicker">TYPED STEPS</span>
          <h2>串行步骤与人工门</h2>
        </div>
        <Badge variant="outline">默认串行</Badge>
      </header>
      <ol>
        {runbook.steps.map((step) => (
          <li key={step.id}>
            <span className="runbook-step-sequence">{step.sequence}</span>
            <div>
              <strong>{step.name}</strong>
              {step.body.kind === "action" ? (
                <>
                  <code>{step.body.action}</code>
                  <span>{step.body.resource}</span>
                </>
              ) : (
                <>
                  <Badge variant="warning">人工门</Badge>
                  <span>
                    {step.body.gate.title} · {step.body.gate.required_role}
                  </span>
                </>
              )}
            </div>
            <small>
              依赖 {step.depends_on.length} ·{" "}
              {step.parallel_group ?? "无并行组"}
            </small>
          </li>
        ))}
      </ol>
    </section>
  );
}

function groupRunbooks(runbooks: RunbookDefinition[]) {
  const grouped = new Map<string, RunbookDefinition[]>();
  for (const runbook of runbooks) {
    const items = grouped.get(runbook.id) ?? [];
    items.push(runbook);
    grouped.set(runbook.id, items);
  }
  for (const items of grouped.values()) {
    items.sort((left, right) =>
      left.version.localeCompare(right.version, undefined, {
        numeric: true,
      }),
    );
  }
  return grouped;
}
