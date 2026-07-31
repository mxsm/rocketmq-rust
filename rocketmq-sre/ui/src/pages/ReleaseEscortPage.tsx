import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  CircleDot,
  Plus,
  RefreshCw,
  Rocket,
  ShieldCheck,
} from "lucide-react";
import {
  type FormEvent,
  useEffect,
  useMemo,
  useState,
} from "react";
import { Link, useNavigate } from "react-router-dom";

import { createReleaseManagementApi } from "@/api/releaseManagementClient";
import type {
  CreateReleaseRequest,
  ReleaseStatus,
  ReleaseWorkflow,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
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
import { createMockReleaseManagementApi } from "@/data/phase3ReleaseDemo";
import {
  ChangeClusterSelect,
  ChangeStatePanel,
  ChangeWorkspaceNav,
} from "@/features/change-management/ChangeWorkspace";
import {
  ReadinessGateGrid,
  ReleaseSafetyBanner,
  ReleaseStatusBadge,
} from "@/features/release-management/ReleaseWorkspace";
import {
  formatReleaseTime,
  releaseStatusLabel,
} from "@/features/release-management/releasePresentation";

interface ReleaseDraft {
  incidentId: string;
  changeId: string;
  releaseRef: string;
  targetVersion: string;
  runbookId: string;
  runbookVersion: string;
  planId: string;
  planHash: string;
  rollbackPlanId: string;
  rollbackPlanHash: string;
}

const emptyDraft: ReleaseDraft = {
  incidentId: "",
  changeId: "",
  releaseRef: "",
  targetVersion: "",
  runbookId: "",
  runbookVersion: "1.0.0",
  planId: "",
  planHash: "",
  rollbackPlanId: "",
  rollbackPlanHash: "",
};

const statusOptions: Array<ReleaseStatus | "all"> = [
  "all",
  "planned",
  "readiness_checking",
  "ready",
  "canary_running",
  "paused",
  "verifying",
  "rolling_back",
  "rolled_back",
  "completed",
  "manual_takeover",
  "failed",
];

export function ReleaseEscortPage() {
  const auth = useAuth();
  const navigate = useNavigate();
  const { clusters, demoMode } = useSreData();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockReleaseManagementApi(auth.requestContext)
          : createReleaseManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const [clusterId, setClusterId] = useState("");
  const [status, setStatus] = useState<ReleaseStatus | "all">("all");
  const [releases, setReleases] = useState<ReleaseWorkflow[]>([]);
  const [draft, setDraft] = useState<ReleaseDraft>(emptyDraft);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string>();
  const [reloadKey, setReloadKey] = useState(0);

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
      .listReleases(
        clusterId,
        status === "all" ? undefined : status,
        200,
        controller.signal,
      )
      .then((page) => setReleases(page.items))
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error
              ? cause.message
              : "发布工作流暂不可用",
          );
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [api, clusterId, reloadKey, status]);

  const createRelease = async (event: FormEvent) => {
    event.preventDefault();
    if (!api || !clusterId) {
      return;
    }
    if (
      Boolean(draft.rollbackPlanId) !==
      Boolean(draft.rollbackPlanHash)
    ) {
      setError("Rollback Plan ID 与 hash 必须同时填写。");
      return;
    }
    setSubmitting(true);
    setError(undefined);
    const request: CreateReleaseRequest = {
      cluster_id: clusterId,
      incident_id: draft.incidentId.trim(),
      change_id: draft.changeId.trim(),
      release_ref: draft.releaseRef.trim(),
      target_version: draft.targetVersion.trim(),
      runbook_id: draft.runbookId.trim(),
      runbook_version: draft.runbookVersion.trim(),
      plan_id: draft.planId.trim(),
      plan_hash: draft.planHash.trim(),
      rollback_plan_id: draft.rollbackPlanId.trim() || null,
      rollback_plan_hash: draft.rollbackPlanHash.trim() || null,
    };
    try {
      const detail = await api.createRelease(request);
      setDraft(emptyDraft);
      await navigate(`/changes/releases/${detail.workflow.id}`);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "发布工作流创建失败",
      );
    } finally {
      setSubmitting(false);
    }
  };

  const active = releases.filter((item) =>
    [
      "readiness_checking",
      "ready",
      "canary_running",
      "paused",
      "verifying",
      "rolling_back",
    ].includes(item.status),
  ).length;
  const regressions = releases.filter(
    (item) => item.regression_detected,
  ).length;
  const completed = releases.filter(
    (item) => item.status === "completed",
  ).length;

  return (
    <div className="page change-page release-page">
      <PageHeader
        eyebrow="P3-12 · RELEASE ESCORT"
        title="发布护航"
        description="把升级准备、what-if、PDB/容量/Quorum/Store 门禁、Canary SLO、Synthetic Probe、验证与类型化回滚放在同一审计链。"
        actions={
          <div className="release-header-actions">
            <ChangeClusterSelect
              clusters={clusters}
              value={clusterId}
              onValueChange={setClusterId}
            />
            <Select
              value={status}
              onValueChange={(value) =>
                setStatus(value as ReleaseStatus | "all")
              }
            >
              <SelectTrigger
                aria-label="发布状态筛选"
                className="release-status-filter"
              >
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {statusOptions.map((option) => (
                  <SelectItem key={option} value={option}>
                    {option === "all"
                      ? "全部状态"
                      : releaseStatusLabel(option)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              aria-label="刷新发布状态"
              onClick={() => setReloadKey((current) => current + 1)}
              size="icon"
              variant="outline"
            >
              <RefreshCw aria-hidden="true" size={15} />
            </Button>
          </div>
        }
      />
      <ChangeWorkspaceNav />
      <ReleaseSafetyBanner />

      <section className="release-summary-strip">
        <div>
          <span className="summary-icon info">
            <Rocket aria-hidden="true" size={17} />
          </span>
          <span>当前范围</span>
          <strong>{releases.length}</strong>
          <small>release workflows</small>
        </div>
        <div>
          <span className="summary-icon live">
            <CircleDot aria-hidden="true" size={17} />
          </span>
          <span>正在护航</span>
          <strong>{active}</strong>
          <small>active / paused</small>
        </div>
        <div>
          <span className="summary-icon warning">
            <AlertTriangle aria-hidden="true" size={17} />
          </span>
          <span>检测到回归</span>
          <strong>{regressions}</strong>
          <small>fail-closed</small>
        </div>
        <div>
          <span className="summary-icon success">
            <CheckCircle2 aria-hidden="true" size={17} />
          </span>
          <span>已完成</span>
          <strong>{completed}</strong>
          <small>report generated</small>
        </div>
      </section>

      {loading && (
        <ChangeStatePanel
          state="loading"
          title="正在加载发布护航工作流"
          detail="读取发布状态、门禁摘要和最近更新时间。"
        />
      )}
      {error && (
        <ChangeStatePanel
          state="error"
          title="发布护航暂不可用"
          detail={error}
        />
      )}

      <div className="release-list-workspace">
        <section className="release-list-panel">
          <header>
            <div>
              <span className="section-kicker">RELEASE WORKFLOWS</span>
              <h2>护航队列</h2>
              <p>回归会自动暂停；没有有效 rollback approval 时只能进入人工接管。</p>
            </div>
            <Badge variant="secondary">{releases.length} ITEMS</Badge>
          </header>
          <div className="release-list">
            {releases.map((release) => (
              <article className="release-list-row" key={release.id}>
                <span
                  className={`release-row-indicator ${release.status}`}
                  aria-hidden="true"
                />
                <div className="release-row-primary">
                  <header>
                    <strong>{release.release_ref}</strong>
                    <ReleaseStatusBadge status={release.status} />
                  </header>
                  <span>
                    {release.change_id} · target {release.target_version}
                  </span>
                  <footer>
                    <code>{release.id.slice(0, 8)}</code>
                    <span>Runbook {release.runbook_version}</span>
                    <span>{release.created_by}</span>
                  </footer>
                </div>
                <div className="release-row-readiness">
                  <ReadinessGateGrid readiness={release.readiness} />
                </div>
                <time dateTime={release.updated_at}>
                  {formatReleaseTime(release.updated_at)}
                </time>
                <Button asChild size="icon" variant="ghost">
                  <Link
                    aria-label={`打开 ${release.release_ref}`}
                    to={`/changes/releases/${release.id}`}
                  >
                    <ArrowRight aria-hidden="true" size={16} />
                  </Link>
                </Button>
              </article>
            ))}
            {!loading && releases.length === 0 && (
              <ChangeStatePanel
                state="empty"
                title="当前范围没有发布工作流"
                detail="右侧可绑定已批准 Plan、Rollback Plan 与 Runbook 创建护航流程。"
              />
            )}
          </div>
        </section>

        <section className="release-composer-panel">
          <header>
            <span className="composer-icon">
              <Plus aria-hidden="true" size={17} />
            </span>
            <div>
              <span className="section-kicker">PLAN-BOUND RELEASE</span>
              <h2>创建发布护航</h2>
              <p>只绑定不可变对象，不接受 shell、raw request 或任意 patch。</p>
            </div>
          </header>
          <form className="change-form" onSubmit={createRelease}>
            <TextField
              label="Incident UUID"
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  incidentId: value,
                }))
              }
              placeholder="Incident UUID"
              value={draft.incidentId}
            />
            <TextField
              label="Change ID"
              maxLength={256}
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  changeId: value,
                }))
              }
              placeholder="CHG-20260728-018"
              value={draft.changeId}
            />
            <TextField
              label="Release reference"
              maxLength={256}
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  releaseRef: value,
                }))
              }
              placeholder="REL-2026.07.28-PROXY"
              value={draft.releaseRef}
            />
            <TextField
              label="Target version"
              maxLength={128}
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  targetVersion: value,
                }))
              }
              placeholder="5.3.0"
              value={draft.targetVersion}
            />
            <TextField
              label="Runbook UUID"
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  runbookId: value,
                }))
              }
              placeholder="Runbook UUID"
              value={draft.runbookId}
            />
            <TextField
              label="Runbook version"
              maxLength={64}
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  runbookVersion: value,
                }))
              }
              placeholder="1.0.0"
              value={draft.runbookVersion}
            />
            <TextField
              className="span-2"
              label="Approved Plan UUID"
              onChange={(value) =>
                setDraft((current) => ({ ...current, planId: value }))
              }
              placeholder="Action Plan UUID"
              value={draft.planId}
            />
            <TextField
              className="span-2"
              label="Approved Plan hash"
              maxLength={71}
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  planHash: value,
                }))
              }
              placeholder="sha256:…"
              value={draft.planHash}
            />
            <TextField
              className="span-2"
              label="Rollback Plan UUID（可选）"
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  rollbackPlanId: value,
                }))
              }
              placeholder="Rollback Action Plan UUID"
              required={false}
              value={draft.rollbackPlanId}
            />
            <TextField
              className="span-2"
              label="Rollback Plan hash（可选）"
              maxLength={71}
              onChange={(value) =>
                setDraft((current) => ({
                  ...current,
                  rollbackPlanHash: value,
                }))
              }
              placeholder="sha256:…"
              required={false}
              value={draft.rollbackPlanHash}
            />
            <div className="release-form-boundary span-2">
              <ShieldCheck aria-hidden="true" size={15} />
              <span>
                Control Plane 会重新校验 approval、runbook action sequence、集群范围和 expiry。
              </span>
            </div>
            <Button
              className="span-2"
              disabled={submitting}
              type="submit"
            >
              <Rocket aria-hidden="true" size={15} />
              {submitting ? "正在创建…" : "创建发布护航"}
            </Button>
          </form>
        </section>
      </div>
    </div>
  );
}

function TextField({
  label,
  value,
  placeholder,
  onChange,
  className,
  maxLength = 128,
  required = true,
}: {
  label: string;
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
  className?: string;
  maxLength?: number;
  required?: boolean;
}) {
  return (
    <label className={className}>
      <span>{label}</span>
      <input
        maxLength={maxLength}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        required={required}
        value={value}
      />
    </label>
  );
}
