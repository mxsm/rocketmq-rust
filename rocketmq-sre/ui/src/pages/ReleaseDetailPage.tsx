import {
  ArrowLeft,
  CalendarClock,
  FileCheck2,
  Fingerprint,
  RefreshCw,
  Rocket,
  ShieldCheck,
} from "lucide-react";
import {
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { Link, useParams } from "react-router-dom";

import { createReleaseManagementApi } from "@/api/releaseManagementClient";
import type {
  CompleteRollbackRequest,
  PrepareReleaseRequest,
  RecordReleaseObservationRequest,
  ReleaseDetail,
  ReleaseExecutionRequest,
  ReleaseTransitionRequest,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { createMockReleaseManagementApi } from "@/data/phase3ReleaseDemo";
import {
  ChangeStatePanel,
  ChangeWorkspaceNav,
} from "@/features/change-management/ChangeWorkspace";
import {
  type ReleaseDetailActions,
  ReleaseDetailControls,
} from "@/features/release-management/ReleaseDetailControls";
import {
  ReadinessGateGrid,
  ReleaseObservationTimeline,
  ReleaseProgressRail,
  ReleaseSafetyBanner,
  ReleaseStatusBadge,
} from "@/features/release-management/ReleaseWorkspace";
import {
  formatReleaseTime,
  observationPhaseLabel,
} from "@/features/release-management/releasePresentation";

export function ReleaseDetailPage() {
  const { releaseId = "" } = useParams();
  const auth = useAuth();
  const { demoMode } = useSreData();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockReleaseManagementApi(auth.requestContext)
          : createReleaseManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const [detail, setDetail] = useState<ReleaseDetail>();
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string>();
  const [error, setError] = useState<string>();
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    if (!api || !releaseId) {
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void api
      .getRelease(releaseId, controller.signal)
      .then(setDetail)
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error ? cause.message : "发布护航详情暂不可用",
          );
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [api, releaseId, reloadKey]);

  const run = useCallback(
    async (label: string, operation: () => Promise<unknown>) => {
      if (!api || !releaseId) {
        return;
      }
      setBusy(label);
      setError(undefined);
      try {
        await operation();
        setDetail(await api.getRelease(releaseId));
      } catch (cause) {
        setError(
          cause instanceof Error ? cause.message : `${label}执行失败`,
        );
      } finally {
        setBusy(undefined);
      }
    },
    [api, releaseId],
  );

  const actions = useMemo<ReleaseDetailActions | undefined>(() => {
    if (!api || !releaseId) {
      return undefined;
    }
    return {
      prepare: (input: PrepareReleaseRequest) =>
        run("准备检查", () => api.prepareRelease(releaseId, input)),
      start: (input: ReleaseExecutionRequest) =>
        run("启动 Canary", () => api.startRelease(releaseId, input)),
      observe: (input: RecordReleaseObservationRequest) =>
        run("记录观察", () =>
          api.recordReleaseObservation(releaseId, input),
        ),
      pause: (input: ReleaseTransitionRequest) =>
        run("暂停发布", () => api.pauseRelease(releaseId, input)),
      resume: (input: ReleaseTransitionRequest) =>
        run("恢复发布", () => api.resumeRelease(releaseId, input)),
      beginVerification: () =>
        run("进入验证", () =>
          api.beginReleaseVerification(releaseId),
        ),
      complete: () =>
        run("完成发布", () => api.completeRelease(releaseId)),
      startRollback: (input: ReleaseExecutionRequest) =>
        run("启动回滚", () =>
          api.startReleaseRollback(releaseId, input),
        ),
      completeRollback: (input: CompleteRollbackRequest) =>
        run("核对回滚", () =>
          api.completeReleaseRollback(releaseId, input),
        ),
      manualTakeover: (input: ReleaseTransitionRequest) =>
        run("人工接管", () =>
          api.enterManualTakeover(releaseId, input),
        ),
    };
  }, [api, releaseId, run]);

  const workflow = detail?.workflow;
  const before = detail?.observations.filter(
    (item) => item.phase === "before",
  );
  const during = detail?.observations.filter(
    (item) => item.phase === "during",
  );
  const after = detail?.observations.filter(
    (item) => item.phase === "after",
  );

  return (
    <div className="page change-page release-detail-page">
      <PageHeader
        eyebrow="P3-12 · RELEASE ESCORT"
        title={workflow?.release_ref ?? "发布护航详情"}
        description={
          workflow
            ? `${workflow.change_id} · target ${workflow.target_version} · 所有推进都复用批准的执行链路`
            : "读取不可变 Plan、准备门禁、实时观察与发布报告。"
        }
        actions={
          <div className="release-header-actions">
            {workflow && <ReleaseStatusBadge status={workflow.status} />}
            <Button asChild variant="outline">
              <Link to="/changes/releases">
                <ArrowLeft aria-hidden="true" size={15} />
                返回护航队列
              </Link>
            </Button>
            <Button
              aria-label="刷新发布护航详情"
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

      {loading && (
        <ChangeStatePanel
          state="loading"
          title="正在加载发布护航详情"
          detail="读取 Plan 绑定、readiness、观察与报告。"
        />
      )}
      {error && (
        <ChangeStatePanel
          action={
            <Button
              onClick={() => setReloadKey((current) => current + 1)}
              size="sm"
              variant="outline"
            >
              重试
            </Button>
          }
          state="error"
          title="发布护航操作未完成"
          detail={error}
        />
      )}

      {workflow && detail && actions && (
        <>
          <section className="release-detail-hero">
            <div className="release-detail-heading">
              <span className="release-detail-icon">
                <Rocket aria-hidden="true" size={22} />
              </span>
              <div>
                <span className="section-kicker">WORKFLOW CONTROL</span>
                <h2>{workflow.release_ref}</h2>
                <p>
                  {workflow.change_id} · {workflow.created_by} ·{" "}
                  {formatReleaseTime(workflow.created_at)}
                </p>
              </div>
            </div>
            <ReleaseProgressRail status={workflow.status} />
          </section>

          <section className="release-binding-grid">
            <BindingCard
              icon={<Fingerprint aria-hidden="true" size={16} />}
              label="Approved Plan"
              primary={shortId(workflow.plan_id)}
              secondary={shortDigest(workflow.plan_hash)}
            />
            <BindingCard
              icon={<ShieldCheck aria-hidden="true" size={16} />}
              label="Rollback Plan"
              primary={
                workflow.rollback_plan_id
                  ? shortId(workflow.rollback_plan_id)
                  : "未绑定"
              }
              secondary={
                workflow.rollback_plan_hash
                  ? shortDigest(workflow.rollback_plan_hash)
                  : "仅允许人工接管"
              }
              tone={workflow.rollback_plan_id ? "safe" : "warning"}
            />
            <BindingCard
              icon={<FileCheck2 aria-hidden="true" size={16} />}
              label="Typed Runbook"
              primary={shortId(workflow.runbook_id)}
              secondary={`version ${workflow.runbook_version}`}
            />
            <BindingCard
              icon={<CalendarClock aria-hidden="true" size={16} />}
              label="Last reconciled"
              primary={formatReleaseTime(workflow.updated_at)}
              secondary={
                workflow.active_execution_id
                  ? `execution ${shortId(workflow.active_execution_id)}`
                  : "无活动执行"
              }
            />
          </section>

          <div className="release-detail-workspace">
            <main className="release-detail-main">
              <section className="release-panel">
                <header>
                  <div>
                    <span className="section-kicker">
                      DETERMINISTIC READINESS
                    </span>
                    <h2>发布准备门禁</h2>
                  </div>
                  <Badge
                    variant={workflow.readiness ? "success" : "warning"}
                  >
                    {workflow.readiness ? "EVALUATED" : "PENDING"}
                  </Badge>
                </header>
                <ReadinessGateGrid readiness={workflow.readiness} />
                {workflow.readiness && (
                  <footer className="release-readiness-meta">
                    <span>
                      有效至{" "}
                      {formatReleaseTime(workflow.readiness.valid_until)}
                    </span>
                    <code>
                      {workflow.readiness.evidence_ids.length} evidence
                    </code>
                  </footer>
                )}
              </section>

              <section className="release-panel">
                <header>
                  <div>
                    <span className="section-kicker">
                      BOUNDED OBSERVATIONS
                    </span>
                    <h2>发布观察时间线</h2>
                  </div>
                  <Badge
                    variant={
                      workflow.regression_detected
                        ? "destructive"
                        : "secondary"
                    }
                  >
                    {detail.observations.length} SAMPLES
                  </Badge>
                </header>
                <ReleaseObservationTimeline
                  observations={detail.observations}
                />
              </section>

              {detail.report && (
                <ReleaseReportView
                  after={after ?? []}
                  before={before ?? []}
                  during={during ?? []}
                  generatedAt={detail.report.generated_at}
                  reportId={detail.report.id}
                />
              )}
            </main>
            <ReleaseDetailControls
              actions={actions}
              busy={busy}
              workflow={workflow}
            />
          </div>
        </>
      )}
    </div>
  );
}

function BindingCard({
  icon,
  label,
  primary,
  secondary,
  tone,
}: {
  icon: ReactNode;
  label: string;
  primary: string;
  secondary: string;
  tone?: "safe" | "warning";
}) {
  return (
    <article className={`release-binding-card${tone ? ` ${tone}` : ""}`}>
      <span>{icon}</span>
      <div>
        <small>{label}</small>
        <strong>{primary}</strong>
        <code>{secondary}</code>
      </div>
    </article>
  );
}

function ReleaseReportView({
  reportId,
  generatedAt,
  before,
  during,
  after,
}: {
  reportId: string;
  generatedAt: string;
  before: ReleaseDetail["observations"];
  during: ReleaseDetail["observations"];
  after: ReleaseDetail["observations"];
}) {
  const sections = [
    { id: "before", title: "发布前", items: before },
    { id: "during", title: "发布中", items: during },
    { id: "after", title: "发布后", items: after },
  ];
  return (
    <section className="release-panel release-report-panel">
      <header>
        <div>
          <span className="section-kicker">IMMUTABLE REPORT</span>
          <h2>发布结果报告</h2>
          <p>
            {shortId(reportId)} · {formatReleaseTime(generatedAt)}
          </p>
        </div>
        <Badge variant="success">SEALED</Badge>
      </header>
      <div className="release-report-grid">
        {sections.map((section) => (
          <article key={section.id}>
            <strong>{section.title}</strong>
            <span>{section.items.length} 次观察</span>
            {section.items.length === 0 ? (
              <p>未记录该阶段的观察。</p>
            ) : (
              section.items.map((item, index) => (
                <div
                  className={
                    item.regression_detected ? "regression" : "healthy"
                  }
                  key={`${item.observed_at}-${index}`}
                >
                  <small>{observationPhaseLabel(item.phase)}</small>
                  <p>{item.sanitized_summary}</p>
                </div>
              ))
            )}
          </article>
        ))}
      </div>
    </section>
  );
}

function shortId(value: string) {
  return value.slice(0, 8);
}

function shortDigest(value: string) {
  return value.length > 22
    ? `${value.slice(0, 14)}…${value.slice(-6)}`
    : value;
}
