import {
  AlarmClock,
  ArchiveRestore,
  CheckCheck,
  ClipboardCheck,
  DatabaseBackup,
  ShieldAlert,
} from "lucide-react";
import { useMemo } from "react";

import {
  EnterpriseBoundary,
  EnterpriseMetric,
  EnterpriseScopeBar,
  EnterpriseStatus,
} from "@/components/EnterprisePrimitives";
import { PageHeader } from "@/components/PageHeader";
import { DataState, DataSurface } from "@/components/Phase1Primitives";
import { exportCsv, useEnterpriseData } from "@/hooks/useEnterpriseData";

export function DrCenterPage() {
  const resource = useEnterpriseData();
  const snapshot = resource.data;
  const plans = useMemo(
    () =>
      (snapshot?.drPlans.items ?? []).filter(
        (plan) =>
          (!resource.filters.region ||
            plan.region_id === resource.filters.region) &&
          (!resource.filters.search ||
            [plan.name, plan.subject, plan.owner].some((value) =>
              value
                .toLowerCase()
                .includes(resource.filters.search.toLowerCase()),
            )),
      ),
    [resource.filters.region, resource.filters.search, snapshot],
  );
  const exercises = useMemo(
    () =>
      (snapshot?.drExercises.items ?? []).filter(
        (exercise) =>
          (!resource.filters.region ||
            exercise.region_id === resource.filters.region) &&
          (!resource.filters.search ||
            [exercise.id, exercise.mode, exercise.state].some((value) =>
              value
                .toLowerCase()
                .includes(resource.filters.search.toLowerCase()),
            )),
      ),
    [resource.filters.region, resource.filters.search, snapshot],
  );
  const completed = exercises.filter(
    (exercise) => exercise.state === "completed",
  );
  const latestCompleted = completed.at(0);
  const pendingActions =
    snapshot?.drActionItems.items.filter(
      (item) => item.status !== "done" && item.status !== "cancelled",
    ) ?? [];

  const download = () => {
    exportCsv(
      `rocketmq-dr-exercises-${new Date().toISOString().slice(0, 10)}.csv`,
      [
        "exercise_id",
        "plan_id",
        "cluster_id",
        "region_id",
        "mode",
        "state",
        "target_rto_seconds",
        "actual_rto_seconds",
        "target_rpo_seconds",
        "actual_rpo_seconds",
        "cleanup_complete",
        "updated_at",
      ],
      exercises.map((exercise) => [
        exercise.id,
        exercise.plan_id,
        exercise.cluster_id,
        exercise.region_id,
        exercise.mode,
        exercise.state,
        exercise.target.rto_seconds,
        exercise.actual_rto_seconds,
        exercise.target.rpo_seconds,
        exercise.actual_rpo_seconds,
        exercise.cleanup_complete,
        exercise.updated_at,
      ]),
    );
  };

  return (
    <div className="page enterprise-page dr-center-page">
      <PageHeader
        actions={
          <EnterpriseBoundary>
            DR Center 只允许 readiness、tabletop 与 supervised-test；
            不提供生产切流入口。
          </EnterpriseBoundary>
        }
        description="统一查看 AI SRE 自身与 RocketMQ 的恢复计划、RTO/RPO、人工确认点、清理状态、Evidence 和跟进 Action Item。"
        eyebrow="RESILIENCE / DR CENTER"
        title="灾备与恢复中心"
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
              detail={`${plans.filter((plan) => plan.active).length} active`}
              icon={<DatabaseBackup size={18} />}
              label="恢复计划"
              value={plans.length}
            />
            <EnterpriseMetric
              detail={`${exercises.length - completed.length} pending / running`}
              icon={<ArchiveRestore size={18} />}
              label="演练完成"
              tone={completed.length > 0 ? "success" : "warning"}
              value={completed.length}
            />
            <EnterpriseMetric
              detail={
                latestCompleted
                  ? `target ${formatDuration(latestCompleted.target.rto_seconds)}`
                  : "尚无完成记录"
              }
              icon={<AlarmClock size={18} />}
              label="最近实际 RTO"
              tone={latestCompleted ? "success" : "warning"}
              value={
                latestCompleted?.actual_rto_seconds
                  ? formatDuration(latestCompleted.actual_rto_seconds)
                  : "missing"
              }
            />
            <EnterpriseMetric
              detail={`${snapshot.drActionItems.items.length} total`}
              icon={<ClipboardCheck size={18} />}
              label="待跟进"
              tone={pendingActions.length > 0 ? "warning" : "success"}
              value={pendingActions.length}
            />
          </section>

          <EnterpriseScopeBar
            filters={resource.filters}
            onExport={download}
            onFilter={resource.setFilter}
            onReset={resource.resetFilters}
            owners={[]}
            regions={snapshot.fleet.regions.map((region) => ({
              id: region.id,
              label: region.display_name,
            }))}
            showHealth={false}
          />

          <section className="enterprise-split-grid dr-primary-grid">
            <DataSurface
              className="dr-plan-surface"
              description="计划包含允许模式、必需数据源和显式人工确认点。"
              meta={<span>{plans.length} plans</span>}
              title="Recovery Plans"
            >
              <div className="dr-plan-list">
                {plans.map((plan) => (
                  <article key={plan.id}>
                    <header>
                      <div>
                        <span>{plan.subject.replaceAll("_", " ")}</span>
                        <h3>{plan.name}</h3>
                      </div>
                      <EnterpriseStatus
                        label={plan.active ? "active" : "inactive"}
                        value={plan.active ? "active" : "inactive"}
                      />
                    </header>
                    <dl>
                      <div>
                        <dt>RTO</dt>
                        <dd>{formatDuration(plan.target.rto_seconds)}</dd>
                      </div>
                      <div>
                        <dt>RPO</dt>
                        <dd>{formatDuration(plan.target.rpo_seconds)}</dd>
                      </div>
                      <div>
                        <dt>Version</dt>
                        <dd>v{plan.version}</dd>
                      </div>
                      <div>
                        <dt>Owner</dt>
                        <dd>{plan.owner}</dd>
                      </div>
                    </dl>
                    <div className="dr-checkpoint-stack">
                      {plan.checkpoints.map((checkpoint, index) => (
                        <div key={checkpoint.key}>
                          <span>{String(index + 1).padStart(2, "0")}</span>
                          <div>
                            <strong>{checkpoint.title}</strong>
                            <small>
                              {formatDuration(
                                checkpoint.expected_duration_seconds,
                              )}{" "}
                              · {checkpoint.required_evidence_kinds.join(" / ")}
                            </small>
                          </div>
                          {checkpoint.manual_confirmation_required && (
                            <EnterpriseStatus
                              label="manual gate"
                              value="awaiting_manual_confirmation"
                            />
                          )}
                        </div>
                      ))}
                    </div>
                    <footer>
                      <span>{plan.allowed_modes.join(" · ")}</span>
                      <code>{shortId(plan.id)}</code>
                    </footer>
                  </article>
                ))}
              </div>
            </DataSurface>

            <DataSurface
              className="dr-exercise-surface"
              description="终态不可返回 Running；Evidence 缺失不会被显示为通过。"
              meta={<span>{exercises.length} exercises</span>}
              title="Exercise Timeline"
            >
              <div className="dr-exercise-list">
                {exercises.map((exercise) => (
                  <article key={exercise.id}>
                    <span
                      className={`dr-exercise-rail ${exercise.state}`}
                      aria-hidden="true"
                    />
                    <div>
                      <header>
                        <strong>{exercise.mode.replaceAll("_", " ")}</strong>
                        <EnterpriseStatus value={exercise.state} />
                      </header>
                      <small>
                        {shortId(exercise.cluster_id ?? "fleet-wide")} ·{" "}
                        {formatObserved(exercise.updated_at)}
                      </small>
                      <div className="dr-target-row">
                        <span>
                          RTO{" "}
                          <strong>
                            {exercise.actual_rto_seconds
                              ? formatDuration(exercise.actual_rto_seconds)
                              : "missing"}
                          </strong>
                          / {formatDuration(exercise.target.rto_seconds)}
                        </span>
                        <span>
                          RPO{" "}
                          <strong>
                            {exercise.actual_rpo_seconds
                              ? formatDuration(exercise.actual_rpo_seconds)
                              : "missing"}
                          </strong>
                          / {formatDuration(exercise.target.rpo_seconds)}
                        </span>
                      </div>
                      <footer>
                        <span>
                          <CheckCheck size={13} />
                          {exercise.evidence_ids.length} evidence
                        </span>
                        <span>
                          cleanup{" "}
                          {exercise.cleanup_complete ? "complete" : "pending"}
                        </span>
                      </footer>
                    </div>
                  </article>
                ))}
              </div>
            </DataSurface>
          </section>

          <DataSurface
            className="dr-action-surface"
            description="每个 Finding 事务性创建 Action Item，保留 owner、due time 和验证证据。"
            meta={<span>{pendingActions.length} pending</span>}
            title="Recovery Action Items"
          >
            <div className="dr-action-grid">
              {snapshot.drActionItems.items.map((item) => (
                <article key={item.id}>
                  <header>
                    <span>
                      <ShieldAlert size={15} />
                      {shortId(item.finding_id)}
                    </span>
                    <EnterpriseStatus value={item.status} />
                  </header>
                  <h3>{item.title}</h3>
                  <footer>
                    <span>{item.owner ?? "unassigned"}</span>
                    <time dateTime={item.due_at}>
                      due {item.due_at ? formatObserved(item.due_at) : "unset"}
                    </time>
                    <span>{item.evidence_ids.length} evidence</span>
                  </footer>
                </article>
              ))}
            </div>
          </DataSurface>
        </>
      )}
    </div>
  );
}

function formatDuration(seconds: number) {
  if (seconds >= 3_600) {
    return `${(seconds / 3_600).toFixed(seconds % 3_600 === 0 ? 0 : 1)}h`;
  }
  if (seconds >= 60) {
    return `${Math.round(seconds / 60)}m`;
  }
  return `${seconds}s`;
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
