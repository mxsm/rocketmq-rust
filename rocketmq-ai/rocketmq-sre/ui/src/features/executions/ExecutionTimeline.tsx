import {
  CheckCircle2,
  Circle,
  LoaderCircle,
  RotateCcw,
  ShieldAlert,
} from "lucide-react";

import type {
  ExecutionSubmissionView,
  WorkflowStreamEvent,
} from "@/api/types";
import { Badge } from "@/components/ui/badge";

import { formatTimestamp, shortDigest } from "../plans/planPresentation";

const phases = [
  { state: "pending", label: "请求已接收", detail: "校验签名、审批与幂等键" },
  { state: "prechecking", label: "Precheck", detail: "重读资源状态与 precondition hash" },
  { state: "intent_persisted", label: "Intent 持久化", detail: "写入 journal 后才允许 dispatch" },
  { state: "applying", label: "Apply", detail: "带 fencing grant 调用类型化 Agent" },
  { state: "verifying", label: "Verify", detail: "资源条件与技术 SLI 稳定窗口" },
  { state: "succeeded", label: "完成", detail: "before/during/after Evidence 已封存" },
] as const;

const terminalFailures = new Set(["rolled_back", "escalated"]);

export function ExecutionTimeline({
  execution,
  transport,
  events,
}: {
  execution: ExecutionSubmissionView;
  transport: "connecting" | "sse" | "polling";
  events: WorkflowStreamEvent[];
}) {
  const state = execution.state;
  const currentIndex = phases.findIndex((phase) => phase.state === state);
  const fallbackIndex =
    state === "compensating" || state === "reconciling" || state === "unknown"
      ? 4
      : terminalFailures.has(state)
        ? phases.length - 1
        : 0;
  const activeIndex = currentIndex >= 0 ? currentIndex : fallbackIndex;

  return (
    <>
      <section className="change-summary-grid execution-summary" aria-label="执行摘要">
        <Summary label="状态" value={state} />
        <Summary label="传输" value={transport.toUpperCase()} />
        <Summary label="执行 ID" value={shortDigest(execution.execution.id)} mono />
        <Summary label="请求人" value={execution.execution.requested_by} />
        <Summary label="提交时间" value={formatTimestamp(execution.submitted_at)} />
        <Summary
          label="Correlation"
          value={shortDigest(execution.execution.correlation_id)}
          mono
        />
      </section>

      <section className="execution-layout">
        <div className="data-surface">
          <header className="surface-heading">
            <div>
              <h2>受控执行时间线</h2>
              <p>页面通过 SSE 更新；断线后自动退回 10 秒 polling。</p>
            </div>
            <Badge
              variant={
                state === "succeeded"
                  ? "success"
                  : terminalFailures.has(state)
                    ? "destructive"
                    : "warning"
              }
            >
              {state}
            </Badge>
          </header>
          <ol className="execution-timeline">
            {phases.map((phase, index) => {
              const status =
                index < activeIndex
                  ? "complete"
                  : index === activeIndex
                    ? "active"
                    : "pending";
              return (
                <li className={status} key={phase.state}>
                  <span>
                    {status === "complete" ? (
                      <CheckCircle2 size={16} />
                    ) : status === "active" ? (
                      <LoaderCircle size={16} />
                    ) : (
                      <Circle size={14} />
                    )}
                  </span>
                  <div>
                    <strong>{phase.label}</strong>
                    <small>{phase.detail}</small>
                  </div>
                  <code>{phase.state}</code>
                </li>
              );
            })}
            {(state === "compensating" || state === "rolled_back") && (
              <li className="rollback active">
                <span>
                  <RotateCcw size={16} />
                </span>
                <div>
                  <strong>类型化回滚</strong>
                  <small>重新读取最新 generation，执行补偿并再次验证。</small>
                </div>
                <code>{state}</code>
              </li>
            )}
            {state === "escalated" && (
              <li className="escalated active">
                <span>
                  <ShieldAlert size={16} />
                </span>
                <div>
                  <strong>人工接管</strong>
                  <small>资源已隔离；提交验证 Evidence 后才能解除。</small>
                </div>
                <code>quarantined</code>
              </li>
            )}
          </ol>
        </div>

        <aside className="data-surface live-event-panel">
          <header className="surface-heading">
            <div>
              <h2>实时事件</h2>
              <p>仅显示当前集群最近 8 条脱敏事件。</p>
            </div>
            <span>{events.length} events</span>
          </header>
          {events.length === 0 ? (
            <div className="compact-empty">等待执行事件…</div>
          ) : (
            <ul>
              {events.map((event) => (
                <li key={`${event.occurred_at}-${event.aggregate_id}`}>
                  <span>{event.event_type}</span>
                  <strong>{event.aggregate_type}</strong>
                  <small>{formatTimestamp(event.occurred_at)}</small>
                </li>
              ))}
            </ul>
          )}
        </aside>
      </section>
    </>
  );
}

function Summary({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="change-summary-card">
      <span>{label}</span>
      <strong className={mono ? "mono-value" : undefined}>{value}</strong>
    </div>
  );
}
