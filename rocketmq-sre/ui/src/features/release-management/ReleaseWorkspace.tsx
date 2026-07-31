import {
  AlertTriangle,
  Check,
  CheckCircle2,
  Circle,
  CirclePause,
  Clock3,
  Radio,
  ShieldCheck,
  X,
} from "lucide-react";

import type {
  IntegrationDeliveryStatus,
  ReleaseObservation,
  ReleaseReadinessSnapshot,
  ReleaseStatus,
} from "@/api/types";
import { Badge } from "@/components/ui/badge";

import {
  deliveryStatusLabel,
  deliveryStatusTone,
  formatReleaseTime,
  observationPhaseLabel,
  readinessGates,
  releaseProgress,
  releaseStatusLabel,
  releaseStatusTone,
} from "./releasePresentation";

const releaseSteps = [
  "计划绑定",
  "准备检查",
  "门禁通过",
  "Canary",
  "发布后验证",
  "完成",
];

export function ReleaseStatusBadge({
  status,
}: {
  status: ReleaseStatus;
}) {
  return (
    <Badge variant={releaseStatusTone(status)}>
      {status === "canary_running" && <Radio aria-hidden="true" size={11} />}
      {status === "paused" && (
        <CirclePause aria-hidden="true" size={11} />
      )}
      {releaseStatusLabel(status)}
    </Badge>
  );
}

export function DeliveryStatusBadge({
  status,
}: {
  status: IntegrationDeliveryStatus;
}) {
  return (
    <Badge variant={deliveryStatusTone(status)}>
      {deliveryStatusLabel(status)}
    </Badge>
  );
}

export function ReleaseProgressRail({
  status,
}: {
  status: ReleaseStatus;
}) {
  const activeIndex = releaseProgress(status);
  const failed =
    status === "failed" ||
    status === "manual_takeover" ||
    status === "rolled_back";

  return (
    <ol className="release-progress-rail" aria-label="发布护航进度">
      {releaseSteps.map((step, index) => {
        const complete = index < activeIndex;
        const active = index === activeIndex;
        return (
          <li
            className={`${complete ? "complete" : ""}${active ? " active" : ""}${active && failed ? " failed" : ""}`}
            key={step}
          >
            <span className="release-progress-node" aria-hidden="true">
              {complete ? (
                <Check size={12} />
              ) : active && failed ? (
                <X size={12} />
              ) : (
                <Circle size={9} />
              )}
            </span>
            <div>
              <small>0{index + 1}</small>
              <strong>{step}</strong>
            </div>
          </li>
        );
      })}
    </ol>
  );
}

export function ReleaseSafetyBanner() {
  return (
    <aside className="release-safety-banner">
      <ShieldCheck aria-hidden="true" size={18} />
      <div>
        <strong>同一条 Plan → Approval → Executor → Agent 链路</strong>
        <span>
          ITSM 与 ChatOps 只接收脱敏事件；外部审批仍需校验角色、MFA、plan hash、expiry 与集群范围。
        </span>
      </div>
      <code>NO DIRECT AGENT ACCESS</code>
    </aside>
  );
}

export function ReadinessGateGrid({
  readiness,
}: {
  readiness?: ReleaseReadinessSnapshot | null;
}) {
  return (
    <div className="release-gate-grid" aria-label="确定性发布门禁">
      {readinessGates(readiness).map((gate) => (
        <div
          className={gate.passed ? "passed" : "blocked"}
          key={gate.id}
        >
          {gate.passed ? (
            <CheckCircle2 aria-hidden="true" size={16} />
          ) : (
            <AlertTriangle aria-hidden="true" size={16} />
          )}
          <span>{gate.label}</span>
          <strong>{gate.passed ? "PASS" : "BLOCK"}</strong>
        </div>
      ))}
    </div>
  );
}

export function ReleaseObservationTimeline({
  observations,
}: {
  observations: ReleaseObservation[];
}) {
  if (observations.length === 0) {
    return (
      <div className="state-panel empty">
        <Clock3 aria-hidden="true" size={22} />
        <div>
          <strong>尚无护航观察</strong>
          <span>开始执行前必须先记录发布前 SLO 与 synthetic Probe。</span>
        </div>
      </div>
    );
  }
  return (
    <ol className="release-observation-timeline">
      {observations.map((observation, index) => (
        <li
          className={observation.regression_detected ? "regression" : ""}
          key={`${observation.phase}-${observation.observed_at}-${index}`}
        >
          <span className="observation-node" aria-hidden="true" />
          <div>
            <header>
              <strong>{observationPhaseLabel(observation.phase)}</strong>
              <Badge
                variant={
                  observation.regression_detected
                    ? "destructive"
                    : "success"
                }
              >
                {observation.regression_detected ? "REGRESSION" : "HEALTHY"}
              </Badge>
              <time dateTime={observation.observed_at}>
                {formatReleaseTime(observation.observed_at)}
              </time>
            </header>
            <p>{observation.sanitized_summary}</p>
            <footer>
              <span>
                SLO {observation.slo_healthy ? "正常" : "异常"}
              </span>
              <span>
                Probe{" "}
                {observation.synthetic_probe_healthy ? "正常" : "异常"}
              </span>
              <code>{observation.evidence_ids.length} evidence</code>
            </footer>
          </div>
        </li>
      ))}
    </ol>
  );
}
