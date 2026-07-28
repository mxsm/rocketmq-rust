import {
  ArrowDownToLine,
  CheckCircle2,
  Clock3,
  FileDiff,
  RotateCcw,
  ShieldAlert,
} from "lucide-react";

import type { ActionPlanView } from "@/api/types";
import { Badge } from "@/components/ui/badge";

import {
  formatTimestamp,
  sanitizePlanParameters,
  shortDigest,
} from "./planPresentation";

export function PlanDetail({ view }: { view: ActionPlanView }) {
  const { plan } = view;
  return (
    <>
      <section className="change-summary-grid" aria-label="计划摘要">
        <Summary label="状态" value={plan.status} />
        <Summary label="风险等级" value={view.risk.toUpperCase()} tone={view.risk} />
        <Summary label="步骤" value={`${plan.steps.length}`} />
        <Summary label="Critic" value={view.critic_state} />
        <Summary label="过期时间" value={formatTimestamp(plan.expires_at)} />
        <Summary label="Plan hash" value={shortDigest(plan.plan_hash)} mono />
      </section>

      <section className="data-surface change-plan-surface">
        <header className="surface-heading">
          <div>
            <h2>执行步骤与配置差异</h2>
            <p>参数已经过字段级脱敏；审批始终绑定当前 plan hash 和 precondition hash。</p>
          </div>
          <Badge variant={plan.diagnosis_execution_eligible ? "success" : "destructive"}>
            {plan.diagnosis_execution_eligible ? "诊断允许执行" : "仅人工 Runbook"}
          </Badge>
        </header>
        <ol className="change-step-list">
          {plan.steps.map((step) => (
            <li key={step.id}>
              <span className="change-step-index">{step.sequence}</span>
              <div className="change-step-main">
                <header>
                  <div>
                    <strong>{step.action}</strong>
                    <code>{step.resource}</code>
                  </div>
                  <Badge variant={step.max_impact === "one_replica" ? "warning" : "secondary"}>
                    {step.max_impact}
                  </Badge>
                </header>
                <div className="change-step-grid">
                  <section>
                    <h3>
                      <FileDiff size={14} />
                      配置差异
                    </h3>
                    <pre>{JSON.stringify(sanitizePlanParameters(step.parameters), null, 2)}</pre>
                  </section>
                  <section>
                    <h3>
                      <CheckCircle2 size={14} />
                      验证条件
                    </h3>
                    <TagList
                      icon={<ArrowDownToLine size={12} />}
                      items={step.verification.resource_conditions}
                      prefix="resource"
                    />
                    <TagList
                      icon={<Clock3 size={12} />}
                      items={step.verification.technical_slis}
                      prefix="sli"
                    />
                    <small>
                      稳定窗口 {step.verification.stable_window_seconds}s · 最长等待{" "}
                      {step.verification.max_wait_seconds}s
                    </small>
                  </section>
                  <section>
                    <h3>
                      <RotateCcw size={14} />
                      回滚与接管
                    </h3>
                    <p>
                      {step.compensation.mode === "automatic"
                        ? "类型化自动补偿"
                        : step.compensation.mode === "manual_takeover"
                          ? "失败后进入人工接管"
                          : "不提供自动补偿"}
                    </p>
                    <small>
                      保存 {step.compensation.required_before_fields.length} 个 before 字段 ·
                      超时 {step.compensation.timeout_seconds}s
                    </small>
                  </section>
                  <section>
                    <h3>
                      <ShieldAlert size={14} />
                      证据绑定
                    </h3>
                    <p>{step.evidence_ids.length} 个不可变 Evidence 引用</p>
                    <code>{shortDigest(step.precondition_hash)}</code>
                  </section>
                </div>
              </div>
            </li>
          ))}
        </ol>
      </section>
    </>
  );
}

function Summary({
  label,
  value,
  tone,
  mono = false,
}: {
  label: string;
  value: string;
  tone?: string;
  mono?: boolean;
}) {
  return (
    <div className={`change-summary-card${tone ? ` ${tone}` : ""}`}>
      <span>{label}</span>
      <strong className={mono ? "mono-value" : undefined}>{value}</strong>
    </div>
  );
}

function TagList({
  icon,
  items,
  prefix,
}: {
  icon: React.ReactNode;
  items: string[];
  prefix: string;
}) {
  return (
    <div className="condition-tags">
      {items.map((item) => (
        <span key={item}>
          {icon}
          {prefix}:{item}
        </span>
      ))}
    </div>
  );
}
