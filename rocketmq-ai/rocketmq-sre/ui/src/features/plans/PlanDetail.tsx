import { CheckCircle2, FileDiff, RotateCcw, ShieldAlert } from "lucide-react";

import type { ActionPlanView } from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { actionText } from "@/features/actions/actionDefinitions";
import { useActionFormMessages } from "@/i18n/actionFormMessages";

import { reviewPlanStep, supportedPlanReview } from "./planActionReview";
import { shortDigest } from "./planPresentation";
import { verificationLabels } from "./verificationLabels";

export function PlanDetail({ view }: { view: ActionPlanView }) {
  const { plan } = view;
  const { m, english, formatDateTime } = useActionFormMessages();
  const compatible = supportedPlanReview(plan);
  const impacts = { single_resource: m.singleResource, single_instance: m.singleInstance, one_replica: m.oneReplica, allowlisted_fields: m.allowlistedFields };
  const count = (value: number) => Number.isSafeInteger(value) && value >= 0 ? String(value) : m.imprecise;
  const condition = (code: string) => Object.hasOwn(verificationLabels, code)
    ? actionText(verificationLabels[code], english) : m.unknownCondition;
  return <>
    <section className="change-summary-grid" aria-label={m.reviewTitle}>
      <Summary label={m.status} value={plan.status} />
      <Summary label={m.risk} value={view.risk.toUpperCase()} />
      <Summary label={m.steps} value={String(plan.steps.length)} />
      <Summary label="Critic" value={view.critic_state} />
      <Summary label={m.expiry} value={formatDateTime(plan.expires_at)} />
      <Summary label="Plan hash" value={shortDigest(plan.plan_hash)} mono />
    </section>
    <section className="data-surface change-plan-surface">
      <header className="surface-heading">
        <div><h2>{m.reviewTitle}</h2><p>{m.reviewHint}</p></div>
        <Badge variant="secondary">{plan.diagnosis_execution_eligible ? m.diagnosisConfirmed : m.diagnosisUnconfirmed}</Badge>
      </header>
      {!compatible && <p role="status" className="inline-alert warning">{m.historical}</p>}
      <ol className="change-step-list">
        {plan.steps.slice(0, 16).map((step) => {
          const review = reviewPlanStep(step);
          return <li key={step.id}>
            <span className="change-step-index">{step.sequence}</span>
            <div className="change-step-main">
              <header>
                <div><strong>{review ? actionText(review.definition.title, english) : m.historical}</strong>
                  <code>{step.action} / {step.descriptor_version}</code><code>{step.resource}</code></div>
                <Badge variant="secondary">{m.impact} · {Object.hasOwn(impacts, step.max_impact) ? impacts[step.max_impact] : m.unknownImpact}</Badge>
              </header>
              <div className="change-step-grid">
                <section><h3><FileDiff size={14} />{m.requested}</h3>
                  <p>{m.beforeUnknown}</p>
                  {review ? <dl className="action-review-values">
                    {review.rows.map(({ field, value }) => <div key={(field.patch ? "patch." : "") + field.key}>
                      <dt>{actionText(field.label, english)}</dt>
                      <dd>{field.kind === "reference" ? m.masked : String(value)}</dd>
                    </div>)}
                  </dl> : <p>{m.historical}</p>}
                </section>
                <section><h3><CheckCircle2 size={14} />{m.verification}</h3>
                  <ul className="action-review-conditions">
                    {[...step.verification.resource_conditions, ...step.verification.technical_slis].slice(0, 32)
                      .map((item, index) => <li key={index}>{condition(item)}</li>)}
                  </ul>
                  {!step.verification.resource_conditions.length && !step.verification.technical_slis.length && <p>{m.noCondition}</p>}
                  <small>{m.stableWindow} {count(step.verification.stable_window_seconds)} {m.seconds} · {m.maxWait} {count(step.verification.max_wait_seconds)} {m.seconds}</small>
                </section>
                <section><h3><RotateCcw size={14} />{m.compensation}</h3>
                  <p>{step.compensation.mode === "automatic" ? m.automatic : step.compensation.mode === "manual_takeover" ? m.manual : m.noAutomatic}</p>
                  <small>{m.beforeCount} {step.compensation.required_before_fields.length} · {m.timeout} {count(step.compensation.timeout_seconds)} {m.seconds}</small>
                  <p>{m.compensationHint}</p>
                </section>
                <section><h3><ShieldAlert size={14} />{m.evidence}</h3>
                  <p>{step.evidence_ids.length}</p><code>{shortDigest(step.precondition_hash)}</code>
                </section>
              </div>
            </div>
          </li>;
        })}
      </ol>
    </section>
  </>;
}

function Summary({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return <div className="change-summary-card"><span>{label}</span><strong className={mono ? "mono-value" : undefined}>{value}</strong></div>;
}
