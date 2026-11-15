import type { ExecutionObservationView } from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { actionDefinition, actionText } from "@/features/actions/actionDefinitions";
import { shortDigest } from "@/features/plans/planPresentation";
import { verificationLabels } from "@/features/plans/verificationLabels";
import { useExecutionObservationMessages } from "@/i18n/executionObservationMessages";

type Fact = ExecutionObservationView["history"];
type Step = ExecutionObservationView["steps"][number];
type Verification = Step["verifications"][number];

export function ExecutionFact({ fact, current = true }: { fact: Fact; current?: boolean }) {
  const { m, source, reason } = useExecutionObservationMessages();
  const statuses = { not_observed: m.notObserved, unknown: m.unknown, observed: m.observed, satisfied: m.satisfied, not_satisfied: m.notSatisfied };
  return <div className="execution-observation-fact">
    {current ? <>
      <Badge variant={fact.state === "not_satisfied" ? "destructive" : fact.state === "unknown" || fact.state === "not_observed" ? "warning" : "secondary"}>
        {statuses[fact.state] ?? m.unknown}
      </Badge>
      <small>{m.source} · {source(fact.source)}</small>
      <ul>{fact.reason_codes.map((code, index) => <li key={index}>{reason(code)}</li>)}</ul>
    </> : <p className="muted">{m.currentUnconfirmed}</p>}
  </div>;
}

export function ExecutionObservationPanel({ view, current }: { view: ExecutionObservationView; current: boolean }) {
  const { m, english } = useExecutionObservationMessages();
  return <section className="data-surface execution-observation-panel">
    <header className="surface-heading"><div><h2>{m.action}</h2><p>{m.declaredHint}</p></div>
      <Badge variant="secondary">{m.historical}</Badge>
    </header>
    <ol className="execution-observation-steps">
      {view.steps.map((step) => {
        const definition = actionDefinition(step.action, step.descriptor_version);
        const target = definition && definition.id !== "security.credential_rotate_overlap.v1" ? step.target : m.hiddenTarget;
        return <li key={step.plan_step_id}>
          <header className="execution-step-heading"><strong>{m.step} {step.sequence} · {definition ? actionText(definition.title, english) : step.action}</strong>
            {step.partial && <Badge variant="warning">{m.partial}</Badge>}
          </header>
          <dl className="execution-observation-values">
            <div><dt>{m.target}</dt><dd>{target}</dd></div>
            <div><dt>{m.version}</dt><dd>{step.action} / {step.descriptor_version}</dd></div>
          </dl>
          <div className="execution-fact-grid">
            <section><h3>{m.effect}</h3><ExecutionFact fact={step.effect} current={current} /></section>
            <section><h3>{m.compensation}</h3><ExecutionFact fact={step.compensation} current={current} /></section>
          </div>
          <SavedVerificationDeclaration step={step} />
          {step.attempts.map((attempt) => <AttemptRecord key={`${attempt.step_id}:${attempt.attempt}:${attempt.compensation}`} attempt={attempt} current={current} />)}
          {step.verifications.length ? step.verifications.map((verification) =>
            <VerificationRecord key={`${verification.step_id}:${verification.attempt}:${verification.compensation}`} value={verification} />)
            : <p className="muted">{m.verification} · {m.notObserved}</p>}
        </li>;
      })}
    </ol>
  </section>;
}

function SavedVerificationDeclaration({ step }: { step: Step }) {
  const { m } = useExecutionObservationMessages();
  const spec = step.declared_verification;
  return <details className="execution-policy-declaration"><summary>{m.declared}</summary>
    {spec ? <>
      <p>{m.declaredHint}</p>
      <ConditionList title={m.resourceConditions} items={spec.resource_conditions} />
      <ConditionList title={m.technicalConditions} items={spec.technical_slis} />
      <dl className="execution-observation-values">
        <NumberValue label={m.requiredWindow} value={spec.stable_window_seconds} />
        <NumberValue label={m.maxWait} value={spec.max_wait_seconds} />
      </dl>
    </> : <p>{m.noDeclaration}</p>}
  </details>;
}

function AttemptRecord({ attempt, current }: { attempt: Step["attempts"][number]; current: boolean }) {
  const { m, formatDateTime, english } = useExecutionObservationMessages();
  const effect = { prepared: ["已准备", "Prepared"], dispatched: ["已派发", "Dispatched"], confirmed: ["结果已确认", "Result confirmed"], unknown: [m.unknown, m.unknown] };
  const when = (value: string | null | undefined) => value && Number.isFinite(Date.parse(value)) ? formatDateTime(value) : m.noData;
  return <section className="execution-attempt-record">
    <h4>{m.attempt} {attempt.attempt} · {attempt.compensation ? m.compensation : m.forward}</h4>
    <dl className="execution-observation-values">
      <div><dt>{m.intentAt}</dt><dd>{when(attempt.intended_at)}</dd></div>
      <div><dt>{m.resultAt}</dt><dd>{when(attempt.result_at)}</dd></div>
      <div><dt>{m.effect}</dt><dd>{attempt.effect_state && Object.hasOwn(effect, attempt.effect_state) ? effect[attempt.effect_state][english ? 1 : 0] : m.unknown}</dd></div>
      <div><dt>{m.leaseEpoch}</dt><dd>{/^(0|[1-9]\d{0,19})$/u.test(attempt.lease_epoch) ? attempt.lease_epoch : m.noData}</dd></div>
    </dl>
    <ExecutionFact fact={attempt.fact} current={current} />
  </section>;
}

function VerificationRecord({ value }: { value: Verification }) {
  const { m, formatDateTime, reason } = useExecutionObservationMessages();
  const when = (time: string | null | undefined) => time && Number.isFinite(Date.parse(time)) ? formatDateTime(time) : m.noData;
  const proofs: Record<Verification["proof_state"], string> = {
    not_observed: m.notObserved, legacy_missing: m.legacyProof, invalid: m.invalidProof,
    unavailable: m.missingProof, inconclusive: m.incompleteProof, valid_historical: m.historicalProof,
  };
  const origins: Record<Verification["provenance"], string> = { recorded_only: m.recordedOnly, synthetic_fixture: m.synthetic, production_read: m.productionRead };
  const outcomes = { succeeded: m.satisfied, failed: m.notSatisfied, inconclusive: m.unknown };
  return <article className="execution-verification-record">
    <header className="execution-step-heading"><h4>{m.verification} · {m.attempt} {value.attempt} · {value.compensation ? m.compensation : m.forward}</h4>
      <Badge variant={value.provenance === "synthetic_fixture" ? "warning" : "secondary"}>{origins[value.provenance]}</Badge>
    </header>
    <dl className="execution-observation-values">
      <div><dt>{m.recordedOutcome}</dt><dd>{outcomes[value.recorded_outcome] ?? m.unknown}</dd></div>
      <div><dt>{m.proof}</dt><dd>{proofs[value.proof_state] ?? m.unknown}</dd></div>
      <NumberValue label={m.requiredWindow} value={value.required_stable_window_seconds} />
      <NumberValue label={m.observedWindow} value={value.observed_stable_seconds} />
      <NumberValue label={m.maxWait} value={value.max_wait_seconds} />
      <NumberValue label={m.samples} value={value.sample_count} seconds={false} />
      <NumberValue label={m.ageLimit} value={value.max_source_age_seconds} />
      <NumberValue label={m.gapLimit} value={value.max_sample_gap_seconds} />
      <div><dt>{m.sourceFirst}</dt><dd>{when(value.source_first_at)}</dd></div>
      <div><dt>{m.sourceLast}</dt><dd>{when(value.source_last_at)}</dd></div>
      <div><dt>{m.resourceTime}</dt><dd>{when(value.resource_observed_at)}</dd></div>
      <div><dt>{m.technicalTime}</dt><dd>{when(value.technical_sli_observed_at)}</dd></div>
      <div><dt>{m.collectionTime}</dt><dd>{when(value.collected_at)}</dd></div>
      <div><dt>{m.declaredTargetVersion}</dt><dd>{value.declared_target_version ?? m.noData}</dd></div>
      <div><dt>{m.targetVersionProof}</dt><dd>{value.target_version_verified && value.proof_state === "valid_historical" && value.provenance === "production_read" ? m.targetVersionObserved : m.targetVersionUnknown}</dd></div>
      <div><dt>{m.policyDigest}</dt><dd>{value.policy_digest ? shortDigest(value.policy_digest) : m.noData}</dd></div>
    </dl>
    <ConditionList title={m.satisfiedConditions} items={value.satisfied_conditions} />
    <ConditionList title={m.failedConditions} items={value.failed_conditions} />
    <ConditionList title={m.missingConditions} items={value.missing_conditions} />
    <ul className="execution-observation-reasons">{value.reason_codes.map((code, index) => <li key={index}>{reason(code)}</li>)}</ul>
    <details><summary>{m.evidence} · {value.evidence_ids.length}</summary>
      <ul className="execution-evidence-references">{value.evidence_ids.map((id) => <li key={id}><code>{id}</code></li>)}</ul>
    </details>
  </article>;
}

function NumberValue({ label, value, seconds = true }: { label: string; value: number | null | undefined; seconds?: boolean }) {
  const { m } = useExecutionObservationMessages();
  const valid = value != null && Number.isSafeInteger(value) && value >= 0;
  return <div><dt>{label}</dt><dd>{value == null ? m.noData : valid ? `${value}${seconds ? ` ${m.seconds}` : ""}` : m.unavailableNumber}</dd></div>;
}

function ConditionList({ title, items }: { title: string; items: readonly string[] }) {
  const { m, english } = useExecutionObservationMessages();
  const label = (item: string) => {
    const code = item.replace(/^(resource|sli):/u, "");
    return Object.hasOwn(verificationLabels, code) ? actionText(verificationLabels[code], english) : m.unknownCondition;
  };
  return <section className="execution-condition-list"><h5>{title}</h5>
    {items.length ? <ul>{items.map((item, index) => <li key={index}>{label(item)}</li>)}</ul> : <p className="muted">{m.noData}</p>}
  </section>;
}
