import { ArrowRight, CheckCircle2, FileCheck2, Radar } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { ApiError } from "@/api/client";
import type {
  ActionReadinessRequest, ActionReadinessView, CreatePlanResponse,
  DiagnosisRevision, ExecutionPreconditionEvidenceView, Incident,
  PrepareExecutionPreconditionRequest,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useOptionalWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSupervisedApi } from "@/hooks/useSupervisedApi";
import { useActionReadinessMessages } from "@/i18n/actionReadinessMessages";
import { useActionReadinessLabels } from "@/i18n/actionReadinessReasons";

import { ActionReadinessPanel } from "./ActionReadinessPanel";
import {
  ActionReadinessScopeError, createdPlanMatches, preparedEvidenceMatches,
  readinessMatches, readinessOperation,
} from "./actionReadinessPresentation";
import { TypedActionForm } from "@/features/actions/TypedActionForm";
import { actionDefinition } from "@/features/actions/actionDefinitions";
import { buildActionParameters, initialActionDraft, validActionResource } from "@/features/actions/actionParameters";
import { useReadinessExpiry } from "./useReadinessExpiry";
import { planNavigationSearch } from "@/features/plans/planReadinessPresentation";

interface Props {
  incident: Incident;
  revisions: DiagnosisRevision[];
  onChanged: () => void | Promise<void>;
}

type Operation = "read" | "confirm" | "precondition" | "plan";
type ReadFailure = "read" | "forbidden" | "scope" | "conflict";

export function SupervisedDiagnosisPanel(props: Props) {
  const auth = useAuth();
  const scope = useOptionalWorkspaceScope();
  const location = useLocation();
  const revision = props.revisions.at(-1);
  const owner = JSON.stringify([
    auth.status, auth.session?.tenantId, auth.session?.subject,
    [...(auth.session?.roles ?? [])].sort(),
    [...(auth.session?.clusterIds ?? [])].sort(),
    scope?.scopeKey ?? location.search,
    props.incident.id, props.incident.tenant_id, props.incident.cluster_id,
    props.incident.resource, props.incident.status,
    revision?.id, revision?.status, revision?.partial, revision?.execution_eligible,
    revision?.primary_model_invocation_id, revision?.evidence_ids,
  ]);
  return (
    <SupervisedDiagnosisSession
      key={owner}
      {...props}
      scopeValid={!scope || !scope.invalid && scope.clusterId === props.incident.cluster_id}
    />
  );
}

function SupervisedDiagnosisSession({
  incident, revisions, onChanged, scopeValid,
}: Props & { scopeValid: boolean }) {
  const { auth, api } = useSupervisedApi();
  const navigate = useNavigate();
  const workspace = useOptionalWorkspaceScope();
  const m = useActionReadinessMessages();
  const latest = revisions.at(-1);
  const [fields, setFields] = useState(() => initialActionDraft(
    "observability.logger_level_ttl.v1", isBrokerResource(incident.resource) ? incident.resource : "",
  ));
  const [reason, setReason] = useState(m.defaultReason);
  const [view, setView] = useState<ActionReadinessView>();
  const [proof, setProof] = useState<ExecutionPreconditionEvidenceView>();
  const [plan, setPlan] = useState<CreatePlanResponse>();
  const [busy, setBusy] = useState<Operation>();
  const [error, setError] = useState<ReadFailure>();
  const [message, setMessage] = useState("");
  const generation = useRef(0);
  const pending = useRef<{ controller: AbortController; kind: Operation } | undefined>(undefined);
  useEffect(() => () => pending.current?.controller.abort(), []);
  const expired = useReadinessExpiry(view);
  const owner = {
    tenantId: auth.session?.tenantId ?? "",
    subject: auth.session?.subject ?? "",
    incidentId: incident.id,
    evidenceIds: latest?.evidence_ids ?? [],
  };
  const definition = actionDefinition(fields.actionId)!;
  const parameters = buildActionParameters(fields);
  const candidateValid = Boolean(parameters && validActionResource(fields.resource.trim()));
  const request: ActionReadinessRequest = {
    cluster_id: incident.cluster_id,
    diagnosis_revision_id: latest?.id ?? "",
    action_id: definition.id,
    descriptor_version: definition.version,
    resource: fields.resource.trim(),
    parameters: parameters ?? {},
    ...(proof ? { precondition_evidence_id: proof.evidence.evidence_id } : {}),
  };
  const current = view && readinessMatches(view, request, owner) ? view : undefined;
  const can = (kind: keyof ActionReadinessView["operations"]) =>
    !busy && !expired && (kind === "confirm_diagnosis" || candidateValid) && readinessOperation(current, kind);
  const scopeAllowed = scopeValid && auth.status === "authenticated" &&
    auth.session?.tenantId === incident.tenant_id && auth.hasClusterScope(incident.cluster_id) &&
    (!latest || latest.incident_id === incident.id);

  const clearCandidate = () => {
    generation.current += 1;
    if (pending.current && pending.current.kind !== "confirm") {
      pending.current.controller.abort();
      pending.current = undefined;
      setBusy(undefined);
    }
    setView(undefined);
    setProof(undefined);
    setPlan(undefined);
    setError(undefined);
    setMessage("");
  };

  const run = async (
    kind: Operation,
    task: (signal: AbortSignal, active: () => boolean) => Promise<void>,
  ) => {
    if (pending.current || !api || !latest || !scopeAllowed) return;
    const controller = new AbortController();
    const version = generation.current;
    pending.current = { controller, kind };
    const active = () => !controller.signal.aborted &&
      pending.current?.controller === controller &&
      (kind === "confirm" || generation.current === version);
    setBusy(kind);
    setError(undefined);
    setMessage("");
    try {
      await task(controller.signal, active);
    } catch (failure) {
      if (!active()) return;
      setView(undefined);
      if (failure instanceof ActionReadinessScopeError) {
        setProof(undefined);
        setPlan(undefined);
        setError("scope");
      } else if (failure instanceof ApiError && [401, 403, 404].includes(failure.status)) {
        setProof(undefined);
        setPlan(undefined);
        setError("forbidden");
      } else if (failure instanceof ApiError && failure.status === 409) {
        setProof(undefined);
        setPlan(undefined);
        setError("conflict");
        await onChanged();
      } else if (kind === "read") {
        setError("read");
      } else {
        setMessage(m.failure);
        // Confirmation may already have committed even if its response was lost.
        if (kind === "confirm") await onChanged();
      }
    } finally {
      if (pending.current?.controller === controller) {
        pending.current = undefined;
        if (!controller.signal.aborted) setBusy(undefined);
      }
    }
  };

  const readReadiness = () => run("read", async (signal, active) => {
    if (!api) return;
    setView(undefined);
    const result = await api.readActionReadiness(incident.id, request, signal);
    if (!active()) return;
    if (!readinessMatches(result, request, owner)) throw new ActionReadinessScopeError();
    setView(result);
  });

  const confirmDiagnosis = () => {
    if (!can("confirm_diagnosis") || reason.trim().length < 8) return;
    return run("confirm", async (signal, active) => {
      if (!api || !latest) return;
      const result = await api.confirmDiagnosisExecution(
        incident.id, latest.id,
        { human_confirmed: true, reason: reason.trim() }, signal,
      );
      if (!active()) return;
      if (result.incident_id !== incident.id || result.cluster_id !== incident.cluster_id ||
          result.source_revision_id !== latest.id || !result.execution_eligible) {
        throw new ActionReadinessScopeError();
      }
      setView(undefined);
      setProof(undefined);
      setPlan(undefined);
      setMessage(m.diagnosisSaved);
      // The receipt does not select a revision. The refreshed Incident owns that choice.
      await onChanged();
    });
  };

  const preparePrecondition = () => {
    if (!can("prepare_precondition")) return;
    return run("precondition", async (signal, active) => {
      if (!api) return;
      setView(undefined);
      setProof(undefined);
      setPlan(undefined);
      const input: PrepareExecutionPreconditionRequest = {
        cluster_id: request.cluster_id,
        diagnosis_revision_id: request.diagnosis_revision_id,
        action_id: request.action_id,
        descriptor_version: request.descriptor_version,
        resource: request.resource,
        parameters: request.parameters as PrepareExecutionPreconditionRequest["parameters"],
      };
      const result = await api.prepareExecutionPrecondition(incident.id, input, signal);
      if (!active()) return;
      if (!preparedEvidenceMatches(result, request, owner)) throw new ActionReadinessScopeError();
      const bound = { ...request, precondition_evidence_id: result.evidence.evidence_id };
      if (result.readiness && !readinessMatches(result.readiness, bound, owner)) {
        throw new ActionReadinessScopeError();
      }
      setProof(result);
      const readiness = result.readiness ?? await api.readActionReadiness(incident.id, bound, signal);
      if (!active()) return;
      if (!readinessMatches(readiness, bound, owner)) throw new ActionReadinessScopeError();
      setView(readiness);
      setMessage(m.prepared);
    });
  };

  const createPlan = () => {
    if (!can("create_plan") || !current || !proof || plan) return;
    const evidenceIds = [...new Set([
      ...current.binding.diagnostic_evidence.map((item) => item.evidence_id),
      proof.evidence.evidence_id,
    ])];
    return run("plan", async (signal, active) => {
      if (!api) return;
      const result = await api.createPlan({
        cluster_id: incident.cluster_id, incident_id: incident.id,
        diagnosis_revision_id: request.diagnosis_revision_id,
        steps: [{
          action_id: request.action_id, descriptor_version: request.descriptor_version,
          resource: request.resource,
          parameters: request.parameters as PrepareExecutionPreconditionRequest["parameters"],
          evidence_ids: evidenceIds,
        }],
      }, signal);
      if (!active()) return;
      if (!createdPlanMatches(result, request, owner, evidenceIds)) throw new ActionReadinessScopeError();
      setPlan(result);
      setMessage(result.kind === "action_plan" ? m.planCreated : m.manualCreated);
    });
  };

  return (
    <section className="data-surface supervised-diagnosis-panel">
      <header className="surface-heading">
        <div><h2>{m.title}</h2><p>{m.introduction}</p></div>
      </header>
      {!scopeAllowed ? <p role="alert">{m.scopeMismatch}</p> : !latest ? (
        <p>{m.diagnosisUnavailable}</p>
      ) : (
        <>
          <div className="action-candidate-form-block">
            <h3>{m.candidateTitle}</h3>
            <p className="muted">{m.candidateHint}</p>
            <TypedActionForm value={fields} onChange={(change) => {
              clearCandidate();
              setFields((previous) => ({ ...previous, ...change }));
            }} onActionChange={(action) => {
              clearCandidate();
              setFields(initialActionDraft(action));
            }} />
          </div>
          <ActionReadinessPanel
            view={current} request={request} owner={owner}
            loading={busy === "read"} disabled={Boolean(busy) || !api}
            error={error} onRefresh={() => void readReadiness()}
          />
          <div className="supervised-diagnosis-steps">
            <article>
              <div className="supervised-step-heading">
                <span>01</span><div><strong>{m.diagnosisTitle}</strong></div><CheckCircle2 size={17} />
              </div>
              <p>{m.diagnosisHint}</p>
              <Badge variant="secondary">
                {current ? current.diagnosis.is_confirmed ? m.diagnosisConfirmed : m.diagnosisUnconfirmed : m.awaitingServer}
              </Badge>
              <label className="form-field">
                <span>{m.confirmationReason}</span>
                <Input value={reason} maxLength={512} onChange={(event) => setReason(event.target.value)} />
              </label>
              <Button disabled={!can("confirm_diagnosis") || reason.trim().length < 8}
                onClick={() => void confirmDiagnosis()} variant="outline">
                {busy === "confirm" ? m.confirming : m.confirmDiagnosis}
              </Button>
              <OperationBlockers view={current} kind="confirm_diagnosis" />
              {current && !current.diagnosis.is_current && (
                <Button disabled={Boolean(busy)} variant="outline" onClick={() => void run("read", async () => {
                  setView(undefined);
                  await onChanged();
                })}>
                  {m.refreshDiagnosis}
                </Button>
              )}
            </article>
            <article>
              <div className="supervised-step-heading">
                <span>02</span><div><strong>{m.preconditionTitle}</strong></div><Radar size={17} />
              </div>
              <p>{m.preconditionHint}</p>
              <p className="muted">{proof ? `${m.proof} · ${shortId(proof.evidence.evidence_id)}` : m.noProof}</p>
              <Button disabled={!can("prepare_precondition")} onClick={() => void preparePrecondition()} variant="outline">
                {busy === "precondition" ? m.preparing : m.prepare}
              </Button>
              <OperationBlockers view={current} kind="prepare_precondition" />
            </article>
            <article>
              <div className="supervised-step-heading">
                <span>03</span><div><strong>{m.planTitle}</strong></div><FileCheck2 size={17} />
              </div>
              <p>{m.planHint}</p>
              <p className="muted">{m.approvalHint}</p>
              <Button disabled={!can("create_plan") || !proof || Boolean(plan)} onClick={() => void createPlan()}>
                {busy === "plan" ? m.creatingPlan : m.createPlan}
              </Button>
              <OperationBlockers view={current} kind="create_plan" />
              {plan?.kind === "action_plan" && (
                <>
                  <Button variant="outline" onClick={() => navigate({
                    pathname: `/changes/plans/${plan.plan.id}`,
                    search: planNavigationSearch(plan.plan, workspace?.window),
                  })}>
                    {m.openPlan}<ArrowRight size={15} />
                  </Button>
                  <p className="muted">{m.openPlanScopeHint}</p>
                </>
              )}
            </article>
          </div>
          <p className="supervised-action-status" role="status" aria-live="polite">
            {busy ? m.pending : message}
          </p>
        </>
      )}
    </section>
  );
}

function shortId(value: string) {
  return value.length > 24 ? `${value.slice(0, 12)}…${value.slice(-8)}` : value;
}

function OperationBlockers({ view, kind }: {
  view?: ActionReadinessView;
  kind: keyof ActionReadinessView["operations"];
}) {
  const labels = useActionReadinessLabels();
  const operation = view?.operations[kind];
  if (!operation || operation.allowed || !operation.reason_codes.length) return null;
  return <ul className="action-operation-blockers">
    {operation.reason_codes.map((reason, index) => <li key={`${reason}-${index}`}>{labels.reason(reason)}</li>)}
  </ul>;
}

function isBrokerResource(resource: string | undefined): resource is string {
  return Boolean(resource && (resource.startsWith("broker/") || resource.startsWith("broker:")));
}
