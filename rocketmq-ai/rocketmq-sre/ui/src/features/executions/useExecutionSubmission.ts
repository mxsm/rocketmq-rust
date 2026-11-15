import { useCallback, useLayoutEffect, useRef, useState } from "react";
import type { ActionPlanView, SubmitExecutionRequest } from "@/api/types";
import type { SupervisedSreApi } from "@/api/supervisedClient";
import { ApiError } from "@/api/client";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useDeadlineClock } from "@/hooks/useDeadlineClock";
import { ExecutionScopeError, originalSubmissionMatches } from "./executionPresentation";
import { executionLookupMatches, planExecutionsMatch } from "./executionObservationPresentation";
import { readSubmissionIntent, rememberSubmissionIntent, resolveSubmissionIntent, type SubmissionIntentOwner, type SubmissionIntentRead } from "./submissionIntentStorage";

type Failure = "read" | "forbidden" | "scope" | "storage";
type Operation = { ownerKey: string; kind: "submit" | "lookup"; error?: Failure; notObserved?: boolean; acceptedId?: string; navigationFailed?: boolean };

export function useExecutionSubmission({ api, view, owner, enabled, onAccepted }: {
  api?: SupervisedSreApi;
  view?: ActionPlanView;
  owner: SubmissionIntentOwner;
  enabled: boolean;
  onAccepted: (executionId: string) => void;
}) {
  const ownerKey = JSON.stringify([owner, view?.plan.id, view?.plan.plan_hash, view?.precondition_hash]);
  const activeOwner = useRef(ownerKey);
  const pending = useRef<AbortController | undefined>(undefined);
  const [operation, setOperation] = useState<Operation>();
  const [, refreshStorage] = useState(0);
  useLayoutEffect(() => {
    activeOwner.current = ownerKey;
    return () => {
      pending.current?.abort();
      pending.current = undefined;
    };
  }, [ownerKey]);
  const selected = view && {
    plan_id: view.plan.id, plan_hash: view.plan.plan_hash, precondition_hash: view.precondition_hash,
  };
  let intent: SubmissionIntentRead = { status: "unavailable" };
  if (selected) {
    try { intent = readSubmissionIntent(window.sessionStorage, owner, selected); } catch { /* Storage access itself may be blocked. */ }
  }
  const readable = Boolean(api && view && view.plan.tenant_id === owner.tenantId && view.plan.cluster_id === owner.clusterId);
  const tenantId = owner.tenantId;
  const clusterId = owner.clusterId;
  const load = useCallback(async (signal: AbortSignal) => {
    if (!api || !view || !readable) return undefined;
    const page = await api.listPlanExecutions(view.plan.id, view.plan.plan_hash, signal);
    if (!planExecutionsMatch(page, { tenantId, clusterIds: [clusterId] }, view.plan.id, view.plan.plan_hash)) throw new ExecutionScopeError();
    return page;
  }, [api, clusterId, readable, tenantId, view]);
  const discovery = useAsyncResource(load);
  const current = operation?.ownerKey === ownerKey ? operation : undefined;
  const page = discovery.loading || discovery.error || current?.error === "forbidden" || current?.error === "scope" ? undefined : discovery.data;
  const now = useDeadlineClock([page?.valid_until]);
  const fresh = Boolean(page && Date.parse(page.valid_until) > now);
  const busy = Boolean(current && !current.error && !current.notObserved && !current.acceptedId);
  const canSubmit = enabled && readable && intent.status === "empty" && !busy && fresh &&
    page?.partial === false && page.items.length === 0 && !current?.acceptedId && current?.error !== "storage";
  const readOwner = { tenantId, clusterIds: [clusterId], subject: owner.subject };

  const run = async (kind: "submit" | "lookup", request: SubmitExecutionRequest) => {
    if (!api || pending.current || !readable) return;
    const controller = new AbortController();
    pending.current = controller;
    const active = () => !controller.signal.aborted && activeOwner.current === ownerKey && pending.current === controller;
    setOperation({ ownerKey, kind });
    try {
      let acceptedId: string | undefined;
      if (kind === "submit") {
        const value = await api.submitExecution(request, controller.signal);
        if (!active()) return;
        if (!originalSubmissionMatches(value, readOwner, request)) throw new ExecutionScopeError();
        acceptedId = value.execution.id;
      } else {
        const value = await api.lookupExecution(request, controller.signal);
        if (!active()) return;
        if (!executionLookupMatches(value, readOwner, request)) throw new ExecutionScopeError();
        if (value.status === "accepted" && value.receipt) acceptedId = value.receipt.execution_id;
      }
      if (!active()) return;
      if (acceptedId) {
        try { resolveSubmissionIntent(window.sessionStorage, owner, request); } catch { /* A stored uncertainty can be read again after navigation. */ }
        setOperation({ ownerKey, kind, acceptedId });
        refreshStorage((value) => value + 1);
        try { onAccepted(acceptedId); } catch {
          // Failed navigation cannot erase known acceptance and enable another POST.
          if (active()) setOperation({ ownerKey, kind, acceptedId, navigationFailed: true });
        }
      } else {
        // An empty read snapshot does not settle an in-flight submission.
        setOperation({ ownerKey, kind, notObserved: true });
      }
    } catch (error) {
      if (!active()) return;
      const failure: Failure = error instanceof ExecutionScopeError ? "scope" :
        error instanceof ApiError && [401, 403, 404].includes(error.status) ? "forbidden" : "read";
      setOperation({ ownerKey, kind, error: failure });
    } finally {
      if (pending.current === controller) {
        pending.current = undefined;
        if (!controller.signal.aborted) refreshStorage((value) => value + 1);
      }
    }
  };

  const submit = async (preconditionHash: string) => {
    if (!canSubmit || !selected || selected.precondition_hash !== preconditionHash || pending.current) return;
    const request: SubmitExecutionRequest = { ...selected, idempotency_key: crypto.randomUUID() };
    let remembered = false;
    try { remembered = rememberSubmissionIntent(window.sessionStorage, owner, request); } catch { /* Never POST without a durable local selector. */ }
    refreshStorage((value) => value + 1);
    if (!remembered) {
      setOperation({ ownerKey, kind: "submit", error: "storage" });
      return;
    }
    await run("submit", request);
  };
  const lookup = async () => {
    if (intent.status !== "uncertain" || pending.current) return;
    await run("lookup", intent.intent.request);
  };
  const refresh = async () => {
    await discovery.reload();
    if (activeOwner.current === ownerKey) setOperation((previous) => previous?.ownerKey === ownerKey &&
      (previous.error === "scope" || previous.error === "forbidden") ? undefined : previous);
  };
  return {
    canSubmit, submit, lookup, intent, current, busy, page, fresh,
    discoveryLoading: discovery.loading, discoveryError: discovery.error,
    refresh,
  };
}
