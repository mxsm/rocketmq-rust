import { useEffect, useRef, useState } from "react";

import type {
  CreateInvestigationRunRequest,
  InvestigationRunListPage,
} from "@/api/types";
import { ApiError } from "@/api/client";
import { useAuth } from "@/auth/AuthContext";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { useRunMessages } from "@/i18n/runMessages";
import { sameRunScope, validateRun } from "./runPresentation";
import { RunDialog } from "./RunDialog";

const costFields = [
  "read_queries",
  "model_calls",
  "evidence_items",
  "output_bytes",
] as const;
const budgetFields = [
  "max_steps",
  "max_in_flight",
  "max_attempts_per_step",
  "max_query_retries",
] as const;
const labels = {
  read_queries: "readQueries",
  model_calls: "modelCalls",
  evidence_items: "evidenceItems",
  output_bytes: "outputBytes",
  max_steps: "maxSteps",
  max_in_flight: "maxInFlight",
  max_attempts_per_step: "maxAttempts",
  max_query_retries: "maxRetries",
} as const;

export function CreateRunForm({
  page,
  onClose,
  onCreated,
}: {
  page: InvestigationRunListPage;
  onClose: () => void;
  onCreated: (id: string) => void;
}) {
  const m = useRunMessages();
  const { api } = useSreData();
  const auth = useAuth();
  const scope = useWorkspaceScope();
  const options = page.creation;
  const [packs, setPacks] = useState(options.compiled_packs.slice(0, 1));
  const [budget, setBudget] = useState(() =>
    structuredClone(options.default_budget),
  );
  const [duration, setDuration] = useState(options.default_duration_seconds);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(false);
  const controller = useRef(new AbortController());
  const pending = useRef<CreateInvestigationRunRequest | undefined>(undefined);
  const submitting = useRef(false);
  useEffect(() => {
    const current = new AbortController();
    controller.current = current;
    return () => current.abort();
  }, []);
  const changed = () => {
    pending.current = undefined;
    setRetry(false);
    setError("");
  };
  const submit = async () => {
    if (submitting.current || !options.allowed) return;
    const values = [
      ...costFields.map((key) => [
        budget.limit[key],
        options.maximum_budget.limit[key],
        key === "model_calls" ? 0 : 1,
      ]),
      ...budgetFields.map((key) => [
        budget[key],
        options.maximum_budget[key],
        key === "max_query_retries" ? 0 : 1,
      ]),
      [duration, options.max_duration_seconds, 1],
    ];
    if (
      !packs.length ||
      Date.parse(scope.window.end) - Date.parse(scope.window.start) >
        options.max_window_seconds * 1000 ||
      !values.every(
        ([value, max, min]) =>
          Number.isSafeInteger(value) && value >= min && value <= max,
      )
    ) {
      setError(m.formInvalid);
      return;
    }
    submitting.current = true;
    setBusy(true);
    setError("");
    const request = pending.current ?? {
      schema_version: "rocketmq-sre.investigation-run.v1",
      run_id: crypto.randomUUID(),
      idempotency_key: crypto.randomUUID(),
      scope: {
        tenant_id: page.tenant_id,
        cluster_id: page.cluster_id,
        investigation_id: page.investigation_id,
        resource: options.parent_resource,
        time_range: { start: scope.window.start, end: scope.window.end },
      },
      packs,
      budget,
      deadline: new Date(Date.now() + duration * 1000).toISOString(),
    };
    pending.current = request;
    try {
      const response = await api.createInvestigationRun(
        page.investigation_id,
        request,
        controller.current.signal,
      );
      if (controller.current.signal.aborted) return;
      validateRun(response.run, {
        id: request.run_id,
        tenantId: page.tenant_id,
        clusterId: page.cluster_id,
        investigationId: page.investigation_id,
      });
      if (
        response.idempotency_key !== request.idempotency_key ||
        response.actor_subject !== auth.session?.subject ||
        !sameRunScope(response.run.initial_scope, request.scope)
      )
        throw new Error("Invalid creation receipt");
      // The original queued snapshot may be an immutable replay. Read current controls next.
      onCreated(request.run_id);
    } catch (reason) {
      if (!controller.current.signal.aborted) {
        setError(
          reason instanceof ApiError && reason.status === 409
            ? m.conflict
            : m.commandFailure,
        );
        setRetry(true);
      }
    } finally {
      submitting.current = false;
      if (!controller.current.signal.aborted) setBusy(false);
    }
  };
  return (
    <RunDialog title={m.create} onClose={onClose}>
      <form
        className="run-form"
        onSubmit={(event) => {
          event.preventDefault();
          void submit();
        }}
      >
        <p>{m.createHint}</p>
        <p className="run-scope-caption">
          {m.actualResource}:{" "}
          {options.parent_resource
            ? `${options.parent_resource.kind} · ${options.parent_resource.key}`
            : page.cluster_id}
          <br />
          {scope.window.start} → {scope.window.end}
        </p>
        <fieldset disabled={busy}>
          <legend>{m.packs}</legend>
          <p className="muted">{m.packHint}</p>
          {options.compiled_packs.map((pack) => (
            <label
              className="run-checkbox"
              key={`${pack.pack_id}@${pack.pack_version}`}
            >
              <input
                type="checkbox"
                checked={packs.some(
                  (item) =>
                    item.pack_id === pack.pack_id &&
                    item.pack_version === pack.pack_version,
                )}
                onChange={(event) => {
                  changed();
                  setPacks(
                    event.target.checked
                      ? [...packs, pack]
                      : packs.filter(
                          (item) =>
                            item.pack_id !== pack.pack_id ||
                            item.pack_version !== pack.pack_version,
                        ),
                  );
                }}
              />
              {pack.pack_id} · {pack.pack_version}
            </label>
          ))}
        </fieldset>
        <fieldset disabled={busy} className="run-budget-fields">
          <legend>{m.budget}</legend>
          {costFields.map((key) => (
            <label key={key}>
              {m[labels[key]]}
              <input
                type="number"
                min={key === "model_calls" ? 0 : 1}
                max={options.maximum_budget.limit[key]}
                required
                value={budget.limit[key]}
                onChange={(event) => {
                  changed();
                  setBudget({
                    ...budget,
                    limit: {
                      ...budget.limit,
                      [key]: Number(event.target.value),
                    },
                  });
                }}
              />
            </label>
          ))}
          {budgetFields.map((key) => (
            <label key={key}>
              {m[labels[key]]}
              <input
                type="number"
                min={key === "max_query_retries" ? 0 : 1}
                max={options.maximum_budget[key]}
                required
                value={budget[key]}
                onChange={(event) => {
                  changed();
                  setBudget({ ...budget, [key]: Number(event.target.value) });
                }}
              />
            </label>
          ))}
          <label>
            {m.duration}
            <input
              type="number"
              min={1}
              max={options.max_duration_seconds}
              required
              value={duration}
              onChange={(event) => {
                changed();
                setDuration(Number(event.target.value));
              }}
            />
          </label>
        </fieldset>
        <p className="muted">{m.budgetHint}</p>
        {error && <p role="alert">{error}</p>}
        <Button type="submit" disabled={busy}>
          {busy ? m.pending : retry ? m.retryRequest : m.submitCreate}
        </Button>
      </form>
    </RunDialog>
  );
}
