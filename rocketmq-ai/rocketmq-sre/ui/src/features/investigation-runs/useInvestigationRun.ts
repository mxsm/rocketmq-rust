import { useCallback, useEffect, useRef, useState } from "react";

import { ApiError } from "@/api/client";
import type {
  InvestigationCommandRequest,
  InvestigationRun,
  InvestigationRunControls,
  InvestigationRunEvent,
  InvestigationRunEventPage,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import {
  compareRevision,
  decimal,
  RunScopeError,
  terminalRunStatuses,
  validCommands,
  validateRun,
} from "./runPresentation";

interface RunState {
  owner: string;
  run?: InvestigationRun;
  controls?: InvestigationRunControls;
  events: InvestigationRunEvent[];
  loading: boolean;
  error?: unknown;
  resynchronized: boolean;
}

export function useInvestigationRun(investigationId: string, runId: string) {
  const { api } = useSreData();
  const auth = useAuth();
  const scope = useWorkspaceScope();
  const tenantId = auth.session?.tenantId ?? "";
  const owner = `${scope.scopeKey}:${investigationId}:${runId}`;
  const [state, setState] = useState<RunState>();
  const refreshRef = useRef<() => Promise<void>>(async () => {});
  const lifetime = useRef<AbortSignal | undefined>(undefined);

  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller.signal;
    const expected = {
      id: runId,
      investigationId,
      tenantId,
      clusterId: scope.clusterId,
    };
    let latest: InvestigationRun | undefined;
    let cursor = "0";
    let pending = false;
    let inFlight: Promise<void> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;
    const adopt = (run: InvestigationRun) => {
      validateRun(run, expected);
      if (
        latest &&
        compareRevision(run.revision, latest.revision) >= 0 &&
        compareRevision(run.control_epoch, latest.control_epoch) < 0
      )
        throw new RunScopeError();
      if (!latest || compareRevision(run.revision, latest.revision) >= 0)
        latest = run;
    };
    const checkPage = (page: InvestigationRunEventPage) => {
      if (
        page.schema_version !==
          "rocketmq-sre.investigation-run-event-page.v1" ||
        page.tenant_id !== tenantId ||
        page.cluster_id !== scope.clusterId ||
        page.investigation_id !== investigationId ||
        page.run_id !== runId ||
        !decimal(page.next_sequence) ||
        !decimal(page.latest_sequence) ||
        (!page.snapshot_required &&
          compareRevision(page.next_sequence, cursor) < 0) ||
        compareRevision(page.next_sequence, page.latest_sequence) > 0 ||
        !Array.isArray(page.events) ||
        page.events.length > 50
      )
        throw new RunScopeError();
      let previous = cursor;
      for (const event of page.events) {
        if (
          event.schema_version !== "rocketmq-sre.investigation-run-event.v1" ||
          event.tenant_id !== tenantId ||
          event.cluster_id !== scope.clusterId ||
          event.investigation_id !== investigationId ||
          event.run_id !== runId ||
          !decimal(event.sequence) ||
          !decimal(event.revision) ||
          !decimal(event.control_epoch) ||
          BigInt(event.sequence) !== BigInt(previous) + 1n ||
          compareRevision(event.sequence, page.next_sequence) > 0
        )
          throw new RunScopeError();
        previous = event.sequence;
      }
      if (page.has_more && compareRevision(page.next_sequence, cursor) <= 0)
        throw new RunScopeError();
      if (
        !page.snapshot_required &&
        (previous !== page.next_sequence ||
          page.has_more !==
            compareRevision(page.next_sequence, page.latest_sequence) < 0)
      )
        throw new RunScopeError();
      if (page.snapshot_required) {
        if (
          !page.current_snapshot ||
          page.events.length ||
          page.has_more ||
          page.next_sequence !== page.latest_sequence
        )
          throw new RunScopeError();
        adopt(page.current_snapshot);
      }
    };
    const cycle = async () => {
      setState((previous) => ({
        ...(previous?.owner === owner
          ? previous
          : { events: [], resynchronized: false }),
        owner,
        loading: true,
      }));
      try {
        const controls = await api.getInvestigationRunControls(
          runId,
          controller.signal,
        );
        if (controller.signal.aborted) return;
        if (
          controls.schema_version !== "rocketmq-sre.investigation-run.v1" ||
          (controls.receipt_replay_allowed !== undefined &&
            typeof controls.receipt_replay_allowed !== "boolean") ||
          !validCommands(controls.commands) ||
          !controls.budget_usage
        )
          throw new RunScopeError();
        adopt(controls.run);
        setState((previous) => ({
          owner,
          run: latest,
          controls:
            controls.run.revision === latest?.revision &&
            controls.run.control_epoch === latest.control_epoch
              ? controls
              : undefined,
          events: previous?.owner === owner ? previous.events : [],
          resynchronized:
            previous?.owner === owner ? previous.resynchronized : false,
          loading: true,
        }));
        // A bounded page per cycle catches up without an unbounded history loop.
        const page = await api.getInvestigationRunEvents(
          runId,
          cursor,
          controller.signal,
        );
        if (controller.signal.aborted) return;
        checkPage(page);
        cursor = page.next_sequence;
        if (
          page.snapshot_required &&
          latest &&
          compareRevision(latest.revision, controls.run.revision) > 0
        )
          pending = true;
        setState((previous) => ({
          owner,
          run: latest,
          controls:
            controls.run.revision === latest?.revision &&
            controls.run.control_epoch === latest.control_epoch
              ? controls
              : undefined,
          events: page.snapshot_required
            ? []
            : [
                ...(previous?.owner === owner ? previous.events : []),
                ...page.events,
              ].slice(-128),
          resynchronized:
            page.snapshot_required ||
            (previous?.owner === owner ? previous.resynchronized : false),
          loading: false,
        }));
        if (page.has_more) pending = true;
      } catch (error) {
        if (controller.signal.aborted) return;
        const invalid =
          error instanceof RunScopeError ||
          (error instanceof ApiError && [401, 403, 404].includes(error.status));
        if (invalid) {
          latest = undefined;
          stopped = true;
        }
        setState((previous) => ({
          owner,
          run: invalid ? undefined : latest,
          controls: undefined,
          events: invalid ? [] : (previous?.events ?? []),
          resynchronized: false,
          loading: false,
          error,
        }));
      }
    };
    const request = (): Promise<void> => {
      if (controller.signal.aborted) return Promise.resolve();
      stopped = false;
      if (inFlight) {
        pending = true;
        return inFlight;
      }
      if (timer) clearTimeout(timer);
      inFlight = (async () => {
        let cycles = 0;
        do {
          pending = false;
          await cycle();
          cycles += 1;
        } while (
          pending &&
          cycles < 4 &&
          !stopped &&
          !controller.signal.aborted
        );
      })().finally(() => {
        inFlight = undefined;
        if (
          !controller.signal.aborted &&
          !stopped &&
          (pending || !latest || !terminalRunStatuses.has(latest.status))
        )
          timer = setTimeout(() => void request(), 2000);
      });
      return inFlight;
    };
    refreshRef.current = request;
    void request();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
      refreshRef.current = async () => {};
    };
  }, [api, investigationId, owner, runId, scope.clusterId, tenantId]);

  const refresh = useCallback(() => refreshRef.current(), []);
  const submit = useCallback(
    async (request: InvestigationCommandRequest) => {
      const signal = lifetime.current;
      if (!signal || signal.aborted) return;
      const receipt = await api.commandInvestigationRun(runId, request, signal);
      if (signal.aborted) return;
      if (
        receipt.run_id !== runId ||
        receipt.tenant_id !== tenantId ||
        receipt.command_id !== request.command_id ||
        receipt.idempotency_key !== request.idempotency_key ||
        receipt.actor_subject !== auth.session?.subject ||
        !decimal(receipt.applied_revision)
      )
        throw new RunScopeError();
      // An immutable replay receipt can be older than the visible run. It is never a snapshot.
      await refreshRef.current();
      return receipt;
    },
    [api, auth.session?.subject, runId, tenantId],
  );
  const current: RunState =
    state?.owner === owner
      ? state
      : { owner, events: [], loading: true, resynchronized: false };
  return { ...current, refresh, submit };
}
