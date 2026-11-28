import { useLayoutEffect } from "react";

import type { CollectionEnvelope, IncidentView } from "@/api/types";

import {
  incidentIsStale,
  latestDiagnosisIsPartial,
} from "./incidentPresentation";

interface IncidentTool {
  name: string;
  description: string;
  inputSchema: {
    type: "object";
    properties: Record<string, never>;
    additionalProperties: false;
  };
  annotations: { readOnlyHint: true };
  execute: (
    input: unknown,
    options: { signal: AbortSignal },
  ) => Promise<unknown>;
}

interface IncidentModelContext {
  registerTool(
    tool: IncidentTool,
    options: { signal: AbortSignal },
  ): Promise<void>;
}

type ReadIncidents = (
  signal: AbortSignal,
) => Promise<CollectionEnvelope<IncidentView>>;

export function useIncidentListTool(
  scopeKey: string | undefined,
  read: ReadIncidents,
  selectVisible: (items: IncidentView[]) => IncidentView[],
) {
  useLayoutEffect(() => {
    const context = (
      document as Document & { modelContext?: IncidentModelContext }
    ).modelContext;
    if (
      !scopeKey ||
      !window.isSecureContext ||
      typeof context?.registerTool !== "function"
    )
      return;
    const registration = new AbortController();
    const tool: IncidentTool = {
      name: "list_incident_queue",
      description:
        "List up to 25 incidents matching this page's filters in the signed-in operator's current cluster. Returns IDs, status, severity and evidence freshness.",
      inputSchema: {
        type: "object",
        properties: {},
        additionalProperties: false,
      },
      annotations: { readOnlyHint: true },
      async execute(input, { signal }) {
        if (
          typeof input !== "object" ||
          input === null ||
          Array.isArray(input) ||
          Object.keys(input).length
        ) {
          throw new Error("This tool accepts an empty object.");
        }
        const active = AbortSignal.any([registration.signal, signal]);
        active.throwIfAborted();
        let result: CollectionEnvelope<IncidentView>;
        try {
          result = await read(active);
        } catch {
          active.throwIfAborted();
          throw new Error("The scoped incident queue is unavailable.");
        }
        active.throwIfAborted();
        const visible = selectVisible(result.items);
        const observed = new Date(result.observed_at);
        return {
          observed_at: result.observed_at,
          partial: result.partial,
          has_more: visible.length > 25 || Boolean(result.next_cursor),
          incidents: visible.slice(0, 25).map((view) => ({
            id: view.incident.id,
            status: view.incident.status,
            severity: view.incident.severity ?? null,
            updated_at: view.incident.updated_at,
            partial: latestDiagnosisIsPartial(view),
            stale: incidentIsStale(view, observed),
          })),
        };
      },
    };
    const register = async () => {
      try {
        await context.registerTool(tool, { signal: registration.signal });
      } catch {
        // An unavailable browser tool leaves the normal page controls usable.
      }
    };
    void register();
    return () => registration.abort();
  }, [scopeKey, read, selectVisible]);
}
