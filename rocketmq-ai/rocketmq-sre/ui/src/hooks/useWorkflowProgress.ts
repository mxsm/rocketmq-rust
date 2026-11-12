import { useEffect, useState } from "react";

import type { SreApi } from "@/api/client";
import type { WorkflowStreamEvent } from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { useSreData } from "@/data/SreDataContext";

export type ProgressTransport = "connecting" | "sse" | "polling";
interface ProgressState {
  transport: ProgressTransport;
  events: WorkflowStreamEvent[];
}
interface EventTarget {
  aggregateType?: WorkflowStreamEvent["aggregate_type"];
  aggregateId?: string;
}
interface Listener extends EventTarget {
  active?: boolean;
  refreshing?: boolean;
  queued?: boolean;
  clusterId: string;
  update: (state: ProgressState) => void;
  poll?: () => void | Promise<void>;
  events: WorkflowStreamEvent[];
}
interface Subscription {
  controller: AbortController;
  listeners: Set<Listener>;
  transport: ProgressTransport;
  timer?: number;
}

// The API instance already owns the identity/token boundary. One stream serves
// every mounted consumer for that identity and tenant, then filters per listener.
const subscriptions = new WeakMap<SreApi, Map<string, Subscription>>();

function refresh(listener: Listener) {
  if (!listener.active || !listener.poll) return;
  if (listener.refreshing) {
    listener.queued = true;
    return;
  }
  listener.refreshing = true;
  void Promise.resolve()
    .then(() => (listener.active ? listener.poll?.() : undefined))
    .catch(() => undefined)
    .finally(() => {
      listener.refreshing = false;
      if (listener.queued) {
        listener.queued = false;
        refresh(listener);
      }
    });
}

function listen(api: SreApi, tenantId: string, listener: Listener) {
  listener.active = true;
  let tenants = subscriptions.get(api);
  if (!tenants) {
    tenants = new Map();
    subscriptions.set(api, tenants);
  }
  let subscription = tenants.get(tenantId);
  if (!subscription) {
    subscription = {
      controller: new AbortController(),
      listeners: new Set(),
      transport: "connecting",
    };
    tenants.set(tenantId, subscription);
    const owned = subscription;
    const seen = new Set<string>();
    const fallback = () => {
      if (owned.controller.signal.aborted || owned.timer !== undefined) return;
      owned.transport = "polling";
      const poll = () =>
        owned.listeners.forEach((item) => {
          item.update({ transport: "polling", events: item.events });
          refresh(item);
        });
      poll();
      owned.timer = window.setInterval(poll, 10_000);
    };
    void api
      .subscribeWorkflowEvents((event) => {
        if (owned.controller.signal.aborted || event.tenant_id !== tenantId)
          return;
        const key = `${event.tenant_id}:${event.cluster_id}:${
          event.event_id ??
          `${event.aggregate_type}:${event.aggregate_id}:${event.event_type}:${event.correlation_id}:${event.occurred_at}`
        }`;
        if (seen.has(key)) return;
        seen.add(key);
        if (seen.size > 256) seen.delete(seen.values().next().value!);
        owned.transport = "sse";
        owned.listeners.forEach((item) => {
          if (
            event.cluster_id !== item.clusterId ||
            (item.aggregateType &&
              event.aggregate_type !== item.aggregateType) ||
            (item.aggregateId && event.aggregate_id !== item.aggregateId)
          )
            return;
          item.events = [event, ...item.events].slice(0, 8);
          item.update({ transport: "sse", events: item.events });
          if (event.payload?.provisional !== true) refresh(item);
        });
      }, owned.controller.signal)
      .then(fallback)
      .catch(fallback);
  }
  subscription.listeners.add(listener);
  listener.update({ transport: subscription.transport, events: [] });
  if (subscription.transport === "polling") refresh(listener);
  const owned = subscription;
  return () => {
    listener.active = false;
    owned.listeners.delete(listener);
    if (!owned.listeners.size) {
      owned.controller.abort();
      if (owned.timer !== undefined) window.clearInterval(owned.timer);
      tenants.delete(tenantId);
    }
  };
}

export function useWorkflowProgress(
  clusterId: string,
  poll?: () => void | Promise<void>,
  target?: EventTarget,
) {
  const { api } = useSreData();
  const auth = useAuth();
  const tenantId = auth.session?.tenantId ?? "";
  const allowed = auth.hasClusterScope(clusterId);
  const aggregateType = target?.aggregateType;
  const aggregateId = target?.aggregateId;
  const key = `${tenantId}:${clusterId}:${aggregateType ?? ""}:${aggregateId ?? ""}`;
  const [state, setState] = useState<
    ProgressState & { owner: SreApi; key: string }
  >();

  useEffect(() => {
    if (!clusterId || !tenantId || !allowed) return;
    return listen(api, tenantId, {
      clusterId,
      poll,
      aggregateType,
      aggregateId,
      events: [],
      update: (next) => setState({ ...next, owner: api, key }),
    });
  }, [
    aggregateId,
    aggregateType,
    allowed,
    api,
    clusterId,
    key,
    poll,
    tenantId,
  ]);
  const current = state?.owner === api && state.key === key ? state : undefined;
  return {
    transport: current?.transport ?? "connecting",
    events: current?.events ?? [],
  };
}
