import type { SreApi } from "@/api/client";
import type { CapabilitySnapshot } from "@/api/types";

// This bounds reuse after an HTTP read, not the age of the source observation.
export const CAPABILITY_CACHE_MAX_AGE_MS = 30_000;

interface Entry {
  snapshot: CapabilitySnapshot;
  receivedAt: number;
}

/** In-memory snapshots owned by one API principal and one authorized roster. */
export class ClusterCapabilityCache {
  private readonly entries = new Map<string, Entry>();
  private readonly listeners = new Set<() => void>();
  private readonly allowed: Set<string>;
  private version = 0;

  constructor(
    private readonly api: SreApi,
    clusterIds: readonly string[],
    private readonly now = () => performance.now(),
  ) {
    this.allowed = new Set(clusterIds);
  }

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  };

  getVersion = () => this.version;

  invalidate = () => {
    this.entries.clear();
    this.changed();
  };

  expire = () => {
    for (const entry of this.entries.values()) {
      if (this.now() - entry.receivedAt >= CAPABILITY_CACHE_MAX_AGE_MS) {
        // Expire the cohort together, avoiding one refresh per worker response.
        this.invalidate();
        return;
      }
    }
  };

  private changed() {
    this.version += 1;
    this.listeners.forEach((listener) => listener());
  }

  private current(id: string) {
    const entry = this.entries.get(id);
    return entry && this.now() - entry.receivedAt < CAPABILITY_CACHE_MAX_AGE_MS
      ? entry
      : undefined;
  }

  async load(clusterIds: readonly string[], signal: AbortSignal) {
    const version = this.version;
    const ids = [...new Set(clusterIds)];
    const missing = ids.filter((id) => this.allowed.has(id) && !this.current(id));
    const active = () => !signal.aborted && version === this.version;
    let next = 0;
    const worker = async () => {
      while (active() && next < missing.length) {
        const id = missing[next++];
        try {
          const snapshot = await this.api.getClusterCapabilities(id, signal);
          if (active() && snapshot.cluster_id === id) {
            this.entries.set(id, { snapshot, receivedAt: this.now() });
          }
        } catch {
          // Failures and wrong-cluster replies are never cached or used as fallback.
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(4, missing.length) }, worker));
    if (!active()) throw new DOMException("Aborted", "AbortError");

    const snapshots: Record<string, CapabilitySnapshot> = {};
    let expiresAt = Infinity;
    for (const id of ids) {
      const entry = this.current(id);
      if (!entry) continue;
      expiresAt = Math.min(expiresAt, entry.receivedAt + CAPABILITY_CACHE_MAX_AGE_MS);
      const elapsed = Math.max(0, this.now() - entry.receivedAt);
      snapshots[id] = {
        ...entry.snapshot,
        data_sources: entry.snapshot.data_sources.map((source) => ({
          ...source,
          freshness_ms: source.freshness_ms === undefined
            ? undefined
            : source.freshness_ms + elapsed,
        })),
      };
    }
    return {
      snapshots,
      unavailable: ids.some((id) => !snapshots[id]),
      expiresAt,
    };
  }
}
