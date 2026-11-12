import { useCallback, useMemo } from "react";

import type { CapabilitySnapshot } from "@/api/types";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";

/** Reordering Fleet rows never restarts successful reads; identity owns the cache. */
export function useClusterCapabilitySnapshots(clusterIds: readonly string[]) {
  const { api } = useSreData();
  const owner = useMemo(
    () => ({ api, cache: new Map<string, CapabilitySnapshot>() }),
    [api],
  );
  const cache = owner.cache;
  const idsKey = JSON.stringify([...new Set(clusterIds)].sort());
  const load = useCallback(async (signal: AbortSignal) => {
    const ids = JSON.parse(idsKey) as string[];
    const missing = ids.filter((id) => !cache.has(id));
    let next = 0;
    const worker = async () => {
      while (!signal.aborted && next < missing.length) {
        const id = missing[next++];
        try {
          const snapshot = await api.getClusterCapabilities(id, signal);
          if (!signal.aborted && snapshot.cluster_id === id) {
            cache.set(id, snapshot);
          }
        } catch {
          // Failed or mismatched snapshots remain explicitly unavailable and retryable.
        }
      }
    };
    await Promise.all(
      Array.from({ length: Math.min(4, missing.length) }, worker),
    );
    if (signal.aborted) throw new DOMException("Aborted", "AbortError");
    return {
      snapshots: Object.fromEntries(
        ids.flatMap((id) => {
          const snapshot = cache.get(id);
          return snapshot ? [[id, snapshot]] : [];
        }),
      ) as Record<string, CapabilitySnapshot>,
      unavailable: ids.some((id) => !cache.has(id)),
    };
  }, [api, cache, idsKey]);
  const resource = useAsyncResource(load);
  const reloadResource = resource.reload;
  const reload = useCallback(() => {
    cache.clear();
    return reloadResource();
  }, [cache, reloadResource]);
  return { ...resource, reload };
}
