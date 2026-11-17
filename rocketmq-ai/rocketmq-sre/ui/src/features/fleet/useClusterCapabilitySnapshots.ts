import { useCallback, useEffect, useSyncExternalStore } from "react";

import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";

/** The provider retains bounded snapshots when navigation unmounts this reader. */
export function useClusterCapabilitySnapshots(clusterIds: readonly string[]) {
  const { capabilityCache: cache } = useSreData();
  const version = useSyncExternalStore(cache.subscribe, cache.getVersion);
  const idsKey = JSON.stringify([...new Set(clusterIds)].sort());
  const load = useCallback((signal: AbortSignal) => {
    // Bind the request to an invalidation generation before effects can run.
    if (version !== cache.getVersion()) {
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    }
    return cache.load(JSON.parse(idsKey) as string[], signal);
  }, [cache, idsKey, version]);
  const resource = useAsyncResource(load);
  const expiresAt = resource.data?.expiresAt;
  useEffect(() => {
    if (expiresAt === undefined || !Number.isFinite(expiresAt)) return;
    const timer = window.setTimeout(cache.expire, Math.max(0, Math.ceil(expiresAt - performance.now())));
    return () => window.clearTimeout(timer);
  }, [cache, expiresAt]);
  useEffect(() => {
    // Background tabs may throttle the deadline timer.
    window.addEventListener("focus", cache.expire);
    document.addEventListener("visibilitychange", cache.expire);
    return () => {
      window.removeEventListener("focus", cache.expire);
      document.removeEventListener("visibilitychange", cache.expire);
    };
  }, [cache]);
  const reloadResource = resource.reload;
  const reload = useCallback(() => {
    cache.invalidate();
    return reloadResource();
  }, [cache, reloadResource]);
  return { ...resource, reload };
}
