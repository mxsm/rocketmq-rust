import { useCallback, useEffect, useRef, useState } from 'react';
import type { TopologySnapshot, TopologySourceId } from '../../types/topology';
import { createTopologyLoader, failedTopologySnapshot } from './topology-loader';
import {
  allSourcesFailed,
  carryForwardFailedLayers,
  diffSnapshots,
  failedSources,
  TOPOLOGY_SOURCES,
  type TopologyChange
} from './topology-model';

/** Entries kept in the change list. */
export const MAX_CHANGE_ENTRIES = 20;

export interface TopologyChangeEntry extends TopologyChange {
  id: string;
  at: number;
}

export interface TopologySnapshotState {
  /** Latest snapshot, with the last good data of any layer that failed this time. */
  snapshot: TopologySnapshot | null;
  previous: TopologySnapshot | null;
  /** Newest first. */
  changes: TopologyChangeEntry[];
  /** Changes found by the latest refresh; null until there are two snapshots to compare. */
  latestChangeCount: number | null;
  querying: boolean;
  /** Set only when nothing could be loaded at all. */
  error: string | null;
  /** When the next scheduled refresh starts; null while querying, hidden or refreshing on demand. */
  nextRefreshAt: number | null;
  /** Completed refreshes, for keying the per-refresh animations. */
  refreshCount: number;
  /** For each layer, when it last loaded successfully. */
  loadedAt: Partial<Record<TopologySourceId, number>>;
}

export interface UseTopologySnapshotOptions {
  /** Time between refreshes; null refreshes only on demand. */
  intervalMs: number | null;
  load?: () => Promise<TopologySnapshot>;
}

const initialState: TopologySnapshotState = {
  snapshot: null,
  previous: null,
  changes: [],
  latestChangeCount: null,
  querying: true,
  error: null,
  nextRefreshAt: null,
  refreshCount: 0,
  loadedAt: {}
};

let sharedLoader: (() => Promise<TopologySnapshot>) | null = null;

/** One loader per page session keeps the knowledge of whether the backend has the aggregated endpoint. */
function defaultLoad() {
  sharedLoader ??= createTopologyLoader();
  return sharedLoader();
}

const isVisible = () => document.visibilityState === 'visible';

/**
 * Polls the topology on an interval. Only one query runs at a time, polling pauses while the page
 * is hidden and resumes with an immediate refresh, and results arriving after unmount are dropped.
 */
export function useTopologySnapshot({ intervalMs, load = defaultLoad }: UseTopologySnapshotOptions) {
  const [state, setState] = useState<TopologySnapshotState>(initialState);
  const mounted = useRef(false);
  const inFlight = useRef(false);
  const timer = useRef<number | null>(null);
  const latest = useRef<{ snapshot: TopologySnapshot | null; loadedAt: Partial<Record<TopologySourceId, number>>; refreshCount: number }>({
    snapshot: null,
    loadedAt: {},
    refreshCount: 0
  });
  const intervalRef = useRef(intervalMs);
  const loadRef = useRef(load);
  loadRef.current = load;
  const runRef = useRef<() => Promise<void>>(async () => undefined);

  const clearTimer = useCallback(() => {
    if (timer.current !== null) {
      window.clearTimeout(timer.current);
      timer.current = null;
    }
  }, []);

  const schedule = useCallback(() => {
    clearTimer();
    const interval = intervalRef.current;
    if (!mounted.current || interval === null || !isVisible()) {
      setState((current) => (current.nextRefreshAt === null ? current : { ...current, nextRefreshAt: null }));
      return;
    }
    timer.current = window.setTimeout(() => {
      timer.current = null;
      void runRef.current();
    }, interval);
    const nextRefreshAt = Date.now() + interval;
    setState((current) => ({ ...current, nextRefreshAt }));
  }, [clearTimer]);

  runRef.current = async () => {
    if (inFlight.current || !mounted.current) return;
    inFlight.current = true;
    clearTimer();
    setState((current) => ({ ...current, querying: true, nextRefreshAt: null }));

    let result: TopologySnapshot;
    try {
      result = await loadRef.current();
    } catch (error) {
      result = failedTopologySnapshot(error, 0, Date.now());
    }
    inFlight.current = false;
    if (!mounted.current) return;

    const previous = latest.current.snapshot;
    if (allSourcesFailed(result) && !previous) {
      const message = result.sources.find((source) => source.message)?.message ?? 'The topology could not be loaded.';
      setState((current) => ({ ...current, querying: false, error: message }));
      schedule();
      return;
    }

    const merged = carryForwardFailedLayers(result, previous);
    const failed = failedSources(result);
    const loadedBefore = latest.current.loadedAt;
    const loadedAt = { ...loadedBefore };
    for (const source of TOPOLOGY_SOURCES) {
      if (!failed.has(source) && result.sources.some((status) => status.source === source)) loadedAt[source] = result.generatedAt;
    }
    // A layer that never loaded before has nothing to compare against.
    const skip = new Set([...failed, ...TOPOLOGY_SOURCES.filter((source) => loadedBefore[source] === undefined)]);
    const found = previous ? diffSnapshots(previous, merged, skip) : [];
    const refreshCount = latest.current.refreshCount + 1;
    const entries = found.map((change, index) => ({ ...change, id: `${refreshCount}-${index}`, at: merged.generatedAt }));
    latest.current = { snapshot: merged, loadedAt, refreshCount };

    setState((current) => ({
      ...current,
      snapshot: merged,
      previous,
      changes: [...entries, ...current.changes].slice(0, MAX_CHANGE_ENTRIES),
      latestChangeCount: previous ? found.length : null,
      querying: false,
      error: null,
      refreshCount,
      loadedAt
    }));
    schedule();
  };

  useEffect(() => {
    mounted.current = true;
    void runRef.current();
    return () => {
      mounted.current = false;
      clearTimer();
    };
  }, [clearTimer]);

  useEffect(() => {
    if (intervalRef.current === intervalMs) return;
    intervalRef.current = intervalMs;
    if (!inFlight.current) schedule();
  }, [intervalMs, schedule]);

  useEffect(() => {
    const onVisibilityChange = () => {
      if (!isVisible()) {
        clearTimer();
        setState((current) => (current.nextRefreshAt === null ? current : { ...current, nextRefreshAt: null }));
      } else if (intervalRef.current !== null) {
        void runRef.current();
      }
    };
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => document.removeEventListener('visibilitychange', onVisibilityChange);
  }, [clearTimer]);

  const refresh = useCallback(() => {
    void runRef.current();
  }, []);

  return { ...state, refresh };
}
