import { useEffect, useMemo } from "react";
import type { AsyncResource } from "./useAsyncResource";

/** Merge timer/SSE/manual reads. The AsyncResource owns error presentation. */
export function useSerialResourceRefresh(reload: AsyncResource<unknown>["reload"]) {
  const slot = useMemo(() => {
    let active = false;
    let queued = false;
    let running: Promise<void> | undefined;
    return {
      start: () => { active = true; },
      stop: () => { active = false; queued = false; },
      refresh: (): Promise<void> => {
        if (!active) return Promise.resolve();
        if (running) { queued = true; return running; }
        running = Promise.resolve().then(async () => {
          do {
            queued = false;
            if (!active) return;
            // reload settles on success, failure, cancellation and unmount.
            await reload();
          } while (active && queued);
        }).finally(() => { running = undefined; });
        return running;
      },
    };
  }, [reload]);
  useEffect(() => { slot.start(); return () => slot.stop(); }, [slot]);
  return slot.refresh;
}
