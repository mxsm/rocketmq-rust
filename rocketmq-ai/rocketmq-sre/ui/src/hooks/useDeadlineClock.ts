import { useEffect, useState } from "react";

/** Rerender at actual deadlines without polling or requesting a new grant. */
export function useDeadlineClock(deadlines: readonly (string | null | undefined)[]) {
  const [version, update] = useState(0);
  const now = Date.now();
  const future = deadlines.map((value) => value ? Date.parse(value) : NaN)
    .filter((value) => Number.isFinite(value) && value > now);
  const next = future.length ? Math.min(...future) : undefined;
  useEffect(() => {
    if (next === undefined) return;
    const timer = window.setTimeout(() => update((value) => value + 1), Math.min(Math.max(next - Date.now(), 0), 2_147_483_647));
    return () => window.clearTimeout(timer);
  }, [next, version]);
  return now;
}
