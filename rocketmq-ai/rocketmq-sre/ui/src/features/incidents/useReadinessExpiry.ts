import { useEffect, useState } from "react";

import type { ActionReadinessView } from "@/api/types";
import { readinessExpired } from "./actionReadinessPresentation";

/** Stop offering stale operations without issuing another source request. */
export function useReadinessExpiry(view?: ActionReadinessView) {
  const [version, update] = useState(0);
  const until = view?.valid_until;
  useEffect(() => {
    if (!until) return;
    const remaining = Date.parse(until) - Date.now();
    if (!Number.isFinite(remaining) || remaining <= 0) return;
    const timer = window.setTimeout(
      () => update((current) => current + 1),
      Math.min(remaining + 1, 2_147_483_647),
    );
    return () => window.clearTimeout(timer);
  }, [until, version]);
  return readinessExpired(view);
}
