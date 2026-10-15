import { useEffect, useRef, useState } from 'react';
import { usePrefersReducedMotion } from './usePrefersReducedMotion';

const FALLBACK_DURATION_MS = 700;

/** Reads `--duration-slower`, so the count-up keeps pace with the motion tokens. */
function readDurationMs() {
  if (typeof document === 'undefined') return FALLBACK_DURATION_MS;
  const raw = getComputedStyle(document.documentElement).getPropertyValue('--duration-slower').trim();
  const value = Number.parseFloat(raw);
  if (!Number.isFinite(value) || value <= 0) return FALLBACK_DURATION_MS;
  return raw.endsWith('ms') ? value : value * 1000;
}

/** Quartic ease-out, a close stand-in for the `--ease-out` curve. */
function easeOut(progress: number) {
  return 1 - (1 - progress) ** 4;
}

function decimalPlaces(value: number) {
  if (Number.isInteger(value)) return 0;
  return Math.min(4, String(value).split('.')[1]?.length ?? 0);
}

/**
 * Animates from the previously shown number to `target` whenever `target` changes.
 *
 * The first value is shown as is: the count-up marks a change, it is not an entrance.
 * With reduced motion, or a non-finite target, the new value is shown at once.
 */
export function useCountUp(target: number) {
  const [display, setDisplay] = useState(target);
  const shownRef = useRef(target);
  const reducedMotion = usePrefersReducedMotion();

  useEffect(() => {
    const from = shownRef.current;
    if (from === target) return undefined;
    if (reducedMotion || !Number.isFinite(target) || !Number.isFinite(from)) {
      shownRef.current = target;
      setDisplay(target);
      return undefined;
    }

    const duration = readDurationMs();
    const places = Math.max(decimalPlaces(from), decimalPlaces(target));
    const startedAt = performance.now();
    let frame = 0;
    const tick = (now: number) => {
      const progress = Math.min(1, (now - startedAt) / duration);
      const next = progress === 1 ? target : Number((from + (target - from) * easeOut(progress)).toFixed(places));
      shownRef.current = next;
      setDisplay(next);
      if (progress < 1) frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [reducedMotion, target]);

  return display;
}
