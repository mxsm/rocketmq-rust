import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useCountUp } from './useCountUp';

const DURATION_MS = 700;

function stubReducedMotion(matches: boolean) {
  vi.spyOn(window, 'matchMedia').mockImplementation((query: string) => ({
    matches,
    media: query,
    onchange: null,
    addListener: () => undefined,
    removeListener: () => undefined,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
    dispatchEvent: () => false
  }));
}

describe('useCountUp', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['requestAnimationFrame', 'cancelAnimationFrame', 'performance'] });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('shows the first value without animating', () => {
    const { result } = renderHook(({ value }) => useCountUp(value), { initialProps: { value: 128 } });

    expect(result.current).toBe(128);
  });

  it('moves from the shown value to a new target and lands on it exactly', () => {
    const { result, rerender } = renderHook(({ value }) => useCountUp(value), { initialProps: { value: 100 } });

    rerender({ value: 200 });
    expect(result.current).toBe(100);

    act(() => { vi.advanceTimersByTime(DURATION_MS / 2); });
    expect(result.current).toBeGreaterThan(100);
    expect(result.current).toBeLessThan(200);
    expect(Number.isInteger(result.current)).toBe(true);

    act(() => { vi.advanceTimersByTime(DURATION_MS); });
    expect(result.current).toBe(200);
  });

  it('keeps the precision of fractional targets', () => {
    const { result, rerender } = renderHook(({ value }) => useCountUp(value), { initialProps: { value: 1.5 } });

    rerender({ value: 2.75 });
    act(() => { vi.advanceTimersByTime(DURATION_MS * 2); });

    expect(result.current).toBe(2.75);
  });

  it('shows the new value at once when reduced motion is requested', () => {
    stubReducedMotion(true);
    const { result, rerender } = renderHook(({ value }) => useCountUp(value), { initialProps: { value: 10 } });

    rerender({ value: 90 });

    expect(result.current).toBe(90);
  });
});
