import { expect, it } from 'vitest';
import { diagnosticBytes, diagnosticTime, observationAge } from './storageModel';

it('keeps unmeasured storage distinct from a measured zero and preserves exact byte counts', () => {
    expect(diagnosticBytes(null).display).toBe('Not measured');
    expect(diagnosticBytes(0).display).toBe('0 bytes');
    const bytes = diagnosticBytes(8_400_000);
    expect(bytes.display).toContain('MB');
    expect(bytes.exact).toBe(`${(8_400_000).toLocaleString()} bytes`);
    for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1]) expect(diagnosticBytes(value).display).toBe('Unknown / unavailable');
});

it('does not turn absent or invalid timestamps into the current time', () => {
    expect(diagnosticTime(null)).toBe('Not observed');
    expect(diagnosticTime(Number.NaN)).toBe('Unknown / unavailable');
    expect(diagnosticTime(Number.MAX_VALUE)).toBe('Unknown / unavailable');
    expect(diagnosticTime(0)).toBe(new Date(0).toLocaleString());
});

it('identifies stale and future observations without assuming a fresh check from local rendering', () => {
    expect(observationAge(100_000, 159_999)).toBe('current');
    expect(observationAge(100_000, 160_000)).toBe('stale');
    expect(observationAge(200_000, 100_000)).toBe('unknown');
    expect(observationAge(null, 100_000)).toBe('unknown');
});
