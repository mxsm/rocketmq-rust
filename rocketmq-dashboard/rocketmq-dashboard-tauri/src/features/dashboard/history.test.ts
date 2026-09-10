import { expect, it } from 'vitest';
import { localHistoryDay } from './history';

it('uses local calendar boundaries instead of assuming every day has 24 hours', () => {
    expect(localHistoryDay('2026-03-08')).toEqual({ beginMs: new Date(2026, 2, 8).getTime(), endMs: new Date(2026, 2, 9).getTime() });
    expect(() => localHistoryDay('2026-02-30')).toThrow();
    expect(() => localHistoryDay('invalid')).toThrow();
});
