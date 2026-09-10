import { describe, expect, it } from 'vitest';
import { offsetResetRequest } from './offsetReset';
import { ConsumerRequestGeneration } from '../consumer/scope';

describe('offset reset confirmation', () => {
    it('keeps the exact entity and converts local time to milliseconds', () => {
        expect(offsetResetRequest('orders', 'orders-reader', '2026-09-10T12:34', false)).toEqual({
            topic: 'orders', consumerGroupList: ['orders-reader'], resetTime: new Date(2026, 8, 10, 12, 34).getTime(), force: false,
        });
    });
    it('rejects empty identities, invalid dates and pre-epoch input', () => {
        for (const value of ['', 'invalid', '2026-02-30T12:00', '2026-09-10T25:00', '1900-01-01T12:00']) {
            expect(() => offsetResetRequest('orders', 'reader', value, true)).toThrow();
        }
        expect(() => offsetResetRequest('orders', '', '2026-09-10T12:00', false)).toThrow();
    });
    it('prevents an old reset from refreshing a new group after disposal', async () => {
        const generation = new ConsumerRequestGeneration();
        const current = generation.begin();
        generation.invalidate();
        const readGroups: string[] = [];
        await Promise.resolve();
        if (current()) readGroups.push('previous-group');
        expect(readGroups).toEqual([]);
    });
});
