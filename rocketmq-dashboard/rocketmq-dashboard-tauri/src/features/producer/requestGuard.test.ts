import { describe, expect, it } from 'vitest';
import { ProducerRequestGuard } from './requestGuard';

describe('Producer request ownership', () => {
    it('discards a delayed response after changing the Group or starting a newer lookup', async () => {
        const guard = new ProducerRequestGuard();
        const applied: string[] = [];
        let completeOld!: (value: string) => void;
        const old = new Promise<string>(resolve => { completeOld = resolve; });
        const oldCurrent = guard.begin();
        const pending = old.then(value => { if (oldCurrent()) applied.push(value); });
        guard.invalidate();
        const newCurrent = guard.begin();
        if (newCurrent()) applied.push('new-group');
        completeOld('old-group');
        await pending;
        expect(applied).toEqual(['new-group']);
        guard.invalidate();
        expect(newCurrent()).toBe(false);
    });
});
