import { describe, expect, it, vi } from 'vitest';
import { createPageToolbarStore } from './pageToolbar';

describe('page toolbar ownership', () => {
    it('does not let an old page unmount clear the next refresh action', () => {
        const store = createPageToolbarStore();
        const oldRefresh = vi.fn();
        const currentRefresh = vi.fn();
        const removeOld = store.register({ refresh: oldRefresh, pending: false });
        const removeCurrent = store.register({ refresh: currentRefresh, pending: false });
        removeOld();
        store.getSnapshot()?.refresh();
        expect(oldRefresh).not.toHaveBeenCalled();
        expect(currentRefresh).toHaveBeenCalledTimes(1);
        removeCurrent();
        expect(store.getSnapshot()).toBeNull();
    });

    it('isolates navigation and connection scopes without retaining old refresh state', () => {
        const oldScope = createPageToolbarStore();
        oldScope.register({ refresh: vi.fn(), pending: true, refreshedAt: 1000 });
        const currentScope = createPageToolbarStore();
        expect(currentScope.getSnapshot()).toBeNull();
        expect(oldScope.getSnapshot()?.pending).toBe(true);
    });

    it('publishes refresh state and releases subscriptions', () => {
        const store = createPageToolbarStore();
        const listener = vi.fn();
        const unsubscribe = store.subscribe(listener);
        const release = store.register({ refresh: vi.fn(), pending: false });
        expect(listener).toHaveBeenCalledTimes(1);
        release();
        expect(listener).toHaveBeenCalledTimes(2);
        unsubscribe();
        store.register({ refresh: vi.fn(), pending: true });
        expect(listener).toHaveBeenCalledTimes(2);
    });
});
