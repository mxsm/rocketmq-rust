import { describe, expect, it } from 'vitest';
import { findEntity, initialNavigation, navigationReducer } from './navigation';

describe('entity navigation', () => {
    it('returns to the exact source entry and reopens an entity as a new visit', () => {
        const list = navigationReducer(initialNavigation, { type: 'open', tab: 'Topic', environmentId: 'env-a' });
        const topic = navigationReducer(list, { type: 'open', tab: 'Topic', environmentId: 'env-a', target: { kind: 'topic', name: 'Orders', detail: 'consumers' } });
        const consumer = navigationReducer(topic, { type: 'open', tab: 'Consumer', environmentId: 'env-a', target: { kind: 'consumer', name: 'OrderReaders', detail: 'progress', proxyAddress: '127.0.0.1:8080' } });
        const returned = navigationReducer(consumer, { type: 'back' });
        expect(returned.current).toEqual(topic.current);
        expect(navigationReducer(returned, { type: 'back' }).current).toEqual(list.current);
        const reopened = navigationReducer(returned, { type: 'open', tab: 'Consumer', environmentId: 'env-a', target: consumer.current.target! });
        expect(reopened.current.target).toEqual(consumer.current.target);
        expect(reopened.current.id).not.toBe(consumer.current.id);
    });

    it('clears old targets and return history when the environment changes', () => {
        const source = navigationReducer(initialNavigation, { type: 'open', tab: 'Cluster', environmentId: 'env-a', target: { kind: 'broker', address: '127.0.0.1:10911', detail: 'config' } });
        const next = navigationReducer(source, { type: 'reset', environmentId: 'env-b' });
        expect(next.current).toMatchObject({ tab: 'Cluster', target: null, environmentId: 'env-b' });
        expect(next.history).toEqual([]);
        expect(navigationReducer(next, { type: 'back' })).toBe(next);
    });

    it('never selects the first entity for an explicit missing or case-mismatched target', () => {
        const items = [{ name: 'Orders' }, { name: 'Payments' }];
        expect(findEntity(items, (item) => item.name, 'Payments')).toBe(items[1]);
        expect(findEntity(items, (item) => item.name, 'Missing')).toBeNull();
        expect(findEntity(items, (item) => item.name, 'orders')).toBeNull();
        expect(findEntity(items, (item) => item.name, null)).toBe(items[0]);
    });

    it('bounds navigation history while preserving the most recent source', () => {
        let state = initialNavigation;
        for (let index = 0; index < 40; index++) state = navigationReducer(state, { type: 'open', tab: 'Topic', environmentId: 'env-a' });
        expect(state.history).toHaveLength(20);
        expect(navigationReducer(state, { type: 'back' }).current.id).toBe(39);
    });
});
