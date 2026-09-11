import { describe, expect, it, vi } from 'vitest';
import { createProducerLookup } from './producerLookup';
import type { ProducerConnectionQueryRequest, ProducerConnectionView } from './types/producer.types';

const request = { topic: 'orders.events', producerGroup: 'orders-producer' };
const response = (scope: ProducerConnectionQueryRequest = request): ProducerConnectionView => ({ ...scope, connectionCount: 0, connections: [] });
function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(complete => { resolve = complete; });
    return { promise, resolve };
}

describe('Producer connection lookup', () => {
    it('coalesces duplicate submits and accepts a successful empty response', async () => {
        const pending = deferred<ProducerConnectionView>();
        const query = vi.fn(() => pending.promise);
        const lookup = createProducerLookup(query, () => true);
        lookup.start();
        const first = lookup.search(request);
        expect(lookup.search(request)).toBe(first);
        await Promise.resolve();
        expect(query).toHaveBeenCalledTimes(1);
        pending.resolve(response());
        expect(await first).toBe(true);
        expect(lookup.getSnapshot()).toMatchObject({ pending: false, error: '', hasSearched: true, result: response() });
    });

    it('discards an old input response after a new scope finishes', async () => {
        const old = deferred<ProducerConnectionView>();
        const query = vi.fn().mockReturnValueOnce(old.promise).mockResolvedValueOnce(response({ topic: 'payments.events', producerGroup: 'payments-producer' }));
        const lookup = createProducerLookup(query, () => true);
        lookup.start();
        const first = lookup.search(request);
        await Promise.resolve();
        lookup.reset();
        await lookup.search({ topic: 'payments.events', producerGroup: 'payments-producer' });
        old.resolve(response());
        expect(await first).toBe(false);
        expect(lookup.getSnapshot().result?.producerGroup).toBe('payments-producer');
    });

    it('keeps the last successful observation on a refresh failure', async () => {
        const query = vi.fn().mockResolvedValueOnce(response()).mockRejectedValueOnce(new Error('unavailable'));
        const lookup = createProducerLookup(query, () => true);
        lookup.start();
        await lookup.search(request);
        const observed = lookup.getSnapshot().receivedAt;
        expect(await lookup.search(request)).toBe(false);
        expect(lookup.getSnapshot()).toMatchObject({ result: response(), receivedAt: observed, pending: false, hasSearched: true });
        expect(lookup.getSnapshot().error).not.toBe('');
        lookup.reset();
        expect(lookup.getSnapshot()).toMatchObject({ result: null, receivedAt: null, error: '', hasSearched: false });
    });

    it('rejects a mismatched response without presenting it as an empty success', async () => {
        const lookup = createProducerLookup(async () => response({ ...request, producerGroup: 'another-group' }), () => true);
        lookup.start();
        expect(await lookup.search(request)).toBe(false);
        expect(lookup.getSnapshot()).toMatchObject({ result: null, receivedAt: null, hasSearched: true });
        expect(lookup.getSnapshot().error).not.toBe('');
    });

    it('invalidates on page disposal and ignores changed-environment results', async () => {
        const pending = deferred<ProducerConnectionView>();
        let current = true;
        const lookup = createProducerLookup(() => pending.promise, () => current);
        lookup.start();
        const first = lookup.search(request);
        await Promise.resolve();
        current = false;
        pending.resolve(response());
        expect(await first).toBe(false);
        expect(lookup.getSnapshot().result).toBeNull();
        lookup.stop();
        expect(lookup.getSnapshot().pending).toBe(false);
        expect(await lookup.search(request)).toBe(false);
    });

    it('validates empty fields and never dispatches a disposed preflight', async () => {
        const query = vi.fn(async () => response());
        const lookup = createProducerLookup(query, () => true);
        lookup.start();
        expect(await lookup.search({ ...request, topic: ' ' })).toBe(false);
        const pending = lookup.search(request);
        lookup.stop();
        lookup.start();
        expect(await pending).toBe(false);
        expect(query).not.toHaveBeenCalled();
    });
});
