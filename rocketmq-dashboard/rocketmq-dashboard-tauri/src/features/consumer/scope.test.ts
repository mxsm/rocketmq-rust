import { describe, expect, it } from 'vitest';
import type { ConnectionSettingsView } from '../../services/connection.store';
import { ConsumerRequestGeneration, resolveConsumerScope } from './scope';

const settings: ConnectionSettingsView = {
    revision: 1, credentialsConfigured: false, currentNameserverId: null, environmentId: 'env-a',
    nameserver: { currentNamesrv: null, namesrvAddrList: [], useTLS: false, useVIPChannel: false },
    currentProxyId: 'proxy-a',
    proxy: { currentProxyAddr: '127.0.0.1:8080', proxyAddrList: ['127.0.0.1:8080'] },
    endpoints: [{ endpointId: 'proxy-a', kind: 'proxy', address: '127.0.0.1:8080', environmentId: null }],
};

describe('consumer query scope', () => {
    it('uses only the selected configured Proxy and preserves NameServer mode without one', () => {
        expect(resolveConsumerScope(settings, 'proxy')).toEqual({ mode: 'proxy', endpointId: 'proxy-a' });
        const empty = { ...settings, currentProxyId: null };
        expect(resolveConsumerScope(empty, 'proxy')).toBeNull();
        expect(resolveConsumerScope(empty, 'name_server')).toEqual({ mode: 'name_server' });
        expect(resolveConsumerScope({ ...settings, endpoints: [] }, 'proxy')).toBeNull();
        expect(resolveConsumerScope(null, 'name_server')).toBeNull();
    });

    it('uses the new endpoint after a configuration change', () => {
        const changed = { ...settings, currentProxyId: 'proxy-b', proxy: { currentProxyAddr: '127.0.0.1:8082', proxyAddrList: ['127.0.0.1:8082'] }, endpoints: [{ endpointId: 'proxy-b', kind: 'proxy' as const, address: '127.0.0.1:8082', environmentId: null }] };
        expect(resolveConsumerScope(changed, 'proxy')).toEqual({ mode: 'proxy', endpointId: 'proxy-b' });
    });

    it('discards an older response resolving after the new scope or after disposal', async () => {
        const generation = new ConsumerRequestGeneration();
        let release!: (value: string) => void;
        const delayed = new Promise<string>((resolve) => { release = resolve; });
        const oldRequest = generation.begin();
        const applied: string[] = [];
        const older = delayed.then((value) => { if (oldRequest()) applied.push(value); });
        const current = generation.begin();
        if (current()) applied.push('new-scope');
        release('old-scope');
        await older;
        expect(applied).toEqual(['new-scope']);
        generation.invalidate();
        expect(current()).toBe(false);
    });
});
