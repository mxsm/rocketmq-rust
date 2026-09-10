import { SessionStorageService } from './session.storage';
import type { NameServerConfigSnapshot } from '../features/nameserver/types/nameserver.types';
import type { ProxyConfigSnapshot } from '../features/proxy/types/proxy.types';
export interface ConnectionSettingsView {
    revision: number;
    credentialsConfigured: boolean;
    endpoints: Array<{ endpointId: string; kind: 'name_server' | 'proxy'; address: string; environmentId: string | null }>;
    currentNameserverId: string | null; currentProxyId: string | null; environmentId: string | null;
    nameserver: NameServerConfigSnapshot; proxy: ProxyConfigSnapshot;
}
const listeners = new Set<() => void>();
let owner: string | null = null;
let settings: ConnectionSettingsView | null = null;
export const ConnectionStore = {
    getSnapshot: (): ConnectionSettingsView | null => owner === SessionStorageService.getSessionId() ? settings : null,
    subscribe: (listener: () => void): (() => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    accept: (sessionId: string, next: ConnectionSettingsView): void => {
        if (sessionId !== SessionStorageService.getSessionId()) return;
        const previous = ConnectionStore.getSnapshot();
        if (previous && next.revision <= previous.revision) return;
        owner = sessionId; settings = next;
        for (const listener of listeners) listener();
    },
    reset: (): void => { owner = null; settings = null; for (const listener of listeners) listener(); },
};
SessionStorageService.subscribeAuthenticationFailure((reason) => { if (reason === 'invalid') ConnectionStore.reset(); });
