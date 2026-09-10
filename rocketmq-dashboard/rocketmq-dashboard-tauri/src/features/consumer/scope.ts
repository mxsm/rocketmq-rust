import type { ConnectionSettingsView } from '../../services/connection.store';
import type { ConsumerQueryScope } from './types/consumer.types';

export function resolveConsumerScope(settings: ConnectionSettingsView | null, mode: ConsumerQueryScope['mode']): ConsumerQueryScope | null {
    if (!settings) return null;
    if (mode === 'name_server') return { mode: 'name_server' };
    const endpoint = settings.endpoints.find((item) => item.kind === 'proxy' && item.endpointId === settings.currentProxyId && item.address === settings.proxy.currentProxyAddr);
    return endpoint ? { mode: 'proxy', endpointId: endpoint.endpointId } : null;
}

export const consumerScopeKey = (scope: ConsumerQueryScope) => scope.mode === 'name_server' ? 'name_server' : `proxy:${scope.endpointId}`;
export const consumerScopeLabel = (scope: ConsumerQueryScope) => scope.mode === 'name_server' ? 'NameServer discovery' : 'Configured Proxy';

// A later request or page disposal invalidates every callback from the older request.
export class ConsumerRequestGeneration {
    private revision = 0;
    begin(): () => boolean {
        const revision = ++this.revision;
        return () => revision === this.revision;
    }
    invalidate(): void { this.revision++; }
}
