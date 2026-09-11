import { Clock3, Globe, Monitor, RefreshCw } from 'lucide-react';
import { useAppStore } from '../../stores/app.store';
import type { ConnectionSettingsView } from '../../services/connection.store';
import { usePageToolbar } from './pageToolbar';

export function EnvironmentToolbar({ settings }: { settings: ConnectionSettingsView | null }) {
    const { setActiveTab } = useAppStore();
    const toolbar = usePageToolbar();
    const endpoint = settings?.endpoints.find(value => value.endpointId === settings.currentNameserverId);
    return <header className="desktop-toolbar" aria-label="Environment toolbar">
        <span className="desktop-environment" title={settings?.environmentId ?? 'No environment selected'}>
            <Monitor /><span>{settings?.environmentId ? 'Local environment' : 'No environment'}</span>
        </span>
        <button type="button" className="desktop-endpoint" onClick={() => setActiveTab('NameServer')}
            title={endpoint?.address ?? 'Configure a NameServer'}>
            <Globe /><span>NameServer {endpoint?.address ?? 'not configured'}</span>
        </button>
        <div className="desktop-toolbar-status">
            {toolbar?.refreshedAt != null && <span className="desktop-observed" title="Last successful page refresh">
                <Clock3 /><time dateTime={new Date(toolbar.refreshedAt).toISOString()}>{new Date(toolbar.refreshedAt).toLocaleTimeString()}</time>
            </span>}
            {toolbar && <button type="button" className="desktop-refresh" disabled={toolbar.pending} onClick={toolbar.refresh}>
                <RefreshCw className={toolbar.pending ? 'is-refreshing' : undefined} /><span>{toolbar.pending ? 'Refreshing…' : 'Refresh'}</span>
            </button>}
        </div>
    </header>;
}
