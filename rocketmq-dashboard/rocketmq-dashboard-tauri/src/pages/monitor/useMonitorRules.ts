import { useCallback, useEffect, useMemo, useSyncExternalStore } from 'react';
import { ConnectionStore } from '../../services/connection.store';
import { MonitorService } from '../../services/monitor.service';
import { createReadResource } from '../../hooks/readResource';
import { monitorContextMatches, type MonitorContext } from './monitorModel';

export function useMonitorRules(context: MonitorContext) {
    const resource = useMemo(() => createReadResource(MonitorService.list, 'Monitor rules could not be loaded.'), [context.revision, context.environmentId]);
    const state = useSyncExternalStore(resource.subscribe, resource.getSnapshot, resource.getSnapshot);
    const refresh = useCallback(() => monitorContextMatches(context, ConnectionStore.getSnapshot())
        ? resource.read() : Promise.resolve(false), [resource, context.revision, context.environmentId]);
    const afterWrite = useCallback(() => { resource.invalidate(); return refresh(); }, [resource, refresh]);
    useEffect(() => {
        const unsubscribe = ConnectionStore.subscribe(() => {
            if (!monitorContextMatches(context, ConnectionStore.getSnapshot())) resource.invalidate();
        });
        void refresh();
        return () => { unsubscribe(); resource.invalidate(); };
    }, [resource, refresh, context.revision, context.environmentId]);
    return { ...state, refresh, afterWrite, ready: state.data !== null && !state.pending && !state.error && monitorContextMatches(context, ConnectionStore.getSnapshot()) };
}
