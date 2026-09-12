import { useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import { ConnectionStore } from '../../../services/connection.store';
import { createReadResource } from '../../../hooks/readResource';
import { aclContextMatches, type AclContext } from '../aclActions';

/** A mounted Broker scope owns its requests and last successful observations. */
export function useAclRead<T>(load: () => Promise<T>, context: AclContext, fallback: string) {
    const loader = useRef(load);
    loader.current = load;
    const resource = useMemo(() => createReadResource(() => loader.current(), fallback), [context.environmentId, context.revision, fallback]);
    const state = useSyncExternalStore(resource.subscribe, resource.getSnapshot, resource.getSnapshot);
    const refresh = useCallback(() => aclContextMatches(context, ConnectionStore.getSnapshot()) ? resource.read() : Promise.resolve(false), [resource, context.environmentId, context.revision]);
    const refreshAfterWrite = useCallback(() => { resource.invalidate(); return refresh(); }, [resource, refresh]);
    useEffect(() => {
        const unsubscribe = ConnectionStore.subscribe(() => {
            if (!aclContextMatches(context, ConnectionStore.getSnapshot())) resource.invalidate();
        });
        void refresh();
        return () => { unsubscribe(); resource.invalidate(); };
    }, [resource, refresh, context.environmentId, context.revision]);
    return { data: state.data, loading: state.pending, error: state.error, observedAt: state.receivedAt, refresh, refreshAfterWrite,
        ready: state.data !== null && !state.pending && !state.error && aclContextMatches(context, ConnectionStore.getSnapshot()) };
}
