import { useEffect, useMemo, useSyncExternalStore } from 'react';
import { createDashboardRead } from '../readResource';

export function useDashboardRead<T>(loader: (() => Promise<T>) | null, fallback: string) {
    const resource = useMemo(() => createDashboardRead(loader, fallback), [loader, fallback]);
    const state = useSyncExternalStore(resource.subscribe, resource.getSnapshot, resource.getSnapshot);
    useEffect(() => {
        void resource.read();
        return resource.invalidate;
    }, [resource]);
    return { ...state, read: resource.read };
}
