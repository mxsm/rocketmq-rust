import { useEffect, useMemo, useSyncExternalStore } from 'react';
import { createReadResource } from './readResource';

export function useReadResource<T>(loader: (() => Promise<T>) | null, fallback: string) {
    const resource = useMemo(() => createReadResource(loader, fallback), [loader, fallback]);
    const state = useSyncExternalStore(resource.subscribe, resource.getSnapshot, resource.getSnapshot);
    useEffect(() => {
        void resource.read();
        return resource.invalidate;
    }, [resource]);
    return { ...state, read: resource.read };
}
