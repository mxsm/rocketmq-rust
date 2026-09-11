import { createContext, useContext, useLayoutEffect, useMemo, useSyncExternalStore, type ReactNode } from 'react';

export interface PageToolbarState {
    refresh: () => void;
    pending: boolean;
    refreshedAt?: number | null;
}

export function createPageToolbarStore() {
    let snapshot: PageToolbarState | null = null;
    let owner: symbol | null = null;
    const listeners = new Set<() => void>();
    const notify = () => listeners.forEach(listener => listener());
    return {
        getSnapshot: () => snapshot,
        subscribe: (listener: () => void) => {
            listeners.add(listener);
            return () => { listeners.delete(listener); };
        },
        register: (value: PageToolbarState) => {
            const token = Symbol('page-toolbar');
            owner = token;
            snapshot = value;
            notify();
            return () => {
                // A delayed unmount cannot remove a newer page's refresh action.
                if (owner !== token) return;
                owner = null;
                snapshot = null;
                notify();
            };
        },
    };
}

const ToolbarContext = createContext<ReturnType<typeof createPageToolbarStore> | null>(null);

export function PageToolbarProvider({ scope, children }: { scope: string; children: ReactNode }) {
    const store = useMemo(createPageToolbarStore, [scope]);
    return <ToolbarContext.Provider value={store}>{children}</ToolbarContext.Provider>;
}

/** Register the current page's existing read action, never an unrelated global reload. */
export function usePageRefresh({ refresh, pending, refreshedAt }: PageToolbarState) {
    const store = useContext(ToolbarContext);
    useLayoutEffect(() => store?.register({ refresh, pending, refreshedAt }), [store, refresh, pending, refreshedAt]);
}

export function usePageToolbar() {
    const store = useContext(ToolbarContext);
    return useSyncExternalStore(store?.subscribe ?? (() => () => {}), store?.getSnapshot ?? (() => null), () => null);
}
