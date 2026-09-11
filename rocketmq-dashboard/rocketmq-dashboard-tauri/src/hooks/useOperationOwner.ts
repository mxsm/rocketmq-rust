import { useEffect, useLayoutEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import { ConnectionStore } from '../services/connection.store';
import { createOperationController } from './operationController';

/** Keep an accepted write with its original context until the session owner is disposed. */
export function useOperationOwner(revision: number, environmentId: string | null, expectedScope = '', currentScope = '') {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const scope = useRef(currentScope);
    scope.current = currentScope;
    const controller = useMemo(() => createOperationController(() => {
        const current = ConnectionStore.getSnapshot();
        return current?.revision === revision && current.environmentId === environmentId && scope.current === expectedScope;
    }), [revision, environmentId, expectedScope]);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    useLayoutEffect(() => { controller.start(); return controller.stop; }, [controller]);
    useEffect(() => controller.observeContext(), [controller, settings?.revision, settings?.environmentId, currentScope]);
    const contextChanged = state.contextChanged || settings?.revision !== revision || settings?.environmentId !== environmentId || currentScope !== expectedScope;
    return { controller, state, contextChanged, busy: state.operation !== null, blocked: contextChanged || state.operation !== null };
}
export type OperationOwner = ReturnType<typeof useOperationOwner>;
