import { useEffect, useLayoutEffect, useMemo, useSyncExternalStore } from 'react';
import { ConnectionStore } from '../../../services/connection.store';
import { createTopicOperationController } from '../topicOperationController';

export function useTopicOperation(revision: number, environmentId: string | null) {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const controller = useMemo(() => createTopicOperationController(() => {
        const current = ConnectionStore.getSnapshot();
        return current?.revision === revision && current.environmentId === environmentId;
    }), [revision, environmentId]);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    useLayoutEffect(() => { controller.start(); return controller.stop; }, [controller]);
    useEffect(() => controller.observeContext(), [controller, settings?.revision, settings?.environmentId]);
    const contextChanged = state.contextChanged || settings?.revision !== revision || settings?.environmentId !== environmentId;
    return { controller, state, contextChanged, busy: state.operation !== null, blocked: contextChanged || state.operation !== null };
}
export type TopicOperationOwner = ReturnType<typeof useTopicOperation>;
