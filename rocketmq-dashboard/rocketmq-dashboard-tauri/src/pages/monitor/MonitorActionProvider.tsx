import { createContext, useContext, useLayoutEffect, useMemo, useSyncExternalStore, type ReactNode } from 'react';
import { MonitorService } from '../../services/monitor.service';
import { ConnectionStore } from '../../services/connection.store';
import { createMonitorActions } from './monitorActions';

const Context = createContext<ReturnType<typeof createMonitorActions> | null>(null);
export function MonitorActionProvider({ children }: { children: ReactNode }) {
    const controller = useMemo(() => createMonitorActions(() => {
        const settings = ConnectionStore.getSnapshot();
        return settings?.environmentId ? { revision: settings.revision, environmentId: settings.environmentId } : null;
    }, target => target.kind === 'save'
        ? MonitorService.save(target.request)
        : MonitorService.delete({ consumerGroup: target.request.consumerGroup, revision: target.request.expectedRevision })), []);
    useLayoutEffect(() => { controller.start(); return controller.stop; }, [controller]);
    return <Context.Provider value={controller}>{children}</Context.Provider>;
}
export function useMonitorActions() {
    const controller = useContext(Context);
    if (!controller) throw new Error('MonitorActionProvider is required.');
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    return { ...state, submit: controller.submit };
}
