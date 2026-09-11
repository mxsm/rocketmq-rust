import { useCallback, useEffect, useMemo, useSyncExternalStore } from 'react';
import { ConnectionStore } from '../../../services/connection.store';
import { createProxyController } from '../proxyController';

export const useProxyCatalog = () => {
    const controller = useMemo(createProxyController, []);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    const shared = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    useEffect(() => {
        controller.start();
        void controller.refresh();
        const interval = window.setInterval(() => { void controller.refresh(); }, 5_000);
        return () => { window.clearInterval(interval); controller.stop(); };
    }, [controller]);
    useEffect(() => {
        controller.observeRevision(shared?.revision);
    }, [controller, shared?.revision, state.settings?.revision, state.pendingChange]);
    const refresh = useCallback(() => { void controller.refresh(true); }, [controller]);
    const externalChange = Boolean(shared && state.settings && shared.revision > state.settings.revision && !state.pendingChange);
    return { ...state, needsReview: state.needsReview || externalChange, refresh,
        submit: controller.submit, dismissChangeError: controller.dismissChangeError };
};
