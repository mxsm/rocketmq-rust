import { useCallback, useEffect, useMemo, useSyncExternalStore } from 'react';
import { createNameServerController } from '../nameServerController';

const NAMESERVER_REFRESH_INTERVAL_MS = 5_000;

export const useNameServer = () => {
    const controller = useMemo(createNameServerController, []);
    const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot, controller.getSnapshot);
    useEffect(() => {
        controller.start();
        void controller.refresh();
        const interval = window.setInterval(() => { void controller.refresh(); }, NAMESERVER_REFRESH_INTERVAL_MS);
        return () => {
            window.clearInterval(interval);
            controller.stop();
        };
    }, [controller]);
    const refresh = useCallback(() => { void controller.refresh(true); }, [controller]);
    return { ...state, refresh, submit: controller.submit, dismissChangeError: controller.dismissChangeError };
};
