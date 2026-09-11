import type { ConnectionSettingsView } from '../../services/connection.store';
import { dashboardErrorMessage, isDashboardClientError } from '../../services/invoke';
import { NameServerService } from '../../services/nameserver.service';
import type { NameServerStatusItem } from './types/nameserver.types';

export type NameServerChange =
    | { kind: 'add' | 'switch' | 'delete'; address: string }
    | { kind: 'vip' | 'tls'; enabled: boolean };

interface NameServerState {
    settings: ConnectionSettingsView | null;
    observation: { servers: NameServerStatusItem[]; receivedAt: number } | null;
    refreshing: boolean;
    pendingChange: NameServerChange | null;
    needsReview: boolean;
    loadError: string;
    changeError: string;
    failedChange: NameServerChange | null;
    receipt: { message: string; revision: number } | null;
}

const configurationConflict = (error: unknown) =>
    isDashboardClientError(error) && error.code === 'dashboard.configuration_conflict';

export function describeNameServerChange(change: NameServerChange): string {
    switch (change.kind) {
        case 'add': return `Add ${change.address}`;
        case 'switch': return `Use ${change.address}`;
        case 'delete': return `Delete ${change.address}`;
        case 'vip': return `${change.enabled ? 'Enable' : 'Disable'} VIP channel`;
        case 'tls': return `${change.enabled ? 'Enable' : 'Disable'} TLS`;
    }
}

/** Owns page reads and writes. A successful write remains successful if probing fails. */
export function createNameServerController(service: Omit<typeof NameServerService, 'prototype'> = NameServerService) {
    let state: NameServerState = {
        settings: null, observation: null, refreshing: false, pendingChange: null,
        needsReview: false, loadError: '', changeError: '', failedChange: null, receipt: null,
    };
    const listeners = new Set<() => void>();
    let active = false;
    let generation = 0;
    let reading: Promise<boolean> | null = null;
    const publish = (patch: Partial<NameServerState>) => {
        state = { ...state, ...patch };
        listeners.forEach(listener => listener());
    };

    const refresh = (review = false): Promise<boolean> => {
        if (!active || state.pendingChange || (state.needsReview && !review)) return Promise.resolve(false);
        if (reading) return reading;
        const request = ++generation;
        publish({ refreshing: true });
        reading = (async () => {
            // Strict Mode cleanup can retire the page before dispatching its first request.
            await Promise.resolve();
            if (!active || request !== generation) return false;
            try {
                const result = await service.getHomePageInfo();
                if (!active || request !== generation) return false;
                const revision = state.settings?.revision;
                if (revision !== undefined && result.settings.revision < revision) return false;
                if (!review && revision !== undefined && result.settings.revision !== revision) {
                    publish({ needsReview: true, observation: null, loadError: '' });
                    return false;
                }
                publish({
                    settings: result.settings,
                    observation: { servers: result.servers, receivedAt: Date.now() },
                    needsReview: false, loadError: '',
                });
                return true;
            } catch (error) {
                if (active && request === generation) {
                    publish({
                        loadError: dashboardErrorMessage(error, 'Could not refresh NameServer settings.'),
                        ...(configurationConflict(error) ? { needsReview: true, observation: null } : {}),
                    });
                }
                return false;
            } finally {
                if (active && request === generation) {
                    reading = null;
                    publish({ refreshing: false });
                }
            }
        })();
        return reading;
    };

    const submit = async (change: NameServerChange): Promise<boolean> => {
        if (!active || !state.settings || state.pendingChange || state.needsReview) return false;
        const revision = state.settings.revision;
        const request = ++generation;
        reading = null;
        publish({ pendingChange: change, refreshing: false, changeError: '', failedChange: null });
        try {
            const result = await (() => {
                switch (change.kind) {
                    case 'add': return service.addNameServer(change.address, revision);
                    case 'switch': return service.switchNameServer(change.address, revision);
                    case 'delete': return service.deleteNameServer(change.address, revision);
                    case 'vip': return service.updateVipChannel(change.enabled, revision);
                    case 'tls': return service.updateUseTls(change.enabled, revision);
                }
            })();
            if (!active || request !== generation) return false;
            // Probe evidence belongs to the previous settings, even when addresses match.
            publish({
                settings: result.settings, observation: null, loadError: '',
                receipt: { message: result.message, revision: result.settings.revision },
                pendingChange: null,
            });
            void refresh();
            return true;
        } catch (error) {
            if (active && request === generation) {
                publish({
                    pendingChange: null, failedChange: change,
                    changeError: dashboardErrorMessage(error, 'The NameServer change could not be completed.'),
                    ...(configurationConflict(error) ? { needsReview: true, observation: null } : {}),
                });
            }
            return false;
        }
    };

    return {
        getSnapshot: () => state,
        subscribe: (listener: () => void) => {
            listeners.add(listener);
            return () => { listeners.delete(listener); };
        },
        start: () => { active = true; },
        stop: () => {
            active = false;
            generation++;
            reading = null;
            state = { ...state, refreshing: false, pendingChange: null };
        },
        refresh,
        submit,
        dismissChangeError: () => publish({ changeError: '', failedChange: null }),
    };
}
