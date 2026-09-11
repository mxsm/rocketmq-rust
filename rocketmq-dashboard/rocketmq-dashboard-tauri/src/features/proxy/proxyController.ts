import type { ConnectionSettingsView } from '../../services/connection.store';
import { dashboardErrorMessage, isDashboardClientError } from '../../services/invoke';
import { ProxyService } from '../../services/proxy.service';

export interface ProxyChange {
    readonly kind: 'add' | 'switch' | 'delete';
    readonly address: string;
}

interface ProxyState {
    settings: ConnectionSettingsView | null;
    refreshing: boolean;
    refreshedAt: number | null;
    pendingChange: ProxyChange | null;
    needsReview: boolean;
    loadError: string;
    changeError: string;
    failedChange: ProxyChange | null;
    receipt: { change: ProxyChange; message: string; revision: number } | null;
}

const isConflict = (error: unknown) =>
    isDashboardClientError(error) && error.code === 'dashboard.configuration_conflict';

export function describeProxyChange(change: ProxyChange): string {
    const verbs = { add: 'Add', switch: 'Use', delete: 'Delete' };
    return `${verbs[change.kind]} ${change.address}`;
}

/** The catalog owns configuration reads; it does not probe Proxy health. */
export function createProxyController(service: Omit<typeof ProxyService, 'prototype'> = ProxyService) {
    let state: ProxyState = { settings: null, refreshing: false, refreshedAt: null,
        pendingChange: null, needsReview: false, loadError: '', changeError: '', failedChange: null, receipt: null };
    const listeners = new Set<() => void>();
    let active = false;
    let generation = 0;
    let knownRevision = -1;
    let reading: Promise<boolean> | null = null;
    const publish = (patch: Partial<ProxyState>) => {
        state = { ...state, ...patch };
        listeners.forEach(listener => listener());
    };
    const observeRevision = (revision: number | undefined) => {
        if (revision === undefined) return;
        knownRevision = Math.max(knownRevision, revision);
        if (active && state.settings && knownRevision > state.settings.revision && !state.pendingChange && !state.needsReview) {
            publish({ needsReview: true, refreshedAt: null });
        }
    };

    const refresh = (review = false): Promise<boolean> => {
        if (!active || state.pendingChange || (state.needsReview && !review)) return Promise.resolve(false);
        if (reading) return reading;
        const request = ++generation;
        publish({ refreshing: true });
        reading = (async () => {
            await Promise.resolve();
            if (!active || request !== generation) return false;
            try {
                const result = await service.getHomePageInfo();
                if (!active || request !== generation) return false;
                const revision = result.settings.revision;
                if (revision < knownRevision || (!review && state.settings && revision !== state.settings.revision)) {
                    observeRevision(revision);
                    publish({ needsReview: true, refreshedAt: null, loadError: '' });
                    return false;
                }
                knownRevision = revision;
                publish({ settings: result.settings, refreshedAt: Date.now(), needsReview: false, loadError: '' });
                return true;
            } catch (error) {
                if (active && request === generation) publish({
                    loadError: dashboardErrorMessage(error, 'Could not refresh Proxy settings.'),
                    ...(isConflict(error) ? { needsReview: true, refreshedAt: null } : {}),
                });
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

    const submit = async (change: ProxyChange): Promise<boolean> => {
        if (!active || !state.settings || state.pendingChange || state.needsReview) return false;
        const revision = state.settings.revision;
        const request = ++generation;
        reading = null;
        publish({ pendingChange: change, refreshing: false, changeError: '', failedChange: null });
        try {
            const result = await (() => {
                switch (change.kind) {
                    case 'add': return service.addProxyAddr(change.address, revision);
                    case 'switch': return service.switchProxyAddr(change.address, revision);
                    case 'delete': return service.deleteProxyAddr(change.address, revision);
                }
            })();
            if (!active || request !== generation) return false;
            knownRevision = Math.max(knownRevision, result.settings.revision);
            publish({
                settings: result.settings, pendingChange: null, refreshedAt: null, loadError: '',
                needsReview: result.settings.revision < knownRevision,
                receipt: { change, message: result.message, revision: result.settings.revision },
            });
            // The accepted snapshot and receipt remain visible if this read fails.
            void refresh();
            return true;
        } catch (error) {
            if (active && request === generation) publish({
                pendingChange: null, failedChange: change,
                changeError: dashboardErrorMessage(error, 'The Proxy change could not be completed.'),
                needsReview: isConflict(error) || knownRevision > revision,
            });
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
        observeRevision, refresh, submit,
        dismissChangeError: () => publish({ failedChange: null, changeError: '' }),
    };
}
