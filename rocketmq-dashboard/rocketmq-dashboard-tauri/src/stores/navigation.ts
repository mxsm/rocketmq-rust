import type { ConsumerQueryScope } from '../features/consumer/types/consumer.types';
export type Tab = 'NameServer' | 'Proxy' | 'Dashboard' | 'Cluster' | 'Topic' |
    'Consumer' | 'Producer' | 'Message' | 'MessageTrace' | 'DLQ' | 'ACL' |
    'Account' | 'Sessions' | 'Audit' | 'Monitors' | 'Storage';

export type EntityTarget =
    | { kind: 'topic'; name: string; detail: 'overview' | 'status' | 'route' | 'consumers' | 'config' }
    | { kind: 'consumer'; name: string; detail: 'overview' | 'progress' | 'clients' | 'config'; scope: ConsumerQueryScope }
    | { kind: 'broker'; address: string; detail: 'overview' | 'status' | 'config' };

export interface NavigationLocation {
    id: number;
    tab: Tab;
    target: EntityTarget | null;
    environmentId: string | null;
}

export interface NavigationState {
    current: NavigationLocation;
    history: NavigationLocation[];
    sequence: number;
}

export type NavigationAction =
    | { type: 'open'; tab: Tab; target?: EntityTarget; environmentId: string | null }
    | { type: 'back' }
    | { type: 'reset'; environmentId: string | null };

export const initialNavigation: NavigationState = {
    current: { id: 0, tab: 'Dashboard', target: null, environmentId: null },
    history: [], sequence: 0,
};

export function navigationReducer(state: NavigationState, action: NavigationAction): NavigationState {
    if (action.type === 'back') {
        const current = state.history[state.history.length - 1];
        return current ? { ...state, current, history: state.history.slice(0, -1) } : state;
    }
    const id = state.sequence + 1;
    if (action.type === 'reset') {
        const keepMounted = ['NameServer', 'Proxy', 'Account', 'Sessions', 'Audit'].includes(state.current.tab);
        return { current: { id: keepMounted ? state.current.id : id, tab: state.current.tab, target: null, environmentId: action.environmentId }, history: [], sequence: id };
    }
    return {
        current: { id, tab: action.tab, target: action.target ?? null, environmentId: action.environmentId },
        history: [...state.history, state.current].slice(-20), sequence: id,
    };
}

export function findEntity<T>(items: readonly T[], identity: (item: T) => string, requested: string | null): T | null {
    return requested === null ? items[0] ?? null : items.find((item) => identity(item) === requested) ?? null;
}
