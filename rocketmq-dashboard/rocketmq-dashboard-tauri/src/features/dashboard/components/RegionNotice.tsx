import { PageState } from '../../../components/layout/PageState';
import type { ReadState } from '../readResource';

export const formatObservation = (value: number | null) => value === null ? 'Not observed' : new Date(value).toLocaleString();

export function RegionNotice({ state, label }: { state: ReadState<unknown>; label: string }) {
    if (state.error) return <PageState kind={state.data === null ? 'error' : 'stale'}
        title={`${label} ${state.data === null ? 'unavailable' : 'refresh failed'}`}
        description={<>{state.error}{state.data !== null && <span> Showing previous data received {formatObservation(state.receivedAt)}.</span>}</>} />;
    if (state.pending) return state.data === null
        ? <PageState kind="loading" title={`Loading ${label.toLowerCase()}…`} />
        : <p className="ops-dashboard-read-state" role="status">Refreshing {label.toLowerCase()}… Previous data received {formatObservation(state.receivedAt)}.</p>;
    return null;
}
