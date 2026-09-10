import { useEffect, useState } from 'react';
import { DashboardService } from '../../../services/dashboard.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { useAppStore } from '../../../stores/app.store';
import type { DashboardOverview } from '../types/dashboard.types';

export function GlobalOverview() {
    const { setActiveTab, openConsumer } = useAppStore();
    const [data, setData] = useState<DashboardOverview | null>(null);
    const [error, setError] = useState('');
    const [pending, setPending] = useState(false);
    const [refresh, setRefresh] = useState(0);
    useEffect(() => {
        let current = true;
        setPending(true); setError('');
        void DashboardService.getOverview().then(value => { if (current) setData(value); })
            .catch(error => { if (current) { setData(null); setError(dashboardErrorMessage(error, 'Overview unavailable.')); } })
            .finally(() => { if (current) setPending(false); });
        return () => { current = false; };
    }, [refresh]);
    return <section aria-label="Global overview" className="space-y-4">
        <div className="flex justify-between"><h2 className="text-lg font-semibold">Cluster overview</h2><button type="button" disabled={pending} onClick={() => setRefresh(value => value + 1)}>{pending ? 'Refreshing…' : 'Refresh overview'}</button></div>
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {data && <>
            <p role="status">{data.status === 'READY' ? 'Queries returned' : data.status} · Observed {new Date(data.observedAtMs).toLocaleString()}{pending ? ' · Showing the previous observation while refreshing' : ''}</p>
            <div className="grid grid-cols-2 xl:grid-cols-4 gap-4">
                {([['Topics', data.topics], ['Consumer groups', data.consumerGroups], ['Producer groups', data.producerGroups], ['Observed lag', data.totalLag]] as const).map(([label, metric]) =>
                    <article key={label} className="dashboard-metric-card is-info"><div className="dashboard-metric-copy"><span>{label}</span><strong>{metric.value === null ? 'Unknown' : metric.value.toLocaleString()}</strong><small>{metric.quality === 'reported' ? 'Reported count; source coverage not exposed' : metric.quality === 'partial' ? 'Partial observation; other targets may be missing' : metric.quality}</small></div></article>)}
            </div>
            <div className="flex flex-wrap gap-4">
                {(data.status === 'UNCONFIGURED' || data.status === 'DOWN') && <button type="button" onClick={() => setActiveTab('NameServer')}>Configure or inspect NameServer connection</button>}
                {(data.brokers.value === 0 || data.brokers.quality !== 'complete') && <button type="button" onClick={() => setActiveTab('Cluster')}>Inspect Broker availability</button>}
                {(data.totalLag.value ?? 0) > 0 && <button type="button" onClick={() => setActiveTab('Consumer')}>Inspect Consumer backlog</button>}
                {data.laggingGroups.map(group => <button key={group} type="button" onClick={() => openConsumer(group, 'progress', { mode: 'name_server' })}>{group} progress</button>)}
            </div>
        </>}
        {!data && !error && <p role="status">Waiting for cluster observations…</p>}
    </section>;
}
