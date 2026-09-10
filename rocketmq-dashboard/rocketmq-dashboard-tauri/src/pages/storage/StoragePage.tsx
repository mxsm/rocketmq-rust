import React, { useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { StorageService, type StorageStatus } from '../../services/storage.service';
import { HistoryService, type CollectorStatus } from '../../services/history.service';
import { ConnectionStore } from '../../services/connection.store';
import { dashboardErrorMessage } from '../../services/invoke';
import { useAppStore } from '../../stores/app.store';

const time = (value: number | null) => value === null ? 'Not observed' : new Date(value).toLocaleString();
const bytes = (value: number | null) => value === null ? 'Unknown / unavailable' : `${value.toLocaleString()} bytes`;

export const StoragePage = () => {
    const [status, setStatus] = useState<StorageStatus | null>(null);
    const [collector, setCollector] = useState<CollectorStatus | null>(null);
    const [error, setError] = useState('');
    const [collectorError, setCollectorError] = useState('');
    const [busy, setBusy] = useState(false);
    const [now, setNow] = useState(Date.now());
    const generation = useRef(0);
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const { setActiveTab } = useAppStore();
    const refresh = async () => {
        const current = ++generation.current;
        setBusy(true); setError(''); setCollectorError('');
        const [storage, history] = await Promise.allSettled([StorageService.status(), HistoryService.status()]);
        if (current !== generation.current) return;
        if (storage.status === 'fulfilled') setStatus(storage.value);
        else setError(dashboardErrorMessage(storage.reason, 'Storage status could not be read.'));
        if (history.status === 'fulfilled') setCollector(history.value);
        else setCollectorError(dashboardErrorMessage(history.reason, 'Collector status could not be read.'));
        setNow(Date.now()); setBusy(false);
    };
    useEffect(() => {
        void refresh();
        const timer = window.setInterval(() => setNow(Date.now()), 15_000);
        return () => { generation.current++; window.clearInterval(timer); };
    }, []);
    const stale = status && (Boolean(error) || now - status.checkedAtMs >= 60_000);
    return <section className="p-6 space-y-5">
        <div className="flex justify-between"><h2 className="text-xl font-semibold">Storage and diagnostics</h2><button disabled={busy} onClick={() => void refresh()}>{busy ? 'Checking…' : 'Refresh / retry'}</button></div>
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {stale && <p role="status" className="text-amber-600">This is an older snapshot. Refresh before relying on these values.</p>}
        {status && <div className="rounded border p-4 space-y-2">
            <h3 className="font-semibold">Local database: {status.available ? 'Available' : 'Unavailable'}</h3>
            {status.error && <p role="alert" className="text-red-600">{status.error}</p>}
            <dl className="grid grid-cols-2 gap-2 text-sm">
                <dt>Backend / mode</dt><dd>{status.backend} / {status.mode}</dd>
                <dt>Schema version</dt><dd>{status.schemaVersion ?? 'Unknown'}</dd>
                <dt>Observation started</dt><dd>{time(status.observedSinceMs)}</dd>
                <dt>Last check</dt><dd>{time(status.checkedAtMs)}</dd>
                <dt>Latest committed write</dt><dd>{time(status.lastWriteMs)}</dd>
                <dt>Allocated database pages</dt><dd>{bytes(status.databaseBytes)}</dd>
                <dt>Reusable database pages</dt><dd>{bytes(status.reusableBytes)}</dd>
                <dt>Filesystem free space</dt><dd>{bytes(status.diskFreeBytes)}</dd>
            </dl>
            <p className="text-sm text-gray-500">Page sizes describe the SQLite database, excluding WAL and filesystem overhead. Free disk space is not measured. Refreshing diagnostics does not create a write.</p>
        </div>}
        <div className="rounded border p-4 space-y-2">
            <h3 className="font-semibold">History collector</h3>
            {collectorError && <p role="alert" className="text-red-600">{collectorError}{collector ? ' Previously loaded values are shown below.' : ''}</p>}
            {collector && <><p>Interval {collector.intervalSeconds}s · Retention {collector.retentionDays} days</p><p>Last sample: {time(collector.lastSampleMs)} · Last write: {time(collector.lastWriteMs)}</p>{collector.lastError && <p className="text-amber-600">{collector.lastError}</p>}</>}
        </div>
        <div className="rounded border p-4 space-y-2">
            <h3 className="font-semibold">Connection configuration</h3>
            <p>{settings?.environmentId ? `Environment selected · configuration revision ${settings.revision}` : 'No NameServer environment selected'}</p>
            <p className="text-sm text-gray-500">Database availability and saved connection settings do not establish Broker reachability. Use Dashboard or Cluster to query the cluster.</p>
            <div className="flex gap-4"><button onClick={() => setActiveTab('NameServer')}>NameServer settings</button><button onClick={() => setActiveTab('Cluster')}>Query Cluster</button></div>
        </div>
    </section>;
};
