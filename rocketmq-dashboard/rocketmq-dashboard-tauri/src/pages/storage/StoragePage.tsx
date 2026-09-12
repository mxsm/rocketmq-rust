import { useCallback, useEffect, useState, useSyncExternalStore } from 'react';
import { ArrowRight, CircleCheck, CircleHelp, CircleX } from 'lucide-react';
import { StorageService } from '../../services/storage.service';
import { HistoryService } from '../../services/history.service';
import { ConnectionStore } from '../../services/connection.store';
import { getConnectionSettings } from '../../services/connection.service';
import { useReadResource } from '../../hooks/useReadResource';
import { usePageRefresh } from '../../app/layout/pageToolbar';
import { useAppStore } from '../../stores/app.store';
import { Button } from '../../components/ui/LegacyButton';
import { PageState } from '../../components/layout/PageState';
import { DiagnosticSection } from './DiagnosticSection';
import { diagnosticBytes, diagnosticCount, diagnosticTime, observationAge } from './storageModel';
import './storage.css';

function Bytes({ value }: { value: number | null }) {
    const measurement = diagnosticBytes(value);
    return <span title={measurement.exact}>{measurement.display}</span>;
}

export const StoragePage = () => {
    const storage = useReadResource(StorageService.status, 'Storage status could not be read.');
    const history = useReadResource(HistoryService.status, 'History collector status could not be read.');
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const connection = useReadResource(settings ? null : getConnectionSettings, 'Connection settings could not be read.');
    const { setActiveTab } = useAppStore();
    const [, updateClock] = useState(0);
    const now = Date.now();
    const [refreshedAt, setRefreshedAt] = useState<number | null>(null);
    const refresh = useCallback(() => { void storage.read(); void history.read(); if (!settings) void connection.read(); }, [storage.read, history.read, connection.read, settings]);
    useEffect(() => {
        const timer = window.setInterval(() => updateClock(value => value + 1), 15_000);
        return () => window.clearInterval(timer);
    }, []);
    useEffect(() => {
        if (!storage.pending && !history.pending && !storage.error && !history.error && storage.receivedAt !== null && history.receivedAt !== null) {
            setRefreshedAt(Math.min(storage.receivedAt, history.receivedAt));
        }
    }, [storage.pending, history.pending, storage.error, history.error, storage.receivedAt, history.receivedAt]);
    usePageRefresh({ refresh, pending: storage.pending || history.pending || connection.pending, refreshedAt });
    const status = storage.data;
    const collector = history.data;
    const age = observationAge(status?.checkedAtMs, now);
    const previous = Boolean(storage.error || age !== 'current');
    const available = status?.available === true;
    const HealthIcon = previous ? CircleHelp : available ? CircleCheck : CircleX;
    const endpoint = settings?.endpoints.find(endpoint => endpoint.endpointId === settings.currentNameserverId && endpoint.kind === 'name_server')?.address ?? settings?.nameserver.currentNamesrv;
    const collectorAge = observationAge(history.receivedAt, now);
    return <div className="ops-storage">
        {storage.pending && <PageState kind="loading" title={status ? 'Refreshing local diagnostics' : 'Reading local diagnostics'} />}
        {storage.error && <PageState kind="error" title="Storage diagnostics could not be refreshed" description={storage.error + (status ? ' The last successful observation is retained.' : '')}
            action={<Button variant="outline" disabled={storage.pending} onClick={() => { void storage.read(); }}>Retry storage</Button>} />}
        {status && <>
            <section className="ops-storage-health" data-tone={previous ? 'warning' : !available ? 'danger' : status.error ? 'warning' : 'success'} aria-label="Local database availability">
                <HealthIcon aria-hidden="true" /><div><h2>{previous ? available ? 'Local database available at last check' : 'Local database unavailable at last check' : available ? 'Local database available' : 'Local database unavailable'}</h2>
                    <p>{status.backend === 'sqlite' ? 'SQLite' : status.backend || 'Unknown backend'} / {status.mode === 'singleNode' ? 'Single node' : status.mode || 'Unknown mode'} · Schema version {diagnosticCount(status.schemaVersion)}</p>
                </div>
            </section>
            {status.error && <PageState kind="error" title="Local database check reported an error" description={status.error} />}
            {age === 'stale' && <PageState kind="stale" title="Older storage observation" description="This check is at least one minute old. Refresh before relying on these values." />}
            {age === 'unknown' && <PageState kind="partial" title="Observation age is unknown" description="The reported check time is unavailable or ahead of the local clock." />}
            <DiagnosticSection title="Storage" rows={[
                ['Allocated database pages', <Bytes value={status.databaseBytes} />],
                ['Reusable pages', <Bytes value={status.reusableBytes} />],
                ['Filesystem free space', <Bytes value={status.diskFreeBytes} />],
            ]} note="Page allocation excludes WAL and filesystem overhead. An unmeasured value is not a zero or a disk-usage percentage." />
            <DiagnosticSection title="Activity" rows={[
                ['Observed since', diagnosticTime(status.observedSinceMs)], ['Last check', diagnosticTime(status.checkedAtMs)],
                ['Latest committed write', diagnosticTime(status.lastWriteMs)],
            ]} note="Refreshing diagnostics does not create a write. The committed-write time comes from local storage activity." />
        </>}
        <DiagnosticSection title="History collector" rows={collector ? [
            ['Interval', diagnosticCount(collector.intervalSeconds, 's')], ['Retention', diagnosticCount(collector.retentionDays, 'days')],
            ['Last sample', diagnosticTime(collector.lastSampleMs)], ['Last write', diagnosticTime(collector.lastWriteMs)],
        ] : undefined} note={<>
            {collector && !history.error && !collector.lastError && (collector.lastSampleMs === null ? 'No sample observed yet. ' : 'No collector error reported. ')}
            These are local collector observations. The status does not identify the sampled environment.
        </>}>
            {history.pending && <PageState kind="loading" title="Reading collector status" />}
            {history.error && <PageState kind="error" title="Collector status could not be refreshed" description={history.error + (collector ? ' Previously observed values remain above.' : '')}
                action={<Button variant="outline" disabled={history.pending} onClick={() => { void history.read(); }}>Retry collector</Button>} />}
            {collector?.lastError && <PageState kind="partial" title="History collection reported an error" description={collector.lastError} />}
            {collector && collectorAge === 'stale' && <PageState kind="stale" title="Older collector observation" description="Refresh to read the current collector status." />}
        </DiagnosticSection>
        <DiagnosticSection title="Connection context" rows={settings ? [
            ['Environment', <span className="ops-storage-value" tabIndex={(settings.environmentId?.length ?? 0) > 160 ? 0 : undefined}>{settings.environmentId ?? 'No environment selected'}</span>],
            ['NameServer', <span className="ops-storage-value ops-storage-address" tabIndex={(endpoint?.length ?? 0) > 160 ? 0 : undefined}>{endpoint || 'Not configured'}</span>],
            ['Configuration revision', diagnosticCount(settings.revision)],
        ] : undefined} note={<>
            Database availability and saved connection settings do not establish Broker reachability.
            <div className="ops-storage-actions"><Button variant="ghost" icon={ArrowRight} onClick={() => setActiveTab('NameServer')}>NameServer settings</Button><Button variant="ghost" icon={ArrowRight} onClick={() => setActiveTab('Cluster')}>Open cluster</Button></div>
        </>}>
            {connection.pending && <PageState kind="loading" title="Reading connection context" />}
            {connection.error && <PageState kind="error" title="Connection context unavailable" description={connection.error} action={<Button variant="outline" onClick={() => { void connection.read(); }}>Retry connection</Button>} />}
        </DiagnosticSection>
    </div>;
};
