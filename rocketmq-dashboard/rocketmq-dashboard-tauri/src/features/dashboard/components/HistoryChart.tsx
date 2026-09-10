import { useEffect, useMemo, useRef, useState } from 'react';
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { HistoryService, type CollectorStatus, type HistorySample } from '../../../services/history.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { ConsumerRequestGeneration } from '../../consumer/scope';
import { useTopicCatalog } from '../../topic/hooks/useTopicCatalog';
import { localHistoryDay, todayLocal } from '../history';

export function HistoryChart() {
    const [date, setDate] = useState(todayLocal);
    const [metric, setMetric] = useState<'broker-count' | 'topic-count' | 'topic-total-messages'>('broker-count');
    const [topic, setTopic] = useState('');
    const [samples, setSamples] = useState<HistorySample[]>([]);
    const [next, setNext] = useState<number | null>(null);
    const [status, setStatus] = useState<CollectorStatus | null>(null);
    const [pending, setPending] = useState(false);
    const [error, setError] = useState('');
    const [refresh, setRefresh] = useState(0);
    const generation = useRef(new ConsumerRequestGeneration());
    const { data: topics } = useTopicCatalog();
    const load = async (beforeMs?: number) => {
        const current = generation.current.begin();
        setPending(true); setError('');
        if (beforeMs === undefined) { setSamples([]); setNext(null); }
        try {
            if (metric === 'topic-total-messages' && !topic.trim()) throw new Error('Select a Topic to read its stored message history.');
            const [page, collector] = await Promise.all([
                HistoryService.query(metric === 'broker-count' ? 'broker' : 'topic', { ...localHistoryDay(date), topicName: metric === 'topic-total-messages' ? topic.trim() : undefined, beforeMs, limit: 1000 }),
                HistoryService.status(),
            ]);
            if (!current()) return;
            setSamples(previous => beforeMs === undefined ? page.samples : [...previous, ...page.samples]); setNext(page.nextBeforeMs); setStatus(collector);
        } catch (error) { if (current()) setError(dashboardErrorMessage(error, 'History could not be read.')); }
        finally { if (current()) setPending(false); }
    };
    useEffect(() => { void load(); return () => generation.current.invalidate(); }, [date, metric, topic, refresh]);
    const points = useMemo(() => [...samples].sort((a, b) => a.timestampMs - b.timestampMs), [samples]);
    return <section className="space-y-4 rounded border p-4" aria-label="Persisted history">
        <h2 className="text-lg font-semibold">Stored metric history</h2>
        <div className="flex flex-wrap gap-4">
            <label>Date <input type="date" value={date} onChange={event => setDate(event.target.value)} className="rounded border p-2 dark:bg-gray-900" /></label>
            <label>Metric <select value={metric} onChange={event => setMetric(event.target.value as typeof metric)} className="rounded border p-2 dark:bg-gray-900"><option value="broker-count">Broker count</option><option value="topic-count">Topic count</option><option value="topic-total-messages">Topic total messages</option></select></label>
            {metric === 'topic-total-messages' && <label>Topic <input list="history-topics" value={topic} onChange={event => setTopic(event.target.value)} className="rounded border p-2 dark:bg-gray-900" /><datalist id="history-topics">{topics?.items.map(item => <option key={item.topic} value={item.topic} />)}</datalist></label>}
            <button type="button" disabled={pending} onClick={() => setRefresh(value => value + 1)}>Refresh history</button>
        </div>
        <p>Local calendar day in {Intl.DateTimeFormat().resolvedOptions().timeZone}. Only stored samples are shown; failed observations are omitted.</p>
        {status && <p>Sample interval: {status.intervalSeconds}s · Retention: {status.retentionDays} days · Last write: {status.lastWriteMs ? new Date(status.lastWriteMs).toLocaleString() : 'Waiting for a successful sample'}{status.lastError ? ` · ${status.lastError}` : ''}</p>}
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {pending && <p role="status">Reading stored samples…</p>}
        {!pending && !error && points.length === 0 && <p>No samples for this selection. Wait for collection or choose another date or Topic.</p>}
        {points.length > 0 && <div className="h-72"><ResponsiveContainer width="100%" height="100%"><LineChart data={points}><CartesianGrid strokeDasharray="3 3" /><XAxis dataKey="timestampMs" type="number" domain={['dataMin', 'dataMax']} tickFormatter={value => new Date(value).toLocaleTimeString()} /><YAxis /><Tooltip labelFormatter={value => new Date(Number(value)).toLocaleString()} /><Line type="stepAfter" dataKey="value" stroke="#3b82f6" dot={points.length < 60} isAnimationActive={false} /></LineChart></ResponsiveContainer></div>}
        {next !== null && <button type="button" disabled={pending} onClick={() => void load(next)}>Load older samples in this day</button>}
    </section>;
}
