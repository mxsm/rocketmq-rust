import { useMemo, useState, type ReactNode } from 'react';
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/input';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import type { HistorySample } from '../../../services/history.service';
import type { DashboardData } from '../hooks/useDashboardData';
import { historyPoints } from '../historySeries';
import { formatMetric } from '../brokers';
import { RegionNotice, formatObservation } from './RegionNotice';

export const chartTooltipStyle = {
    background: 'var(--ops-panel)', color: 'var(--ops-text)', border: '1px solid var(--ops-border)',
    borderRadius: 8, fontSize: 13,
};

function HistoryPanel({ title, metric, dimension = '', state, interval, action }: {
    title: string; metric: HistorySample['metric']; dimension?: string;
    state: DashboardData['brokerHistory']; interval: number | null; action?: ReactNode;
}) {
    const points = useMemo(() => historyPoints(state.data?.samples ?? [], metric, dimension, interval),
        [state.data, metric, dimension, interval]);
    const samples = points.filter(point => point.value !== null);
    const latest = samples.at(-1);
    return <PageSection title={title} className="ops-dashboard-history-panel" action={action}
        description={latest ? `Last sample ${formatObservation(latest.timestampMs)} · ${formatMetric(latest.value)}` : 'Only stored observations are shown.'}>
        <RegionNotice state={state} label={title} />
        {samples.length ? <>
            <div className="ops-dashboard-chart" role="img" aria-label={`${title}: ${samples.length} stored samples. Latest value ${latest?.value}.`}>
                <ResponsiveContainer width="100%" height="100%" minWidth={0}>
                    <AreaChart data={points} margin={{ top: 12, right: 20, bottom: 8, left: 0 }}>
                        <CartesianGrid stroke="var(--ops-border)" strokeDasharray="3 3" vertical={false} />
                        <XAxis dataKey="timestampMs" type="number" domain={['dataMin', 'dataMax']} tick={{ fill: 'var(--ops-muted)', fontSize: 12 }}
                            tickFormatter={value => new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} minTickGap={38} />
                        <YAxis allowDecimals={false} tick={{ fill: 'var(--ops-muted)', fontSize: 12 }} width={54} />
                        <Tooltip contentStyle={chartTooltipStyle} labelFormatter={value => new Date(Number(value)).toLocaleString()}
                            formatter={(value: number) => [formatMetric(value), 'Count']} />
                        <Area dataKey="value" name="Count" type="stepAfter" connectNulls={false}
                            fill="var(--ops-accent)" fillOpacity={interval === null ? 0 : 0.1}
                            stroke={interval === null ? 'transparent' : 'var(--ops-accent)'} strokeWidth={2}
                            dot={interval === null || samples.length < 60 ? { r: 2, fill: 'var(--ops-accent)', stroke: 'var(--ops-accent)' } : false}
                            activeDot={{ r: 4 }} isAnimationActive={false} />
                    </AreaChart>
                </ResponsiveContainer>
            </div>
            <details className="ops-dashboard-values"><summary>View stored samples ({samples.length})</summary>
                <div className="ops-dashboard-table-scroll" tabIndex={0} role="region" aria-label={`${title} values`}>
                    <table className="ops-dashboard-table"><thead><tr><th scope="col">Observed at</th><th scope="col">Count</th></tr></thead>
                        <tbody>{samples.map(point => <tr key={point.timestampMs}><td>{formatObservation(point.timestampMs)}</td><td>{formatMetric(point.value)}</td></tr>)}</tbody>
                    </table>
                </div>
            </details>
        </> : !state.pending && !state.error && <PageState kind="empty" title="No stored samples"
            description="Choose another date or wait for the next successful collection." />}
        {state.data?.nextBeforeMs != null && <Button variant="secondary" disabled={state.pending} onClick={() => void state.loadOlder()}>Load older {title.toLowerCase()} samples</Button>}
    </PageSection>;
}

export function HistoryChart({ data }: { data: DashboardData }) {
    const [topicDraft, setTopicDraft] = useState('');
    const collector = data.collector.data;
    const interval = collector && collector.intervalSeconds > 0 ? collector.intervalSeconds : null;
    return <section className="ops-dashboard-history" aria-label="Stored metric history">
        <div className="ops-dashboard-history-grid">
            <HistoryPanel title="Topic count" metric="topic-count" state={data.topicHistory} interval={interval}
                action={<label className="ops-dashboard-date">History date<Input type="date" value={data.date} disabled={data.pending}
                    onChange={event => data.setDate(event.target.value)} /></label>} />
            <HistoryPanel title="Broker count" metric="broker-count" state={data.brokerHistory} interval={interval} />
        </div>
        <div className="ops-dashboard-collector">
            <p>Local calendar day in {Intl.DateTimeFormat().resolvedOptions().timeZone}. Gaps are not zero values.</p>
            <RegionNotice state={data.collector} label="History collector status" />
            {collector && <p>Sample interval: {collector.intervalSeconds}s · Retention: {collector.retentionDays} days · Last write: {formatObservation(collector.lastWriteMs)}</p>}
            {collector?.lastError && <PageState kind="partial" title="History collection needs attention" description={collector.lastError} />}
            {interval === null && <p>Sampling interval unavailable. Stored points are shown without connecting lines.</p>}
        </div>
        <form className="ops-dashboard-topic-query" onSubmit={event => { event.preventDefault(); if (!data.pending) data.setHistoryTopic(topicDraft.trim()); }}>
            <label htmlFor="dashboard-history-topic">Topic message history</label>
            <Input id="dashboard-history-topic" list="dashboard-history-topics" value={topicDraft} disabled={data.pending}
                onChange={event => setTopicDraft(event.target.value)} placeholder="Select or enter a Topic" required />
            <datalist id="dashboard-history-topics">{data.topics.data?.items.map(item => <option key={item.topic} value={item.topic} />)}</datalist>
            <Button type="submit" disabled={data.pending || !topicDraft.trim()}>View history</Button>
            {data.historyTopic && <Button variant="ghost" disabled={data.pending} onClick={() => { data.setHistoryTopic(''); setTopicDraft(''); }}>Clear selection</Button>}
        </form>
        <RegionNotice state={data.topics} label="Topic suggestions" />
        {data.historyTopic && <HistoryPanel title={`Message count · ${data.historyTopic}`} metric="topic-total-messages"
            dimension={data.historyTopic} state={data.messageHistory} interval={interval} />}
    </section>;
}
