import { useMemo, useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import type { DashboardData } from '../hooks/useDashboardData';
import { brokerMetric, formatMetric } from '../brokers';
import { RegionNotice, formatObservation } from './RegionNotice';
import { chartTooltipStyle } from './HistoryChart';

interface ChartValue { name: string; value: number }

function CountChart({ title, values, unit }: { title: string; values: ChartValue[]; unit: string }) {
    return <>
        <div className="ops-dashboard-chart" role="img" aria-label={title} style={{ height: Math.max(220, values.length * 30 + 40) }}>
            <ResponsiveContainer width="100%" height="100%" minWidth={0}>
                <BarChart data={values} layout="vertical" margin={{ top: 8, right: 24, bottom: 4, left: 4 }}>
                    <CartesianGrid stroke="var(--ops-border)" strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" allowDecimals={false} tick={{ fill: 'var(--ops-muted)', fontSize: 12 }} />
                    <YAxis type="category" dataKey="name" width={130} tick={{ fill: 'var(--ops-muted)', fontSize: 12 }}
                        tickFormatter={(value: string) => value.length > 18 ? value.slice(0, 15) + '…' : value} />
                    <Tooltip contentStyle={chartTooltipStyle} cursor={{ fill: 'var(--ops-panel-strong)' }}
                        formatter={(value: number) => [formatMetric(value), unit]} />
                    <Bar dataKey="value" name={unit} fill="var(--ops-accent)" radius={[0, 4, 4, 0]} barSize={18} isAnimationActive={false} />
                </BarChart>
            </ResponsiveContainer>
        </div>
        <details className="ops-dashboard-values"><summary>View values</summary>
            <div className="ops-dashboard-table-scroll" tabIndex={0} role="region" aria-label={`${title} values`}>
                <table className="ops-dashboard-table"><thead><tr><th scope="col">Resource</th><th scope="col">{unit}</th></tr></thead>
                    <tbody>{values.map(item => <tr key={item.name}><th scope="row">{item.name}</th><td>{formatMetric(item.value)}</td></tr>)}</tbody>
                </table>
            </div>
        </details>
    </>;
}

export function DashboardCharts({ brokers, topics }: { brokers: DashboardData['brokers']; topics: DashboardData['topicCharts'] }) {
    const [open, setOpen] = useState(false);
    const brokerTop = useMemo(() => (brokers.data?.items ?? [])
        .map(broker => ({ name: `${broker.brokerName}-${broker.brokerId} · ${broker.address}`, value: brokerMetric(broker, 'todayReceivedTotal') }))
        .filter((item): item is ChartValue => item.value !== null)
        .sort((a, b) => b.value - a.value || a.name.localeCompare(b.name)).slice(0, 10), [brokers.data]);
    const brokerTps = useMemo(() => (brokers.data?.items ?? []).map(broker => ({
        name: `${broker.brokerName}-${broker.brokerId} · ${broker.address}`,
        produce: brokerMetric(broker, 'produceTps'), consume: brokerMetric(broker, 'consumeTps'),
    })).sort((a, b) => ((b.produce ?? -1) + (b.consume ?? -1)) - ((a.produce ?? -1) + (a.consume ?? -1))).slice(0, 10), [brokers.data]);
    const panels = [
        { title: 'Topic queue Top 10', unit: 'Read + write queues', values: (topics.data?.topicQueueTop ?? []).map(item => ({ name: item.topic, value: item.totalQueueCount })) },
        { title: 'Topic messages Top 10', unit: 'Total messages', values: (topics.data?.topicTop ?? []).map(item => ({ name: item.topic, value: item.totalMsg })) },
        { title: 'Topic type distribution', unit: 'Topics', values: (topics.data?.topicCategoryDistribution ?? []).map(item => ({ name: item.category, value: item.count })) },
    ];
    return <details className="ops-dashboard-analysis" onToggle={event => setOpen(event.currentTarget.open)}>
        <summary><ChevronDown aria-hidden="true" /><span><strong>Broker and Topic analysis</strong><small>Rankings, current TPS, queue counts and Topic types</small></span>
            {(brokers.error || topics.error) && <span className="ops-dashboard-warning">Some analysis data is unavailable</span>}</summary>
        {open && <div className="ops-dashboard-analysis-body">
            <RegionNotice state={brokers} label="Broker analysis" /><RegionNotice state={topics} label="Topic analysis" />
            <p className="ops-dashboard-read-state">Broker response received {formatObservation(brokers.receivedAt)} · Topic response received {formatObservation(topics.receivedAt)}.
                Missing Broker metrics are excluded from rankings. Topic values are reported; source coverage is not exposed.</p>
            <div className="ops-dashboard-analysis-grid">
                <PageSection title="Broker received Top 10" description="Cumulative get counters from available Broker status responses.">
                    {brokerTop.length ? <CountChart title="Broker received Top 10" values={brokerTop} unit="Received messages" />
                        : <PageState kind="empty" title="No reported Broker counters" description="Read failures and unreported counters are not treated as zero." />}
                </PageSection>
                <PageSection title="Broker TPS snapshot" description="Top 10 by reported produce + consume TPS, in messages / second.">
                    {brokerTps.length ? <div className="ops-dashboard-table-scroll" tabIndex={0} role="region" aria-label="Broker TPS values">
                        <table className="ops-dashboard-table"><thead><tr><th scope="col">Broker</th><th scope="col">Produce</th><th scope="col">Consume</th></tr></thead>
                            <tbody>{brokerTps.map(item => <tr key={item.name}><th scope="row">{item.name}</th><td>{formatMetric(item.produce, 2)}</td><td>{formatMetric(item.consume, 2)}</td></tr>)}</tbody>
                        </table></div> : <PageState kind="empty" title="No Broker status observations" />}
                </PageSection>
                {panels.map(panel => <PageSection key={panel.title} title={panel.title} description={panel.unit}>
                    {panel.values.length ? <CountChart {...panel} />
                        : <PageState kind={topics.pending ? 'loading' : topics.error ? 'error' : 'empty'} title={topics.pending ? 'Reading Topic data…' : topics.error ? 'Topic analysis unavailable' : 'No reported Topic values'} />}
                </PageSection>)}
            </div>
        </div>}
    </details>;
}
