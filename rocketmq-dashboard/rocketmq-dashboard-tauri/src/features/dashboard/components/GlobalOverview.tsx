import { ArrowRight, CircleCheck, CircleHelp, CircleAlert } from 'lucide-react';
import { Button } from '../../../components/ui/LegacyButton';
import { PageState } from '../../../components/layout/PageState';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import { useAppStore } from '../../../stores/app.store';
import { brokerState, formatMetric, qualityDescription, throughputMetric } from '../brokers';
import type { DashboardData } from '../hooks/useDashboardData';
import type { OverviewMetric } from '../types/dashboard.types';
import { RegionNotice, formatObservation } from './RegionNotice';

const unknown: OverviewMetric = { value: null, quality: 'unknown' };

function Metric({ label, metric, unit, action, onClick, decimals = 0 }: {
    label: string; metric: OverviewMetric; unit: string; action: string; onClick: () => void; decimals?: number;
}) {
    return <article className="ops-dashboard-metric">
        <h2>{label}</h2>
        <strong>{formatMetric(metric.quality === 'unknown' ? null : metric.value, decimals)}</strong>
        <span>{unit}</span>{metric.quality !== 'complete' && <small>{qualityDescription(metric)}</small>}
        <Button variant="ghost" onClick={onClick}>{action}<ArrowRight aria-hidden="true" /></Button>
    </article>;
}

export function GlobalOverview({ overview, brokers }: Pick<DashboardData, 'overview' | 'brokers'>) {
    const { setActiveTab, openConsumer } = useAppStore();
    const data = overview.data;
    const catalog = brokers.data;
    const items = catalog?.items ?? [];
    const active = items.filter(broker => brokerState(broker) === 'active').length;
    const failed = items.filter(broker => brokerState(broker) === 'unavailable').length;
    const unreported = items.filter(broker => brokerState(broker) === 'unknown').length;
    const inactive = items.filter(broker => brokerState(broker) === 'inactive').length;
    const unconfigured = data?.status === 'UNCONFIGURED';
    const allActive = items.length > 0 && active === items.length;
    const stale = Boolean(brokers.error && catalog);
    const tone = stale ? 'warning' : unconfigured || !catalog ? 'neutral' : allActive ? 'success' : 'warning';
    const Icon = tone === 'success' ? CircleCheck : tone === 'warning' ? CircleAlert : CircleHelp;
    const headline = unconfigured ? 'Connect a NameServer to get started'
        : !catalog ? 'Broker availability unknown'
        : !items.length ? 'No Brokers discovered'
        : `${active} of ${items.length} Brokers active`;
    const incomplete = data && [data.brokers, data.topics, data.consumerGroups, data.producerGroups, data.totalLag]
        .some(metric => metric.quality === 'unknown' || metric.quality === 'partial');
    const producer = data?.producerGroups ?? unknown;
    return <>
        <section className="ops-dashboard-health" aria-label="Cluster availability and data quality">
            <div className="ops-dashboard-health-row">
                <div className="ops-dashboard-health-title" data-tone={tone}>
                    <Icon aria-hidden="true" />
                    <div><h2>{headline}{stale && ' · Previous observation'}</h2>
                        <p>{catalog && !unconfigured
                            ? `${items.length - failed} status reads succeeded · ${failed} failed · ${inactive} inactive · ${unreported} activity unknown`
                            : unconfigured ? 'Choose a connection to inspect the cluster and its message traffic.'
                            : brokers.pending ? 'Waiting for Broker status observations…' : 'Review the connection or retry the Broker status query.'}</p>
                        {data && <small>Overview observed {formatObservation(data.observedAtMs)}</small>}
                    </div>
                </div>
                <dl className="ops-dashboard-connection">
                    <div><dt>NameServer</dt><dd>{catalog?.currentNamesrv || 'Not observed'}</dd></div>
                    <div><dt>Connection</dt><dd>{catalog ? `VIP ${catalog.useVipChannel ? 'on' : 'off'} / TLS ${catalog.useTls ? 'on' : 'off'}` : 'Unknown'}</dd></div>
                </dl>
            </div>
            <RegionNotice state={brokers} label="Broker status" />
            <RegionNotice state={overview} label="Overview" />
            {unconfigured && <PageState kind="empty" title="No NameServer configured"
                description="Save a NameServer endpoint to start reading cluster data."
                action={<Button onClick={() => setActiveTab('NameServer')}>Configure connection</Button>} />}
            {data?.status === 'DOWN' && <PageState kind="error" title="Overview queries unavailable"
                description="No overview metric could be obtained. Check the connection and Broker status."
                action={<Button variant="secondary" onClick={() => setActiveTab('NameServer')}>Review connection</Button>} />}
            {incomplete && !unconfigured && data?.status !== 'DOWN' && <div className="ops-dashboard-partial" role="status">
                <CircleAlert aria-hidden="true" /><span><strong>Partial data.</strong> Some metrics are incomplete or unavailable. Producer groups: {formatMetric(producer.value)}.</span>
                <Button variant="ghost" onClick={() => setActiveTab('Producer')}>Review producers<ArrowRight aria-hidden="true" /></Button>
            </div>}
            {data && !incomplete && <div className="ops-dashboard-quality">
                <StatusBadge tone={data.status === 'PARTIAL' ? 'warning' : 'neutral'}>{data.status === 'READY' ? 'Queries returned' : data.status}</StatusBadge>
                {!incomplete && <Button variant="ghost" onClick={() => setActiveTab('Producer')}>Producer groups: {formatMetric(producer.value)}</Button>}
            </div>}
        </section>
        <section className="ops-dashboard-metrics" aria-label="Operational metrics">
            <Metric label="Topics" metric={data?.topics ?? unknown} unit="Total topics" action="View topics" onClick={() => setActiveTab('Topic')} />
            <Metric label="Consumer groups" metric={data?.consumerGroups ?? unknown} unit="Total groups" action="View consumers" onClick={() => setActiveTab('Consumer')} />
            <Metric label="Current throughput" metric={throughputMetric(catalog)} unit="Messages / sec · Produce + consume" decimals={2} action="View Brokers" onClick={() => setActiveTab('Cluster')} />
            <Metric label="Observed lag" metric={data?.totalLag ?? unknown} unit="Messages behind" action="Inspect backlog" onClick={() => setActiveTab('Consumer')} />
        </section>
        {!!data?.laggingGroups.length && <div className="ops-dashboard-lagging" aria-label="Consumer groups with observed lag">
            <strong>Groups to inspect</strong>
            {data.laggingGroups.map(group => <Button key={group} variant="secondary"
                onClick={() => openConsumer(group, 'progress', { mode: 'name_server' })}>{group}<ArrowRight aria-hidden="true" /></Button>)}
        </div>}
    </>;
}
