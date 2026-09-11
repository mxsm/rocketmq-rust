import { useMemo } from 'react';
import { ChevronRight } from 'lucide-react';
import { Button } from '../../../components/ui/LegacyButton';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import { useAppStore } from '../../../stores/app.store';
import { brokerMetric, brokerState, formatMetric } from '../brokers';
import type { DashboardData } from '../hooks/useDashboardData';
import { formatObservation } from './RegionNotice';

const statusLabel = { active: 'Active', inactive: 'Inactive', unknown: 'Activity unknown', unavailable: 'Status unavailable' } as const;

export function BrokerOverview({ brokers }: Pick<DashboardData, 'brokers'>) {
    const { openBroker } = useAppStore();
    const rows = useMemo(() => [...(brokers.data?.items ?? [])].sort((a, b) =>
        (brokerMetric(b, 'todayReceivedTotal') ?? -1) - (brokerMetric(a, 'todayReceivedTotal') ?? -1) ||
        a.brokerName.localeCompare(b.brokerName) || a.address.localeCompare(b.address)), [brokers.data]);
    return <PageSection title="Brokers"
        description="Received is the cumulative get counter; production deltas use the Broker’s local day."
        action={<span className="ops-dashboard-observed">{brokers.error ? 'Previous response' : 'Response received'}<br />{formatObservation(brokers.receivedAt)}</span>}>
        {rows.length > 0 ? <div className="ops-dashboard-table-scroll" tabIndex={0} role="region" aria-label="Broker statistics">
            <table className="ops-dashboard-table"><thead><tr>
                <th scope="col">Broker name</th><th scope="col">Address</th><th scope="col">Status</th>
                <th scope="col" className="is-number">Received total</th><th scope="col" className="is-number">Produced today</th>
                <th scope="col" className="is-number">Produced yesterday</th><th scope="col"><span className="sr-only">Details</span></th>
            </tr></thead><tbody>{rows.map(broker => {
                const state = brokerState(broker);
                return <tr key={`${broker.clusterName}:${broker.brokerName}:${broker.brokerId}:${broker.address}`}>
                    <th scope="row"><Button variant="ghost" className="ops-dashboard-broker-link" title={`${broker.clusterName} · ${broker.role} · ID ${broker.brokerId}`} onClick={() => openBroker(broker.address, 'status')}>
                        {broker.brokerName}</Button></th>
                    <td className="ops-dashboard-mono">{broker.address}</td>
                    <td><StatusBadge tone={state === 'active' ? 'success' : state === 'unavailable' ? 'danger' : 'warning'}>{statusLabel[state]}</StatusBadge>
                        {broker.statusLoadError && <small className="ops-dashboard-broker-error">{broker.statusLoadError}</small>}</td>
                    <td className="is-number">{formatMetric(brokerMetric(broker, 'todayReceivedTotal'))}</td>
                    <td className="is-number">{formatMetric(brokerMetric(broker, 'todayProduce'))}</td>
                    <td className="is-number">{formatMetric(brokerMetric(broker, 'yesterdayProduce'))}</td>
                    <td><Button variant="ghost" aria-label={`Inspect ${broker.brokerName} at ${broker.address}`} onClick={() => openBroker(broker.address, 'status')}><ChevronRight aria-hidden="true" /></Button></td>
                </tr>;
            })}</tbody></table>
        </div> : <PageState kind={brokers.pending ? 'loading' : brokers.error ? 'error' : 'empty'}
            title={brokers.pending ? 'Reading Broker status…' : brokers.error ? 'Broker list unavailable' : 'No Brokers discovered'}
            description={brokers.error || 'Brokers registered with the selected NameServer will appear here.'} />}
    </PageSection>;
}
