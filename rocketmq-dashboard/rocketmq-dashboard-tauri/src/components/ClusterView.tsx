import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Pencil, X } from 'lucide-react';
import { useAppStore, useNavigationState } from '../stores/app.store';
import { ConnectionStore } from '../services/connection.store';
import { ClusterService } from '../services/cluster.service';
import { useReadResource } from '../hooks/useReadResource';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { useClusterCatalog } from '../features/cluster/hooks/useClusterCatalog';
import { brokerIdentity, brokerKey, brokerState, brokerRate, brokerTps, type BrokerIdentity } from '../features/cluster/brokerIdentity';
import { BrokerEntries } from '../features/cluster/components/BrokerEntries';
import { useBrokerConfigEditor } from '../features/cluster/brokerConfigEditorContext';
import type { ClusterBrokerCardItem } from '../features/cluster/types/cluster.types';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './ui/tabs';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { StatusBadge } from './layout/StatusBadge';
import '../features/cluster/cluster.css';

function BrokerStatus({ broker }: { broker: ClusterBrokerCardItem }) {
    const state = brokerState(broker);
    return <StatusBadge tone={state === 'Active' ? 'success' : state === 'Unavailable' ? 'danger' : state === 'Inactive' ? 'warning' : 'neutral'}>{state}</StatusBadge>;
}

const counterText = (broker: ClusterBrokerCardItem, value: number, keys: string[]) =>
    !broker.statusLoadError && keys.every(key => /^-?\d+$/.test(broker.rawStatus[key]?.trim() ?? '')) && Number.isFinite(value) ? value.toLocaleString() : 'Unknown';

export const ClusterView = () => {
    const { navigation, setActiveTab } = useAppStore();
    const target = navigation.target?.kind === 'broker' ? navigation.target : null;
    const catalog = useClusterCatalog();
    const data = catalog.data;
    const [selectedCluster, setSelectedCluster] = useNavigationState('cluster', '');
    const [query, setQuery] = useNavigationState('brokerSearch', '');
    const [selection, setSelection] = useNavigationState<BrokerIdentity | null>('selectedBroker', null);
    const [detailMode, setDetailMode] = useNavigationState<'status' | 'config'>('brokerDetail', target?.detail === 'status' ? 'status' : 'config');
    const alive = useRef(false);
    useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
    const openEditor = useBrokerConfigEditor();
    const items = data?.items ?? [];
    const requested = selection ? items.find(item => brokerKey(item) === brokerKey(selection))
        : target ? items.find(item => item.address === target.address) : undefined;
    const cluster = selectedCluster || selection?.clusterName || requested?.clusterName || (target ? '' : data?.clusters[0]) || '';
    const clusterItems = useMemo(() => items.filter(item => item.clusterName === cluster)
        .sort((a, b) => a.brokerName.localeCompare(b.brokerName) || a.brokerId - b.brokerId), [data?.items, cluster]);
    const filtered = clusterItems.filter(item => (item.brokerName + ' ' + item.address).toLowerCase().includes(query.trim().toLowerCase()));
    const selected = selection || target ? requested : clusterItems[0];
    const selectedVisible = selected && filtered.some(item => brokerKey(item) === brokerKey(selected)) ? selected : null;
    const missing = Boolean((selection || target) && data && !catalog.pending && !catalog.error && !selected);
    useEffect(() => { if (!selection && selected) setSelection(brokerIdentity(selected)); }, [selection, selected]);
    const identity = selectedVisible ? brokerKey(selectedVisible) : '';
    const address = selectedVisible?.address ?? '';
    const loadDetail = useCallback(async () => {
        const result = await (detailMode === 'config' ? ClusterService.getClusterBrokerConfig({ brokerAddr: address })
            : ClusterService.getClusterBrokerStatus({ brokerAddr: address }));
        if (result.brokerAddr !== address) throw new Error('Unexpected Broker response');
        return result;
    }, [identity, address, detailMode]);
    const details = useReadResource(selectedVisible ? loadDetail : null, detailMode === 'config' ? 'Broker configuration could not be read.' : 'Broker runtime could not be read.');
    const refresh = useCallback(() => { void catalog.refresh(); void details.read(); }, [catalog.refresh, details.read]);
    const observedAt = selectedVisible ? catalog.receivedAt !== null && details.receivedAt !== null
        ? Math.min(catalog.receivedAt, details.receivedAt) : null : catalog.receivedAt;
    const [refreshedAt, setRefreshedAt] = useState<number | null>(null);
    useEffect(() => {
        if (!catalog.pending && !details.pending && !catalog.error && !details.error) setRefreshedAt(observedAt);
    }, [catalog.pending, details.pending, catalog.error, details.error, observedAt]);
    usePageRefresh({ refresh, pending: catalog.pending || details.pending, refreshedAt });
    const editConfiguration = () => {
        if (!selectedVisible || detailMode !== 'config' || !details.data || details.error || details.pending) return;
        const revision = ConnectionStore.getSnapshot()?.revision;
        openEditor(selectedVisible, () => {
            if (alive.current && ConnectionStore.getSnapshot()?.revision === revision) void details.read();
        });
    };

    return <div className="ops-cluster">
        <section className="ops-cluster-filters" aria-label="Cluster filters">
            <label className="ops-cluster-select"><span>Cluster</span><select value={cluster} disabled={!data?.clusters.length}
                onChange={event => {
                    const next = event.target.value; setSelectedCluster(next); setQuery('');
                    const first = items.find(item => item.clusterName === next);
                    setSelection(first ? brokerIdentity(first) : null);
                }}>
                {!cluster && <option value="">{catalog.pending ? 'Loading clusters…' : 'No cluster selected'}</option>}
                {cluster && !data?.clusters.includes(cluster) && <option value={cluster}>{cluster} · unavailable</option>}
                {(data?.clusters ?? []).map(name => <option key={name} value={name}>{name}</option>)}
            </select></label>
            <Input label="Filter Broker" placeholder="Broker name or address" value={query} onChange={event => setQuery(event.target.value)} />
            {query && <Button variant="ghost" icon={X} aria-label="Clear Broker filter" onClick={() => setQuery('')}>Clear</Button>}
        </section>
        {catalog.error && <PageState kind="error" title={data ? 'Cluster refresh failed' : 'Unable to load cluster data'} description={data
            ? catalog.error + ' Showing the last successful catalog read.' : catalog.error}
            action={<Button variant="outline" disabled={catalog.pending} onClick={() => void catalog.refresh()}>Retry catalog</Button>} />}
        {catalog.pending && <PageState kind="loading" title={data ? 'Refreshing cluster data' : 'Loading clusters and Brokers'} />}
        <dl className="ops-cluster-summary" aria-label="Selected cluster summary">
            <div><dd>{data ? data.clusters.length : '—'}</dd><dt>Clusters</dt></div>
            <div><dd>{data ? clusterItems.length : '—'}</dd><dt>Brokers</dt></div>
            <div><dd>{data ? clusterItems.filter(item => item.role.toUpperCase() === 'MASTER').length : '—'}</dd><dt>Masters</dt></div>
            <div><dd>{data ? clusterItems.filter(item => item.role.toUpperCase() === 'SLAVE').length : '—'}</dd><dt>Slaves</dt></div>
        </dl>
        {missing && <PageState kind="partial" title="Selected Broker is no longer available"
            description={<>{selection?.brokerName ?? target?.address} is not in the current catalog. Select a Broker explicitly before viewing or editing another target.</>}
            action={<Button variant="outline" onClick={() => setActiveTab('Cluster')}>Open cluster list</Button>} />}
        <PageSection title="Brokers" description={'Brokers in the selected cluster and their basic information.'}>
            <div className="ops-cluster-table-scroll" role="region" aria-label="Broker inventory" tabIndex={0}>
                <table className="ops-cluster-broker-table"><thead><tr><th scope="col">Broker name</th><th scope="col">Role</th><th scope="col">Address</th>
                    <th scope="col">Status</th><th scope="col">Produce + consume TPS</th><th scope="col">Refreshed</th></tr></thead>
                    <tbody>{filtered.map(broker => <tr key={brokerKey(broker)} data-selected={selectedVisible && brokerKey(selectedVisible) === brokerKey(broker)}>
                        <th scope="row"><label className="ops-cluster-broker-choice"><input type="radio" name="cluster-broker"
                            checked={Boolean(selectedVisible && brokerKey(selectedVisible) === brokerKey(broker))}
                            onChange={() => setSelection(brokerIdentity(broker))} aria-label={'Select ' + broker.brokerName + ' at ' + broker.address} />
                            <strong>{broker.brokerName}</strong></label></th>
                        <td><StatusBadge tone={broker.role.toUpperCase() === 'MASTER' ? 'accent' : 'neutral'}>{broker.role || 'Unknown'}</StatusBadge></td>
                        <td><code>{broker.address}</code></td><td><BrokerStatus broker={broker} /></td>
                        <td className="ops-cluster-number">{brokerTps(broker)?.toFixed(2) ?? 'Unknown'}</td>
                        <td>{catalog.receivedAt ? new Date(catalog.receivedAt).toLocaleTimeString() : 'Not read'}</td>
                    </tr>)}</tbody>
                </table>
            </div>
            {data && !filtered.length && <PageState kind="empty" title={query ? 'No Brokers match this filter' : 'No Brokers in this cluster'}
                description={query ? 'Clear the filter to return to the saved selection.' : 'Select another cluster or check the NameServer connection.'} />}
            <div className="ops-cluster-table-note"><span>{filtered.length} shown{data ? ' / ' + clusterItems.length + ' in this cluster' : ''} · Refreshed times are dashboard reads.</span>
                {data && <span>NameServer: {data.currentNamesrv || 'Not selected'} · VIP {data.useVipChannel ? 'on' : 'off'} · TLS {data.useTls ? 'on' : 'off'}</span>}</div>
        </PageSection>
        {selectedVisible ? <PageSection title="Broker details" description="Configuration and runtime information for the selected Broker."
            action={detailMode === 'config' && <Button icon={Pencil} onClick={editConfiguration}
                disabled={!details.data || Boolean(details.error) || details.pending || catalog.pending || Boolean(catalog.error)}>Edit configuration</Button>}>
            <div className="ops-cluster-detail-identity"><h3>{selectedVisible.brokerName}</h3><BrokerStatus broker={selectedVisible} />
                <dl><div><dt>Address</dt><dd>{selectedVisible.address}</dd></div><div><dt>Role / ID</dt><dd>{selectedVisible.role || 'Unknown'} / {selectedVisible.brokerId}</dd></div>
                    <div><dt>Version</dt><dd>{selectedVisible.version || 'Unknown'}</dd></div></dl>
            </div>
            {selectedVisible.statusLoadError && <PageState kind="partial" title="Broker runtime was unavailable" description={selectedVisible.statusLoadError} />}
            <Tabs value={detailMode} onValueChange={value => setDetailMode(value === 'status' ? 'status' : 'config')}>
                <TabsList aria-label="Broker detail view"><TabsTrigger value="status">Runtime</TabsTrigger><TabsTrigger value="config">Configuration</TabsTrigger></TabsList>
                {(['status', 'config'] as const).map(mode => <TabsContent key={mode} value={mode}>
                    {details.pending && <PageState kind="loading" title={'Reading Broker ' + (mode === 'config' ? 'configuration' : 'runtime')} />}
                    {details.error && <PageState kind="error" title="Broker details could not be refreshed" description={details.error}
                        action={<Button variant="outline" disabled={details.pending} onClick={() => void details.read()}>Retry details</Button>} />}
                    {details.data && <><p className="ops-cluster-detail-note">{details.error || details.pending ? 'Last successful detail read: ' : 'Detail read: '}
                        {details.receivedAt ? new Date(details.receivedAt).toLocaleTimeString() : 'Not read'}{mode === 'config' ? ' · Only changed configuration values are submitted.' : ''}</p>
                        {mode === 'status' && <><p className="ops-cluster-detail-note">Catalog counters · {catalog.receivedAt ? new Date(catalog.receivedAt).toLocaleTimeString() : 'Not read'}</p><dl className="ops-cluster-counters">
                            {[
                                ['Produce TPS', brokerRate(selectedVisible, 'produce')?.toFixed(2) ?? 'Unknown'],
                                ['Consume TPS', brokerRate(selectedVisible, 'consume')?.toFixed(2) ?? 'Unknown'],
                                ['Today produced', counterText(selectedVisible, selectedVisible.todayProduce, ['msgPutTotalTodayMorning', 'msgPutTotalTodayNow'])],
                                ['Today consumed', counterText(selectedVisible, selectedVisible.todayConsume, ['msgGetTotalTodayMorning', 'msgGetTotalTodayNow'])],
                                ['Yesterday produced', counterText(selectedVisible, selectedVisible.yesterdayProduce, ['msgPutTotalYesterdayMorning', 'msgPutTotalTodayMorning'])],
                                ['Yesterday consumed', counterText(selectedVisible, selectedVisible.yesterdayConsume, ['msgGetTotalYesterdayMorning', 'msgGetTotalTodayMorning'])],
                            ].map(([name, value]) => <div key={name}><dt>{name}</dt><dd>{value}</dd></div>)}
                        </dl></>}
                        <BrokerEntries key={identity + ':' + mode} entries={details.data.entries} /></>}
                </TabsContent>)}
            </Tabs>
        </PageSection> : <PageState kind="empty" title={selected && query ? 'Selected Broker is outside the filter' : 'Select a Broker to inspect details'}
            description={selected && query ? 'Clear the filter to inspect the selected Broker, or explicitly select another row.' : 'Configuration edits always use the selected Broker identity.'} />}
    </div>;
};
