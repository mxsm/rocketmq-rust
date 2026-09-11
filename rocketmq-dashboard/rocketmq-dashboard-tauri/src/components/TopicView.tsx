import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDown, LockKeyhole, Plus } from 'lucide-react';
import { useAppStore, useNavigationState } from '../stores/app.store';
import { ConnectionStore } from '../services/connection.store';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { useTopicCatalog } from '../features/topic/hooks/useTopicCatalog';
import { useTopicDetail, TopicDetails, type TopicDetailTab } from '../features/topic/components/TopicDetails';
import { useTopicAction, type TopicAction } from '../features/topic/topicActionContext';
import { allTopicCategories, canSendToTopic, isProtectedTopic } from '../features/topic/topicModel';
import { filterTopics } from '../features/topic/filters';
import { TOPIC_MESSAGE_TYPE_OPTIONS } from '../features/topic/types/topic.types';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Tabs, TabsList, TabsTrigger, TabsContent } from './ui/tabs';
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem, DropdownMenuCheckboxItem, DropdownMenuSeparator } from './ui/dropdown-menu';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { StatusBadge } from './layout/StatusBadge';
import { Pagination } from './Pagination';
import '../features/topic/topic.css';

const detailTabs: [TopicDetailTab, string][] = [['overview', 'Overview'], ['route', 'Routes'], ['status', 'Statistics'], ['consumers', 'Consumers'], ['config', 'Configuration']];
const pageSize = 12;

export function TopicView() {
    const { navigation } = useAppStore();
    const target = navigation.target?.kind === 'topic' ? navigation.target : null;
    const catalog = useTopicCatalog();
    const [search, setSearch] = useNavigationState('search', '');
    const [cluster, setCluster] = useNavigationState('clusterFilter', '');
    const [broker, setBroker] = useNavigationState('brokerFilter', '');
    const [categories, setCategories] = useNavigationState('filters', allTopicCategories);
    const [messageType, setMessageType] = useNavigationState('messageTypeFilter', '');
    const [selection, setSelection] = useNavigationState<string | null>('selection', target?.name ?? null);
    const [tab, setTab] = useNavigationState<TopicDetailTab>('detail', target?.detail ?? 'overview');
    const [configBroker, setConfigBroker] = useNavigationState('configBroker', '');
    const [page, setPage] = useNavigationState('page', 1);
    const open = useTopicAction();
    const alive = useRef(false);
    useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
    const items = catalog.data?.items ?? [];
    const targets = catalog.data?.targets ?? [];
    const filtered = useMemo(() => filterTopics((catalog.data?.items ?? []).map(item => ({ ...item, name: item.topic, type: item.category })),
        { search, clusterName: cluster, brokerName: broker, messageType, categories }), [catalog.data, search, cluster, broker, messageType, categories]);
    const selected = selection === null ? items[0] : items.find(item => item.topic === selection);
    useEffect(() => { if (selection === null && selected) setSelection(selected.topic); }, [selection, selected]);
    const visible = selected && filtered.some(item => item.topic === selected.topic) ? selected : null;
    const detail = useTopicDetail(visible?.topic ?? null, tab, configBroker);
    const pages = Math.ceil(filtered.length / pageSize);
    const currentPage = Math.max(1, Math.min(page, pages || 1));
    const pageItems = filtered.slice((currentPage - 1) * pageSize, currentPage * pageSize);
    const refresh = useCallback(() => { void catalog.refresh(); void detail.read(); }, [catalog.refresh, detail.read]);
    const onAction = useCallback((action: TopicAction) => {
        if (!catalog.data || catalog.pending || catalog.error) return;
        const revision = ConnectionStore.getSnapshot()?.revision;
        open(action, () => {
            if (alive.current && ConnectionStore.getSnapshot()?.revision === revision) refresh();
        });
    }, [open, catalog.data, catalog.pending, catalog.error, refresh]);
    const createAction = useMemo(() => <Button icon={Plus} onClick={() => onAction({ kind: 'create', targets: catalog.data?.targets ?? [] })}
        disabled={!catalog.data || catalog.pending || Boolean(catalog.error) || !catalog.data.targets.length}>Create Topic</Button>,
    [onAction, catalog.data, catalog.pending, catalog.error]);
    const [refreshedAt, setRefreshedAt] = useState<number | null>(null);
    const observedAt = visible ? catalog.receivedAt !== null && detail.receivedAt !== null ? Math.min(catalog.receivedAt, detail.receivedAt) : null : catalog.receivedAt;
    useEffect(() => { if (!catalog.pending && !detail.pending && !catalog.error && !detail.error) setRefreshedAt(observedAt); },
        [catalog.pending, detail.pending, catalog.error, detail.error, observedAt]);
    usePageRefresh({ refresh, pending: catalog.pending || detail.pending, refreshedAt, actions: createAction });
    const clearFilters = () => { setSearch(''); setCluster(''); setBroker(''); setMessageType(''); setCategories(allTopicCategories()); setPage(1); };
    const brokerOptions = [...new Set(targets.filter(item => !cluster || item.clusterName === cluster).flatMap(item => item.brokerNames))];
    const changedFilters = Boolean(search || cluster || broker || messageType || Object.values(categories).some(value => !value));
    const blocked = catalog.pending || Boolean(catalog.error) || !catalog.data;
    const mayEdit = visible && !isProtectedTopic(visible) && !blocked;
    const allowedMessageAction = visible && canSendToTopic(visible) && !blocked;
    return <div className="ops-topics">
        <section className="ops-topic-filters" aria-label="Topic filters">
            <Input label="Search Topics" placeholder="Search Topic name…" value={search} onChange={event => { setSearch(event.target.value); setPage(1); }} />
            <label className="ops-topic-select"><span>Cluster</span><select value={cluster} onChange={event => { setCluster(event.target.value); setBroker(''); setPage(1); }}>
                <option value="">All</option>{cluster && !targets.some(item => item.clusterName === cluster) && <option value={cluster}>{cluster} · unavailable</option>}
                {targets.map(item => <option key={item.clusterName}>{item.clusterName}</option>)}
            </select></label>
            <label className="ops-topic-select"><span>Broker</span><select value={broker} onChange={event => { setBroker(event.target.value); setPage(1); }}>
                <option value="">All</option>{broker && !brokerOptions.includes(broker) && <option value={broker}>{broker} · unavailable</option>}
                {brokerOptions.map(name => <option key={name}>{name}</option>)}
            </select></label>
            <div className="ops-topic-select"><span>Type</span><DropdownMenu><DropdownMenuTrigger asChild>
                <Button variant="outline" aria-label="Filter Topic categories" icon={ChevronDown}>{Object.values(categories).every(Boolean) ? 'All types' : Object.values(categories).filter(Boolean).length + ' types'}</Button>
            </DropdownMenuTrigger><DropdownMenuContent align="end">{Object.entries(categories).map(([category, checked]) =>
                <DropdownMenuCheckboxItem key={category} checked={checked} onCheckedChange={value => { setCategories({ ...categories, [category]: value }); setPage(1); }} onSelect={event => event.preventDefault()}>{category}</DropdownMenuCheckboxItem>)}
            </DropdownMenuContent></DropdownMenu></div>
            <label className="ops-topic-select"><span>Message type</span><select value={messageType} onChange={event => { setMessageType(event.target.value); setPage(1); }}>
                <option value="">All</option>{TOPIC_MESSAGE_TYPE_OPTIONS.map(value => <option key={value}>{value}</option>)}
            </select></label>
            <Button variant="outline" disabled={!changedFilters} onClick={clearFilters}>Clear filters</Button>
        </section>
        {catalog.pending && <PageState kind="loading" title={catalog.data ? 'Refreshing Topics' : 'Loading Topics'} />}
        {catalog.error && <PageState kind="error" title="Topic catalog could not be refreshed" description={catalog.error + (catalog.data ? ' Showing the last successful read.' : '')}
            action={<Button variant="outline" disabled={catalog.pending} onClick={() => void catalog.refresh()}>Retry catalog</Button>} />}
        <div className="ops-topic-workspace">
            <PageSection title={'Topics' + (catalog.data ? ' (' + filtered.length + ')' : '')} className="ops-topic-directory">
                <div className="ops-topic-table-scroll" role="region" aria-label="Topic directory" tabIndex={0}><table>
                    <thead><tr><th scope="col">Topic name</th><th scope="col">Type</th><th scope="col">Queues (R/W)</th></tr></thead>
                    <tbody>{pageItems.map(item => <tr key={item.topic} data-selected={item.topic === visible?.topic}>
                        <th scope="row"><button type="button" className="ops-topic-choice" aria-pressed={item.topic === visible?.topic}
                            onClick={() => { setSelection(item.topic); setConfigBroker(''); }}>
                            {isProtectedTopic(item) && <LockKeyhole size={16} aria-hidden="true" />}<span>{item.topic}</span></button></th>
                        <td><StatusBadge tone={isProtectedTopic(item) ? 'neutral' : item.category === 'NORMAL' ? 'success' : 'accent'}>{item.category}</StatusBadge></td>
                        <td>{item.readQueueCount} / {item.writeQueueCount}</td>
                    </tr>)}</tbody>
                </table></div>
                {catalog.data && !filtered.length && <PageState kind="empty" title={changedFilters ? 'No Topics match these filters' : 'No Topics returned'}
                    description={changedFilters ? 'Clear filters to return to the catalog.' : 'Create a Topic or check the current NameServer connection.'} />}
                <div className="ops-topic-directory-footer"><p className="ops-topic-note">Protected system Topics are read-only.</p>
                    {pages > 0 && <><span className="ops-topic-note">{(currentPage - 1) * pageSize + 1}–{Math.min(currentPage * pageSize, filtered.length)} of {filtered.length}</span>
                        <Pagination currentPage={currentPage} totalPages={pages} onPageChange={setPage} disabled={catalog.pending} /></>}
                </div>
                {visible && !pageItems.some(item => item.topic === visible.topic) && <Button variant="ghost" onClick={() => setPage(Math.floor(filtered.findIndex(item => item.topic === visible.topic) / pageSize) + 1)}>Show selected Topic in list</Button>}
            </PageSection>
            {visible ? <PageSection className="ops-topic-inspector" title={visible.topic} description="Topic details and routing information."
                action={<div className="ops-topic-actions"><Button variant="outline" disabled={!mayEdit} onClick={() => onAction({ kind: 'edit', topic: visible, targets, brokerName: configBroker || undefined })}>Edit</Button>
                    <Button disabled={!allowedMessageAction} onClick={() => onAction({ kind: 'send', topic: visible })}>Send message</Button>
                    <DropdownMenu><DropdownMenuTrigger asChild><Button variant="outline" icon={ChevronDown} aria-label="More Topic actions" disabled={!mayEdit}>More</Button></DropdownMenuTrigger>
                        <DropdownMenuContent align="end"><DropdownMenuItem disabled={!allowedMessageAction} onSelect={() => onAction({ kind: 'reset', topic: visible })}>Reset Consumer offset</DropdownMenuItem>
                            <DropdownMenuItem disabled={!allowedMessageAction} onSelect={() => onAction({ kind: 'skip', topic: visible })}>Skip accumulated messages</DropdownMenuItem>
                            <DropdownMenuSeparator /><DropdownMenuItem variant="destructive" onSelect={() => onAction({ kind: 'delete', topic: visible })}>Delete Topic</DropdownMenuItem>
                        </DropdownMenuContent></DropdownMenu></div>}>
                {isProtectedTopic(visible) && <p className="ops-topic-note">This system Topic is protected. Only inspection is available.</p>}
                {!isProtectedTopic(visible) && !canSendToTopic(visible) && <p className="ops-topic-note">Retry and dead-letter Topics do not support test sends or offset changes here.</p>}
                <Tabs value={tab} onValueChange={value => setTab(value as TopicDetailTab)}><TabsList className="ops-tabs-underlined" aria-label="Topic detail view">{detailTabs.map(([value, label]) =>
                    <TabsTrigger key={value} value={value}>{label}</TabsTrigger>)}</TabsList>
                    {detailTabs.map(([value]) => <TabsContent key={value} value={value}>
                        {value === 'config' && <label className="ops-topic-select"><span>Configuration Broker</span><select value={configBroker} onChange={event => setConfigBroker(event.target.value)}>
                            <option value="">Compare available Brokers</option>{configBroker && !visible.brokers.includes(configBroker) && <option value={configBroker}>{configBroker} · unavailable</option>}
                            {visible.brokers.map(name => <option key={name}>{name}</option>)}</select></label>}
                        {detail.pending && <PageState kind="loading" title="Reading Topic details" />}
                        {detail.error && <PageState kind="error" title="Topic details could not be refreshed" description={detail.error}
                            action={<Button variant="outline" disabled={detail.pending} onClick={() => void detail.read()}>Retry details</Button>} />}
                        {(detail.data || value === 'overview') && <TopicDetails key={visible.topic + ':' + value} topic={visible} tab={value} data={detail.data}
                            onShowConsumers={() => setTab('consumers')} onAction={action => { if (!detail.pending && !detail.error) onAction(action); }} allowMutation={Boolean(mayEdit) && !detail.pending && !detail.error} />}
                        {detail.receivedAt && <p className="ops-topic-note">{detail.error || detail.pending ? 'Last successful detail read: ' : 'Detail read: '}{new Date(detail.receivedAt).toLocaleString()}</p>}
                    </TabsContent>)}
                </Tabs>
            </PageSection> : <PageState kind="empty" title={selected ? 'Selected Topic is outside these filters' : selection ? 'Selected Topic is unavailable' : 'Select a Topic'}
                description={selected ? 'Clear filters to inspect the saved selection or choose another Topic explicitly.' : selection ? selection + ' was not found in the current catalog. Choose another Topic explicitly.' : 'Choose a Topic from the directory to inspect routes, statistics and Consumers.'}
                action={selected && changedFilters ? <Button variant="outline" onClick={clearFilters}>Clear filters</Button> : undefined} />}
        </div>
    </div>;
}
