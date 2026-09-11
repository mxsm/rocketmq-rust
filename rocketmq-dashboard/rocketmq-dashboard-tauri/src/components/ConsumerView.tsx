import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { Activity, ChevronDown, LockKeyhole, Plus } from 'lucide-react';
import { ConnectionStore } from '../services/connection.store';
import { getConnectionSettings } from '../services/connection.service';
import { useAppStore, useNavigationState } from '../stores/app.store';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { useConsumerCatalog } from '../features/consumer/hooks/useConsumerCatalog';
import { ConsumerDetails, useConsumerDetails, type ConsumerDetailTab } from '../features/consumer/components/ConsumerDetails';
import { useConsumerAction, type ConsumerAction } from '../features/consumer/consumerActionContext';
import { consumerScopeKey, resolveConsumerScope } from '../features/consumer/scope';
import { consumerCount, consumerTimestamp } from '../features/consumer/consumerModel';
import { isReadOnlyConsumer } from '../features/consumer/mutation';
import type { ConsumerQueryScope } from '../features/consumer/types/consumer.types';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { StatusBadge } from './layout/StatusBadge';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger, DropdownMenuCheckboxItem } from './ui/dropdown-menu';
import { Pagination } from './Pagination';
import '../features/consumer/consumer.css';

const detailTabs = [['overview', 'Overview'], ['progress', 'Progress'], ['clients', 'Connections'], ['config', 'Configuration'], ['reset', 'Reset offset']] as const;
const pageSize = 6;
export function ConsumerView() {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const { consumerQueryMode, setConsumerQueryMode, setActiveTab, navigation } = useAppStore();
    const [mode, setMode] = useNavigationState<ConsumerQueryScope['mode']>('queryMode', navigation.target?.kind === 'consumer' ? navigation.target.scope.mode : consumerQueryMode);
    const [error, setError] = useState('');
    const changeMode = (value: ConsumerQueryScope['mode']) => { setMode(value); setConsumerQueryMode(value); };
    useEffect(() => { let active = true; if (!settings) void getConnectionSettings().catch(() => { if (active) setError('Connection settings could not be read.'); }); return () => { active = false; }; }, [settings]);
    const scope = useMemo(() => resolveConsumerScope(settings, mode), [settings, mode]);
    if (!settings) return <PageState kind={error ? 'error' : 'loading'} title={error || 'Reading connection settings'} />;
    if (!scope) return <PageState kind="empty" title="Select a Proxy to use Proxy query mode" description="The selected endpoint must be present in the connection settings."
        action={<div className="ops-consumer-actions"><Button onClick={() => setActiveTab('Proxy')}>Configure Proxy</Button><Button variant="outline" onClick={() => changeMode('name_server')}>Use NameServer</Button></div>} />;
    return <ConsumerCatalog key={consumerScopeKey(scope)} scope={scope} endpoint={scope.mode === 'proxy' ? settings.proxy.currentProxyAddr ?? '' : settings.nameserver.currentNamesrv ?? ''} changeMode={changeMode} />;
}
function ConsumerCatalog({ scope, endpoint, changeMode }: { scope: ConsumerQueryScope; endpoint: string; changeMode: (mode: ConsumerQueryScope['mode']) => void }) {
    const { navigation, setActiveTab } = useAppStore();
    const target = navigation.target?.kind === 'consumer' && consumerScopeKey(navigation.target.scope) === consumerScopeKey(scope) ? navigation.target : null;
    const prefix = consumerScopeKey(scope);
    const [search, setSearch] = useNavigationState('search:' + prefix, '');
    const [categories, setCategories] = useNavigationState<Record<string, boolean>>('filters:' + prefix, { NORMAL: true, FIFO: true, SYSTEM: true });
    const [selection, setSelection] = useNavigationState<string | null>('selection:' + prefix, target?.name ?? null);
    const [tab, setTab] = useNavigationState<ConsumerDetailTab>('detail:' + prefix, target?.detail ?? 'overview');
    const [page, setPage] = useNavigationState('page:' + prefix, 1);
    const catalog = useConsumerCatalog(scope);
    const open = useConsumerAction();
    const alive = useRef(false);
    useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
    const filtered = useMemo(() => catalog.items.filter(item => (categories[item.category] ?? true) &&
        (item.rawGroupName.toLowerCase().includes(search.trim().toLowerCase()) || item.displayGroupName.toLowerCase().includes(search.trim().toLowerCase()))), [catalog.data, search, categories]);
    const selected = selection === null ? catalog.items[0] : catalog.items.find(item => item.rawGroupName === selection);
    useEffect(() => { if (selection === null && selected) setSelection(selected.rawGroupName); }, [selection, selected]);
    const visible = selected && filtered.some(item => item.rawGroupName === selected.rawGroupName) ? selected : null;
    const detail = useConsumerDetails(visible?.rawGroupName ?? null, scope, tab);
    const refresh = useCallback(() => { void catalog.refresh(); void detail.read(); }, [catalog.refresh, detail.read]);
    const blocked = catalog.pending || Boolean(catalog.error) || !catalog.data;
    const onAction = useCallback((action: ConsumerAction) => {
        if (blocked) return;
        const revision = ConnectionStore.getSnapshot()?.revision;
        open(action, () => { if (alive.current && ConnectionStore.getSnapshot()?.revision === revision) refresh(); });
    }, [blocked, open, refresh]);
    const createAction = useMemo(() => <Button icon={Plus} disabled={blocked} onClick={() => onAction({ kind: 'create', scope })}>Create Group</Button>, [blocked, onAction, scope]);
    const [refreshedAt, setRefreshedAt] = useState<number | null>(null);
    const observedAt = visible ? catalog.receivedAt !== null && detail.receivedAt !== null ? Math.min(catalog.receivedAt, detail.receivedAt) : null : catalog.receivedAt;
    useEffect(() => { if (!catalog.pending && !detail.pending && !catalog.error && !detail.error) setRefreshedAt(observedAt); }, [observedAt, catalog.pending, detail.pending, catalog.error, detail.error]);
    usePageRefresh({ refresh, pending: catalog.pending || detail.pending, refreshedAt, actions: createAction });
    const pages = Math.ceil(filtered.length / pageSize);
    const currentPage = Math.max(1, Math.min(page, pages || 1));
    const items = filtered.slice((currentPage - 1) * pageSize, currentPage * pageSize);
    const filteredOut = Boolean(search || Object.values(categories).some(value => !value));
    const clear = () => { setSearch(''); setCategories({ NORMAL: true, FIFO: true, SYSTEM: true }); setPage(1); };
    const protectedGroup = isReadOnlyConsumer(visible ?? null);
    const mutationBlocked = blocked || protectedGroup;
    return <div className="ops-consumers"><section className="ops-consumer-filters" aria-label="Consumer query filters">
        <div className="ops-consumer-scope"><span>Query scope</span><div role="group" aria-label="Consumer query mode">
            <Button variant={scope.mode === 'name_server' ? 'primary' : 'outline'} aria-pressed={scope.mode === 'name_server'} onClick={() => changeMode('name_server')}>NameServer</Button>
            <Button variant={scope.mode === 'proxy' ? 'primary' : 'outline'} aria-pressed={scope.mode === 'proxy'} onClick={() => changeMode('proxy')}>Proxy</Button></div></div>
        <div className="ops-consumer-endpoint"><span>{scope.mode === 'proxy' ? 'Configured Proxy' : 'NameServer'}</span><button type="button" className="ops-consumer-link" title={endpoint} onClick={() => setActiveTab(scope.mode === 'proxy' ? 'Proxy' : 'NameServer')}>{endpoint || 'Not configured'}</button></div>
        <Input label="Search Consumer group" placeholder="Search Consumer group…" value={search} onChange={event => { setSearch(event.target.value); setPage(1); }} />
        <DropdownMenu><DropdownMenuTrigger asChild><Button variant="outline" icon={ChevronDown} aria-label="Filter Consumer categories">{Object.values(categories).every(Boolean) ? 'All types' : 'Filter types'}</Button></DropdownMenuTrigger>
            <DropdownMenuContent>{Object.entries(categories).map(([name, checked]) => <DropdownMenuCheckboxItem key={name} checked={checked} onSelect={event => event.preventDefault()} onCheckedChange={value => { setCategories({ ...categories, [name]: value }); setPage(1); }}>{name}</DropdownMenuCheckboxItem>)}</DropdownMenuContent></DropdownMenu>
        {filteredOut && <Button variant="outline" onClick={clear}>Clear filters</Button>}
    </section>
        {catalog.pending && <PageState kind="loading" title={catalog.data ? 'Refreshing Consumer catalog' : 'Reading Consumer catalog'} />}
        {catalog.error && <PageState kind="error" title="Consumer catalog could not be refreshed" description={catalog.error + (catalog.data ? ' Showing the last successful read.' : '')} action={<Button variant="outline" disabled={catalog.pending} onClick={() => void catalog.refresh()}>Retry catalog</Button>} />}
        <section className="ops-consumer-directory" aria-label="Consumer groups"><div className="ops-consumer-scroll" role="region" aria-label="Consumer group list" tabIndex={0}><table>
            <thead><tr>{['Consumer group', 'Clients', 'Type', 'Catalog lag', 'Connection observation', 'Catalog updated'].map(label => <th scope="col" key={label}>{label}</th>)}</tr></thead>
            <tbody>{items.map(item => <tr key={item.rawGroupName} data-selected={item.rawGroupName === visible?.rawGroupName}><th scope="row"><button type="button" className="ops-consumer-choice" title={item.rawGroupName} aria-pressed={item.rawGroupName === visible?.rawGroupName} onClick={() => setSelection(item.rawGroupName)}>
                {isReadOnlyConsumer(item) && <LockKeyhole size={15} aria-hidden="true" />}<span>{item.displayGroupName}</span></button></th><td>{consumerCount(item.connectionCount)}</td><td>{item.category}</td>
                <td>{item.diffTotal > 0 ? consumerCount(item.diffTotal) : 'Not verified'}</td><td><StatusBadge tone={item.connectionCount > 0 ? 'success' : 'neutral'}>{item.connectionCount > 0 ? 'Clients observed' : 'Not confirmed'}</StatusBadge></td><td>{consumerTimestamp(item.updateTimestamp)}</td></tr>)}</tbody>
        </table></div>
            {catalog.data && !items.length && <PageState kind="empty" title={filteredOut ? 'No groups match these filters' : 'No Consumer groups returned'} />}
            <div className="ops-consumer-directory-footer"><p className="ops-consumer-note">Zero values may mean unavailable observations. Inspect progress or connections to verify.</p>
                {pages > 0 && <span className="ops-consumer-note">{(currentPage - 1) * pageSize + 1}–{Math.min(currentPage * pageSize, filtered.length)} of {filtered.length}</span>}
                {pages > 1 && <Pagination currentPage={currentPage} totalPages={pages} onPageChange={setPage} disabled={catalog.pending} />}
            </div>
            {visible && !items.some(item => item.rawGroupName === visible.rawGroupName) && <Button variant="ghost" onClick={() => setPage(Math.floor(filtered.findIndex(item => item.rawGroupName === visible.rawGroupName) / pageSize) + 1)}>Show selected group in list</Button>}
        </section>
        {visible ? <PageSection className="ops-consumer-inspector" title={visible.displayGroupName} description="Lag reflects available Broker observations."
            action={<div className="ops-consumer-actions"><Button variant="outline" icon={Activity} onClick={() => setTab('clients')}>Client diagnostics</Button>
                <Button variant="outline" disabled={mutationBlocked} onClick={() => onAction({ kind: 'edit', consumer: visible, scope })}>Edit group</Button>
                <DropdownMenu><DropdownMenuTrigger asChild><Button variant="outline" icon={ChevronDown} aria-label="More Consumer actions">More</Button></DropdownMenuTrigger><DropdownMenuContent align="end">
                    <DropdownMenuItem disabled={catalog.pending} onSelect={() => { void catalog.refreshGroup(visible.rawGroupName); void detail.read(); }}>Refresh selected group</DropdownMenuItem>
                    <DropdownMenuItem disabled={mutationBlocked || scope.mode === 'proxy'} onSelect={() => setTab('reset')}>Reset offset</DropdownMenuItem>
                    <DropdownMenuItem variant="destructive" disabled={mutationBlocked} onSelect={() => onAction({ kind: 'delete', consumer: visible, scope })}>Delete group</DropdownMenuItem>
                </DropdownMenuContent></DropdownMenu></div>}>
            {protectedGroup && <p className="ops-consumer-note">This system Consumer group is read-only.</p>}
            <Tabs value={tab} onValueChange={value => setTab(value as ConsumerDetailTab)}><TabsList className="ops-tabs-underlined" aria-label="Consumer detail view">{detailTabs.map(([value, label]) => <TabsTrigger key={value} value={value} disabled={value === 'reset' && protectedGroup}>{label}</TabsTrigger>)}</TabsList>
                {detailTabs.map(([value]) => <TabsContent key={value} value={value}>
                    {detail.pending && <PageState kind="loading" title="Reading Consumer details" />}
                    {detail.error && <PageState kind="error" title="Consumer detail read failed" description={detail.error + (detail.data ? ' Showing the last successful read.' : '')} action={<Button variant="outline" disabled={detail.pending} onClick={() => void detail.read()}>Retry details</Button>} />}
                    <ConsumerDetails key={visible.rawGroupName + ':' + value} consumer={visible} scope={scope} tab={value} data={detail.data} blocked={blocked || detail.pending || Boolean(detail.error)} mutationBlocked={mutationBlocked} onTab={setTab}
                        onEdit={address => { if (!detail.pending && !detail.error) onAction({ kind: 'edit', consumer: visible, scope, address }); }}
                        onReset={topic => { if (!detail.pending && !detail.error) onAction({ kind: 'reset', consumer: visible, scope, topic }); }} />
                    {detail.receivedAt && <p className="ops-consumer-note">{detail.pending || detail.error ? 'Last successful detail read: ' : 'Detail read: '}{consumerTimestamp(detail.receivedAt)}</p>}
                </TabsContent>)}
            </Tabs>
        </PageSection> : <PageState kind="empty" title={selected ? 'Selected group is outside these filters' : selection ? 'Selected group is unavailable' : 'Select a Consumer group'}
            description={selection ? 'Saved selection: ' + selection + '. Choose another group explicitly or clear filters.' : 'Select a group to inspect progress, connections and configuration.'} action={selected && filteredOut ? <Button variant="outline" onClick={clear}>Clear filters</Button> : undefined} />}
    </div>;
}
