import { useCallback, useEffect, useId, useState, type ReactNode } from 'react';
import { Search } from 'lucide-react';
import { useProducerConnections } from '../features/producer/hooks/useProducerConnections';
import { ProducerGroupDirectory } from '../features/producer/components/ProducerGroupDirectory';
import { ProducerService } from '../services/producer.service';
import { useReadResource } from '../hooks/useReadResource';
import { usePageRefresh } from '../app/layout/pageToolbar';
import { PageSection } from './layout/PageSection';
import { PageState } from './layout/PageState';
import { Button } from './ui/LegacyButton';
import { Input } from './ui/LegacyInput';
import { Pagination } from './Pagination';
import type { ProducerConnectionItem, ProducerConnectionView } from '../features/producer/types/producer.types';
import '../features/producer/producer.css';

export function ProducerView() {
    const lookup = useProducerConnections();
    const loadDirectory = useCallback(() => ProducerService.listProducerGroups(), []);
    const directory = useReadResource(loadDirectory, 'Producer group discovery failed.');
    const [refreshedAt, setRefreshedAt] = useState<number | null>(null);
    const refresh = useCallback(() => {
        void directory.read();
        void lookup.topics.read();
        if (lookup.hasSearched) void lookup.search();
    }, [directory.read, lookup.topics.read, lookup.hasSearched, lookup.search]);
    const pending = directory.pending || lookup.topics.pending || lookup.pending;
    useEffect(() => {
        if (pending || directory.error || lookup.topics.error || lookup.error || !directory.receivedAt || !lookup.topics.receivedAt) return;
        setRefreshedAt(Math.min(directory.receivedAt, lookup.topics.receivedAt, lookup.hasSearched ? lookup.receivedAt ?? Infinity : Infinity));
    }, [pending, directory.error, directory.receivedAt, lookup.topics.error, lookup.topics.receivedAt, lookup.error, lookup.receivedAt, lookup.hasSearched]);
    usePageRefresh({ refresh, pending, refreshedAt });
    const suggestionsId = useId();
    const readState = <>
        {lookup.pending && <PageState kind="loading" title="Querying Producer connections" />}
        {lookup.error && <PageState kind="error" title="Connection lookup failed" description={lookup.error + (lookup.result ? ' Showing the last successful read for this input pair.' : '')} />}
        {!lookup.hasSearched && <PageState kind="empty" title="No lookup yet" description="Choose or enter a Topic and group, then query connections." />}
    </>;
    return <div className="ops-producers">
        <ProducerGroupDirectory directory={directory} refresh={() => { void directory.read(); }} selectedGroup={lookup.producerGroup} onSelect={lookup.setProducerGroup} />
        <div className="ops-producer-workspace">
            <PageSection title={lookup.producerGroup.trim() || 'Query connections'} className="ops-producer-query">
                <form className="ops-producer-query-form" aria-label="Producer connection query" onSubmit={event => { event.preventDefault(); void lookup.search(); }}>
                    <Input label="Topic" placeholder="Choose or enter a Topic" list={suggestionsId} value={lookup.selectedTopic} onChange={event => lookup.setSelectedTopic(event.target.value)} required />
                    <datalist id={suggestionsId}>{lookup.topics.data?.topics.map(topic => <option key={topic} value={topic} />)}</datalist>
                    <Input label="Producer group" placeholder="Enter a Producer group" value={lookup.producerGroup} onChange={event => lookup.setProducerGroup(event.target.value)} required />
                    <Button type="submit" icon={Search} disabled={lookup.pending || !lookup.selectedTopic.trim() || !lookup.producerGroup.trim()}>{lookup.pending ? 'Querying…' : 'Query connections'}</Button>
                </form>
                {lookup.topics.pending && <p className="ops-producer-note">Loading Topic suggestions; manual input remains available.</p>}
                {lookup.topics.error && <PageState kind="partial" title="Topic suggestions unavailable" description="Enter a Topic manually to query connections." action={<Button variant="outline" disabled={lookup.topics.pending} onClick={() => { void lookup.topics.read(); }}>Retry Topic suggestions</Button>} />}
            </PageSection>
            {lookup.result ? <ProducerClients key={JSON.stringify([lookup.result.topic, lookup.result.producerGroup])} result={lookup.result}>
                {readState}<p className="ops-producer-note">Topic: {lookup.result.topic} · Group: {lookup.result.producerGroup} · Last successful read: {new Date(lookup.receivedAt!).toLocaleString()}</p>
            </ProducerClients> : <PageSection title="Connections" description="Clients reported for the exact Topic and Producer group query.">{readState}</PageSection>}
        </div>
    </div>;
}

function ProducerClients({ result, children }: { result: ProducerConnectionView; children: ReactNode }) {
    const [selectedId, setSelectedId] = useState<string | null>(result.connections[0]?.clientId ?? null);
    const [page, setPage] = useState(1);
    const selected = result.connections.find(client => client.clientId === selectedId);
    const pageCount = Math.max(1, Math.ceil(result.connections.length / 6));
    const currentPage = Math.min(page, pageCount);
    return <div className="ops-producer-clients">
        <PageSection title={`Connections (${result.connectionCount})`} description="Clients reported for the exact Topic and Producer group query.">
        {children}
        {!result.connections.length && <PageState kind="empty" title="No connections returned" description="The lookup succeeded without reporting connected clients for this Topic and group." />}
        {result.connections.length > 0 && <div className="ops-producer-scroll" role="region" aria-label="Producer connection list" tabIndex={0}><table><thead><tr>
            {['Client ID', 'Language', 'Version', 'Address'].map(label => <th scope="col" key={label}>{label}</th>)}
        </tr></thead><tbody>{result.connections.slice((currentPage - 1) * 6, currentPage * 6).map(client => <tr key={client.clientId} data-selected={selectedId === client.clientId}>
            <th scope="row"><button type="button" className="ops-producer-choice" title={client.clientId} aria-pressed={selectedId === client.clientId} onClick={() => setSelectedId(client.clientId)}>{client.clientId}</button></th>
            <td>{client.language || 'Unknown'}</td><td>{producerVersion(client)}</td><td>{client.clientAddr || 'Not reported'}</td>
        </tr>)}</tbody></table></div>}
        {pageCount > 1 && <Pagination currentPage={currentPage} totalPages={pageCount} onPageChange={setPage} />}
        </PageSection>
        <PageSection title="Client details" description="Metadata returned for the selected Producer connection.">
            {selected ? <dl className="ops-producer-properties">{[
                ['Client ID', selected.clientId], ['Language', selected.language || 'Unknown'], ['Version', producerVersion(selected)],
                ['Address', selected.clientAddr || 'Not reported'], ['Producer group', result.producerGroup], ['Topic', result.topic],
            ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd tabIndex={String(value).length > 200 ? 0 : undefined}>{value}</dd></div>)}</dl>
                : <PageState kind="empty" title={selectedId ? 'Selected client is no longer in this result' : 'Select a client'} description="Select a returned client explicitly to inspect its metadata." />}
        </PageSection>
    </div>;
}
function producerVersion(client: ProducerConnectionItem) {
    return client.versionDesc || (Number.isFinite(client.version) && client.version >= 0 ? String(client.version) : 'Unknown');
}
