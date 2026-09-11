import { useMemo } from 'react';
import { RefreshCw } from 'lucide-react';
import { useNavigationState } from '../../../stores/app.store';
import type { ReadState } from '../../../hooks/readResource';
import type { ProducerGroupItem } from '../types/producer.types';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import { Input } from '../../../components/ui/LegacyInput';
import { Button } from '../../../components/ui/LegacyButton';
import { Pagination } from '../../../components/Pagination';

interface Props {
    directory: ReadState<ProducerGroupItem[]>;
    refresh: () => void;
    selectedGroup: string;
    onSelect: (group: string) => void;
}
export function ProducerGroupDirectory({ directory, refresh, selectedGroup, onSelect }: Props) {
    const [query, setQuery] = useNavigationState('producerDirectorySearch', '');
    const [page, setPage] = useNavigationState('producerDirectoryPage', 1);
    const filtered = useMemo(() => (directory.data ?? []).filter(item => item.producerGroup.toLowerCase().includes(query.trim().toLowerCase())), [directory.data, query]);
    const totalPages = Math.max(1, Math.ceil(filtered.length / 8));
    const currentPage = Math.min(page, totalPages);
    return <PageSection title="Producer groups" className="ops-producer-directory">
        <div className="ops-producer-directory-controls">
            <Input aria-label="Search Producer groups" placeholder="Search Producer groups…" value={query} onChange={event => { setQuery(event.target.value); setPage(1); }} />
            <Button variant="outline" icon={RefreshCw} disabled={directory.pending} onClick={refresh} aria-label="Refresh Producer directory">Refresh</Button>
        </div>
        {directory.pending && <PageState kind="loading" title={directory.data ? 'Refreshing directory' : 'Discovering Producer groups'} />}
        {directory.error && <PageState kind="error" title="Producer directory unavailable" description={directory.error + (directory.data ? ' Showing the last successful directory.' : '') + ' Manual connection lookup remains available.'} />}
        <div className="ops-producer-scroll ops-producer-directory-table" role="region" aria-label="Producer group directory" tabIndex={0}>
            <table><thead><tr><th scope="col">Producer group</th><th scope="col">Reported connections</th></tr></thead><tbody>
                {filtered.slice((currentPage - 1) * 8, currentPage * 8).map(item => <tr key={item.producerGroup} data-selected={selectedGroup.trim() === item.producerGroup}>
                    <th scope="row"><button className="ops-producer-choice" type="button" title={item.producerGroup} aria-pressed={selectedGroup.trim() === item.producerGroup} onClick={() => onSelect(item.producerGroup)}>{item.producerGroup}</button></th>
                    <td>{Number.isFinite(item.reportedConnectionCount) && item.reportedConnectionCount >= 0 ? item.reportedConnectionCount.toLocaleString() : 'Unknown'}</td>
                </tr>)}
            </tbody></table>
            {!directory.pending && directory.data && !filtered.length && <PageState kind="empty" title={query.trim() ? 'No groups match this search' : 'No Producer groups reported'} description="Discovery coverage is unavailable. You can still query a group manually." />}
        </div>
        {totalPages > 1 && <Pagination currentPage={currentPage} totalPages={totalPages} onPageChange={setPage} disabled={directory.pending} />}
        <p className="ops-producer-note">Directory counts come from Broker reports and may overlap. Coverage is unavailable; these are not a complete client list.</p>
        {directory.receivedAt && <p className="ops-producer-note">Last successful directory read: {new Date(directory.receivedAt).toLocaleString()}</p>}
    </PageSection>;
}
